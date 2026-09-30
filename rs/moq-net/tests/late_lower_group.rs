//! A group created below one the subscriber already received is still delivered while
//! the subscription's max age tolerates it.
//!
//! `Subscription::max_age` promises that a non-zero budget "tolerates that much reordering
//! before giving up". The subscriber here asks for everything from group 0 with a budget
//! far larger than the gap, is attached before anything is published, and has read all of
//! group 1 before the publisher creates group 0. The track then finishes, so the read ends
//! on the track's own end rather than on a timeout.

mod support;

use std::time::Duration;

use moq_net::track::{Info, Position, Subscription};
use moq_net::{Error, Hop, Timestamp, Version};
use support::harness::{MockConnectOptions, connect_mock};

/// A guard against hangs only: paused time makes every wait below immediate.
const TIMEOUT: Duration = Duration::from_secs(10);
const FRAMES: usize = 4;
/// Far longer than the reordering here, and than the publisher's retention window.
const MAX_AGE: Duration = Duration::from_secs(60);

fn produce_origin(hop: u64) -> moq_net::origin::Producer {
	let (producer, driver) = moq_net::origin::Producer::new(moq_net::origin::Config::new(Hop::new(hop).unwrap()));
	tokio::spawn(support::harness::run(driver));
	producer
}

fn write_group(track: &moq_net::track::Producer, sequence: u64) {
	let mut group = track.create_group(moq_net::group::Info { sequence }).unwrap();
	for _ in 0..FRAMES {
		group.write_frame(Timestamp::now(), &b"frame"[..]).unwrap();
	}
	group.finish().unwrap();
}

/// Publish group 1, wait until the subscriber has read all of it, then publish group 0
/// and finish the track. `hops` sessions sit between publisher and subscriber (0 is the
/// model alone). Returns each delivered group's sequence and frame count, in arrival
/// order, and the error the read ended with, if any.
async fn round(version: Version, hops: u64) -> (Vec<(u64, usize)>, Option<Error>) {
	let publisher = produce_origin(1);
	let broadcast = publisher.create_broadcast("bcast").unwrap();
	let track = broadcast.create_track("data", Info::default()).unwrap();
	broadcast.announce(Default::default()).unwrap();

	let mut pairs = Vec::new();
	let mut upstream = publisher.clone();
	for hop in 0..hops {
		let downstream = produce_origin(hop + 2);
		let mut options = MockConnectOptions::new(version);
		options.server_publish = Some(upstream.consume());
		options.client_subscribe = Some(downstream.clone());
		pairs.push(connect_mock(options).await);
		upstream = downstream;
	}

	let consumer = upstream.consume();
	consumer.routed("bcast").await.expect("routed");
	let remote = consumer.request_broadcast("bcast").await.expect("broadcast resolves");

	let (read_tx, mut read_rx) = tokio::sync::mpsc::unbounded_channel();
	let reader = tokio::spawn(async move {
		let subscription = Subscription::default()
			.with_start(Position::group(0))
			.with_max_age(MAX_AGE);
		let mut sub = remote
			.track("data")
			.unwrap()
			.subscribe(subscription)
			.await
			.expect("subscribe");
		let mut got = Vec::new();
		loop {
			let mut group = match sub.recv_group().await {
				Ok(Some(group)) => group,
				Ok(None) => return (got, None),
				Err(err) => return (got, Some(err)),
			};
			let mut frames = 0;
			loop {
				match group.read_frame().await {
					Ok(Some(_)) => frames += 1,
					Ok(None) => break,
					Err(err) => return (got, Some(err)),
				}
			}
			got.push((group.sequence, frames));
			let _ = read_tx.send(group.sequence);
		}
	});

	track.used().await.expect("subscriber appeared");
	write_group(&track, 1);
	assert_eq!(read_rx.recv().await, Some(1), "group 1 is read first");
	write_group(&track, 0);
	track.finish().unwrap();

	let got = tokio::time::timeout(TIMEOUT, reader)
		.await
		.expect("the read never ended")
		.expect("reader panicked");
	drop((track, broadcast, pairs, publisher, upstream));
	got
}

/// Run every case, then fail once listing each that did not deliver both groups whole
/// followed by the clean end.
async fn assert_both_groups(versions: &[&str], hops: &[u64]) {
	let mut failures = Vec::new();
	for version in versions {
		for &hops in hops {
			let (got, err) = round(version.parse().unwrap(), hops).await;
			if err.is_some() || got != [(1, FRAMES), (0, FRAMES)] {
				failures.push(format!("{version}, {hops} hop(s): got {got:?}, err={err:?}"));
			}
		}
	}
	assert!(
		failures.is_empty(),
		"group 0 is well within the {MAX_AGE:?} budget above the group 0 floor, so both groups \
		 arrive whole, then the clean end: {failures:#?}"
	);
}

/// Over a moq-lite session, direct and through a relay, the late lower group is delivered.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_late_lower_group_within_max_age_is_delivered_over_moq_lite() {
	assert_both_groups(&["moq-lite-05", "moq-lite-06", "moq-lite-07-wip"], &[1, 2]).await;
}

/// Control: the same subscription on the model alone, with no session in between.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_late_lower_group_within_max_age_is_delivered_in_process() {
	assert_both_groups(&["moq-lite-07-wip"], &[0]).await;
}

/// Control: the same subscription over moq-transport, direct and through a relay.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_late_lower_group_within_max_age_is_delivered_over_moq_transport() {
	assert_both_groups(&["moq-transport-14", "moq-transport-17", "moq-transport-22"], &[1, 2]).await;
}
