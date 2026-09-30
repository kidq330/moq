//! A group sequence that is aborted and then created again reaches the application
//! once, in full, and the subscription carries on to the groups after it.
//!
//! This is what a track looks like to a client that unsubscribes and resubscribes
//! while a group is in flight: the old subscription's stream for the group is reset
//! (the group is aborted with the frames that had arrived), and the new subscription
//! delivers the same sequence again from its first frame. The earlier group is slow,
//! so the consumer is still on it when both copies of the next one are there.
//!
//! Driven on the model directly: no network, no relay, a paused clock.

use std::time::Duration;

use bytes::Bytes;
use moq_mux::catalog::hang::Container;
use moq_mux::container::{Consumer, Container as _, Frame, Kind};
use moq_net::Timestamp;

fn ts(group: u64, index: u64) -> Timestamp {
	// 1 s groups at 30 fps.
	Timestamp::from_micros(group * 1_000_000 + index * 33_000).unwrap()
}

fn write(group: &mut moq_net::group::Producer, sequence: u64, indices: std::ops::Range<u64>) {
	for index in indices {
		let frame = Frame {
			timestamp: ts(sequence, index),
			payload: Bytes::from(vec![sequence as u8, index as u8]),
			keyframe: index == 0,
			duration: None,
		};
		Container::Legacy(Kind::Video).write(group, &[frame]).unwrap();
	}
}

/// Read until the consumer has nothing more to give right now, or fails.
async fn drain(consumer: &mut Consumer<Container>) -> (Vec<(u8, u8)>, Option<String>) {
	let mut got = Vec::new();
	while let Ok(res) = tokio::time::timeout(Duration::from_millis(50), consumer.read()).await {
		match res {
			Ok(Some(frame)) => got.push((frame.payload[0], frame.payload[1])),
			Ok(None) => break,
			Err(err) => return (got, Some(format!("{err:?}"))),
		}
	}
	(got, None)
}

#[tokio::test(start_paused = true)]
async fn recreated_group_is_not_delivered_twice() {
	let track = moq_net::broadcast::Info::new()
		.produce()
		.create_track("video", hang::container::track_info(hang::catalog::PRIORITY.video))
		.unwrap();

	// The resubscribed client: a 4 s latency budget, as a client catching up would ask for.
	let subscription = moq_net::track::Subscription::default().with_max_age(Duration::from_secs(4));
	let mut consumer = Consumer::new(track.subscribe(subscription), Container::Legacy(Kind::Video));

	// Group 4 is slow (congestion): its first frames are here, the rest are not.
	let mut group4 = track.create_group(moq_net::group::Info { sequence: 4 }).unwrap();
	write(&mut group4, 4, 0..2);

	// Group 5, first incarnation: the old subscription's stream, ten frames in.
	let mut group5_old = track.create_group(moq_net::group::Info { sequence: 5 }).unwrap();
	write(&mut group5_old, 5, 0..10);

	// The consumer plays what it can of group 4 and then waits for the rest of it,
	// with group 5 queued behind.
	assert_eq!(drain(&mut consumer).await, (vec![(4, 0), (4, 1)], None));

	// The relay resets the old subscription's stream...
	group5_old.abort(moq_net::Error::Cancel).unwrap();

	// ...and serves group 5 again on the new subscription.
	let mut group5_new = track
		.create_group(moq_net::group::Info { sequence: 5 })
		.expect("an aborted sequence can be re-created");
	write(&mut group5_new, 5, 0..30);
	group5_new.finish().unwrap();

	// Congestion clears: group 4 completes, group 6 follows.
	write(&mut group4, 4, 2..30);
	group4.finish().unwrap();
	let mut group6 = track.create_group(moq_net::group::Info { sequence: 6 }).unwrap();
	write(&mut group6, 6, 0..30);
	group6.finish().unwrap();
	track.finish().unwrap();

	let (got, err) = drain(&mut consumer).await;

	let group5: Vec<u8> = got.iter().filter(|(g, _)| *g == 5).map(|(_, i)| *i).collect();
	let mut seen = std::collections::HashSet::new();
	let duplicates: Vec<&(u8, u8)> = got.iter().filter(|frame| !seen.insert(**frame)).collect();

	assert!(
		err.is_none(),
		"read failed with {err:?} after {} frames; group 5 as delivered: {group5:?}",
		got.len(),
	);
	assert!(
		duplicates.is_empty(),
		"{} frames delivered twice: {duplicates:?}\ngroup 5 as delivered: {group5:?}",
		duplicates.len(),
	);
	assert_eq!(group5, (0..30).collect::<Vec<u8>>());
}
