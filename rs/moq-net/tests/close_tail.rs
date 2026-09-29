//! A track's tail across a graceful close: what the subscriber's transport holds when the
//! publisher closes still belongs to the track.
//!
//! `Session::close` returns once the peer's transport has acknowledged everything the
//! session queued. An acknowledgement says the transport has the data, not that the
//! subscriber has read it, so the mock acknowledges every FIN as soon as it is sent,
//! like a real transport would. The final group is in the subscriber's transport,
//! complete, before the close.
//!
//! Time is paused and the runtime is single-threaded.

mod support;

use std::time::Duration;

use moq_net::{Hop, Timestamp, Version};
use support::harness::{MockConnectOptions, connect_mock};

const TIMEOUT: Duration = Duration::from_secs(10);

/// Only lite-07 can require the subscriber to FIN its Subscribe Stream, which a graceful close
/// waits for; lite-05 and lite-06 keep the ack-based close (quest/m1/close-tail.md).
const VERSIONS: &[&str] = &["moq-lite-07-wip"];

fn produce_origin(hop: u64) -> moq_net::origin::Producer {
	let (producer, driver) = moq_net::origin::Producer::new(moq_net::origin::Config::new(Hop::new(hop).unwrap()));
	tokio::spawn(support::harness::run(driver));
	producer
}

struct Outcome {
	/// What the publisher's close returned.
	close: Result<(), moq_net::Error>,
	frames: Vec<Vec<u8>>,
	err: Option<moq_net::Error>,
}

fn write_group(track: &moq_net::track::Producer, payload: &'static [u8]) {
	let mut group = track.append_group().unwrap();
	group.write_frame(Timestamp::ZERO, payload).unwrap();
	group.finish().unwrap();
}

/// Publish a head group and let the subscriber read it, then write the final group,
/// finish the track and, if `close`, close the publisher's session.
async fn round(version: &str, close: bool) -> Outcome {
	let publisher = produce_origin(1);
	let broadcast = publisher.create_broadcast("bcast").unwrap();
	let track = broadcast.create_track("video", None).unwrap();
	broadcast.announce(Default::default()).unwrap();

	let subscriber = produce_origin(2);
	let mut options = MockConnectOptions::new(version.parse::<Version>().unwrap());
	options.client_publish = Some(publisher.consume());
	options.server_subscribe = Some(subscriber.clone());
	let pair = connect_mock(options).await;

	let consumer = subscriber.consume();
	tokio::time::timeout(TIMEOUT, consumer.routed("bcast"))
		.await
		.expect("announce timeout")
		.expect("routed");
	let remote = tokio::time::timeout(TIMEOUT, consumer.request_broadcast("bcast"))
		.await
		.expect("resolve timeout")
		.expect("broadcast resolves");

	let (head_tx, head_rx) = tokio::sync::oneshot::channel();
	let reader = tokio::spawn(async move {
		let subscription = moq_net::track::Subscription::default().with_start(moq_net::track::Position::group(0));
		let mut sub = remote
			.track("video")
			.unwrap()
			.subscribe(subscription)
			.await
			.expect("subscribe");
		let mut frames = Vec::new();
		let mut head = Some(head_tx);
		let err = 'track: loop {
			let mut group = match sub.recv_group().await {
				Ok(Some(group)) => group,
				Ok(None) => break None,
				Err(err) => break Some(err),
			};
			loop {
				match group.read_frame().await {
					Ok(Some(frame)) => frames.push(frame.payload.to_vec()),
					Ok(None) => break,
					Err(err) => break 'track Some(err),
				}
			}
			if let Some(tx) = head.take() {
				let _ = tx.send(());
			}
		};
		(frames, err)
	});

	tokio::time::timeout(TIMEOUT, track.used())
		.await
		.expect("no subscriber appeared")
		.unwrap();

	write_group(&track, b"head");
	tokio::time::timeout(TIMEOUT, head_rx)
		.await
		.expect("the head group never arrived")
		.unwrap();

	pair.client_transport.ack_fins();
	write_group(&track, b"tail");
	track.finish().unwrap();
	drop(track);

	let (close, open) = match close {
		true => {
			// Requested before the driver has written the final group, so the group and
			// the close reach the subscriber together.
			let close = tokio::time::timeout(TIMEOUT, pair.client.close())
				.await
				.expect("close timed out");
			(close, None)
		}
		false => (Ok(()), Some(pair.client)),
	};

	let (frames, err) = tokio::time::timeout(TIMEOUT, reader)
		.await
		.expect("the subscription never ended")
		.expect("reader panicked");
	drop((open, pair.server, broadcast, publisher, subscriber));
	Outcome { close, frames, err }
}

/// When the close reports that everything was delivered, the subscriber reads the whole
/// track, then a clean end.
#[tokio::test]
async fn a_group_acknowledged_before_the_close_is_delivered() {
	tokio::time::pause();
	for version in VERSIONS {
		let outcome = round(version, true).await;
		outcome
			.close
			.unwrap_or_else(|err| panic!("{version}: the close did not drain: {err}"));
		assert!(
			outcome.err.is_none() && outcome.frames == [b"head".as_slice(), b"tail".as_slice()],
			"{version}: the close returned Ok, the subscriber got {:?}, err={:?}",
			outcome
				.frames
				.iter()
				.map(|frame| String::from_utf8_lossy(frame).into_owned())
				.collect::<Vec<_>>(),
			outcome.err,
		);
	}
}

/// The same track is delivered whole while the publisher's session stays open.
#[tokio::test]
async fn a_finished_track_is_delivered_while_the_session_stays() {
	tokio::time::pause();
	for version in VERSIONS {
		let outcome = round(version, false).await;
		assert!(
			outcome.err.is_none() && outcome.frames == [b"head".as_slice(), b"tail".as_slice()],
			"{version}: got {} frame(s), err={:?}",
			outcome.frames.len(),
			outcome.err,
		);
	}
}
