//! A viewer resuming mid-group through a forwarding peer leaves the next viewer with
//! nothing.
//!
//! A viewer that already read a group's head and subscribes again asks for the rest of
//! it (`Frame Start`, lite-06 and later). The peer's upstream subscription ended with the
//! first one, so it forwards that request upstream, and its shared copy of the group
//! presumably lacks the head. A later viewer asking for the whole group is sent
//! SUBSCRIBE_START for it and then nothing.

mod support;

use std::time::Duration;

use moq_net::{Hop, Timestamp, Version, origin, track};
use support::harness::{MockConnectOptions, MockPair, connect_mock};

const TIMEOUT: Duration = Duration::from_secs(10);
const VERSIONS: &[&str] = &["moq-lite-05", "moq-lite-06", "moq-lite-07-wip"];

fn produce_origin(hop: u64) -> origin::Producer {
	let (producer, driver) = origin::Producer::new(origin::Config::new(Hop::new(hop).unwrap()));
	tokio::spawn(support::harness::run(driver));
	producer
}

/// A viewer on a session of its own to `peer`.
struct Viewer {
	_pair: MockPair,
	origin: origin::Producer,
}

impl Viewer {
	async fn connect(version: Version, peer: &origin::Producer, hop: u64) -> Self {
		let origin = produce_origin(hop);
		let mut options = MockConnectOptions::new(version);
		options.server_publish = Some(peer.consume());
		options.client_subscribe = Some(origin.clone());
		let pair = connect_mock(options).await;
		origin.consume().routed("bcast").await.unwrap();
		Self { _pair: pair, origin }
	}

	async fn subscribe(&self) -> track::Subscriber {
		let broadcast = self.origin.consume().request_broadcast("bcast").await.unwrap();
		broadcast.track("catalog").unwrap().subscribe(None).await.unwrap()
	}
}

async fn served(sub: &mut track::Subscriber) -> bool {
	let Ok(Ok(Some(mut group))) = tokio::time::timeout(TIMEOUT, sub.recv_group()).await else {
		return false;
	};
	matches!(group.read_frame().await, Ok(Some(_)))
}

/// Whether a second viewer is served after the first one reads the group, drops its
/// subscription, and (when `resubscribe`) subscribes again.
async fn round(version: &str, resubscribe: bool) -> bool {
	let version: Version = version.parse().unwrap();
	let publisher = produce_origin(1);
	let broadcast = publisher.create_broadcast("bcast").unwrap();
	let track = broadcast.create_track("catalog", None).unwrap();
	broadcast.announce(Default::default()).unwrap();
	let mut group = track.append_group().unwrap();
	group.write_frame(Timestamp::now(), &b"{}"[..]).unwrap();
	group.finish().unwrap();

	let peer = produce_origin(2);
	let mut options = MockConnectOptions::new(version);
	options.server_publish = Some(publisher.consume());
	options.client_subscribe = Some(peer.clone());
	let _upstream = connect_mock(options).await;
	peer.consume().routed("bcast").await.unwrap();

	let first = Viewer::connect(version, &peer, 3).await;
	let mut sub = first.subscribe().await;
	assert!(served(&mut sub).await, "{version}: first viewer");
	drop(sub);
	tokio::task::yield_now().await;
	let _again = match resubscribe {
		true => {
			let mut again = first.subscribe().await;
			assert!(served(&mut again).await, "{version}: first viewer again");
			Some(again)
		}
		false => None,
	};

	let second = Viewer::connect(version, &peer, 4).await;
	served(&mut second.subscribe().await).await
}

/// Intentionally red until fixed.
#[tokio::test]
async fn a_second_viewer_is_served_after_the_first_resumes() {
	tokio::time::pause();
	let mut unserved = Vec::new();
	for version in VERSIONS {
		if !round(version, true).await {
			unserved.push(*version);
		}
	}
	assert!(unserved.is_empty(), "the second viewer got nothing on {unserved:?}");
}

/// Control: when the first viewer only leaves, the second is served.
#[tokio::test]
async fn a_second_viewer_is_served_after_the_first_leaves() {
	tokio::time::pause();
	for version in VERSIONS {
		assert!(round(version, false).await, "{version}");
	}
}
