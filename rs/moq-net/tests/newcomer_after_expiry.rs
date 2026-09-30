//! A peer forwarding a track to two viewers stops serving it to a third once its only
//! group has been idle past the peer's cache expiry.
//!
//! Each viewer's request gets a front of its own on the forwarding peer, and a front
//! that parks keeps a warm copy sharing the group with the peer's upstream copy. When
//! the warm copy is dropped its track closes and stops protecting the group, so idle
//! expiry aborts it, in the upstream copy too.

mod support;

use std::time::Duration;

use moq_net::{Hop, Timestamp, Version, cache, origin, track};
use support::harness::{MockConnectOptions, MockPair, connect_mock};

const EXPIRY: Duration = Duration::from_secs(1);
const TIMEOUT: Duration = Duration::from_secs(10);
/// The versions with TRACK_INFO, whose query before SUBSCRIBE parks the viewer's front.
const VERSIONS: &[&str] = &["moq-lite-05", "moq-lite-06", "moq-lite-07-wip"];

fn produce_origin(config: origin::Config) -> origin::Producer {
	let (producer, driver) = origin::Producer::new(config);
	tokio::spawn(support::harness::run(driver));
	producer
}

fn hop(id: u64) -> origin::Config {
	origin::Config::new(Hop::new(id).unwrap())
}

/// A viewer on a session of its own to `peer`, subscribed to the track.
async fn view(version: Version, peer: &origin::Producer, id: u64) -> (MockPair, origin::Producer, track::Subscriber) {
	let viewer = produce_origin(hop(id));
	let mut options = MockConnectOptions::new(version);
	options.server_publish = Some(peer.consume());
	options.client_subscribe = Some(viewer.clone());
	let pair = connect_mock(options).await;
	let consumer = viewer.consume();
	consumer.routed("bcast").await.unwrap();
	let broadcast = consumer.request_broadcast("bcast").await.unwrap();
	let sub = broadcast.track("catalog").unwrap().subscribe(None).await.unwrap();
	(pair, viewer, sub)
}

async fn served(sub: &mut track::Subscriber) -> bool {
	let Ok(Ok(Some(mut group))) = tokio::time::timeout(TIMEOUT, sub.recv_group()).await else {
		return false;
	};
	matches!(group.read_frame().await, Ok(Some(_)))
}

/// Whether a third viewer is served after `idle`, per version.
async fn round(version: &str, idle: Duration) -> bool {
	let version: Version = version.parse().unwrap();
	let publisher = produce_origin(hop(1));
	let broadcast = publisher.create_broadcast("bcast").unwrap();
	let track = broadcast.create_track("catalog", None).unwrap();
	broadcast.announce(Default::default()).unwrap();
	let mut group = track.append_group().unwrap();
	group.write_frame(Timestamp::now(), &b"{}"[..]).unwrap();
	group.finish().unwrap();

	let mut config = hop(2);
	config.pool = cache::Pool::new(cache::Config::default().with_expiry(EXPIRY));
	let peer = produce_origin(config);
	let mut options = MockConnectOptions::new(version);
	options.server_publish = Some(publisher.consume());
	options.client_subscribe = Some(peer.clone());
	let _upstream = connect_mock(options).await;
	peer.consume().routed("bcast").await.unwrap();

	let mut first = view(version, &peer, 3).await;
	assert!(served(&mut first.2).await, "{version}: first viewer");
	let mut second = view(version, &peer, 4).await;
	assert!(served(&mut second.2).await, "{version}: second viewer");

	tokio::time::sleep(idle).await;
	let mut third = view(version, &peer, 5).await;
	served(&mut third.2).await
}

/// Intentionally red until fixed.
#[tokio::test]
async fn a_third_viewer_is_served_after_the_expiry() {
	tokio::time::pause();
	let mut unserved = Vec::new();
	for version in VERSIONS {
		if !round(version, EXPIRY * 3).await {
			unserved.push(*version);
		}
	}
	assert!(unserved.is_empty(), "the third viewer got nothing on {unserved:?}");
}

/// Control: before the expiry, the third viewer is served.
#[tokio::test]
async fn a_third_viewer_is_served_before_the_expiry() {
	tokio::time::pause();
	for version in VERSIONS {
		assert!(round(version, Duration::ZERO).await, "{version}");
	}
}
