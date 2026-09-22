//! A GOAWAY that lands mid-group, over the in-memory mock transport.
//!
//! A relay origin subscribes to a publisher over session A. A sends a GOAWAY, so A's
//! route is repriced to `Cost::DRAIN`; a second session B to the same publisher then
//! joins, as a client migrating on GOAWAY does, and the relay's track moves to B while
//! group 1 is half read. A reader awaiting only that group must still get every frame.

mod support;

use std::time::Duration;

use moq_net::{Hop, Timestamp, Version, goaway::Goaway, group, origin::Cost};
use support::harness::{MockConnectOptions, connect_mock};

fn produce_origin(hop: u64) -> moq_net::origin::Producer {
	let (producer, driver) = moq_net::origin::Producer::new(moq_net::origin::Config::new(Hop::new(hop).unwrap()));
	tokio::spawn(support::harness::run(driver));
	producer
}

/// The rest of `group` as payloads, ending in "<end>", "<err>", or "<stalled>" when it
/// parks for a second of paused clock.
async fn read_rest(group: &mut group::Consumer) -> Vec<String> {
	let mut got = Vec::new();
	loop {
		match tokio::time::timeout(Duration::from_secs(1), group.read_frame()).await {
			Err(_) => got.push("<stalled>".into()),
			Ok(Ok(Some(frame))) => {
				got.push(String::from_utf8_lossy(&frame.payload).into_owned());
				continue;
			}
			Ok(Ok(None)) => got.push("<end>".into()),
			Ok(Err(err)) => got.push(format!("<{err}>")),
		}
		return got;
	}
}

#[tokio::test(start_paused = true)]
async fn a_goaway_mid_group_keeps_the_rest_of_the_group() {
	let mut failures = Vec::new();
	for version in ["moq-lite-05", "moq-lite-06"] {
		let version: Version = version.parse().unwrap();
		let publisher = produce_origin(1);
		let relay = produce_origin(2);

		let broadcast = publisher.create_broadcast("cam").unwrap();
		let track = broadcast.create_track("video", None).unwrap();
		broadcast.announce(Default::default()).unwrap();

		let connect = || {
			let mut options = MockConnectOptions::new(version);
			options.server_publish = Some(publisher.consume());
			options.client_subscribe = Some(relay.clone());
			connect_mock(options)
		};
		let a = connect().await;

		let consumer = relay.consume();
		consumer.routed("cam").await.unwrap();
		let remote = consumer.request_broadcast("cam").await.unwrap();
		let mut sub = remote.track("video").unwrap().subscribe(None).await.unwrap();

		let ts = |ms| Timestamp::from_millis(ms).unwrap();
		let mut g1 = track.append_group().unwrap();
		g1.write_frame(ts(0), b"a".as_ref()).unwrap();
		let mut r1 = sub.recv_group().await.unwrap().unwrap();
		assert_eq!(&r1.read_frame().await.unwrap().unwrap().payload[..], b"a");

		let mut announced = consumer.announced();
		a.server.drain().send(Goaway::new()).unwrap();
		a.client.draining().recv().await.expect("GOAWAY arrives");
		let _b = connect().await;
		// B's route outranks the drained A once the relay prices it.
		loop {
			let update = announced.next().await.expect("update");
			if update.kind.is_active() && update.route.cost != Cost::DRAIN {
				break;
			}
		}

		g1.write_frame(ts(10), b"b".as_ref()).unwrap();
		g1.write_frame(ts(20), b"c".as_ref()).unwrap();
		g1.finish().unwrap();

		// Only the half-read group is polled, as a player finishing its current group does.
		let got = read_rest(&mut r1).await;
		if got != ["b", "c", "<end>"] {
			failures.push((version, got));
		}
	}
	assert!(failures.is_empty(), "group 1 lost its tail across the GOAWAY: {failures:?}");
}
