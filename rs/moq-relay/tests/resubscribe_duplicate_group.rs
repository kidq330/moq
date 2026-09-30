//! An unsubscribe followed at once by a resubscribe of the same track, through a relay
//! on loopback with no impairment, while the publisher has two groups in flight.
//!
//! The second subscription must hand every frame to the application at most once, and
//! its reads must not fail. The outcome depends on how the cancel races the new
//! subscription, so the test runs several rounds and reports each.

use std::{net::TcpListener, time::Duration};

use moq_mux::catalog::hang::Container;
use moq_mux::container::{Consumer, Container as _, Frame, Kind};
use moq_tokio::moq_net;

const TIMEOUT: Duration = Duration::from_secs(10);
/// How long a subscription stays silent before a round moves on.
const QUIET: Duration = Duration::from_millis(300);
const ROUNDS: usize = 20;

fn client() -> moq_tokio::Client {
	let mut config = moq_tokio::connect::Config::default();
	config.tls.insecure = Some(true);
	config.once = Some(true);
	config.websocket.delay = std::time::Duration::ZERO;
	config.bind = Some("127.0.0.1:0".parse().expect("parse bind"));
	config.init(Default::default()).expect("client init")
}

fn spawn_relay() -> (u16, std::process::Child) {
	let probe = TcpListener::bind("127.0.0.1:0").expect("bind probe");
	let port = probe.local_addr().expect("local addr").port();
	drop(probe);

	let cfg = format!(
		"[log]\nlevel = \"warn\"\n\n[listen]\ntcp.bind = \"127.0.0.1:{port}\"\n\n[connect]\nbind = \"127.0.0.1:0\"\n\n[auth]\npublic = \"**\"\n"
	);
	let cfg_path = std::env::temp_dir().join(format!("moq-resubscribe-relay-{port}.toml"));
	std::fs::write(&cfg_path, cfg).expect("write relay config");

	let child = std::process::Command::new(env!("CARGO_BIN_EXE_moq-relay"))
		.arg(&cfg_path)
		.spawn()
		.expect("spawn moq-relay binary");

	let deadline = std::time::Instant::now() + Duration::from_secs(10);
	while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
		assert!(std::time::Instant::now() < deadline, "relay never listened on {port}");
		std::thread::sleep(Duration::from_millis(50));
	}
	(port, child)
}

fn write(group: &mut moq_net::group::Producer, sequence: u64, indices: std::ops::Range<u64>) {
	for index in indices {
		let frame = Frame {
			timestamp: moq_net::Timestamp::from_micros(sequence * 1_000_000 + index * 33_000).unwrap(),
			payload: bytes::Bytes::from(vec![sequence as u8, index as u8]),
			keyframe: index == 0,
			duration: None,
		};
		Container::Legacy(Kind::Video).write(group, &[frame]).unwrap();
	}
}

/// Read until nothing arrives for `quiet`.
async fn drain(consumer: &mut Consumer<Container>, quiet: Duration) -> (Vec<(u8, u8)>, Option<String>) {
	let mut got = Vec::new();
	while let Ok(res) = tokio::time::timeout(quiet, consumer.read()).await {
		match res {
			Ok(Some(frame)) => got.push((frame.payload[0], frame.payload[1])),
			Ok(None) => break,
			Err(err) => return (got, Some(format!("{err:?}"))),
		}
	}
	(got, None)
}

/// What the second subscription handed to the application.
#[derive(Debug)]
struct Outcome {
	duplicates: Vec<(u8, u8)>,
	error: Option<String>,
}

/// One round on its own broadcast: both subscriptions start at group 4.
async fn round(url: &url::Url, name: &str) -> Outcome {
	let pub_origin = moq_tokio::origin::spawn();
	let broadcast = pub_origin.create_broadcast(name).expect("create broadcast");
	broadcast.announce(Default::default()).expect("announce broadcast");
	let track = broadcast
		.create_track("video", hang::container::track_info(hang::catalog::PRIORITY.video))
		.expect("create track");

	let _pub_session = tokio::time::timeout(
		TIMEOUT,
		client()
			.with_reconnect(false)
			.with_publisher(&pub_origin)
			.connect(url.clone())
			.established(),
	)
	.await
	.expect("publisher connect timeout")
	.expect("publisher connect failed");

	let sub_origin = moq_tokio::origin::spawn();
	let sub_consumer = sub_origin.consume();
	let mut announcements = sub_consumer.announced();
	let _sub_session = tokio::time::timeout(
		TIMEOUT,
		client()
			.with_reconnect(false)
			.with_subscriber(sub_origin)
			.connect(url.clone())
			.established(),
	)
	.await
	.expect("subscriber connect timeout")
	.expect("subscriber connect failed");

	let bc = loop {
		let update = tokio::time::timeout(TIMEOUT, announcements.next())
			.await
			.expect("announce timeout")
			.expect("origin closed");
		if update.kind.is_active() && update.prefix.as_str() == name {
			break sub_consumer
				.request_broadcast(&update.prefix)
				.await
				.expect("announced broadcast resolves");
		}
	};

	let subscribe = |start: u64| {
		let track = bc.track("video").unwrap();
		async move {
			let subscription = moq_net::track::Subscription::default()
				.with_start(moq_net::track::Position::group(start))
				.with_max_age(Duration::from_secs(4));
			let subscriber = tokio::time::timeout(TIMEOUT, track.subscribe(subscription))
				.await
				.expect("subscribe timeout")
				.expect("subscribe");
			Consumer::new(subscriber, Container::Legacy(Kind::Video))
		}
	};

	// Groups 4 and 5 are both in flight: 4 is the slow one, 5 is the live edge.
	let mut group4 = track.create_group(moq_net::group::Info { sequence: 4 }).unwrap();
	write(&mut group4, 4, 0..2);
	let mut group5 = track.create_group(moq_net::group::Info { sequence: 5 }).unwrap();
	write(&mut group5, 5, 0..10);

	// First subscription plays what there is, then waits.
	let mut first = subscribe(4).await;
	let (played, error) = drain(&mut first, QUIET).await;
	assert_eq!(error, None, "first subscription failed after {played:?}");

	// Unsubscribe and resubscribe in the same instant.
	drop(first);
	let mut second = subscribe(4).await;
	let (mut order, mut error) = drain(&mut second, QUIET).await;

	// The publisher catches up and one more group follows.
	write(&mut group5, 5, 10..30);
	group5.finish().unwrap();
	write(&mut group4, 4, 2..30);
	group4.finish().unwrap();
	let mut group6 = track.create_group(moq_net::group::Info { sequence: 6 }).unwrap();
	write(&mut group6, 6, 0..30);
	group6.finish().unwrap();

	if error.is_none() {
		let (rest, err) = drain(&mut second, QUIET).await;
		order.extend(rest);
		error = err;
	}

	let mut seen = std::collections::HashSet::new();
	let duplicates = order.iter().copied().filter(|frame| !seen.insert(*frame)).collect();
	Outcome { duplicates, error }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resubscribe_delivers_no_frame_twice() {
	let (port, mut relay) = spawn_relay();
	let url: url::Url = format!("tcp://127.0.0.1:{port}").parse().unwrap();

	let mut failures = Vec::new();
	for index in 0..ROUNDS {
		let outcome = round(&url, &format!("resub-{index}")).await;
		let clean = outcome.duplicates.is_empty() && outcome.error.is_none();
		eprintln!(
			"round {index}: {} frames twice, read error {:?}",
			outcome.duplicates.len(),
			outcome.error
		);
		if !clean {
			failures.push((index, outcome));
		}
	}

	let _ = relay.kill();
	let _ = relay.wait();

	if let Some((index, outcome)) = failures.first() {
		panic!(
			"{} of {ROUNDS} rounds repeated frames or failed; first is round {index}\nduplicates: {:?}\nerror: {:?}",
			failures.len(),
			outcome.duplicates,
			outcome.error,
		);
	}
}
