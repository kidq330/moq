//! Regression: the moq-mux container `Consumer` drops whole groups of a
//! catch-up subscription even though they arrive within the max age budget.
//!
//! ## The bug
//!
//! Groups ride separate streams, so their first frames race. When arrival order
//! inverts sequence order, `container::Consumer::poll_read` drops the late
//! group at two sites, neither of which consults `self.max_age` or the
//! subscription's requested start:
//!
//! 1. The startup branch (guarded by `self.startup`) walks pending groups in
//!    ascending sequence order and, on the *first* group with a buffered frame,
//!    latches it as `current` and does `self.pending.drain(0..i)`, discarding
//!    every earlier group.
//! 2. The group transition's sequence-gap arm: when the next group's *stream*
//!    hasn't arrived at all, a buffered higher sequence is taken as proof the
//!    missing one was evicted ("the relay delivers in order"), but the
//!    newest-first stream scheduler breaks that assumption: the stream is
//!    merely late. The `poll_empty` FIN-gate doesn't apply here, since it only
//!    protects a zero-frame group whose stream has already arrived.
//!
//! Either way, when the dropped group's frames then arrive, `poll_read_finish`
//! discards them too (`sequence < self.current`, "skipping old group"). This is
//! group loss, not a slow-group skip: the subscription pinned `start = group 0`
//! with a 1s max age budget (which the consumer inherits), so the lost groups
//! are neither past the budget nor unrequested. They're dropped purely because
//! a sibling stream won the race.
//!
//! ## The reproduction
//!
//! A late-join catch-up: the publisher writes 30 frames (payload byte 0 = frame
//! index) across 3 groups (keyframe every 10) and `finish()`es the track
//! *before* the subscriber joins asking for group 0. All three groups are then
//! served at once, and the stream scheduler explicitly prefers higher group
//! sequences (`moq-net/src/lite/priority.rs`: "higher group value = higher
//! priority"), so a newer group's first frame reliably beats an older one's
//! somewhere in the pipeline. The race the consumer loses isn't a scheduling
//! accident here; it's the documented serving order. The real relay runs as a
//! separate OS process, same as membrane_moq_plugin's Sink -> relay -> Source
//! round-trip. The publisher session stays alive until the subscriber drains,
//! keeping the *separate* session-drop tail-loss race (moq-net) out of the
//! picture.
//!
//! Observed signature (fires within a few rounds): the middle group goes
//! missing via the gap arm, indices [10..20), or a head prefix via the startup
//! latch. Deterministic in-process distillations of both sites live next to
//! the code: `moq-mux container::consumer::tests::`
//! `startup_keeps_late_arriving_head_group_within_max_age` and
//! `gap_keeps_late_arriving_group_within_max_age`.

use std::{net::TcpListener, time::Duration};

use moq_tokio::moq_net::{self, Origin};

const TIMEOUT: Duration = Duration::from_secs(10);
const ROUNDS: usize = 40;
const FRAMES: u64 = 30;
const GROUP_SIZE: u64 = 10;
const FRAME_DT_MS: u64 = 33;

/// A one-shot moq_tokio client for the plaintext `tcp://` transport: skips TLS
/// verify and binds the IPv4 loopback family the relay listens on.
fn client() -> moq_tokio::Client {
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
	let mut config = moq_tokio::connect::Config::default();
	config.tls.insecure = Some(true);
	// One-shot: a background redial would re-register with the relay behind
	// the assertions' back.
	config.once = Some(true);
	config.websocket.delay = Duration::ZERO.into();
	config.bind = Some("127.0.0.1:0".parse().expect("parse bind"));
	config.init(Default::default()).expect("client init")
}

/// Connect one-shot, returning the client alongside the connection since the
/// client owns the transport endpoint and has to outlive it.
async fn connect_once(
	client: moq_tokio::Client,
	url: url::Url,
) -> moq_tokio::Result<(moq_tokio::Client, moq_tokio::Connection)> {
	let connection = client.clone().with_reconnect(false).connect(url).established().await?;
	Ok((client, connection))
}

/// Spawn the actual `moq-relay` binary as a separate OS process (stream-only qmux
/// over TCP, fully public auth), so serving is scheduled independently of the
/// in-process publisher and subscriber.
fn spawn_relay_subprocess() -> (u16, std::process::Child) {
	let probe = TcpListener::bind("127.0.0.1:0").expect("bind probe");
	let port = probe.local_addr().expect("local addr").port();
	drop(probe);

	let cfg =
		format!("[log]\nlevel = \"info\"\n\n[server]\ntcp.bind = \"127.0.0.1:{port}\"\n\n[auth]\npublic = \"\"\n");
	let cfg_path = std::env::temp_dir().join(format!("moq-headloss-relay-{port}.toml"));
	std::fs::write(&cfg_path, cfg).expect("write relay config");

	let child = std::process::Command::new(env!("CARGO_BIN_EXE_moq-relay"))
		.arg(&cfg_path)
		.spawn()
		.expect("spawn moq-relay binary");

	let deadline = std::time::Instant::now() + Duration::from_secs(10);
	loop {
		if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
			break;
		}
		if std::time::Instant::now() >= deadline {
			panic!("relay subprocess never listened on {port}");
		}
		std::thread::sleep(Duration::from_millis(50));
	}
	(port, child)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catch_up_subscription_keeps_groups_within_the_max_age_budget() {
	let (port, mut relay_child) = spawn_relay_subprocess();
	let url: url::Url = format!("tcp://127.0.0.1:{port}").parse().expect("parse url");

	let mut failure: Option<String> = None;

	for round in 0..ROUNDS {
		let bcast = format!("test-{round}");

		// Publisher: hang catalog + media_producer, exactly like the plugin's
		// Sink. Legacy (duration-less) container, write-buffered.
		let pub_origin = moq_tokio::origin::spawn(Origin::random());
		let mut broadcast = pub_origin
			.create_broadcast(&bcast, moq_net::broadcast::Route::new().with_announce(true))
			.expect("create broadcast");
		let mut catalog = moq_mux::catalog::Producer::new(&mut broadcast).expect("catalog producer");
		let track = broadcast.create_track("video", None).expect("create track");
		let mut producer = catalog
			.media_producer(track, moq_mux::catalog::hang::Container::Legacy)
			.expect("media producer")
			.with_buffer(Duration::from_millis(500));

		let (_pub_client, pub_session) = tokio::time::timeout(
			TIMEOUT,
			connect_once(client().with_publisher(pub_origin.consume()), url.clone()),
		)
		.await
		.expect("publisher connect timeout")
		.expect("publisher connect failed");

		// The whole track exists, finished, before anyone subscribes. Keyframe
		// every GROUP_SIZE frames, so 3 complete groups. Payload byte 0 = index.
		for i in 0..FRAMES {
			let frame = moq_mux::container::Frame {
				timestamp: moq_net::Timestamp::from_micros(i * FRAME_DT_MS * 1000).expect("ts"),
				payload: bytes::Bytes::from(vec![i as u8; 128]),
				keyframe: i % GROUP_SIZE == 0,
				duration: None,
			};
			producer.write(frame).expect("write frame");
		}
		producer.finish().expect("finish track");

		// Subscriber joins late and asks for the whole track from group 0.
		let sub_origin = moq_tokio::origin::spawn(Origin::random());
		let mut announcements = sub_origin.consume().announced();
		let (_sub_client, sub_session) =
			tokio::time::timeout(TIMEOUT, connect_once(client().with_subscriber(sub_origin), url.clone()))
				.await
				.expect("subscriber connect timeout")
				.expect("subscriber connect failed");

		let bc = loop {
			let moq_net::announce::Update { path, broadcast: bc } = tokio::time::timeout(TIMEOUT, announcements.next())
				.await
				.expect("announce timeout")
				.expect("origin closed");
			if path.as_str() == bcast
				&& let Some(bc) = bc
			{
				break bc;
			}
		};

		// Reads from group 0 behind a 1s max age budget, which the consumer
		// inherits from the subscription. All three groups are served at once
		// (newest-first per the stream scheduler), so some older group's first
		// frame loses the arrival race and the consumer drains it away.
		// Collects each frame's index marker (payload byte 0) so we can name
		// exactly what went missing.
		let subscription = moq_net::track::Subscription::default()
			.with_start(moq_net::track::Position::group(0))
			.with_max_age(Duration::from_secs(1));
		let track_sub = tokio::time::timeout(TIMEOUT, bc.track("video").expect("track").subscribe(subscription))
			.await
			.expect("subscribe timeout")
			.expect("subscribe");
		let mut consumer = moq_mux::container::Consumer::new(track_sub, moq_mux::catalog::hang::Container::Legacy);

		let mut indices = Vec::new();
		let mut err = None;
		loop {
			match tokio::time::timeout(TIMEOUT, consumer.read())
				.await
				.expect("read timeout")
			{
				Ok(Some(frame)) => indices.push(frame.payload[0]),
				Ok(None) => break,
				Err(e) => {
					err = Some(format!("{e:?}"));
					break;
				}
			}
		}
		indices.sort_unstable();

		drop(producer);
		drop(broadcast);
		drop(pub_session);
		drop(sub_session);

		// A round that delivers nothing at all is a different (announce/serving)
		// flake, not the consumer group drop. Log and retry so the failure
		// signature stays unambiguous.
		if indices.is_empty() && err.is_none() {
			eprintln!("round {round}: cached track delivered nothing; not the consumer group drop, skipping");
			continue;
		}

		if indices.len() as u64 != FRAMES || err.is_some() {
			let missing: Vec<u8> = (0..FRAMES as u8).filter(|i| !indices.contains(i)).collect();
			failure = Some(format!(
				"round {round}: got {}/{FRAMES} frames, missing indices {missing:?}, err={err:?}\n\
				 (every group was requested via start = group 0 and arrived within the 1s budget; \
				 losing one is the consumer's arrival-race group drop)",
				indices.len(),
			));
			break;
		}
	}

	let _ = relay_child.kill();
	let _ = relay_child.wait();

	if let Some(msg) = failure {
		panic!("{msg}");
	}
}
