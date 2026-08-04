//! Regression: the moq-mux container `Consumer` drops the earliest group of a
//! finished track even with a latency budget that should tolerate it.
//!
//! ## The bug
//!
//! `container::Consumer::poll_read` has a startup branch (`consumer.rs`, guarded
//! by `self.startup`) that walks the pending groups in ascending sequence order
//! and, on the *first* group that has buffered a frame, sets that group as
//! `current` and does `self.pending.drain(0..i)`, discarding every earlier group.
//! It never consults `self.latency`.
//!
//! Groups ride separate QUIC streams, so their first frames race. When a later
//! group's first frame arrives from the relay before the earliest group's does,
//! startup latches onto the later group and drains the earliest one away. If the
//! earliest group's frames then arrive, `poll_read_finish` drops them too
//! (`sequence < self.current` => "skipping old group"). The subscriber is left
//! missing the whole head of the track.
//!
//! This is head loss, not a slow-group skip: the consumer was built
//! `with_latency(1s)` and the subscription pinned `group_start = Some(0)`, so the
//! earliest group is neither past the budget nor unrequested. It's dropped purely
//! because a sibling stream won the first-frame race.
//!
//! ## The reproduction
//!
//! Mirrors membrane_moq_plugin's Sink -> relay -> Source round-trip: a publisher
//! writes 30 frames (payload byte 0 = frame index) paced at ~30fps across 3
//! groups (keyframe every 10), latency-batched, then `finish()`es the track. A
//! lagging subscriber reads behind a 1s budget from group 0. The real relay runs
//! as a separate OS process, which is load-bearing: the first-frame race only
//! shows up when the relay is scheduled independently of the in-process
//! publisher/subscriber. Loops until it catches the drop, then reports exactly
//! which frame indices went missing (the head group's, i.e. 0..10).

use std::{net::TcpListener, time::Duration};

use moq_native::moq_net::{self, Origin};

const TIMEOUT: Duration = Duration::from_secs(10);
const ROUNDS: usize = 120;
const FRAMES: u64 = 30;
const GROUP_SIZE: u64 = 10;
const FRAME_DT_MS: u64 = 33;

/// A moq_native client for the plaintext `tcp://` transport: skips TLS verify and
/// takes the WebSocket path with no head-start delay.
fn client() -> moq_native::Client {
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
	let mut config = moq_native::ClientConfig::default();
	config.tls.disable_verify = Some(true);
	config.websocket.delay = None;
	config.init().expect("client init")
}

/// Spawn the actual `moq-relay` binary as a separate OS process (stream-only qmux
/// over TCP, fully public auth). The separate process is load-bearing: the
/// earliest-group drop only surfaces when the relay is scheduled independently of
/// the in-process publisher and subscriber.
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
async fn latency_budget_keeps_the_earliest_group() {
	let (port, mut relay_child) = spawn_relay_subprocess();
	let url: url::Url = format!("tcp://127.0.0.1:{port}").parse().expect("parse url");

	let mut failure: Option<String> = None;

	for round in 0..ROUNDS {
		let bcast = format!("test-{round}");

		// ── publisher: hang catalog + media_producer, exactly like the plugin's
		// Sink. Legacy (duration-less) container, latency-batched, paced writes.
		let pub_origin = Origin::random().produce();
		let mut broadcast = pub_origin
			.create_broadcast(&bcast, moq_net::broadcast::Route::new().with_announce(true))
			.expect("create broadcast");
		let catalog = moq_mux::catalog::Producer::new(&mut broadcast).expect("catalog producer");
		let track = broadcast.create_track("video", None).expect("create track");
		let mut producer = catalog
			.media_producer(track, moq_mux::catalog::hang::Container::Legacy)
			.expect("media producer")
			.with_latency(Duration::from_millis(500));

		let pub_session = tokio::time::timeout(
			TIMEOUT,
			client().with_publisher(pub_origin.consume()).connect(url.clone()),
		)
		.await
		.expect("publisher connect timeout")
		.expect("publisher connect failed");

		// ── subscriber ──
		let sub_origin = Origin::random().produce();
		let mut announcements = sub_origin.consume().announced();
		let sub_session = tokio::time::timeout(TIMEOUT, client().with_subscriber(sub_origin).connect(url.clone()))
			.await
			.expect("subscriber connect timeout")
			.expect("subscriber connect failed");

		let bc = loop {
			let moq_net::announce::Update { path, broadcast: bc } = tokio::time::timeout(TIMEOUT, announcements.next())
				.await
				.expect("announce timeout")
				.expect("origin closed");
			if path.as_str() == bcast {
				if let Some(bc) = bc {
					break bc;
				}
			}
		};

		// Subscriber reads behind a 1s latency budget, from group 0 (not the live
		// edge) so any loss is unambiguously a dropped group. Collects each frame's
		// index marker (payload byte 0) so we can name exactly what went missing.
		let sub_task = tokio::spawn(async move {
			let mut subscription = moq_net::track::Subscription::default();
			subscription.group_start = Some(0);
			let track_sub = bc
				.track("video")
				.unwrap()
				.subscribe(subscription)
				.await
				.expect("subscribe");
			let mut consumer = moq_mux::container::Consumer::new(track_sub, moq_mux::catalog::hang::Container::Legacy)
				.with_latency(Duration::from_secs(1));
			let mut indices = Vec::new();
			loop {
				match consumer.read().await {
					Ok(Some(frame)) => {
						indices.push(frame.payload[0]);
						// Mimic per-frame downstream handoff cost (NIF -> Erlang message ->
						// Source buffer action), so the receiver reads behind the live edge.
						tokio::time::sleep(Duration::from_millis(10)).await;
					}
					Ok(None) => break (indices, None),
					Err(e) => break (indices, Some(format!("{e:?}"))),
				}
			}
		});

		// Gate on demand, like the plugin's `used()`-based gate: hold media until a
		// downstream consumer has subscribed, so the head isn't lost to a live-edge
		// join. This isolates the earliest-group drop from an ordinary late join.
		tokio::time::timeout(TIMEOUT, producer.used())
			.await
			.expect("no downstream consumer appeared")
			.expect("used() errored");

		// Paced writes, keyframe every GROUP_SIZE frames. Payload byte 0 = index.
		for i in 0..FRAMES {
			let frame = moq_mux::container::Frame {
				timestamp: moq_net::Timestamp::from_micros(i * FRAME_DT_MS * 1000).expect("ts"),
				payload: bytes::Bytes::from(vec![i as u8; 128]),
				keyframe: i % GROUP_SIZE == 0,
				duration: None,
			};
			producer.write(frame).expect("write frame");
			tokio::time::sleep(Duration::from_millis(FRAME_DT_MS)).await;
		}
		producer.finish().expect("finish track");

		let (mut indices, err) = tokio::time::timeout(TIMEOUT, sub_task)
			.await
			.expect("subscriber timeout")
			.expect("subscriber task panicked");
		indices.sort_unstable();

		drop(producer);
		drop(broadcast);
		drop(pub_session);
		drop(sub_session);

		if indices.len() as u64 != FRAMES || err.is_some() {
			let missing: Vec<u8> = (0..FRAMES as u8).filter(|i| !indices.contains(i)).collect();
			failure = Some(format!(
				"round {round}: got {}/{FRAMES} frames, missing indices {missing:?}, err={err:?}\n\
				 (indices 0..{GROUP_SIZE} are the earliest group; losing them is the startup head-drop)",
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
