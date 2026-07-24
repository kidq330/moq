//! Regression: a publisher that `finish()`es a track and immediately tears its
//! session down loses the tail of the final group. Mirrors membrane_moq_plugin's
//! Sink -> relay -> Source round-trip, where the Sink finishes on EOS and the
//! pipeline then drops the session while the lagging Source is still draining.
//!
//! ## Root cause
//!
//! `Session` drop is an abort, not a graceful close. `producer.finish()` marks
//! the track's final sequence locally, but the final group's stream (its FIN plus
//! any latency-batched tail) is still in flight over the transport. Dropping the
//! session resets that stream before the relay has received the complete group,
//! so the relay caches it as dropped-without-finish and every downstream consumer
//! observes it as [`Error::Dropped`](moq_native::moq_net::Error::Dropped) mid-read
//! (group.rs: "Dropped without finish() or abort()"). The tail past the
//! subscriber's read cursor is lost.
//!
//! Confirmed a delivery race, not a deeper relay defect: inserting any grace
//! period between `finish()` and the session drop (or, as the head-loss repro
//! does, keeping the session alive until the subscriber fully drains) delivers the
//! whole tail. There is no flush-and-close primitive on `Session`, so a publisher
//! that finishes and disconnects has no supported way to guarantee its tail lands.
//!
//! ## Isolation
//!
//! Reads from group 0 with a 1s budget, so the subscriber lags a full group and
//! the reset lands squarely on the final group. This shares the round-trip with
//! the *separate* moq-mux consumer startup-skip bug (head loss); to keep the two
//! apart, only the tail signature (a `Dropped` error or missing final-group
//! indices) fails this test. An incidental head-only loss is logged and skipped.

use std::{net::TcpListener, time::Duration};

use moq_native::moq_net::{self, Origin};

const TIMEOUT: Duration = Duration::from_secs(10);
const ROUNDS: usize = 120;
const FRAMES: u64 = 30;
const GROUP_SIZE: u64 = 10;
const FRAME_DT_MS: u64 = 33;

fn client() -> moq_native::Client {
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
	let mut config = moq_native::ClientConfig::default();
	config.tls.disable_verify = Some(true);
	config.websocket.delay = None;
	config.init().expect("client init")
}

fn spawn_relay_subprocess() -> (u16, std::process::Child) {
	let probe = TcpListener::bind("127.0.0.1:0").expect("bind probe");
	let port = probe.local_addr().expect("local addr").port();
	drop(probe);

	let cfg =
		format!("[log]\nlevel = \"info\"\n\n[server]\ntcp.bind = \"127.0.0.1:{port}\"\n\n[auth]\npublic = \"\"\n");
	let cfg_path = std::env::temp_dir().join(format!("moq-tailloss-relay-{port}.toml"));
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
async fn finished_tail_survives_publisher_teardown() {
	let (port, mut relay_child) = spawn_relay_subprocess();
	let url: url::Url = format!("tcp://127.0.0.1:{port}").parse().expect("parse url");

	let mut failure: Option<String> = None;

	for round in 0..ROUNDS {
		let bcast = format!("test-{round}");

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
						tokio::time::sleep(Duration::from_millis(10)).await;
					}
					Ok(None) => break (indices, None),
					Err(e) => break (indices, Some(format!("{e:?}"))),
				}
			}
		});

		tokio::time::timeout(TIMEOUT, producer.used())
			.await
			.expect("no downstream consumer appeared")
			.expect("used() errored");

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

		// The teardown race: drop the publisher session RIGHT AFTER finish(), with no
		// grace for delivery. `Session` drop is an abort, not a graceful close, so the
		// final group's in-flight stream (FIN + any latency-batched tail) is reset
		// before the relay has cached the complete group. Downstream subscribers then
		// observe the group as `Error::Dropped` mid-read and lose its tail. Keeping the
		// session alive briefly (or until the subscriber drains) delivers the whole tail.
		drop(producer);
		drop(broadcast);
		drop(pub_session);

		let (mut indices, err) = tokio::time::timeout(TIMEOUT, sub_task)
			.await
			.expect("subscriber timeout")
			.expect("subscriber task panicked");
		indices.sort_unstable();

		drop(sub_session);

		let missing: Vec<u8> = (0..FRAMES as u8).filter(|i| !indices.contains(i)).collect();
		let tail_missing: Vec<u8> = missing
			.iter()
			.copied()
			.filter(|&i| (i as u64) >= FRAMES - GROUP_SIZE)
			.collect();

		// The tail signature: the final group is lost, either as an explicit Dropped
		// error mid-read or as missing final-group indices. A pure head loss
		// (err=None, only low indices missing) is the *separate* moq-mux consumer
		// startup-skip bug covered by moq_mux_head_loss.rs, not this teardown race, so
		// it's noted and skipped here rather than muddying the tail assertion.
		if err.is_some() || !tail_missing.is_empty() {
			failure = Some(format!(
				"round {round}: got {}/{FRAMES}, missing {missing:?}, err={err:?} \
				 (final group {}..{FRAMES} lost to publisher teardown before delivery)",
				indices.len(),
				FRAMES - GROUP_SIZE,
			));
			break;
		} else if !missing.is_empty() {
			eprintln!(
				"round {round}: incidental head-race loss (the moq-mux consumer bug), missing {missing:?}; skipping"
			);
		}
	}

	let _ = relay_child.kill();
	let _ = relay_child.wait();

	if let Some(msg) = failure {
		panic!("{msg}");
	}
}
