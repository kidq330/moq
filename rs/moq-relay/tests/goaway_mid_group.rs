//! Continuity of a live track across an upstream GOAWAY, per protocol version.
//!
//! The relay's cluster dials sibling A; A sends a GOAWAY redirecting to sibling B while a
//! group is half delivered. The downstream consumer must see every frame exactly once.
//! B either serves the same in-process origin as A ("warm": it has every frame) or is a
//! relay of its own that subscribes to the publisher on demand ("cold": it holds nothing
//! when the cluster's re-subscribe arrives).
//!
//! Two more knobs pick the timing: whether the publisher has moved on to group 2 before
//! the reader asks for the rest of group 1, and whether the cluster has routed to B by
//! then. After an immediate redirect moq-tokio holds the dial to B back by 0.5 to 1 s,
//! so a reader that doesn't wait for the route asks while only the draining A exists.

use std::net::TcpListener;
use std::time::Duration;

use moq_relay::cluster::{self, Peer};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

async fn within<T>(step: &str, fut: impl std::future::Future<Output = T>) -> T {
	tokio::time::timeout(TEST_TIMEOUT, fut)
		.await
		.unwrap_or_else(|_| panic!("timed out: {step}"))
}

fn run_cluster_test<F>(fut: F)
where
	F: std::future::Future<Output = ()> + Send + 'static,
{
	std::thread::Builder::new()
		.stack_size(32 * 1024 * 1024)
		.spawn(move || {
			tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("build test runtime")
				.block_on(fut);
		})
		.expect("spawn test thread")
		.join()
		.expect("test thread panicked");
}

fn bind_free_tcp_server(version: moq_net::Version) -> (u16, moq_tokio::Server) {
	for _ in 0..20 {
		let probe = TcpListener::bind("127.0.0.1:0").expect("bind probe");
		let port = probe.local_addr().expect("local addr").port();
		drop(probe);

		let mut config = moq_tokio::listen::Config::default();
		config.tcp.bind = Some(format!("127.0.0.1:{port}").parse().expect("parse addr"));
		config.version = vec![version];
		if let Ok(server) = config.init(Default::default()) {
			return (port, server);
		}
	}
	panic!("could not bind a free TCP port after 20 attempts");
}

/// A server publishing `origin` to whoever connects. Yields each accepted session.
fn spawn_upstream(
	origin: moq_net::origin::Producer,
	version: moq_net::Version,
) -> (u16, tokio::sync::mpsc::UnboundedReceiver<moq_net::Session>) {
	let (port, server) = bind_free_tcp_server(version);
	let (accepted_tx, accepted_rx) = tokio::sync::mpsc::unbounded_channel();
	tokio::spawn(async move {
		let mut server = server.listen().await.expect("listen");
		while let Some(request) = server.accept().await {
			let scratch = moq_tokio::origin::spawn();
			match request.with_publisher(&origin).with_subscriber(scratch).ok().await {
				Ok(session) => {
					let _ = accepted_tx.send(session);
				}
				Err(err) => tracing::warn!(%err, "upstream accept failed"),
			}
		}
	});
	(port, accepted_rx)
}

async fn wait_listening(port: u16) {
	let deadline = std::time::Instant::now() + Duration::from_secs(5);
	while tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_err() {
		assert!(std::time::Instant::now() < deadline, "port {port} never listened");
		tokio::time::sleep(Duration::from_millis(25)).await;
	}
}

fn client(version: moq_net::Version) -> moq_tokio::Client {
	let mut config = moq_tokio::connect::Config::default();
	config.tls.insecure = Some(true);
	config.version = vec![version];
	config.goaway.handover = Duration::from_secs(2);
	// The sandboxes this runs in may lack IPv6, and every peer here is on loopback.
	config.bind = Some("127.0.0.1:0".parse().unwrap());
	config.init(Default::default()).expect("client init")
}

fn frame(group: &mut moq_net::group::Producer, clock: &mut u64, payload: &str) {
	*clock += 1;
	group
		.write_frame(
			moq_net::Timestamp::from_millis(*clock).unwrap(),
			payload.as_bytes().to_vec(),
		)
		.expect("write frame");
}

/// Read `count` frames of `group`, recording them as "group/payload".
async fn read_frames(group: &mut moq_net::group::Consumer, count: usize, got: &mut Vec<String>) {
	for _ in 0..count {
		let Ok(read) = tokio::time::timeout(Duration::from_secs(2), group.read_frame()).await else {
			got.push(format!("{}/<stalled>", group.sequence));
			return;
		};
		match read {
			Ok(Some(frame)) => got.push(format!(
				"{}/{}",
				group.sequence,
				String::from_utf8_lossy(&frame.payload)
			)),
			Ok(None) => {
				got.push(format!("{}/<end>", group.sequence));
				return;
			}
			Err(err) => {
				got.push(format!("{}/<{err}>", group.sequence));
				return;
			}
		}
	}
}

#[derive(Clone, Copy, Debug)]
struct Case {
	/// B relays from the publisher on demand instead of sharing A's origin.
	cold: bool,
	/// The publisher starts group 2 before the reader asks for the rest of group 1.
	moved_on: bool,
	/// The reader waits for the cluster to route to B before asking.
	b_routed: bool,
}

async fn migrate_mid_group(version: moq_net::Version, case: Case) -> Vec<String> {
	let Case {
		cold,
		moved_on,
		b_routed,
	} = case;
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

	let upstream_origin = moq_tokio::origin::spawn();
	let broadcast = upstream_origin.create_broadcast("cam").expect("create broadcast");
	broadcast.announce(Default::default()).expect("announce");
	let track = broadcast.create_track("video", None).expect("create track");

	let (port_a, mut accepted_a) = spawn_upstream(upstream_origin.clone(), version);
	wait_listening(port_a).await;

	// B: the same origin, or a relay that pulls from a publisher server on demand.
	let mut _b_upstream = None;
	let (port_b, mut accepted_b) = if cold {
		let (port_p, _accepted_p) = spawn_upstream(upstream_origin.clone(), version);
		wait_listening(port_p).await;
		let b_origin = moq_tokio::origin::spawn();
		let connection = within(
			"B dials the publisher",
			client(version)
				.with_reconnect(false)
				.with_subscriber(b_origin.clone())
				.connect(format!("tcp://127.0.0.1:{port_p}/").parse::<url::Url>().unwrap())
				.established(),
		)
		.await
		.expect("B connects to the publisher");
		within("B learns the broadcast", b_origin.consume().routed("cam"))
			.await
			.expect("routed");
		_b_upstream = Some((connection, _accepted_p));
		spawn_upstream(b_origin, version)
	} else {
		spawn_upstream(upstream_origin.clone(), version)
	};
	wait_listening(port_b).await;

	let mut cluster_config = cluster::Config::default();
	cluster_config.connect = vec![Peer::new(format!("tcp://127.0.0.1:{port_a}/"))];
	let cluster = cluster::Cluster::new(cluster::Options::new(cluster_config))
		.expect("cluster init")
		.with_client(client(version));
	let started = cluster.clone().start().await.expect("cluster start");
	let cluster_run = tokio::spawn(started.run());

	let session_a = within("A accepts", accepted_a.recv()).await.expect("A accepts");

	let consumer = cluster.origin.consume();
	within("routed via A", consumer.routed("cam")).await.expect("routed");
	let bc = consumer.request_broadcast("cam").await.expect("broadcast resolves");
	let mut sub = within("subscribe", bc.track("video").expect("track").subscribe(None))
		.await
		.expect("subscribe");

	let mut clock = 0;
	let mut got = Vec::new();

	// Group 0 complete, group 1 half written and half read.
	let mut g0 = track
		.create_group(moq_net::group::Info { sequence: 0 })
		.expect("group 0");
	frame(&mut g0, &mut clock, "a");
	frame(&mut g0, &mut clock, "b");
	g0.finish().expect("finish");
	let mut r0 = within("recv group 0", sub.recv_group())
		.await
		.expect("recv")
		.expect("ended");
	read_frames(&mut r0, 3, &mut got).await;

	let mut g1 = track
		.create_group(moq_net::group::Info { sequence: 1 })
		.expect("group 1");
	frame(&mut g1, &mut clock, "a");
	let mut r1 = within("recv group 1", sub.recv_group())
		.await
		.expect("recv")
		.expect("ended");
	read_frames(&mut r1, 1, &mut got).await;

	let mut announced = consumer.announced();

	// A redirects to B mid-group.
	session_a
		.drain()
		.send(moq_net::goaway::Goaway::redirect(format!("tcp://127.0.0.1:{port_b}/")))
		.expect("send goaway");
	let _session_b = within("B accepts", accepted_b.recv()).await.expect("B accepts");
	if b_routed {
		within("routed via B", async {
			loop {
				let update = announced.next().await.expect("announce update");
				if update.kind.is_active() && update.route.cost != moq_net::origin::Cost::DRAIN {
					break;
				}
			}
		})
		.await;
	} else {
		tokio::time::sleep(Duration::from_millis(300)).await;
	}

	frame(&mut g1, &mut clock, "b");
	frame(&mut g1, &mut clock, "c");
	g1.finish().expect("finish");
	let write_g2 = |clock: &mut u64| {
		let mut g2 = track
			.create_group(moq_net::group::Info { sequence: 2 })
			.expect("group 2");
		frame(&mut g2, clock, "a");
		g2.finish().expect("finish");
	};
	if moved_on {
		write_g2(&mut clock);
	}

	read_frames(&mut r1, 3, &mut got).await;
	if !moved_on {
		write_g2(&mut clock);
	}
	// Whatever else the subscription delivers, until it goes quiet.
	loop {
		match tokio::time::timeout(Duration::from_secs(2), sub.recv_group()).await {
			Err(_) => break,
			Ok(Ok(Some(mut group))) => read_frames(&mut group, 4, &mut got).await,
			Ok(Ok(None)) => {
				got.push("<track ended>".into());
				break;
			}
			Ok(Err(err)) => {
				got.push(format!("<{err}>"));
				break;
			}
		}
	}

	cluster_run.abort();
	got
}

const EXPECTED: [&str; 9] = [
	"0/a", "0/b", "0/<end>", "1/a", "1/b", "1/c", "1/<end>", "2/a", "2/<end>",
];

const LITE: [&str; 3] = ["moq-lite-05", "moq-lite-06", "moq-lite-07-wip"];

fn check(versions: &[&'static str], case: Case) {
	let versions = versions.to_vec();
	run_cluster_test(async move {
		let mut failures = Vec::new();
		for version in versions {
			let v: moq_net::Version = version.parse().unwrap();
			let got = migrate_mid_group(v, case).await;
			eprintln!("{version} {case:?}: {got:?}");
			if got != EXPECTED {
				failures.push(version);
			}
		}
		assert!(
			failures.is_empty(),
			"frames lost or repeated across the GOAWAY on {failures:?}"
		);
	});
}

const ALL: [&str; 8] = [
	"moq-lite-05",
	"moq-lite-06",
	"moq-lite-07-wip",
	"moq-transport-21",
	"moq-transport-20",
	"moq-transport-19",
	"moq-transport-17",
	"moq-transport-14",
];

#[test]
fn a_goaway_mid_group_keeps_every_frame_warm() {
	check(
		&ALL,
		Case {
			cold: false,
			moved_on: true,
			b_routed: false,
		},
	);
}

#[test]
fn a_goaway_mid_group_keeps_every_frame_cold() {
	check(
		&ALL,
		Case {
			cold: true,
			moved_on: true,
			b_routed: false,
		},
	);
}

/// The reader asks once the cluster routes to B, after the publisher has moved on.
#[test]
fn a_goaway_mid_group_keeps_every_frame_routed() {
	check(
		&LITE,
		Case {
			cold: false,
			moved_on: true,
			b_routed: true,
		},
	);
}

/// The reader asks while the publisher is still on group 1.
#[test]
fn a_goaway_mid_group_keeps_every_frame_before_moving_on() {
	check(
		&LITE,
		Case {
			cold: false,
			moved_on: false,
			b_routed: false,
		},
	);
}
