//! A subscriber that unsubscribes and resubscribes at once through a forwarding node is
//! handed the same group sequence twice.
//!
//! publisher -> middle -> client, each hop a qmux session over TCP on 127.0.0.1. The
//! middle node is a bare origin that accepts both sessions, the way a relay does. The
//! publisher holds groups 4 and 5 open. When the client leaves and returns, the middle
//! node cancels its upstream SUBSCRIBE, which resets the group streams and aborts its
//! cached copies, and subscribes again; the publisher serves groups 4 and 5 again from
//! frame 0. The model accepts an aborted sequence as a new group, so the client's new
//! subscription, already handed the first copy, is handed the sequence again.
//!
//! Whether it happens depends on how the cancel races the new subscription, so the test
//! runs several rounds and fails if any round repeats a sequence. The same topology over
//! QUIC has not produced it.

use std::time::Duration;

use moq_tokio::moq_net;

const TIMEOUT: Duration = Duration::from_secs(10);
/// How long a subscription stays silent before a round moves on.
const QUIET: Duration = Duration::from_millis(200);
const ROUNDS: usize = 50;

fn write(group: &mut moq_net::group::Producer, sequence: u64, indices: std::ops::Range<u64>) {
	for index in indices {
		let ts = moq_net::Timestamp::from_micros(sequence * 1_000_000 + index * 33_000).unwrap();
		group.write_frame(ts, vec![sequence as u8, index as u8]).unwrap();
	}
}

/// The sequence of every group the subscription hands over until it goes quiet. The
/// groups are kept, as a reader still playing them would.
async fn recv_all(sub: &mut moq_net::track::Subscriber, keep: &mut Vec<moq_net::group::Consumer>) -> Vec<u64> {
	let mut sequences = Vec::new();
	while let Ok(Ok(Some(group))) = tokio::time::timeout(QUIET, sub.recv_group()).await {
		sequences.push(group.sequence);
		keep.push(group);
	}
	sequences
}

async fn connect(url: &url::Url, config: impl FnOnce(moq_tokio::Client) -> moq_tokio::Client) -> moq_tokio::Connection {
	let mut client_config = moq_tokio::connect::Config::default();
	client_config.bind = Some("127.0.0.1:0".parse().unwrap());
	let client = client_config.init(Default::default()).expect("client init");
	tokio::time::timeout(
		TIMEOUT,
		config(client).with_reconnect(false).connect(url.clone()).established(),
	)
	.await
	.expect("connect timeout")
	.expect("connect")
}

/// The middle node: one origin, published to and subscribed from every session, on its
/// own runtime as a separate relay process would be. Returns its URL.
fn spawn_middle() -> url::Url {
	let (port_tx, port_rx) = std::sync::mpsc::channel();
	std::thread::spawn(move || {
		let runtime = tokio::runtime::Builder::new_multi_thread()
			.worker_threads(2)
			.enable_all()
			.build()
			.unwrap();
		runtime.block_on(async move {
			let middle = moq_tokio::origin::spawn();
			let mut config = moq_tokio::listen::Config::default();
			config.tcp.bind = Some("127.0.0.1:0".parse().unwrap());
			let mut server = config
				.init(Default::default())
				.expect("server init")
				.listen()
				.await
				.expect("listen");
			port_tx
				.send(server.tcp_local_addr().expect("tcp listener bound").port())
				.unwrap();
			let mut sessions = Vec::new();
			while let Some(request) = server.accept().await {
				let session = request
					.with_publisher(&middle)
					.with_subscriber(middle.clone())
					.ok()
					.await;
				sessions.push(session.expect("accept session"));
			}
		});
	});
	let port = port_rx.recv().unwrap();
	format!("tcp://127.0.0.1:{port}").parse().unwrap()
}

/// One round on its own broadcast. Returns the sequences the second subscription handed over.
async fn round(url: &url::Url, name: &str) -> Vec<u64> {
	let publisher = moq_tokio::origin::spawn();
	let broadcast = publisher.create_broadcast(name).unwrap();
	broadcast.announce(Default::default()).unwrap();
	let track = broadcast.create_track("video", None).unwrap();
	let _publisher = connect(url, |c| c.with_publisher(&publisher)).await;

	let client = moq_tokio::origin::spawn();
	let consumer = client.consume();
	let _client = connect(url, |c| c.with_subscriber(client.clone())).await;
	tokio::time::timeout(TIMEOUT, consumer.routed(name))
		.await
		.expect("route timeout")
		.expect("routed");
	let remote = consumer.request_broadcast(name).await.expect("broadcast resolves");

	let subscribe = || async {
		let subscription = moq_net::track::Subscription::default()
			.with_start(moq_net::track::Position::group(4))
			.with_max_age(Duration::from_secs(4));
		tokio::time::timeout(TIMEOUT, remote.track("video").unwrap().subscribe(subscription))
			.await
			.expect("subscribe timeout")
			.expect("subscribe")
	};

	// Group 4 is slow, group 5 is the live edge; both stay open.
	let mut group4 = track.create_group(moq_net::group::Info { sequence: 4 }).unwrap();
	write(&mut group4, 4, 0..2);
	let mut group5 = track.create_group(moq_net::group::Info { sequence: 5 }).unwrap();
	write(&mut group5, 5, 0..10);

	let mut first = subscribe().await;
	let mut kept = Vec::new();
	recv_all(&mut first, &mut kept).await;

	// Unsubscribe and resubscribe in the same instant.
	drop(first);
	kept.clear();
	let mut second = subscribe().await;
	let mut sequences = recv_all(&mut second, &mut kept).await;

	write(&mut group5, 5, 10..30);
	group5.finish().unwrap();
	write(&mut group4, 4, 2..30);
	group4.finish().unwrap();
	let mut group6 = track.create_group(moq_net::group::Info { sequence: 6 }).unwrap();
	write(&mut group6, 6, 0..30);
	group6.finish().unwrap();

	sequences.extend(recv_all(&mut second, &mut kept).await);
	sequences
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resubscribe_through_a_forwarding_node_hands_each_group_once() {
	let url = spawn_middle();
	let mut failures = Vec::new();
	for index in 0..ROUNDS {
		let sequences = round(&url, &format!("resub-{index}")).await;
		let mut unique = sequences.clone();
		unique.sort();
		unique.dedup();
		eprintln!("round {index}: {sequences:?}");
		if unique.len() != sequences.len() {
			failures.push((index, sequences));
		}
	}
	if let Some((index, sequences)) = failures.first() {
		panic!(
			"{} of {ROUNDS} rounds handed a group sequence twice; first is round {index}: {sequences:?}",
			failures.len()
		);
	}
}
