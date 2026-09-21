//! Repro: after a bottleneck squeeze, the sender does not reclaim a widened link,
//! so a lower-priority track stays starved although there is room for it.
//!
//! NOTE: this file is LLM-generated.
//!
//! One process, plus a relay you start yourself:
//!
//! ```text
//! publisher ──▶ relay ──▶ bottleneck (in here, UDP) ──▶ subscriber
//!   tracks high + low       rate steps: wide, squeezed, recovered     priorities 80 / 60
//! ```
//!
//! Both tracks carry 24 frames/s in 1 s groups at about 450 kbit/s each, shaped
//! like video: each group opens with a large keyframe followed by small deltas. The
//! bottleneck shapes only the relay -> subscriber direction (token bucket into a
//! bounded tail-drop FIFO) and delays both directions by 50 ms, so the relay sees a
//! 100 ms RTT. The squeezed rate fits `high` alone; the recovered rate fits both
//! with room to spare.
//!
//! Expected: within a few seconds of the step to the recovered rate the link is
//! used and `low` is back at 24 fps with a fresh newest frame. Observed with the
//! default congestion control (`delay`, BBR): the bottleneck keeps forwarding about
//! the squeezed rate with no drops and an empty queue, and `low` stays starved.
//! With `--quic-congestion-control loss` on the relay it recovers at once.
//!
//! ```text
//! cargo run --bin moq-relay -- --listen 127.0.0.1:4443 --listen-tls-generate localhost --auth-public '**'
//! cargo run -p moq-tokio --example squeeze -- --relay https://localhost:4443/anon --connect-tls-insecure
//! ```

use std::{
	collections::{HashMap, VecDeque},
	net::SocketAddr,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	time::Duration,
};

use anyhow::Context;
use tokio::{net::UdpSocket, sync::mpsc, time::Instant};

const TRACKS: [(&str, u8); 2] = [("high", 80), ("low", 60)];
const FPS: u64 = 24;
const MTU: u64 = 1500;

#[derive(usage::Cli, Clone)]
#[usage(unknown_flags = "error", args_override_self = false)]
#[usage(name = "squeeze")]
#[usage(settings)]
struct Config {
	/// The relay to publish to directly and to subscribe to through the bottleneck.
	#[usage(long)]
	relay: url::Url,

	#[usage(flatten)]
	client: moq_tokio::connect::Config,

	#[usage(flatten)]
	log: moq_tokio::Log,

	/// Payload bytes of a group's first frame, standing in for a keyframe.
	#[usage(long, default = "10000")]
	keyframe_size: usize,

	/// Payload bytes of every other frame. The defaults make about 450 kbit/s
	/// per track at 24 fps, in the shape the demo clips have.
	#[usage(long, default = "2000")]
	frame_size: usize,

	/// Link rate in kbit/s for the wide, squeezed and recovered phases, comma separated.
	#[usage(long, default = "4000,700,1500")]
	rates: String,

	/// Duration in seconds of the wide, squeezed and recovered phases, comma separated.
	#[usage(long, default = "20,30,60")]
	secs: String,

	/// One-way delay of the bottleneck in ms, applied in both directions.
	#[usage(long, default = "50")]
	delay_ms: u64,

	/// Queue depth of the bottleneck in ms at the current rate, before tail drop.
	#[usage(long, default = "200")]
	queue_ms: u64,

	/// Subscriber max age in ms, sent to the relay with both subscriptions.
	#[usage(long, default = "10000")]
	max_age_ms: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let config = Config::parse();
	config.log.init()?;
	let start = Instant::now();

	let phases = phases(&config.rates, &config.secs)?;
	let link = Arc::new(Link::new(phases[0].0, config.delay_ms, config.queue_ms));
	let relay_addr = tokio::net::lookup_host((
		config.relay.host_str().context("relay URL has no host")?,
		config.relay.port().unwrap_or(443),
	))
	.await?
	.find(SocketAddr::is_ipv4)
	.context("relay host has no IPv4 address")?;
	let listen = UdpSocket::bind("127.0.0.1:0").await?;
	let mut via_link = config.relay.clone();
	via_link.set_port(Some(listen.local_addr()?.port())).ok();
	tokio::spawn(run_link(link.clone(), listen, relay_addr));

	let client = config.client.clone().init(Default::default())?;
	let broadcast_name = format!("squeeze-{}", std::process::id());

	// Publisher: straight to the relay.
	let pub_origin = moq_tokio::origin::spawn();
	let broadcast = pub_origin
		.create_broadcast(&broadcast_name)
		.context("failed to create broadcast")?;
	for (name, _priority) in TRACKS {
		let track = broadcast.create_track(name, None)?;
		tokio::spawn(publish(track, config.keyframe_size, config.frame_size, start));
	}
	broadcast
		.announce(Default::default())
		.context("failed to announce broadcast")?;
	let publishing = client.clone().with_publisher(&pub_origin).connect(config.relay.clone());

	// Subscriber: through the bottleneck, its own origin so nothing is served locally.
	let sub_origin = moq_tokio::origin::spawn();
	let subscribing = client.with_subscriber(sub_origin.clone()).connect(via_link);
	let stats: Vec<Arc<TrackStats>> = TRACKS.iter().map(|_| Arc::default()).collect();
	let subscribe = subscribe(
		sub_origin,
		broadcast_name,
		Duration::from_millis(config.max_age_ms),
		stats.clone(),
		start,
	);

	let report = report(&phases, &link, &stats, start);
	tokio::select! {
		res = publishing.closed() => res.context("publisher session closed")?,
		res = subscribing.closed() => res.context("subscriber session closed")?,
		res = subscribe => res.context("subscriber failed")?,
		() = report => {}
	}

	broadcast.finish();
	Ok(())
}

/// Pairs up `--rates` and `--secs` into the three (kbit/s, seconds) phases.
fn phases(rates: &str, secs: &str) -> anyhow::Result<Vec<(u64, u64)>> {
	let parse = |list: &str| {
		list.split(',')
			.map(|v| v.trim().parse::<u64>())
			.collect::<Result<Vec<_>, _>>()
	};
	let (rates, secs) = (
		parse(rates).context("invalid --rates")?,
		parse(secs).context("invalid --secs")?,
	);
	anyhow::ensure!(
		rates.len() == 3 && secs.len() == 3,
		"--rates and --secs take three values each"
	);
	anyhow::ensure!(rates.iter().all(|&rate| rate > 0), "--rates must be positive");
	Ok(rates.into_iter().zip(secs).collect())
}

/// Writes `FPS` frames per second in 1 s groups, the first of each group large
/// like a keyframe. Each payload starts with the send time in ms since `start`,
/// so the subscriber can tell how stale what it receives is.
async fn publish(track: moq_net::track::Producer, keyframe_size: usize, frame_size: usize, start: Instant) {
	let mut ticker = tokio::time::interval(Duration::from_micros(1_000_000 / FPS));
	let payloads = [vec![0u8; keyframe_size.max(8)], vec![0u8; frame_size.max(8)]];

	let mut sequence = 0u64;
	loop {
		let mut group = track.create_group(sequence.into()).expect("failed to create group");
		sequence += 1;
		for frame in 0..FPS {
			ticker.tick().await;
			let mut payload = payloads[usize::from(frame > 0)].clone();
			payload[..8].copy_from_slice(&(start.elapsed().as_millis() as u64).to_be_bytes());
			group
				.write_frame(moq_net::Timestamp::now(), payload)
				.expect("failed to write frame");
		}
		group.finish().expect("failed to finish group");
	}
}

#[derive(Default)]
struct TrackStats {
	frames: AtomicU64,
	newest_sent_ms: AtomicU64,
}

async fn subscribe(
	origin: moq_net::origin::Producer,
	broadcast_name: String,
	max_age: Duration,
	stats: Vec<Arc<TrackStats>>,
	start: Instant,
) -> anyhow::Result<()> {
	let scope = moq_net::Patterns::from(moq_net::Pattern::subtree(&broadcast_name).context("invalid broadcast name")?);
	let consumer = origin
		.scope("", &scope)
		.context("not allowed to consume broadcast")?
		.consume();
	let mut announced = consumer.announced();

	while let Some(update) = announced.next().await {
		if !update.kind.is_active() {
			continue;
		}
		let broadcast = consumer.request_broadcast(&update.prefix).await?;

		for ((name, priority), stats) in TRACKS.into_iter().zip(stats.iter().cloned()) {
			let subscription = moq_net::track::Subscription::default()
				.with_priority(priority)
				.with_max_age(max_age);
			let mut track = broadcast.track(name)?.subscribe(subscription).await?;

			tokio::spawn(async move {
				// Groups are read concurrently: a stalled old group must not hide newer ones.
				while let Ok(Some(mut group)) = track.recv_group().await {
					let stats = stats.clone();
					tokio::spawn(async move {
						while let Ok(Some(frame)) = group.read_frame().await {
							let sent_ms = u64::from_be_bytes(frame.payload[..8].try_into().unwrap());
							stats.frames.fetch_add(1, Ordering::Relaxed);
							stats.newest_sent_ms.fetch_max(sent_ms, Ordering::Relaxed);
						}
					});
				}
				tracing::warn!(track = name, "track ended");
			});
		}
		tracing::info!(elapsed = ?start.elapsed(), "subscribed to both tracks");
	}

	anyhow::bail!("announcements ended")
}

/// Steps the link through the phases, printing one line per second, then a verdict.
async fn report(phases: &[(u64, u64)], link: &Link, stats: &[Arc<TrackStats>], start: Instant) {
	println!(
		"{:>4} {:>5} | {:>9} {:>5} {:>8} | {:>8} {:>8} | {:>8} {:>8}",
		"t", "link", "forwarded", "drops", "queue ms", "high fps", "age s", "low fps", "age s"
	);

	let mut ticker = tokio::time::interval(Duration::from_secs(1));
	ticker.tick().await;
	let mut t = 0;
	let mut tail = Vec::new(); // (forwarded kbit/s, high fps, low fps) over the recovered phase

	for (phase, &(rate, secs)) in phases.iter().enumerate() {
		link.rate_kbps.store(rate, Ordering::Relaxed);

		for _ in 0..secs {
			ticker.tick().await;
			t += 1;
			let forwarded = link.forwarded_bytes.swap(0, Ordering::Relaxed) * 8 / 1000;
			let drops = link.dropped.swap(0, Ordering::Relaxed);
			let queue_ms = link.queue_bytes_max.swap(0, Ordering::Relaxed) * 8 / rate;
			let now_ms = start.elapsed().as_millis() as u64;

			let tracks: Vec<(u64, String)> = stats
				.iter()
				.map(|track| {
					let frames = track.frames.swap(0, Ordering::Relaxed);
					let age = match track.newest_sent_ms.load(Ordering::Relaxed) {
						0 => "-".to_string(),
						sent => format!("{:.1}", now_ms.saturating_sub(sent) as f64 / 1000.0),
					};
					(frames, age)
				})
				.collect();

			println!(
				"{t:>4} {rate:>5} | {forwarded:>9} {drops:>5} {queue_ms:>8} | {:>8} {:>8} | {:>8} {:>8}",
				tracks[0].0, tracks[0].1, tracks[1].0, tracks[1].1
			);

			if phase == 2 {
				tail.push((forwarded, tracks[0].0, tracks[1].0));
			}
		}
	}

	// Skip the first 10 s after the step up: a healthy sender needs a moment too.
	let settled = &tail[tail.len().min(10)..];
	if settled.is_empty() {
		return;
	}
	let mean = |pick: fn(&(u64, u64, u64)) -> u64| settled.iter().map(pick).sum::<u64>() as f64 / settled.len() as f64;
	let (forwarded, high, low) = (mean(|s| s.0), mean(|s| s.1), mean(|s| s.2));
	let healthy = FPS as f64 * 0.9;
	println!(
		"\nrecovered phase after 10 s: link {} kbit/s, forwarded {forwarded:.0} kbit/s, high {high:.1} fps, low {low:.1} fps (publisher sends {FPS})",
		phases[2].0
	);
	println!(
		"verdict: {}",
		if high < healthy {
			"inconclusive, the high-priority track did not play either (is the relay still up?)"
		} else if low >= healthy {
			"low track recovered"
		} else {
			"low track still starved although the link has room"
		}
	);
}

/// The bottleneck's knobs and counters.
struct Link {
	rate_kbps: AtomicU64,
	delay: Duration,
	queue_ms: u64,
	forwarded_bytes: AtomicU64,
	dropped: AtomicU64,
	queue_bytes_max: AtomicU64,
}

impl Link {
	fn new(rate_kbps: u64, delay_ms: u64, queue_ms: u64) -> Self {
		Self {
			rate_kbps: AtomicU64::new(rate_kbps),
			delay: Duration::from_millis(delay_ms),
			queue_ms,
			forwarded_bytes: AtomicU64::new(0),
			dropped: AtomicU64::new(0),
			queue_bytes_max: AtomicU64::new(0),
		}
	}

	fn bytes_per_sec(&self) -> f64 {
		self.rate_kbps.load(Ordering::Relaxed) as f64 * 125.0
	}
}

type Packet = (SocketAddr, Vec<u8>);

/// Plain datagram forwarding is transparent to QUIC. Uplink packets are only
/// delayed; downlink packets go through the shaper, then the same delay.
async fn run_link(link: Arc<Link>, listen: UdpSocket, relay: SocketAddr) {
	let listen = Arc::new(listen);
	let (shaper_tx, shaper_rx) = mpsc::unbounded_channel::<Packet>();
	let (down_tx, down_rx) = mpsc::unbounded_channel::<(Instant, Packet)>();
	tokio::spawn(shape(link.clone(), shaper_rx, down_tx));
	tokio::spawn(delay_line(down_rx, {
		let listen = listen.clone();
		move |(client, data): Packet| {
			let listen = listen.clone();
			async move {
				listen.send_to(&data, client).await.ok();
			}
		}
	}));

	let mut upstreams: HashMap<SocketAddr, mpsc::UnboundedSender<(Instant, Packet)>> = HashMap::new();
	let mut buf = vec![0u8; 65_536];

	loop {
		let Ok((len, client)) = listen.recv_from(&mut buf).await else {
			continue;
		};

		let up_tx = upstreams.entry(client).or_insert_with(|| {
			// One socket towards the relay per client, so replies map back to it.
			let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("failed to bind");
			socket.set_nonblocking(true).expect("failed to set nonblocking");
			socket.connect(relay).expect("failed to connect");
			let socket = Arc::new(UdpSocket::from_std(socket).expect("failed to register socket"));

			let (up_tx, up_rx) = mpsc::unbounded_channel();
			tokio::spawn(delay_line(up_rx, {
				let socket = socket.clone();
				move |(_client, data): Packet| {
					let socket = socket.clone();
					async move {
						socket.send(&data).await.ok();
					}
				}
			}));

			let shaper_tx = shaper_tx.clone();
			tokio::spawn(async move {
				let mut buf = vec![0u8; 65_536];
				while let Ok(len) = socket.recv(&mut buf).await {
					if shaper_tx.send((client, buf[..len].to_vec())).is_err() {
						break;
					}
				}
			});

			up_tx
		});

		up_tx
			.send((Instant::now() + link.delay, (client, buf[..len].to_vec())))
			.ok();
	}
}

/// Releases packets in order once their deadline passes.
async fn delay_line<F, Fut>(mut rx: mpsc::UnboundedReceiver<(Instant, Packet)>, send: F)
where
	F: Fn(Packet) -> Fut,
	Fut: Future<Output = ()>,
{
	while let Some((deadline, packet)) = rx.recv().await {
		tokio::time::sleep_until(deadline).await;
		send(packet).await;
	}
}

/// Token bucket draining a bounded FIFO; a packet that does not fit is dropped.
async fn shape(link: Arc<Link>, mut rx: mpsc::UnboundedReceiver<Packet>, tx: mpsc::UnboundedSender<(Instant, Packet)>) {
	let mut queue: VecDeque<Packet> = VecDeque::new();
	let mut queue_bytes = 0u64;
	let mut tokens = 0f64;
	let mut refilled = Instant::now();

	loop {
		let rate = link.bytes_per_sec();
		let burst = (3.0 * MTU as f64).max(rate * 0.020);
		let now = Instant::now();
		tokens = (tokens + (now - refilled).as_secs_f64() * rate).min(burst);
		refilled = now;

		while let Some((_, data)) = queue.front() {
			if tokens < data.len() as f64 {
				break;
			}
			let packet = queue.pop_front().unwrap();
			tokens -= packet.1.len() as f64;
			queue_bytes -= packet.1.len() as u64;
			link.forwarded_bytes.fetch_add(packet.1.len() as u64, Ordering::Relaxed);
			tx.send((now + link.delay, packet)).ok();
		}

		let wait = match queue.front() {
			Some((_, data)) => Duration::from_secs_f64(((data.len() as f64 - tokens) / rate).max(0.001)),
			None => Duration::from_secs(3600),
		};

		tokio::select! {
			packet = rx.recv() => {
				let Some(packet) = packet else { return };
				let cap = (8 * MTU).max((rate * link.queue_ms as f64 / 1000.0) as u64);
				if queue_bytes + packet.1.len() as u64 > cap {
					link.dropped.fetch_add(1, Ordering::Relaxed);
				} else {
					queue_bytes += packet.1.len() as u64;
					link.queue_bytes_max.fetch_max(queue_bytes, Ordering::Relaxed);
					queue.push_back(packet);
				}
			}
			() = tokio::time::sleep(wait) => {}
		}
	}
}
