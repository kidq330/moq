# A re-created group sequence reaches a subscriber that already received it (catch-up resubscribe on a congested link)

Found by moq_fuzz (finding #1 in `moq_fuzz_findings.md`, where this text lived
until 2026-09-21). Paths like `runs/...` and `mix moq_fuzz...` are relative to
`../../moq_fuzz/`.

**Status:** live and unfiled upstream. Re-checked 2026-09-30 on `upstream/main` @ 5124f8134
(branch squashed to one commit and rebased cleanly): moq-mux test fails with
`TimestampRewind` after 58 frames, group 5 once and complete; relay test fails in
2, 12 and 3 of 20 rounds over three runs (duplicates of group 4 or 5, then
`TimestampRewind`). `fix.diff` still applies and passes the moq-mux test. The relay
test's config now pins `[connect] bind` to IPv4 so the relay starts on a host with
no IPv6. Code refs still hold: `track.rs` `claim_sequence`, `consumer.rs`
`poll_read_finish`, `resume.rs` `let start = state.resume_position()`. No upstream
issue or PR matches; nearest are #4365 (open, different cause, below) and #2761
(closed, a duplicate group after a GOAWAY drain).

**moq-net-level repro, 2026-09-30** (`rs/moq-tokio/tests/resubscribe_duplicate_group.rs`,
no moq-mux, no relay binary): publisher -> middle -> client, qmux over TCP on
127.0.0.1, the middle node a bare origin on its own runtime thread accepting both
sessions. Asserts `recv_group()` on the second subscription never yields a sequence
twice. 50 rounds, about 50 s; 4 of 150 rounds hit, 3 of 3 runs failed, e.g.
`[5, 4, 5, 4, 6]`. What narrowed it:
- Over QUIC it never hit (middle origin 40 rounds, relay binary 20). TCP is needed.
- Pointed at the relay binary over TCP, the same assertion hits at a similar rate
  (6 of 80), so the relay adds nothing beyond a forwarding origin. Mirroring the
  relay's 30 s cache config on the middle origin changed nothing.
- The client must keep the handed-over group handles alive; dropping each one at
  once never shows the second copy.
- On the mock transport (`rs/moq-net/tests/support`), neither a direct session nor a
  publisher -> origin -> client chain reproduced it at any of 0 to 39 scheduler
  yields between drop and resubscribe. The mock delivers a stream reset as soon as
  the peer's task runs; making it deterministic would need a way to hold a reset
  (or the SUBSCRIBE cancel) behind later traffic, which the mock lacks.
- Source of the second copy: the middle node's `lite::subscriber` `GroupRecv` calls
  `create_group` for every group stream, and `claim_sequence` accepts an aborted
  sequence. The decided bullet in `quest/m1/subscribe-drop.md` ("A dropped or
  aborted group is visible to readers...") is one fix direction; a reader-side
  dedupe is the other.

Re-checked 2026-09-29 on `origin/main` @ 9d2a4f6e9 (branch rebased to eee91fb94, two commits): moq-mux test fails with `TimestampRewind` after 58 frames, relay test fails in 9 of 20 rounds. Reproduced reliably on the network (7/7
runs of the reduced scenario) and root-caused; a deterministic, network-free
test fails on moq-dev `origin/main` @ `a93fdb8e8` (2026-09-21). Re-checked 2026-09-28 on `origin/main` @ b234a38c0
(branch rebased to 3fdddf339, `moq-mux` and `hang` dev-dependencies re-added
over main's list): still fails at the same assert.

**The symptom changed between d518b61b4 and a93fdb8e8.** At `d518b61b4`
(2026-09-18) the application got the re-created group in full and then the
stale incarnation's buffered frames again ("10 frames delivered twice:
[(5, 0) ... (5, 9)]"). Since #3711 ("timelines only move forward") the
consumer checks each frame against the group edge, so the stale incarnation's
first frame now fails `read()` with `TimestampRewind`: no duplicate reaches the
application, but the subscription ends there and every later group is lost
(in the test: 58 frames delivered, group 5 once and complete, then the error;
group 6 never arrives). Both layers of the root cause below are unchanged.

**Branch as of 2026-09-29:** one commit, 004398489 on `origin/main` @ 4ad27e16e, only `rs/moq-mux/tests/recreated_group_duplicate.rs` (prose reworded as the invariant), no dependency change. The relay probe left the branch and is `resubscribe_duplicate_group.relay_test.diff`. Issue draft: `resubscribe_duplicate_group.issue.md`. Not pushed. The description below is the earlier branch.

**Through a relay on `main` @ 785ee4c8f (2026-09-29, later the same day):** the
bug reproduces on the wire. `rs/moq-relay/tests/resubscribe_duplicate_group.rs`
(second commit on the branch) unsubscribes and resubscribes at once, both at
group 4, through the relay binary on loopback over TCP. 5 of 5 runs fail, 30 of
100 rounds: the second subscription hands group 5's first 10 frames (up to 16)
to the application twice, then `read()` fails with `TimestampRewind`. With
`resubscribe_duplicate_group.fix.diff` applied: 0 duplicates and no read error
in 20 rounds, but 28 or 48 frames never arrive in most rounds (the prototype's
known limit). Traced: the relay serves the new subscription its cached groups 5
and 4, reopens the upstream SUBSCRIBE at start 4, receives both groups again
from frame 0 and serves them a second time under the same sequences.

What changed since the probe showed nothing (`7abf18101`): #4387 (e713c3099,
"resolve a relayed subscription's start from its source"). Before it, the
relay answered SUBSCRIBE_START with the first group to reach it (5, the newer
stream wins) and group 4 was dropped even on a first subscription. That was
the unexplained part; it is fixed.

**Two other observables on the same probe, not this bug** (raw reads through
the relay binary, `resubscribe_start_narrowed.relay_probe.diff`, which also
carries the temporary tracing; mock version in
`resubscribe_start_narrowed.mock_probe.diff`):

1. *The relay narrows a resubscribe to its own resume position.* Resubscribe
   after a 500 ms pause, or with a changed start: the client sends SUBSCRIBE
   start (4,0), the relay sends upstream start (5,10), one past what it had
   cached (`resume.rs` takeover, `let start = state.resume_position()`), and
   answers SUBSCRIBE_START 5. Group 4 never arrives although the publisher
   holds it open, even when the first subscription had been reading it. 3 of 3
   on the relay, deterministic on the mock transport with two sessions
   (lite-05 and 06); one session without a relay delivers group 4. In one
   round the second subscription was also handed group 5 twice.
2. *A start lowered on a live subscription does not reach back.* First
   subscription at group 5, dropped and resubscribed at group 4 in the same
   instant: the client sends no new SUBSCRIBE but a SUBSCRIBE_UPDATE with
   start 4, the relay forwards it, the publisher receives it and never serves
   group 4. 15 of 15 rounds. The moq-mux consumer then hands over nothing at
   all (90 of 90 frames), groups 5 and 6 included, consistent with it waiting
   on group 4 inside its 4 s budget (read from the code). `doc/lib/rs/moq-net.md`
   says of the local limit that "a lower start does not rewind the reader", so
   this one is probably intended at the publisher; the total stall at the
   consumer is the part an application sees.

Neither is judged or filed.

**Repro in the moq clone** (`../../moq-dev`, branch
`kidq330/bug/resubscribe_duplicate_group`, two commits on `origin/main` @
`9d2a4f6e9`: a46414d0c the moq-mux test, eee91fb94 the relay test; not pushed; formerly `kidq330/resubscribe_duplicate_group` on
`main` @ `d518b61b4`):

- `rs/moq-mux/tests/recreated_group_duplicate.rs`: deterministic, model
  only, paused clock. `cargo test -p moq-mux --test recreated_group_duplicate`
  fails with "read failed with Some("TimestampRewind(TimestampRewind)") after
  58 frames". The test fails on either symptom.
- `rs/moq-relay/tests/resubscribe_duplicate_group.rs`: 20 rounds of
  unsubscribe + immediate resubscribe through the relay binary on loopback
  (`cargo test -p moq-relay --test resubscribe_duplicate_group`, about 20 s).
  Not deterministic: about 3 rounds in 10 fail, so a run fails almost always.
  On the branch at the user's request, as proof the wire produces the abort
  and re-create. Adds `moq-mux` and `hang` as dev-dependencies of moq-relay.
  `resubscribe_duplicate_group.relay_test.diff` is the same commit as a patch.

## Relation to upstream #4365 and #4392 (checked 2026-09-29)

Not the same cause. Both of those are a group in flight whose read never
ends after its track is aborted; the maintainer's quest PR #4412 puts both at
one line in `model/resume.rs` (a resumed group waits on the new copy's cache
and never subscribes to it) and plans `resume-latest` as the fix, in
`resume.rs` only. #4365 also only shows on an origin-routed (spliced) track
and re-creates the *track* under the same name, with a new group sequence.

This bug re-creates a *group sequence* inside one track, and the test reads a
plain broadcast track with no origin route, so no splice and no resumed group
are involved. Its two layers are `track.rs` (an aborted sequence can be
claimed again and is a new arrival) and `moq_mux::container::Consumer` (no
equal-sequence handling). `resume-latest` touches neither. Shared theme only:
abort, then re-create. Worth one "Related:" line in the report, no more.

## How local a fix is (prototyped 2026-09-29, `main` @ 9e2b686d0)

The test still fails there on its own assertion (`TimestampRewind` after 58
frames). `resubscribe_duplicate_group.fix.diff` is an 11-line change in
`moq_mux::container::Consumer::poll_read_finish`, one file: when the track
hands over a sequence that is already in `pending`, the new incarnation
replaces the old one if nothing was played from it, and is dropped otherwise.
With it the test passes and so does the moq-mux suite (869 + 12 + 1).

Limits of the prototype: a group already partly played keeps its aborted
incarnation, so the rest of that group is lost (no duplicate, no error);
doing better needs a frame skip on the replacement. The model layer is left
as is: `track.rs` hands the replacement to a subscriber that already got the
first incarnation, which is arguably right (the first was aborted), so every
other consumer of `recv_group` still sees both. moq-net has its own test for
the late-subscriber case (`recreated_sequence_delivered_once`, #2526).

## Root cause (two layers)
1. `moq_net::model::track`: `create_group` refuses a sequence that is still
   cached but accepts it again once the cached incarnation is *aborted*
   (`claim_sequence`), and the new incarnation gets a fresh arrival entry, so
   an arrival-order subscriber (`recv_group`) that was already handed the
   first incarnation is handed the second as well. On the wire this happens
   on unsubscribe + resubscribe: `lite::subscriber::TrackServe` cancels the
   upstream SUBSCRIBE but keeps the track's producer; the new model
   subscriber starts at arrival index 0 and picks up the still-live group
   from the cache; the relay's reset of the old subscription's stream aborts
   it; the relay serves the group again on the new subscription.
2. `moq_mux::container::Consumer`: `poll_read_finish` only drops
   `sequence < current`, and inserts by `partition_point(sequence < new)`, so
   while the cursor is held on an earlier group both incarnations sit in
   `pending`, the new one in front. The read loop plays the new one in full,
   advances `current`, then finds the old incarnation at the front with
   `sequence <= current` and drains its buffered frames.

That is exactly the network trace: the group in full, then its first 10-12
frames again in one burst. Congestion is what holds the cursor on the earlier
group (the catch-up groups arrive slowly) and delays the relay's reset long
enough for the new subscriber to attach to the live group first.

## What the wire probe showed at d518b61b4

Two more faces of the same thing: resubscribing at the same start loses the
rest of both in-flight groups (48 frames) whenever the upstream SUBSCRIBE is cancelled rather than coalesced (4 of 10 such
rounds in one run, 0 of 4 in another),
and in the catch-up shape the relay's own debug log shows
`serving group subscribe=1 sequence=5` twice on one subscription while
group 4 is never served and the client delivers nothing (every such round).
So the relay, which runs the same model as a subscriber of the publisher,
can put the duplicate on the wire itself when its last downstream
subscriber leaves and returns.

Same class as the July MRE in `../../membrane_moq_plugin/.reference/moq-mre`
(`ISSUE.md`: a fetch refilling an age-evicted sequence was re-delivered),
which #2526 closed by keeping fetched backfill out of the arrival order. A sequence re-created
through `create_group` after an *abort* is still a new visible arrival, and
the consumer still has no equal-sequence dedup. That note's "relays do not
double-serve downstream" no longer holds at HEAD: the lite publisher serves in
arrival order and the probe shows it serving one sequence twice.

## The network run that found it

**Setup:** moq-relay 0.14.15 (moq-dev `9e2054aea`), ex_moq clients
(moq-native 0.19.15 / hang 0.20.9), moq-lite.

**Reproduction:** `runs/seed-105-reduced.json` (from seed 105 of the first
campaign, reduced by `mix moq_fuzz.reduce`):

- two 400 kbit/s tracks; `video2` at 30 fps with 1 s groups;
- link 2000 kbit/s, 50 ms one-way delay, 1% loss, 50 ms queue (tail-dropping);
- one subscriber (a second, idle session was kept by the reducer; it may not
  matter) subscribes `video1` at priority 80 and `video2` at priority 60 with
  a 1 s latency budget, then 300 ms later unsubscribes `video2` and, in the
  same instant, resubscribes `video2` with `group_start = latest − 2` and a
  4 s latency budget.

`mix moq_fuzz.run --replay runs/seed-105-reduced.json`

**Observed:** the new `video2` subscription receives the group that was live
at the time of the switch twice: once in full (30 objects) and once more,
truncated (10–12 objects, arriving in one burst), before the next group
continues. Every object is a genuine one from the ledger; nothing is
corrupt or out of order otherwise.

**What the relay did:** with `--log-level debug`, the relay's
`serving group subscribe=<id> track=video2 sequence=5` lines show group 5
served once on the old subscription (id 2) and once on the new one (id 3):
one stream per subscription, as expected. The relay served each group once
per subscription.

**So the duplicate happens in the client:** the old subscription's in-flight
group stream and the new subscription's stream for the same group both land
in the client's shared per-track model, and the new subscription's consumer
reads both. `moq_net::model::track::create_group` rejects a duplicate
*live* sequence but re-admits an *evicted* one ("an evicted sequence can be
re-created; a live one is a duplicate"), which fits: the old copy of the
group was evicted (1 s latency budget, tail drops) before the new stream
re-created it. The same model code runs inside the relay when it is the
subscriber of an upstream relay, so a relay chain could presumably do the
same to its downstream.

**Not reproduced on a clean link**, nor with a plain unsubscribe+resubscribe
at the live edge on a 450 kbit/s link, nor with `group_start = latest − 2`
on that link: it needs the old group to be evicted while its stream is still
delivering, which the lossy, tail-dropping 2000 kbit/s link provides.

**Open:** which layer should own the fix. Candidates: the consumer replaces
a pending incarnation of the same sequence instead of queueing both (and
skips frames it already delivered from it); or the model does not hand a
re-created sequence to a subscriber that already received it; or
`TrackServe` aborts the cached in-flight groups before a new subscriber can
attach when it cancels upstream. The wire probe's catch-up case (group 4
never served after `SubscribeStart { group: 4 }`) is not analysed yet.
Ready to report upstream to moq-dev/moq with the two tests.

**Also seen without a catch-up join:** seed 202 (`runs/seed-202.json`) shows
the same duplicate on a plain unsubscribe + resubscribe of the same track at
the live edge, 1000 kbit/s link, subscriber far behind (median lag ~10 s).
The truncated second copy arrives in one burst 600 ms after the first.
