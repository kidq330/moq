# A forwarding peer stops serving a newcomer after its only group idles

A moq-net peer forwards a track with one group (a catalog, say) to two
viewers, who stay subscribed. Nothing new is written. Once the group has
been idle past the peer's cache expiry, a third viewer's subscription is
accepted and served nothing, for as long as no new group is written.

- The peer is any moq-net origin fed by one session and served over others,
  `moq-relay` included. Viewers connected straight to the publisher are
  served.
- Only `moq-lite-05` and later, the versions with TRACK_INFO.
- Not with one viewer before the newcomer, nor before the expiry. A
  `max_age` on the newcomer's subscription does not help.

The test, on mocked time, fails on `moq-dev/moq` main @ `5124f8134`; its
control (no idle wait) passes:

```
cargo test -p moq-net --test newcomer_after_expiry
```

## Mechanism

Traced with debug prints on `run_front` (`rs/moq-net/src/model/origin.rs`)
and on `close_cache` and the expiry paths (`rs/moq-net/src/model/track.rs`):

- At the peer each viewer gets a front of its own: fronts are keyed by the
  requester's split-horizon exclusion, and each viewer's session is a
  different hop. All of them share one upstream copy of the track, which
  holds the group as its latest, protected from idle expiry.
- A viewer's TRACK_INFO query makes its front's track used and then unused
  before its SUBSCRIBE arrives, so the front parks: `warm_copy` builds a
  local warm track and `adopt_group`s the group onto it. Adopting shares the
  `group::Producer`; nothing is copied.
- The SUBSCRIBE makes the front splice the upstream copy back in, dropping
  the `WarmCopy`: its track finishes and `close_cache` runs, which puts the
  shared group into that track's eviction order (a closed track protects
  nothing).
- Once idle for the expiry (30 s in `moq-relay`, where with the 15 s sweeps
  it fires about a minute after the last read), the pool's sweep reaches
  the closed warm track and `expire_closed` aborts the group with
  `Error::Old`. The group's state is shared, so that aborts it in the
  upstream copy too, although the upstream copy still protects it.
  Protection is per track, the abort is per group.
- A later viewer's front gets the upstream copy with its only group aborted
  and waits for one that never comes. Viewers subscribed since before keep
  reading, since they already had the group.

NOTE: this document is LLM-generated (Written by Claude Fable 5.1 and Claude Opus 5.5), 2026-09-30.
