# Frames lost when an upstream GOAWAY lands mid-group

NOTE: LLM-generated (Written by Claude Opus 5), 2026-09-22; updated (Written by
Claude Opus 5.5) 2026-09-30. Inspired by model finding F18
(`../../model/findings.html#f18`), the draft's GOAWAY recipe losing what the
subscriber had not read yet.

**Status:** live, unfiled. Re-checked 2026-09-30 on `upstream/main` @ 5124f8134:
the lite cases fail, 3 of 3 runs. PR #4491 (resume-latest) at 6c0833495
fixes the case where the publisher is still on group 1, but not the one where
it has moved on before the cluster routes to B; see "Timing cases" below. Earlier checks: 9d2a4f6e9 (2026-09-29), b234a38c0 (2026-09-28),
79d846f80.

Repro: branch `kidq330/bug/goaway_mid_group_loss`:

```
cargo test -p moq-relay --test goaway_mid_group -- --nocapture --test-threads 1
```

The test binds its clients to `127.0.0.1:0`, so it runs without IPv6.

The moq-net mock repro is `rs/moq-net/tests/goaway_mid_group.rs` (lite-05/06,
paused clock): it stalls on main and passes with #4491.

## Setup

Real TCP, as in `goaway_cluster.rs`, but every endpoint pinned to one version
per round. The relay's cluster dials sibling A. Group 0 is delivered; group 1's
first frame is delivered; then A sends a GOAWAY redirecting to B, and 1/b, 1/c
and group 2 are written after B accepted. B either serves the same in-process
origin as A (warm) or is a relay that pulls from a publisher server on demand
(cold). Expected: `0/a 0/b 0/<end> 1/a 1/b 1/c 1/<end> 2/a 2/<end>`.

## Observed @ 5124f8134 (3 runs per cell)

| version | warm B | cold B |
|---|---|---|
| moq-lite-05, 06, 07-wip | `1/a`, then group 1 stalls: 1/b, 1/c never delivered (every run) | same |
| moq-transport-20, 21 | every frame | every frame |
| moq-transport-17, 19 | every frame | group 1 fails with `Old` after 1/b; 1/c lost (every run for 17, most runs for 19) |
| moq-transport-14 | every frame, then the subscription fails with `transport: closed` | same |

The existing `goaway_cluster.rs` tests migrate only at group boundaries and do
not pin a version (so they run moq-lite). `quest/m1/drain/client-goaway.md`
also assumes live tracks hand over at a group boundary.

## Timing cases (2026-09-30, lite-05/06/07-wip, warm B)

`Case` in the test picks whether the publisher has started group 2 before the
reader asks for the rest of group 1 (`moved_on`), and whether the reader first
waits for the cluster to route to B (`b_routed`). Without that wait the reader
asks while only the draining A is routed: after an immediate redirect
moq-tokio holds the dial to B back by 0.5 to 1 s.

| moved_on | b_routed | main | #4491 @ 6c0833495 |
|---|---|---|---|
| no | no | stalls | every frame |
| yes | no | stalls | stalls |
| yes | yes | every frame | every frame |

A fixed 1.5 s sleep in place of the route wait (not in the test) gave
`1/<not found>` with #4491 and a stall on main, while A was still inside its
2 s handover and held 1/b and 1/c.

## Where to look

- lite: the stall is a group-only reader parking after failover. When the test
  polls `sub.recv_group()` before reading group 1, lite-05 delivers every frame
  (warm and cold). That is the mechanism of `quest/m1/resume-latest.md`
  (#4365, #4392): a resumed `resume::Group` waits on the new copy but only
  `resume::Subscriber::apply`, driven by `recv_group`, subscribes to it. PR
  #4491 implements that quest but does not cover this path (a cluster route
  moving to a remote copy).
- moq-transport-17/19 cold: `ietf/subscriber.rs` `subscribe_join`; a frame
  boundary is refused before draft 20, so the joining FETCH/stitch loses the
  tail of the group (`Old`).
- moq-transport-14: the subscription ends when session A closes instead of
  staying on B.
