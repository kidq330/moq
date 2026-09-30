<!-- Draft, 2026-09-30. Not filed. Branch: kidq330/bug/resubscribe_duplicate_group on kidq330/moq.
     Title: -->

# A resubscribe through a forwarding node hands the client the same group sequence twice

## Summary

A client unsubscribes from a track and subscribes again at once, through a node that forwards it (a relay, or any origin serving one session from another), while the publisher has groups in flight. The new subscription's `recv_group()` yields the same sequence twice: first the copy the middle node had cached, cut short when it cancelled its upstream SUBSCRIBE, then the full copy the publisher serves on the new upstream SUBSCRIBE.

## Expected

Each group sequence is handed to a subscription once.

## Observed

On `main` @ 5124f8134. The repro is moq-net only, with no media layer: publisher -> middle -> client, three qmux sessions over TCP on 127.0.0.1, all in one test process. The middle node is a bare `origin` that accepts both sessions. The publisher creates group 4 (two frames) and group 5 (ten frames) and leaves both open. The client subscribes from group 4, drops the subscription, subscribes again at once, and records the sequence of every group `recv_group()` hands over.

```
git fetch https://github.com/kidq330/moq kidq330/bug/resubscribe_duplicate_group
git checkout FETCH_HEAD
cargo test -p moq-tokio --test resubscribe_duplicate_group
```

```
2 of 50 rounds handed a group sequence twice; first is round 15: [5, 4, 5, 4, 6]
```

It depends on timing: 4 of 150 rounds, and 3 of 3 runs failed (about 50 s each). The same test pointed at the `moq-relay` binary over TCP gives a similar rate (6 of 80 rounds). Over QUIC, with the middle node or the relay binary, it has not happened in 60 rounds.

A media reader sees it as duplicate frames and then an error. `moq_mux::container::Consumer` plays the full copy, then the stale one's frames, and `read()` fails with `TimestampRewind`, ending the subscription. The same branch carries two tests for that. `cargo test -p moq-mux --test recreated_group_duplicate` drives the model directly, deterministic on a paused clock. `cargo test -p moq-relay --test resubscribe_duplicate_group` goes through the relay binary; 2 to 12 of 20 rounds fail.

## Where the second copy comes from

The middle node's subscriber session creates a group for every incoming group stream (`lite::subscriber` `GroupRecv`, `track::Producer::create_group`). When its upstream SUBSCRIBE is cancelled, the publisher resets the in-flight group streams, and the middle node's cached copies of groups 4 and 5 are aborted. When the new SUBSCRIBE arrives, the publisher serves 4 and 5 again from frame 0. `create_group` refuses a sequence that is still live, but `claim_sequence` in `model/track.rs` accepts an aborted one as a new arrival. So the middle node serves it again on the client's subscription, which already had the first copy.

A direct publisher -> client session on the mock transport did not reproduce it at any interleaving tried. Reading the code, the likely reason is that there the new subscription resolves only after the publisher answers it, when the old copies are already aborted, while a forwarding node answers from its own cache first.

## Possible directions

- The model: an aborted group stays visible instead of releasing its sequence. `quest/m1/subscribe-drop.md` already decides "A dropped or aborted group is visible to readers, not silently skipped", noting that #4533 found the Rust model releases an aborted group's sequence.
- The reader: `recv_group` or its consumers dedupe a sequence they have already been handed.

## Context

Related but a different cause: #4365 and #4392 are a read that never ends after a track is aborted. Here the track stays up and a group sequence is handed over twice.

(Written by Claude Opus 5.5)
