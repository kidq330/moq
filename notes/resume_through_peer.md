# A viewer resuming mid-group through a forwarding peer leaves the next viewer with nothing

A moq-net peer forwards a track with one group (a catalog, say). A viewer
reads the group, drops its subscription and subscribes again on the same
session; it is served. A second viewer then subscribes and is served
nothing, with no error.

- Only `moq-lite-06` and later, the versions with `Frame Start` on the wire.
- Needs the forwarding peer: viewers connected straight to the publisher
  are served.
- Not when the first viewer only drops its subscription, and not related
  to the cache's idle expiry (fails with the default cache, no waiting).

The test, on mocked time, fails on `moq-dev/moq` main @ `5124f8134`; its
control (the first viewer leaves without resubscribing) passes:

```
cargo test -p moq-net --test resume_through_peer
```

## Mechanism

Traced with debug prints on SUBSCRIBE and SUBSCRIBE_START
(`rs/moq-net/src/lite/subscriber.rs`) and on `TrackRun::start` and `serve`
(`rs/moq-net/src/lite/publisher.rs`):

- The first viewer's resubscription asks for group 0 from frame 1, since it
  already holds frame 0.
- The peer's upstream subscription ended with the first one, so it sends a
  new SUBSCRIBE upstream with the same bounds, and the publisher serves
  group 0 from frame 1.
- The second viewer asks for the whole track. The peer sends it
  SUBSCRIBE_START for group 0 and serves group 0 from frame 0, and the
  viewer never receives a group.

Inferred, not observed: the peer's shared copy of group 0 now starts at
frame 1, and serving it from frame 0 waits for a frame that never comes.

The moq-lite draft's Positions section says a partial group is only
delivered to a subscriber that asked for one, and a publisher that cannot
serve a group from the requested frame skips it and resolves to a later
group; here the peer promises group 0 and delivers nothing.

NOTE: this document is LLM-generated (Written by Claude Opus 5.5), 2026-09-30.
