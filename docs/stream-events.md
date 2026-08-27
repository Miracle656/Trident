# StreamEvents

The gRPC `StreamEvents` RPC is the internal transport for live events: the Rust
gRPC server consumes the Redis stream the indexer writes and pushes typed
`Event` messages to one subscriber (issue #236). It is not exposed to clients
directly — the Go layer fans it out over SSE and WebSocket.

## Request

| Field         | Meaning                                                       |
| ------------- | ------------------------------------------------------------- |
| `contract_id` | Required. Only events from this contract are pushed.           |
| `topic_0`     | Optional. Only events whose first topic matches are pushed.    |
| `start_id`    | Optional resume point. Empty means live tail.                  |

## Resuming

`start_id` is a Redis stream entry ID — `<millis>-<seq>` — normally the last ID
the client received. On reconnect the server replays from there instead of
dropping everything published while the client was away, which mirrors the SSE
`Last-Event-ID` contract.

Two special values are accepted: `$` (live tail, the default) and `0` (replay
everything still retained in the stream). Anything else is rejected with
`InvalidArgument`; passing a malformed ID through to Redis would surface as an
opaque connection error to the subscriber instead of a clear one.

Replay depth is bounded by `REDIS_STREAM_MAXLEN` on the indexer side. A client
offline longer than that window resumes from the oldest retained entry, so the
stream is not a durable log — historical gaps are filled through `ListEvents`.

### How long can a client be disconnected? (retention window in wall-clock terms)

`REDIS_STREAM_MAXLEN` (default `10000`, see `docs/ENVIRONMENT.md`) is a count
of entries, not a duration — how much wall-clock disconnect time it buys
depends entirely on how fast events are being appended to `trident:events`
across *all* contracts (the stream is shared; it isn't partitioned per
contract). The two are related by:

```
retention_window_seconds ≈ REDIS_STREAM_MAXLEN / events_per_second
```

Rearranged, given the configured `REDIS_STREAM_MAXLEN` and an observed or
expected sustained event rate, "you can be disconnected for approximately X
minutes" is:

```
X_minutes ≈ REDIS_STREAM_MAXLEN / (events_per_second * 60)
```

Worked examples at the default `REDIS_STREAM_MAXLEN=10000`:

| Sustained event rate | Approx. retention window |
| --------------------- | ------------------------- |
| 1 event/s              | ~166 minutes (~2.8 hours) |
| 10 events/s            | ~16.7 minutes             |
| 100 events/s           | ~1.7 minutes              |

The current sustained rate can be read from the indexer's `/metrics` (events
appended over a recent time window) or estimated from `ListEvents` volume for
the tracked contracts. Because the bound is on entry *count*, a burst of
activity (e.g. many contracts minting at once) shrinks the effective window
for everyone until it passes — size `REDIS_STREAM_MAXLEN` for the busiest
sustained rate you expect, not the average, if a longer guaranteed
reconnection window matters more than Redis memory footprint. A client that
reconnects with a `Last-Event-ID` older than the window receives the `gap`
SSE event/`StreamEvents` behavior described above and resumes from the oldest
retained entry rather than silently losing data.

## Cancellation

When a client disconnects, tonic drops the stream, which drops the receiving
half of the channel. The consumer races its blocking `XREAD` against the channel
closing, so it returns immediately rather than staying parked for the remainder
of the 5-second block window. No task outlives its subscriber.

## Backpressure

Each subscriber has a bounded buffer, `STREAM_CHANNEL_BUFFER` (default 128).
When it fills, the consumer blocks on send and stops reading Redis for that
subscriber. That is the intended behaviour: a slow client throttles its own
stream instead of making the server queue events without limit. Other
subscribers are unaffected — each has its own consumer and buffer.
