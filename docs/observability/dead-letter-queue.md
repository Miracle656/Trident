# Dead-letter queues: parse_errors and failed_events

Two tables exist so a single bad or unlucky event can never wedge the whole
poll loop: `parse_errors` (migration 0008) for events that failed to
**decode**, and `failed_events` (migration 0027) for events that decoded
fine but repeatedly failed to **persist** (issue #208).

## Why two tables, not one

- `parse_errors` — the event never became a `SorobanEvent`. XDR decoding
  failed, so there is nothing structured to store beyond the raw payload and
  the parse error. Retrying never helps; this is a poison message.
- `failed_events` — the event *did* decode into a normalised `SorobanEvent`,
  but its `INSERT` into `soroban_events` kept failing (an unexpected
  constraint violation, or an outage that outlasted the retry budget). The
  full normalised event is stored as JSONB so it can be replayed once the
  underlying cause is understood, without re-fetching it from Stellar RPC.

## How an event ends up in failed_events

`streamer::commit_page_with_fallback` (`crates/indexer/src/streamer/mod.rs`)
tries every page as one atomic transaction first — the fast path, and what
keeps events, their outbox rows, and the cursor advance atomic (issue #199).
If that fails after a bounded retry (3 attempts, 200 ms–2 s exponential
backoff), it falls back to committing each event in the page individually,
each with its own bounded retry (3 attempts, 100 ms–1 s backoff). An event
that still cannot be persisted after that is written to `failed_events` and
skipped; the page's cursor and ledger metadata still commit once every event
in it has been either persisted or dead-lettered, so **the cursor always
advances** — a poison row can delay a page but never blocks it indefinitely.

This also structurally distinguishes transient from permanent failures
without having to parse database error strings: an event that fails when
batched with the rest of the page but succeeds on its own was never the
problem — something else in that transaction was, and isolating it recovers
the rest of the page automatically. An event that keeps failing even alone is
the one that's actually broken.

## Metrics

| Metric | Type | Meaning |
|---|---|---|
| `trident_indexer_dead_lettered_total` | counter | Items written to a dead-letter table — `parse_errors` (decode failures, issue #414) or `failed_events` (persist failures, issue #208). Only incremented once the row is durably written, so it never counts an event that was actually lost. |

## Alerting

A healthy indexer keeps `trident_indexer_dead_lettered_total` flat. Any
increase is worth paging on — unlike lag, which recovers on its own, a
dead-lettered event needs a human to look at `failed_events`/`parse_errors`
and decide whether to fix and replay it:

```yaml
- alert: TridentEventsDeadLettered
  expr: increase(trident_indexer_dead_lettered_total[15m]) > 0
  annotations:
    summary: "Events written to a dead-letter table — inspect parse_errors / failed_events"
```

## Inspecting and replaying failed_events

Pending (not yet replayed) rows, oldest first:

```sql
SELECT id, ledger_sequence, contract_id, transaction_hash, error_message, attempts, occurred_at
FROM failed_events
WHERE replayed_at IS NULL
ORDER BY occurred_at
LIMIT 100;
```

`event_payload` holds the full normalised `SorobanEvent` (the same shape
`insert_events_batch` writes to `soroban_events`) as JSONB, so once the root
cause is fixed a row can be replayed by re-inserting it and marking it done:

```sql
-- Re-derive the columns insert_events_batch expects from event_payload,
-- insert (idempotent — the natural key/PK make a repeat insert a no-op),
-- then mark it replayed:
UPDATE failed_events SET replayed_at = NOW() WHERE id = '<id>';
```

There is deliberately no automated replay job — a `failed_events` row means
something about that specific event or that moment surprised the schema or
the database, and an operator should look at `error_message` before deciding
whether to replay as-is, patch the schema, or discard it.
