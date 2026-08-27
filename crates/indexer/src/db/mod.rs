use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use trident_common::{EventType, SorobanEvent, TridentError};
use uuid::Uuid;

use crate::parser::token_events::TokenEvent;

/// Build a bounded Postgres connection pool sized for this service.
///
/// `statement_cache_capacity(0)` disables sqlx's named-prepared-statement cache.
/// This is mandatory when connecting through PgBouncer in transaction pooling
/// mode: a cached prepared statement is bound to one server connection, but in
/// transaction mode the next transaction may be routed to a different server
/// connection where that statement does not exist, which makes the query fail.
/// See docs/deployment.md (issue #87).
#[allow(dead_code)]
pub async fn connect_pool(database_url: &str, pool_size: u32) -> Result<PgPool, TridentError> {
    let connect_options = PgConnectOptions::from_str(database_url)
        .map_err(|e| TridentError::config(anyhow::Error::new(e).context("invalid DATABASE_URL")))?
        .statement_cache_capacity(0);

    PgPoolOptions::new()
        .max_connections(pool_size)
        .connect_with(connect_options)
        .await
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("connect_pool")))
}

// Stable namespace for deterministic event UUIDs (UUIDv5).
// Using the DNS namespace is arbitrary; what matters is that it is fixed.
const EVENT_NS: Uuid = Uuid::NAMESPACE_DNS;

/// Derive a deterministic UUID for an event from its natural key.
/// Using the same inputs will always produce the same UUID, so duplicate
/// events produce the same (ledger_sequence, id) pair and `ON CONFLICT
/// (ledger_sequence, id) DO NOTHING` fires.
fn event_uuid(contract_id: &str, ledger_sequence: u64, event_index: u32) -> Uuid {
    let key = format!("{contract_id}:{ledger_sequence}:{event_index}");
    Uuid::new_v5(&EVENT_NS, key.as_bytes())
}

pub mod outbox;

/// Ledger provenance recorded alongside a committed page.
pub struct LedgerMeta<'a> {
    pub sequence: u64,
    pub hash: &'a str,
    pub timestamp: &'a str,
    pub event_count: i32,
}

/// Everything one RPC page contributes to the database, committed atomically.
///
/// Bundling the events with the cursor advance is what stops a crash between
/// the two from leaving the cursor ahead of the data it claims to cover.
/// A decoded token event paired with the indexed event it projects (issue #211).
pub struct TokenProjection<'a> {
    pub event: &'a SorobanEvent,
    pub token: &'a TokenEvent,
}

/// One (contract, transaction) invocation metrics row, ready to persist into
/// `contract_invocation_metrics` (issue #266).
pub struct InvocationMetricRow<'a> {
    pub contract_id: &'a str,
    pub transaction_hash: &'a str,
    pub ledger_sequence: u64,
    /// ISO 8601 UTC timestamp of the ledger close.
    pub ledger_timestamp: &'a str,
    pub metrics: &'a crate::parser::invocation_metrics::InvocationMetrics,
}

pub struct PageCommit<'a> {
    pub events: &'a [SorobanEvent],
    /// Normalised token-event rows for this page. Every referenced event must
    /// also appear in `events` — the projection is foreign-keyed to it.
    pub token_events: &'a [TokenProjection<'a>],
    /// Per-invocation fee + declared-resource metering for tracked contracts
    /// in this page (issue #266). Empty in index-all mode — metering is
    /// bounded to the allowlist.
    pub invocation_metrics: &'a [InvocationMetricRow<'a>],
    /// Contract storage snapshot changes observed in this page (issue #270).
    /// Empty unless a tracked contract was detected as a SEP-41 token and one
    /// of its holders moved funds in this page.
    pub storage_snapshots: &'a [StorageSnapshotRow<'a>],
    /// Network these storage snapshots belong to (empty string when
    /// `storage_snapshots` is empty).
    pub network: &'a str,
    /// New cursor value, when the page advanced it.
    pub cursor: Option<u64>,
    /// Ledger metadata row, written only when the cursor advanced.
    pub ledger: Option<LedgerMeta<'a>>,
    /// Maximum rows per INSERT statement, bounding statement size and memory.
    pub batch_size: usize,
}

/// Columns of a batch of events, transposed into the parallel arrays that the
/// `UNNEST` insert binds. Building this once avoids re-deriving per row.
struct EventColumns {
    ids: Vec<Uuid>,
    contract_ids: Vec<String>,
    ledger_sequences: Vec<i64>,
    ledger_timestamps: Vec<DateTime<Utc>>,
    transaction_hashes: Vec<String>,
    event_indexes: Vec<i32>,
    event_types: Vec<String>,
    topics: Vec<serde_json::Value>,
    data: Vec<serde_json::Value>,
}

fn event_type_str(event_type: &EventType) -> &'static str {
    match event_type {
        EventType::Contract => "contract",
        EventType::System => "system",
        EventType::Diagnostic => "diagnostic",
    }
}

impl EventColumns {
    fn build(events: &[SorobanEvent]) -> Result<Self, TridentError> {
        let mut cols = EventColumns {
            ids: Vec::with_capacity(events.len()),
            contract_ids: Vec::with_capacity(events.len()),
            ledger_sequences: Vec::with_capacity(events.len()),
            ledger_timestamps: Vec::with_capacity(events.len()),
            transaction_hashes: Vec::with_capacity(events.len()),
            event_indexes: Vec::with_capacity(events.len()),
            event_types: Vec::with_capacity(events.len()),
            topics: Vec::with_capacity(events.len()),
            data: Vec::with_capacity(events.len()),
        };

        for event in events {
            let ledger_ts: DateTime<Utc> = event.ledger_timestamp.parse().map_err(|e| {
                TridentError::storage(anyhow::Error::new(e).context("ledger timestamp parse"))
            })?;
            let topics = serde_json::to_value(&event.topics).map_err(|e| {
                TridentError::storage(anyhow::Error::new(e).context("topics serialise"))
            })?;

            cols.ids.push(event_uuid(
                &event.contract_id,
                event.ledger_sequence,
                event.event_index,
            ));
            cols.contract_ids.push(event.contract_id.clone());
            cols.ledger_sequences.push(event.ledger_sequence as i64);
            cols.ledger_timestamps.push(ledger_ts);
            cols.transaction_hashes.push(event.transaction_hash.clone());
            cols.event_indexes.push(event.event_index as i32);
            cols.event_types
                .push(event_type_str(&event.event_type).to_string());
            cols.topics.push(topics);
            cols.data.push(event.data.clone());
        }

        Ok(cols)
    }
}

/// Insert a batch of events in a single statement (issue #199).
///
/// One round-trip per batch instead of one per row. Duplicate handling is
/// unchanged in spirit: the deterministic UUIDv5 id plus `ON CONFLICT
/// (ledger_sequence, id) DO NOTHING` means a replayed page inserts nothing
/// new. The target is the full (ledger_sequence, id) pair, not just id, since
/// migration 0017 made soroban_events RANGE-partitioned by ledger_sequence —
/// PostgreSQL requires every unique constraint on a partitioned table to
/// include the partition key, so a single-column PK on id alone no longer
/// exists to match against. ledger_sequence is itself part of the input
/// event_uuid() derives id from, so a replayed page always reproduces the
/// same (ledger_sequence, id) pair — the idempotency guarantee is unchanged.
pub async fn insert_events_batch<'e, E>(
    executor: E,
    events: &[SorobanEvent],
) -> Result<(), TridentError>
where
    E: sqlx::PgExecutor<'e>,
{
    if events.is_empty() {
        return Ok(());
    }

    let cols = EventColumns::build(events)?;

    sqlx::query(
        r#"
        INSERT INTO soroban_events
            (id, contract_id, ledger_sequence, ledger_timestamp, transaction_hash,
             event_index, event_type, topics, data)
        SELECT * FROM UNNEST(
            $1::uuid[], $2::text[], $3::bigint[], $4::timestamptz[], $5::text[],
            $6::int[], $7::text[], $8::jsonb[], $9::jsonb[]
        )
        ON CONFLICT (ledger_sequence, id) DO NOTHING
        "#,
    )
    .bind(&cols.ids)
    .bind(&cols.contract_ids)
    .bind(&cols.ledger_sequences)
    .bind(&cols.ledger_timestamps)
    .bind(&cols.transaction_hashes)
    .bind(&cols.event_indexes)
    .bind(&cols.event_types)
    .bind(&cols.topics)
    .bind(&cols.data)
    .execute(executor)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("insert_events_batch")))?;

    Ok(())
}

/// Insert a batch of decoded token events into the `token_events` projection
/// (issue #211).
///
/// Keyed by the originating event's UUID, so replaying a page re-projects the
/// same rows and `ON CONFLICT DO NOTHING` absorbs them.
pub async fn insert_token_events_batch<'e, E>(
    executor: E,
    projections: &[TokenProjection<'_>],
) -> Result<(), TridentError>
where
    E: sqlx::PgExecutor<'e>,
{
    if projections.is_empty() {
        return Ok(());
    }

    let mut event_ids = Vec::with_capacity(projections.len());
    let mut contract_ids = Vec::with_capacity(projections.len());
    let mut event_types = Vec::with_capacity(projections.len());
    let mut from_addresses = Vec::with_capacity(projections.len());
    let mut to_addresses = Vec::with_capacity(projections.len());
    let mut spender_addresses = Vec::with_capacity(projections.len());
    let mut admin_addresses = Vec::with_capacity(projections.len());
    let mut amounts = Vec::with_capacity(projections.len());
    let mut expiration_ledgers = Vec::with_capacity(projections.len());
    let mut asset_codes = Vec::with_capacity(projections.len());
    let mut asset_issuers = Vec::with_capacity(projections.len());
    let mut ledger_sequences = Vec::with_capacity(projections.len());
    let mut ledger_timestamps = Vec::with_capacity(projections.len());
    let mut transaction_hashes = Vec::with_capacity(projections.len());
    let mut event_indexes = Vec::with_capacity(projections.len());

    for projection in projections {
        let event = projection.event;
        let token = projection.token;
        let ledger_ts: DateTime<Utc> = event.ledger_timestamp.parse().map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("ledger timestamp parse"))
        })?;

        event_ids.push(event_uuid(
            &event.contract_id,
            event.ledger_sequence,
            event.event_index,
        ));
        contract_ids.push(event.contract_id.clone());
        event_types.push(token.event_type.as_str().to_string());
        from_addresses.push(token.from.clone());
        to_addresses.push(token.to.clone());
        spender_addresses.push(token.spender.clone());
        admin_addresses.push(token.admin.clone());
        amounts.push(token.amount.clone());
        expiration_ledgers.push(token.expiration_ledger);
        asset_codes.push(token.asset_code.clone());
        asset_issuers.push(token.asset_issuer.clone());
        ledger_sequences.push(event.ledger_sequence as i64);
        ledger_timestamps.push(ledger_ts);
        transaction_hashes.push(event.transaction_hash.clone());
        event_indexes.push(event.event_index as i32);
    }

    sqlx::query(
        r#"
        INSERT INTO token_events
            (event_id, contract_id, event_type, from_address, to_address,
             spender_address, admin_address, amount, expiration_ledger,
             asset_code, asset_issuer,
             ledger_sequence, ledger_timestamp, transaction_hash, event_index)
        SELECT * FROM UNNEST(
            $1::uuid[], $2::text[], $3::text[], $4::text[], $5::text[],
            $6::text[], $7::text[], $8::text[], $9::bigint[],
            $10::text[], $11::text[],
            $12::bigint[], $13::timestamptz[], $14::text[], $15::int[]
        )
        ON CONFLICT (event_id) DO NOTHING
        "#,
    )
    .bind(&event_ids)
    .bind(&contract_ids)
    .bind(&event_types)
    .bind(&from_addresses)
    .bind(&to_addresses)
    .bind(&spender_addresses)
    .bind(&admin_addresses)
    .bind(&amounts)
    .bind(&expiration_ledgers)
    .bind(&asset_codes)
    .bind(&asset_issuers)
    .bind(&ledger_sequences)
    .bind(&ledger_timestamps)
    .bind(&transaction_hashes)
    .bind(&event_indexes)
    .execute(executor)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("insert_token_events_batch"))
    })?;

    Ok(())
}

/// Insert a batch of per-invocation fee/resource metering rows into
/// `contract_invocation_metrics` (issue #266).
///
/// Keyed by `(contract_id, transaction_hash)`; a replayed page inserts
/// nothing new via `ON CONFLICT DO NOTHING`, matching the idempotency of the
/// other page-scoped inserts.
pub async fn insert_invocation_metrics_batch<'e, E>(
    executor: E,
    rows: &[InvocationMetricRow<'_>],
) -> Result<(), TridentError>
where
    E: sqlx::PgExecutor<'e>,
{
    if rows.is_empty() {
        return Ok(());
    }

    let mut contract_ids = Vec::with_capacity(rows.len());
    let mut transaction_hashes = Vec::with_capacity(rows.len());
    let mut ledger_sequences = Vec::with_capacity(rows.len());
    let mut ledger_timestamps = Vec::with_capacity(rows.len());
    let mut fee_charged = Vec::with_capacity(rows.len());
    let mut resource_fee = Vec::with_capacity(rows.len());
    let mut cpu_instructions = Vec::with_capacity(rows.len());
    let mut read_bytes = Vec::with_capacity(rows.len());
    let mut write_bytes = Vec::with_capacity(rows.len());
    let mut provenance = Vec::with_capacity(rows.len());

    for row in rows {
        let ledger_ts: DateTime<Utc> = row.ledger_timestamp.parse().map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("ledger timestamp parse"))
        })?;

        contract_ids.push(row.contract_id.to_string());
        transaction_hashes.push(row.transaction_hash.to_string());
        ledger_sequences.push(row.ledger_sequence as i64);
        ledger_timestamps.push(ledger_ts);
        fee_charged.push(row.metrics.fee_charged);
        resource_fee.push(row.metrics.resource_fee);
        cpu_instructions.push(row.metrics.cpu_instructions);
        read_bytes.push(row.metrics.read_bytes);
        write_bytes.push(row.metrics.write_bytes);
        provenance.push(row.metrics.provenance.to_string());
    }

    sqlx::query(
        r#"
        INSERT INTO contract_invocation_metrics
            (contract_id, transaction_hash, ledger_sequence, ledger_timestamp,
             fee_charged, resource_fee, cpu_instructions, read_bytes, write_bytes, provenance)
        SELECT * FROM UNNEST(
            $1::text[], $2::text[], $3::bigint[], $4::timestamptz[],
            $5::bigint[], $6::bigint[], $7::bigint[], $8::bigint[], $9::bigint[], $10::text[]
        )
        ON CONFLICT (contract_id, transaction_hash) DO NOTHING
        "#,
    )
    .bind(&contract_ids)
    .bind(&transaction_hashes)
    .bind(&ledger_sequences)
    .bind(&ledger_timestamps)
    .bind(&fee_charged)
    .bind(&resource_fee)
    .bind(&cpu_instructions)
    .bind(&read_bytes)
    .bind(&write_bytes)
    .bind(&provenance)
    .execute(executor)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("insert_invocation_metrics_batch"))
    })?;

    Ok(())
}

/// Insert the outbox rows for a batch of events in a single statement (#200).
///
/// Mirrors [`insert_events_batch`]: same deterministic ids, same idempotency.
/// `ON CONFLICT (event_id) DO NOTHING` means a replayed page does not re-queue
/// a delivery for an event that already has one.
pub async fn insert_outbox_batch<'e, E>(
    executor: E,
    events: &[SorobanEvent],
) -> Result<(), TridentError>
where
    E: sqlx::PgExecutor<'e>,
{
    if events.is_empty() {
        return Ok(());
    }

    let mut ids: Vec<Uuid> = Vec::with_capacity(events.len());
    let mut payloads: Vec<serde_json::Value> = Vec::with_capacity(events.len());
    for event in events {
        ids.push(event_uuid(
            &event.contract_id,
            event.ledger_sequence,
            event.event_index,
        ));
        payloads.push(serde_json::to_value(event).map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("outbox payload serialise"))
        })?);
    }

    sqlx::query(
        r#"
        INSERT INTO event_outbox (event_id, payload)
        SELECT * FROM UNNEST($1::uuid[], $2::jsonb[])
        ON CONFLICT (event_id) DO NOTHING
        "#,
    )
    .bind(&ids)
    .bind(&payloads)
    .execute(executor)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("insert_outbox_batch")))?;

    Ok(())
}

/// Persist (or refresh) a tracked contract's parsed spec + detected
/// interfaces, keyed by `(contract_id, network)` (issues #260, #269).
/// Called only when the observed code hash differs from the last one synced,
/// so a redeploy refreshes the row instead of leaving it stale.
pub async fn upsert_contract_spec(
    pool: &PgPool,
    contract_id: &str,
    network: &str,
    spec: &crate::spec::ContractSpec,
) -> Result<(), TridentError> {
    let functions = serde_json::to_value(&spec.functions).map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("serialise spec functions"))
    })?;
    let interfaces = serde_json::to_value(&spec.interfaces).map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("serialise spec interfaces"))
    })?;

    sqlx::query(
        r#"
        INSERT INTO contract_specs
            (contract_id, network, code_hash, has_spec, functions, contract_type, interfaces)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::jsonb)
        ON CONFLICT (contract_id, network) DO UPDATE SET
            code_hash     = EXCLUDED.code_hash,
            has_spec      = EXCLUDED.has_spec,
            functions     = EXCLUDED.functions,
            contract_type = EXCLUDED.contract_type,
            interfaces    = EXCLUDED.interfaces,
            updated_at    = NOW()
        "#,
    )
    .bind(contract_id)
    .bind(network)
    .bind(&spec.code_hash)
    .bind(spec.has_spec)
    .bind(functions)
    .bind(&spec.contract_type)
    .bind(interfaces)
    .execute(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("upsert_contract_spec")))?;

    Ok(())
}

/// Latest persisted value for a single contract-storage key, if any (issue
/// #270). Used to detect whether a freshly observed value has changed before
/// writing a new snapshot row.
pub async fn get_latest_storage_value(
    pool: &PgPool,
    contract_id: &str,
    network: &str,
    storage_key: &str,
) -> Result<Option<serde_json::Value>, TridentError> {
    let row: Option<(Option<serde_json::Value>,)> = sqlx::query_as(
        r#"
        SELECT value_json FROM contract_storage_snapshots
        WHERE contract_id = $1 AND network = $2 AND storage_key = $3
        ORDER BY ledger_sequence DESC
        LIMIT 1
        "#,
    )
    .bind(contract_id)
    .bind(network)
    .bind(storage_key)
    .fetch_optional(pool)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("get_latest_storage_value"))
    })?;

    Ok(row.and_then(|(v,)| v))
}

/// One contract-storage snapshot row, ready to persist (issue #270).
pub struct StorageSnapshotRow<'a> {
    pub contract_id: &'a str,
    pub storage_key: &'a str,
    pub key_json: &'a serde_json::Value,
    pub value_json: Option<&'a serde_json::Value>,
    pub ledger_sequence: u64,
}

/// Insert a batch of contract-storage snapshot changes (issue #270).
/// Callers are expected to have already diffed against
/// [`get_latest_storage_value`] — this only appends, it never overwrites a
/// prior snapshot, so historical values stay queryable.
pub async fn insert_storage_snapshots_batch<'e, E>(
    executor: E,
    network: &str,
    rows: &[StorageSnapshotRow<'_>],
) -> Result<(), TridentError>
where
    E: sqlx::PgExecutor<'e>,
{
    if rows.is_empty() {
        return Ok(());
    }

    let mut contract_ids = Vec::with_capacity(rows.len());
    let mut networks = Vec::with_capacity(rows.len());
    let mut storage_keys = Vec::with_capacity(rows.len());
    let mut key_jsons = Vec::with_capacity(rows.len());
    let mut value_jsons: Vec<Option<serde_json::Value>> = Vec::with_capacity(rows.len());
    let mut ledger_sequences = Vec::with_capacity(rows.len());

    for row in rows {
        contract_ids.push(row.contract_id.to_string());
        networks.push(network.to_string());
        storage_keys.push(row.storage_key.to_string());
        key_jsons.push(row.key_json.clone());
        value_jsons.push(row.value_json.cloned());
        ledger_sequences.push(row.ledger_sequence as i64);
    }

    sqlx::query(
        r#"
        INSERT INTO contract_storage_snapshots
            (contract_id, network, storage_key, key_json, value_json, ledger_sequence)
        SELECT * FROM UNNEST(
            $1::text[], $2::text[], $3::text[], $4::jsonb[], $5::jsonb[], $6::bigint[]
        )
        ON CONFLICT (contract_id, network, storage_key, ledger_sequence) DO NOTHING
        "#,
    )
    .bind(&contract_ids)
    .bind(&networks)
    .bind(&storage_keys)
    .bind(&key_jsons)
    .bind(&value_jsons)
    .bind(&ledger_sequences)
    .execute(executor)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("insert_storage_snapshots_batch"))
    })?;

    Ok(())
}

/// Persist one RPC page — events, outbox rows, cursor, and ledger metadata — in
/// a single transaction (issues #199, #200).
///
/// Events are chunked to `batch_size` so a very large page cannot produce an
/// unbounded statement, but every chunk shares the one transaction: either the
/// whole page and its cursor advance land, or none of it does.
pub async fn commit_page(pool: &PgPool, commit: PageCommit<'_>) -> Result<(), TridentError> {
    let batch_size = commit.batch_size.max(1);

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("commit_page begin")))?;

    for chunk in commit.events.chunks(batch_size) {
        insert_events_batch(&mut *tx, chunk).await?;
        // Outbox rows ride the same transaction as the events they deliver
        // (issue #200): either both land or neither does, so a committed event
        // can never exist without a delivery record for the relay to pick up.
        insert_outbox_batch(&mut *tx, chunk).await?;
    }

    // token_events.event_id logically references soroban_events(id) (the DB-level
    // FK was dropped in migration 0017 — soroban_events is partitioned, so a
    // single-column UNIQUE (id) can't be enforced globally). Referential
    // integrity is instead upheld here: projection rows must follow the event
    // insert inside the same transaction, so a token_events row can never exist
    // without its corresponding soroban_events row already committed.
    for chunk in commit.token_events.chunks(batch_size) {
        insert_token_events_batch(&mut *tx, chunk).await?;
    }

    // Invocation metrics ride the same transaction as the page they were
    // derived from (issue #266), same idempotency contract as the rest.
    for chunk in commit.invocation_metrics.chunks(batch_size) {
        insert_invocation_metrics_batch(&mut *tx, chunk).await?;
    }

    // Storage snapshot changes ride the same transaction as the page that
    // observed them (issue #270).
    for chunk in commit.storage_snapshots.chunks(batch_size) {
        insert_storage_snapshots_batch(&mut *tx, commit.network, chunk).await?;
    }

    if let Some(cursor) = commit.cursor {
        sqlx::query(
            "UPDATE system_state SET value = $1, updated_at = NOW() WHERE key = 'latest_ledger_cursor'",
        )
        .bind(cursor.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("commit_page set_cursor")))?;
    }

    if let Some(ledger) = commit.ledger {
        let ts: DateTime<Utc> = ledger.timestamp.parse().map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("ledger timestamp parse"))
        })?;

        sqlx::query(
            r#"
            INSERT INTO ledger_metadata (ledger_sequence, ledger_hash, ledger_timestamp, event_count)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (ledger_sequence) DO NOTHING
            "#,
        )
        .bind(ledger.sequence as i64)
        .bind(ledger.hash)
        .bind(ts)
        .bind(ledger.event_count)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("commit_page ledger_metadata"))
        })?;
    }

    tx.commit()
        .await
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("commit_page commit")))?;

    Ok(())
}

/// Read the latest processed ledger cursor from system_state.
pub async fn get_cursor(pool: &PgPool) -> Result<u64, TridentError> {
    let row: (String,) =
        sqlx::query_as("SELECT value FROM system_state WHERE key = 'latest_ledger_cursor'")
            .fetch_one(pool)
            .await
            .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("get_cursor")))?;

    row.0
        .parse::<u64>()
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("cursor parse")))
}

/// Check whether the given ledger hash matches the stored hash for `sequence`.
///
/// Returns `Ok(true)` if the hashes match (or no stored hash exists),
/// `Ok(false)` if a reorg is detected (hash mismatch), or an error on DB failure.
pub async fn check_ledger_reorg(
    pool: &PgPool,
    sequence: u64,
    new_hash: &str,
) -> Result<bool, TridentError> {
    if new_hash.is_empty() {
        return Ok(true); // no hash to compare
    }
    let row: Option<(String,)> =
        sqlx::query_as("SELECT ledger_hash FROM ledger_metadata WHERE ledger_sequence = $1")
            .bind(sequence as i64)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                TridentError::storage(anyhow::Error::new(e).context("check_ledger_reorg"))
            })?;

    match row {
        Some((stored_hash,)) if !stored_hash.is_empty() && stored_hash != new_hash => Ok(false),
        _ => Ok(true),
    }
}

/// Rewind the cursor to `target` and delete all ledger metadata and events
/// at sequences > `target`. Used to undo a reorg.
pub async fn rewind_cursor(pool: &PgPool, target: u64) -> Result<(), TridentError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("rewind_cursor begin")))?;

    sqlx::query("DELETE FROM ledger_metadata WHERE ledger_sequence > $1")
        .bind(target as i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("rewind_cursor delete_metadata"))
        })?;

    // soroban_events, not `events` — there is no table by that name, so the
    // original statement here aborted the transaction and left the reorg
    // unrepaired while the logs reported it as handled.
    //
    // token_events references soroban_events(id) ON DELETE CASCADE, so it is
    // cleaned up by this delete. event_outbox keys on event_id with no FK, so
    // orphaned rows are removed explicitly below. The remaining ledger-keyed
    // tables have neither an FK nor a cascade and are deleted by sequence.
    sqlx::query(
        "DELETE FROM event_outbox WHERE event_id IN \
         (SELECT id FROM soroban_events WHERE ledger_sequence > $1)",
    )
    .bind(target as i64)
    .execute(&mut *tx)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("rewind_cursor delete_outbox"))
    })?;

    sqlx::query("DELETE FROM soroban_events WHERE ledger_sequence > $1")
        .bind(target as i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("rewind_cursor delete_events"))
        })?;

    for table in [
        "contract_invocation_metrics",
        "contract_storage_snapshots",
        "parse_errors",
    ] {
        sqlx::query(&format!("DELETE FROM {table} WHERE ledger_sequence > $1"))
            .bind(target as i64)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                TridentError::storage(
                    anyhow::Error::new(e).context(format!("rewind_cursor delete_{table}")),
                )
            })?;
    }

    sqlx::query(
        "UPDATE system_state SET value = $1, updated_at = NOW() WHERE key = 'latest_ledger_cursor'",
    )
    .bind(target.to_string())
    .execute(&mut *tx)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("rewind_cursor set_cursor"))
    })?;

    tx.commit().await.map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("rewind_cursor commit"))
    })?;

    Ok(())
}

/// Detect gaps (missing sequences) in the processed ledger range.
///
/// Returns a list of missing sequence numbers. The scan is bounded to
/// sequences between `from` (inclusive) and `to` (inclusive).
pub async fn detect_ledger_gaps(
    pool: &PgPool,
    from: u64,
    to: u64,
) -> Result<Vec<u64>, TridentError> {
    if from >= to {
        return Ok(vec![]);
    }
    let rows: Vec<(i64,)> = sqlx::query_as(
        r#"
        SELECT generate_series($1::bigint, $2::bigint) AS seq
        EXCEPT
        SELECT ledger_sequence FROM ledger_metadata
        WHERE ledger_sequence >= $1 AND ledger_sequence <= $2
        ORDER BY seq
        "#,
    )
    .bind(from as i64)
    .bind(to as i64)
    .fetch_all(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("detect_ledger_gaps")))?;

    Ok(rows.into_iter().map(|(s,)| s as u64).collect())
}

/// Write indexer health metrics into the `system_state` health columns after
/// every successful poll cycle (issue #62).
///
/// Uses a single `UPDATE` on the known cursor row so there is never a
/// duplicate-key issue and the write is O(1) regardless of table size.
pub async fn update_health_stats(
    pool: &PgPool,
    last_ledger: i64,
    events_in_poll: i32,
    poll_duration: Duration,
) -> Result<(), TridentError> {
    let poll_ms = poll_duration.as_millis().min(i32::MAX as u128) as i32;

    sqlx::query(
        r#"
        UPDATE system_state
        SET
            last_poll_at          = NOW(),
            last_ledger_indexed   = $1,
            events_in_last_poll   = $2,
            poll_duration_ms      = $3,
            events_indexed_total  = COALESCE(events_indexed_total, 0) + $2,
            updated_at            = NOW()
        WHERE key = 'latest_ledger_cursor'
        "#,
    )
    .bind(last_ledger)
    .bind(events_in_poll)
    .bind(poll_ms)
    .execute(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("update_health_stats")))?;

    Ok(())
}

/// Load all contract IDs and their `index_from` values from
/// `indexed_contracts` for the given network (or network-agnostic rows where
/// `network IS NULL`).
///
/// Returns an empty map if the table has no rows — the caller treats an empty
/// map as "index all contracts" (issue #47).
///
/// `index_from` (BIGINT, default 0, added in 0001_init.sql) is the first
/// ledger from which events for that contract should be indexed. Events below
/// it are skipped client-side, because `getEvents` carries a single
/// `startLedger` for the whole request and so cannot express a per-contract
/// boundary (issue #202).
pub async fn load_indexed_contracts(
    pool: &PgPool,
    network: &str,
) -> Result<HashMap<String, i64>, TridentError> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT contract_id, index_from FROM indexed_contracts WHERE network = $1 OR network IS NULL",
    )
    .bind(network)
    .fetch_all(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("load_indexed_contracts")))?;

    Ok(rows.into_iter().collect())
}

/// Read alert state (last_alert_at, alert_fired) from system_state (issue #75).
pub async fn get_alert_state(pool: &PgPool) -> Result<crate::alerting::AlertState, TridentError> {
    let row: (Option<chrono::DateTime<chrono::Utc>>, bool) = sqlx::query_as(
        "SELECT last_alert_at, alert_fired FROM system_state WHERE key = 'latest_ledger_cursor'",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("get_alert_state")))?;

    Ok(crate::alerting::AlertState {
        last_alert_at: row.0,
        alert_fired: row.1,
        rpc_degraded_fired: false,
        rpc_degraded_last_alert_at: None,
    })
}

/// Persist alert state back to system_state after an alerting evaluation (issue #75).
pub async fn set_alert_state(
    pool: &PgPool,
    state: &crate::alerting::AlertState,
) -> Result<(), TridentError> {
    sqlx::query(
        r#"
        UPDATE system_state
        SET last_alert_at = $1,
            alert_fired   = $2,
            updated_at    = NOW()
        WHERE key = 'latest_ledger_cursor'
        "#,
    )
    .bind(state.last_alert_at)
    .bind(state.alert_fired)
    .execute(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("set_alert_state")))?;

    Ok(())
}

/// Record a parse error to parse_errors table for auditing and potential replay.
pub async fn insert_parse_error(
    pool: &PgPool,
    ledger_sequence: u64,
    event_index: u32,
    raw_payload: &str,
    error_message: &str,
) -> Result<(), TridentError> {
    sqlx::query(
        r#"
        INSERT INTO parse_errors
            (ledger_sequence, event_index, raw_payload, error_message)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(ledger_sequence as i64)
    .bind(event_index as i32)
    .bind(raw_payload)
    .bind(error_message)
    .execute(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("insert_parse_error")))?;

    Ok(())
}

/// Contracts among `contract_ids` whose `token_metadata` row is still fresh
/// (resolved or refreshed since `cutoff`), for either a positive or a cached
/// negative ("not a token") result (issue #263). Contracts absent from this
/// set need a fresh resolution attempt.
pub async fn fresh_token_metadata_contract_ids(
    pool: &PgPool,
    contract_ids: &[String],
    network: &str,
    cutoff: DateTime<Utc>,
) -> Result<HashSet<String>, TridentError> {
    if contract_ids.is_empty() {
        return Ok(HashSet::new());
    }

    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT contract_id FROM token_metadata
         WHERE contract_id = ANY($1) AND network = $2 AND updated_at > $3",
    )
    .bind(contract_ids)
    .bind(network)
    .bind(cutoff)
    .fetch_all(pool)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("fresh_token_metadata_contract_ids"))
    })?;

    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// Cache a resolved (or negative) token metadata result for one contract
/// (issue #263). Re-resolving an already-cached contract refreshes the row in
/// place rather than duplicating it.
pub async fn upsert_token_metadata(
    pool: &PgPool,
    contract_id: &str,
    network: &str,
    resolution: &crate::token_metadata::TokenMetadataResolution,
) -> Result<(), TridentError> {
    let (name, symbol, decimals, is_token) = match resolution {
        crate::token_metadata::TokenMetadataResolution::Token(meta) => (
            Some(meta.name.as_str()),
            Some(meta.symbol.as_str()),
            Some(meta.decimals as i32),
            true,
        ),
        crate::token_metadata::TokenMetadataResolution::NotAToken => (None, None, None, false),
    };

    sqlx::query(
        r#"
        INSERT INTO token_metadata (contract_id, network, name, symbol, decimals, is_token)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (contract_id, network) DO UPDATE SET
            name       = EXCLUDED.name,
            symbol     = EXCLUDED.symbol,
            decimals   = EXCLUDED.decimals,
            is_token   = EXCLUDED.is_token,
            updated_at = NOW()
        "#,
    )
    .bind(contract_id)
    .bind(network)
    .bind(name)
    .bind(symbol)
    .bind(decimals)
    .bind(is_token)
    .execute(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("upsert_token_metadata")))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use trident_common::{EventType, SorobanEvent};

    fn make_event(contract_id: &str, ledger_sequence: u64, event_index: u32) -> SorobanEvent {
        SorobanEvent {
            contract_id: contract_id.to_string(),
            ledger_sequence,
            ledger_timestamp: "2024-01-01T00:00:00Z".to_string(),
            transaction_hash: "txhash_abc123".to_string(),
            event_index,
            event_type: EventType::Contract,
            topics: vec![],
            data: json!({}),
        }
    }

    /// Deterministic UUID: same inputs must produce the same id.
    #[test]
    fn event_uuid_is_deterministic() {
        let a = event_uuid("CABC", 100, 0);
        let b = event_uuid("CABC", 100, 0);
        assert_eq!(a, b);
    }

    /// Different natural keys must produce different UUIDs.
    #[test]
    fn event_uuid_varies_with_inputs() {
        let a = event_uuid("CABC", 100, 0);
        let b = event_uuid("CABC", 100, 1);
        assert_ne!(a, b);
    }

    /// Committing the same event twice must not error and the row count in
    /// `soroban_events` must remain 1.
    ///
    /// Uses the shared test database (TEST_DATABASE_URL) like the other
    /// integration tests; skips when it is not configured.
    #[tokio::test]
    async fn batch_insert_is_idempotent() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            // Hard-fail under the rust-integration CI job (REQUIRE_TEST_SERVICES)
            // so a misconfigured DB URL cannot silently skip and go green.
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let event = make_event("CABC_CONTRACT_001", 42, 0);

        // Isolate from other tests sharing the database.
        sqlx::query("DELETE FROM soroban_events WHERE contract_id = $1")
            .bind(&event.contract_id)
            .execute(&pool)
            .await
            .expect("cleanup failed");

        let events = [event.clone()];
        insert_events_batch(&pool, &events)
            .await
            .expect("first insert failed");
        insert_events_batch(&pool, &events)
            .await
            .expect("second insert must not error");

        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM soroban_events WHERE contract_id = $1")
                .bind(&event.contract_id)
                .fetch_one(&pool)
                .await
                .expect("count query failed");

        assert_eq!(count.0, 1, "duplicate insert should be silently ignored");
    }

    /// An empty batch must be a no-op rather than an invalid statement.
    #[tokio::test]
    async fn empty_batch_is_a_noop() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();
        insert_events_batch(&pool, &[])
            .await
            .expect("empty batch must succeed");
    }

    /// A page larger than `batch_size` must still land in full — chunking splits
    /// the statement, not the transaction — and a replay must insert nothing new.
    #[tokio::test]
    async fn commit_page_chunks_large_pages_and_advances_cursor_once() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_id = format!("CBATCH_{}", Uuid::new_v4());
        let events: Vec<SorobanEvent> = (0..25).map(|i| make_event(&contract_id, 900, i)).collect();

        fn commit(events: &[SorobanEvent]) -> PageCommit<'_> {
            PageCommit {
                events,
                token_events: &[],
                invocation_metrics: &[],
                storage_snapshots: &[],
                network: "testnet",
                cursor: Some(900),
                ledger: Some(LedgerMeta {
                    sequence: 900,
                    hash: "hash900",
                    timestamp: "2024-01-01T00:00:00Z",
                    event_count: events.len() as i32,
                }),
                // Deliberately smaller than the page so chunking is exercised.
                batch_size: 10,
            }
        }

        commit_page(&pool, commit(&events))
            .await
            .expect("commit_page failed");

        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM soroban_events WHERE contract_id = $1")
                .bind(&contract_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count.0, 25, "every chunk of the page must land");
        assert_eq!(get_cursor(&pool).await.unwrap(), 900);

        commit_page(&pool, commit(&events))
            .await
            .expect("replay failed");

        let recount: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM soroban_events WHERE contract_id = $1")
                .bind(&contract_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(recount.0, 25, "replaying a page must insert nothing new");

        sqlx::query("DELETE FROM soroban_events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// Upserting token metadata twice for the same (contract_id, network)
    /// must update the row in place, not duplicate it, and a resolved
    /// contract must count as fresh until the refresh interval elapses
    /// (issue #263).
    #[tokio::test]
    async fn token_metadata_upsert_refreshes_in_place() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_id = format!("CTOKENMETA_{}", Uuid::new_v4());
        let network = "testnet";

        sqlx::query("DELETE FROM token_metadata WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();

        // Not yet resolved: absent from the fresh set.
        let fresh = fresh_token_metadata_contract_ids(
            &pool,
            std::slice::from_ref(&contract_id),
            network,
            Utc::now() - chrono::Duration::days(1),
        )
        .await
        .unwrap();
        assert!(!fresh.contains(&contract_id));

        let token = crate::token_metadata::TokenMetadataResolution::Token(
            crate::token_metadata::TokenMetadata {
                name: "Example Token".to_string(),
                symbol: "EXT".to_string(),
                decimals: 7,
            },
        );
        upsert_token_metadata(&pool, &contract_id, network, &token)
            .await
            .expect("upsert failed");

        let row: (String, String, i32, bool) = sqlx::query_as(
            "SELECT name, symbol, decimals, is_token FROM token_metadata
             WHERE contract_id = $1 AND network = $2",
        )
        .bind(&contract_id)
        .bind(network)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            row,
            ("Example Token".to_string(), "EXT".to_string(), 7, true)
        );

        // Fresh (updated within the last day) after resolution.
        let fresh = fresh_token_metadata_contract_ids(
            &pool,
            std::slice::from_ref(&contract_id),
            network,
            Utc::now() - chrono::Duration::days(1),
        )
        .await
        .unwrap();
        assert!(fresh.contains(&contract_id));

        // Re-resolving as "not a token" updates the same row rather than
        // inserting a second one.
        upsert_token_metadata(
            &pool,
            &contract_id,
            network,
            &crate::token_metadata::TokenMetadataResolution::NotAToken,
        )
        .await
        .expect("re-upsert failed");

        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM token_metadata WHERE contract_id = $1")
                .bind(&contract_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count.0, 1, "re-resolution must update, not duplicate");

        let is_token: (bool,) =
            sqlx::query_as("SELECT is_token FROM token_metadata WHERE contract_id = $1")
                .bind(&contract_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(!is_token.0);

        sqlx::query("DELETE FROM token_metadata WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// A reorg rewind must actually remove the rows above the fork point.
    ///
    /// This is a regression test for a `DELETE FROM events` that referenced a
    /// table which does not exist: the statement errored, the transaction
    /// rolled back, and the rewind silently did nothing while the logs
    /// reported a reorg as handled. Asserting on row counts after the call is
    /// what catches that — the query string alone looks plausible.
    #[tokio::test]
    async fn rewind_cursor_deletes_rows_above_the_fork_point() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract = "CREORG_TEST_CONTRACT";
        let keep = make_event(contract, 5_000, 0);
        let discard = make_event(contract, 5_001, 0);

        sqlx::query("DELETE FROM soroban_events WHERE contract_id = $1")
            .bind(contract)
            .execute(&pool)
            .await
            .expect("cleanup failed");

        insert_events_batch(&pool, &[keep.clone(), discard.clone()])
            .await
            .expect("seed insert failed");

        rewind_cursor(&pool, 5_000).await.expect("rewind failed");

        let remaining: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM soroban_events WHERE contract_id = $1 AND ledger_sequence > $2",
        )
        .bind(contract)
        .bind(5_000_i64)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            remaining.0, 0,
            "rows above the fork point must be deleted by the rewind"
        );

        let kept: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM soroban_events WHERE contract_id = $1 AND ledger_sequence = $2",
        )
        .bind(contract)
        .bind(5_000_i64)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(kept.0, 1, "rows at or below the fork point must survive");

        sqlx::query("DELETE FROM soroban_events WHERE contract_id = $1")
            .bind(contract)
            .execute(&pool)
            .await
            .unwrap();
    }
}
