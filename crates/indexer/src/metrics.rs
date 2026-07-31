//! Prometheus metrics for the indexer, served from a `GET /metrics` HTTP
//! endpoint (default port 9090, configurable via `METRICS_PORT`).
//!
//! [`install`] sets up the global recorder and starts the HTTP listener; the
//! `record_*`/`set_*` helpers below are called from the streamer at the
//! relevant points in `poll_once`.

use std::net::SocketAddr;

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use trident_common::TridentError;

pub const LEDGER_LAG: &str = "trident_indexer_ledger_lag";
/// Target Stellar ledger close time, used to convert ledger-count lag into an
/// estimated wall-clock staleness figure (issue #294). Not measured
/// per-deployment — the indexer does not retain per-ledger close timing once
/// a page is processed — so this uses Stellar's protocol-target close time
/// rather than a rolling average. Documented alongside the metric in
/// docs/observability/data-freshness.md; keep both in sync if this changes.
pub const AVG_LEDGER_CLOSE_SECONDS: f64 = 5.0;
/// Estimated wall-clock staleness: `trident_indexer_ledger_lag *
/// AVG_LEDGER_CLOSE_SECONDS` (issue #294). A derived convenience gauge, not
/// an independent measurement — see [`AVG_LEDGER_CLOSE_SECONDS`].
pub const LEDGER_LAG_SECONDS_ESTIMATED: &str = "trident_indexer_ledger_lag_seconds_estimated";
pub const EVENTS_TOTAL: &str = "trident_indexer_events_total";
pub const EVENTS_SKIPPED_TOTAL: &str = "trident_indexer_events_skipped_total";
pub const PARSE_ERRORS_TOTAL: &str = "trident_indexer_parse_errors_total";
pub const POLL_DURATION_SECONDS: &str = "trident_indexer_poll_duration_seconds";
pub const POLL_ERRORS_TOTAL: &str = "trident_indexer_poll_errors_total";
pub const RPC_RETRIES_TOTAL: &str = "trident_indexer_rpc_retries_total";
pub const EFFECTIVE_POLL_INTERVAL_MS: &str = "trident_indexer_effective_poll_interval_ms";
pub const RPC_TIMEOUTS_TOTAL: &str = "trident_indexer_rpc_timeouts_total";
pub const RPC_ACTIVE_ENDPOINT: &str = "trident_indexer_rpc_active_endpoint";
pub const RPC_FAILOVERS_TOTAL: &str = "trident_indexer_rpc_failovers_total";
pub const OUTBOX_BACKLOG: &str = "trident_indexer_outbox_backlog";
pub const OUTBOX_PUBLISHED_TOTAL: &str = "trident_indexer_outbox_published_total";
pub const OUTBOX_PUBLISH_FAILURES_TOTAL: &str = "trident_indexer_outbox_publish_failures_total";
/// RPC call latency in seconds, labelled by `method` (e.g. `getEvents`) and
/// `endpoint` (the pool index serving the call, `0` = primary). Covers every
/// call regardless of outcome, so `_count` doubles as a per-method,
/// per-endpoint call-volume counter (issue #294).
pub const RPC_CALL_DURATION_SECONDS: &str = "trident_indexer_rpc_call_duration_seconds";
/// RPC call failures labelled by `method` and a coarse `error_type`: one of
/// `timeout`, `rate_limited`, `http_4xx`, `http_5xx`, `invalid_cursor`,
/// `rpc_error`, `empty_result`, or `transport` (issue #294).
pub const RPC_ERRORS_TOTAL: &str = "trident_indexer_rpc_errors_total";
/// Unix timestamp (seconds) of the most recent completed poll cycle. Use
/// `time() - trident_indexer_last_poll_timestamp_seconds > N` as a
/// dead-man's-switch alert for a stalled indexer (#218).
pub const HEARTBEAT_TIMESTAMP: &str = "trident_indexer_last_poll_timestamp_seconds";
/// Bounded per-contract event counter. Labels: `contract` (allowlisted contract ID or `"other"`).
/// Cardinality: |allowlist| + 1. In index-all mode (no allowlist) all events land in `"other"`.
pub const EVENTS_BY_CONTRACT_TOTAL: &str = "trident_indexer_events_by_contract_total";
pub const EVENT_DECODE_DURATION_SECONDS: &str = "trident_indexer_event_decode_duration_seconds";
/// Health score (0-100) for each RPC endpoint. Label: `endpoint` (URL).
pub const RPC_HEALTH_SCORE: &str = "trident_rpc_health_score";
/// Indexer's own Postgres pool, documented in docs/metrics-catalog.md.
pub const DB_POOL_SIZE: &str = "trident_indexer_db_pool_size";
pub const DB_POOL_IDLE_CONNECTIONS: &str = "trident_indexer_db_pool_idle_connections";

/// Install the global Prometheus recorder and start serving `/metrics` on
/// `port`. Must be called once, before the streamer starts recording.
pub fn install(port: u16) -> Result<(), TridentError> {
    let addr: SocketAddr = ([0, 0, 0, 0], port).into();

    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .map_err(|e| {
            TridentError::config(anyhow::Error::new(e).context("failed to start metrics exporter"))
        })?;

    describe_gauge!(
        LEDGER_LAG,
        "Difference between chain tip and indexer cursor (ledgers)"
    );
    describe_gauge!(
        LEDGER_LAG_SECONDS_ESTIMATED,
        "Estimated wall-clock lag: ledger lag * average ledger close time (issue #294)"
    );
    describe_counter!(EVENTS_TOTAL, "Total events processed since startup");
    describe_counter!(
        EVENTS_SKIPPED_TOTAL,
        "Events skipped (diagnostic, failed call, or contract filter)"
    );
    describe_counter!(PARSE_ERRORS_TOTAL, "Total events that failed XDR decoding");
    describe_histogram!(
        POLL_DURATION_SECONDS,
        "Time per poll_once cycle, in seconds"
    );
    describe_counter!(POLL_ERRORS_TOTAL, "Poll cycles that returned an error");
    describe_counter!(
        RPC_RETRIES_TOTAL,
        "Total RPC retries triggered by transient failures"
    );
    describe_gauge!(
        EFFECTIVE_POLL_INTERVAL_MS,
        "Current adaptive poll interval in milliseconds (issue #198)"
    );
    describe_counter!(
        RPC_TIMEOUTS_TOTAL,
        "RPC calls aborted by the connect or request timeout (issue #214)"
    );
    describe_gauge!(
        RPC_ACTIVE_ENDPOINT,
        "Index of the RPC endpoint currently in use, 0 = primary (issue #213)"
    );
    describe_counter!(
        RPC_FAILOVERS_TOTAL,
        "Times the indexer failed over to another RPC endpoint (issue #213)"
    );
    describe_gauge!(
        OUTBOX_BACKLOG,
        "Committed events not yet published to the Redis stream (issue #200)"
    );
    describe_counter!(
        OUTBOX_PUBLISHED_TOTAL,
        "Events published to the Redis stream by the outbox relay (issue #200)"
    );
    describe_counter!(
        OUTBOX_PUBLISH_FAILURES_TOTAL,
        "Outbox publish attempts that failed (issue #200)"
    );
    describe_gauge!(
        HEARTBEAT_TIMESTAMP,
        "Unix timestamp (seconds) of the most recent completed poll cycle (#218)"
    );
    describe_histogram!(
        RPC_CALL_DURATION_SECONDS,
        "RPC call latency in seconds, labelled by method and endpoint index (issue #294)"
    );
    describe_counter!(
        RPC_ERRORS_TOTAL,
        "RPC call failures labelled by method and error_type (issue #294)"
    );
    describe_counter!(
        EVENTS_BY_CONTRACT_TOTAL,
        "Events processed per contract (bounded: allowlisted contract IDs + 'other' bucket)"
    );
    describe_histogram!(
        EVENT_DECODE_DURATION_SECONDS,
        "Time to XDR-decode a single event, in seconds (per-event parse latency)"
    );
    describe_gauge!(
        RPC_HEALTH_SCORE,
        "Health score (0-100) for each RPC endpoint (multi-RPC failover)"
    );

    // Counters only render in the scrape output once touched at least once;
    // seed them at zero so /metrics is complete from the very first scrape.
    counter!(EVENTS_TOTAL).increment(0);
    counter!(EVENTS_SKIPPED_TOTAL).increment(0);
    counter!(PARSE_ERRORS_TOTAL).increment(0);
    counter!(POLL_ERRORS_TOTAL).increment(0);
    counter!(RPC_RETRIES_TOTAL).increment(0);
    counter!(RPC_TIMEOUTS_TOTAL).increment(0);
    counter!(RPC_FAILOVERS_TOTAL).increment(0);
    counter!(OUTBOX_PUBLISHED_TOTAL).increment(0);
    counter!(OUTBOX_PUBLISH_FAILURES_TOTAL).increment(0);
    gauge!(RPC_ACTIVE_ENDPOINT).set(0.0);
    gauge!(OUTBOX_BACKLOG).set(0.0);
    gauge!(LEDGER_LAG).set(0.0);
    gauge!(LEDGER_LAG_SECONDS_ESTIMATED).set(0.0);
    gauge!(EFFECTIVE_POLL_INTERVAL_MS).set(0.0);
    gauge!(HEARTBEAT_TIMESTAMP).set(0.0);
    gauge!(DB_POOL_SIZE).set(0.0);
    gauge!(DB_POOL_IDLE_CONNECTIONS).set(0.0);

    tracing::info!(port, "Metrics endpoint listening");
    Ok(())
}

/// Publish ledger-count lag and its derived estimated-seconds-behind gauge
/// together, so the two figures can never drift out of sync (issue #294).
pub fn set_ledger_lag(lag: i64) {
    gauge!(LEDGER_LAG).set(lag as f64);
    gauge!(LEDGER_LAG_SECONDS_ESTIMATED).set(lag as f64 * AVG_LEDGER_CLOSE_SECONDS);
}

pub fn set_effective_poll_interval(ms: u64) {
    gauge!(EFFECTIVE_POLL_INTERVAL_MS).set(ms as f64);
}

/// Stamp the heartbeat to the current Unix time. Called at the end of every
/// poll cycle (success or failure) so a dead-man's switch alert can detect a
/// stalled-but-not-crashed indexer (#218).
pub fn set_heartbeat_timestamp(secs: f64) {
    gauge!(HEARTBEAT_TIMESTAMP).set(secs);
}

/// Stamp the heartbeat to now. Convenience wrapper over
/// [`set_heartbeat_timestamp`] for the poll loop, which has no reason to read
/// the clock itself.
pub fn record_heartbeat() {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    set_heartbeat_timestamp(secs);
}

/// Publish the indexer's own Postgres pool utilisation (docs/metrics-catalog.md).
pub fn set_db_pool_stats(size: u32, idle: u32) {
    gauge!(DB_POOL_SIZE).set(size as f64);
    gauge!(DB_POOL_IDLE_CONNECTIONS).set(idle as f64);
}

pub fn record_events_processed(count: u64) {
    if count > 0 {
        counter!(EVENTS_TOTAL).increment(count);
    }
}

pub fn record_events_skipped(count: u64) {
    if count > 0 {
        counter!(EVENTS_SKIPPED_TOTAL).increment(count);
    }
}

pub fn record_parse_error() {
    counter!(PARSE_ERRORS_TOTAL).increment(1);
}

pub fn record_poll_duration(seconds: f64) {
    histogram!(POLL_DURATION_SECONDS).record(seconds);
}

pub fn record_poll_error() {
    counter!(POLL_ERRORS_TOTAL).increment(1);
}

pub fn record_rpc_retry() {
    counter!(RPC_RETRIES_TOTAL).increment(1);
}

/// Count an RPC call that hit the connect or overall request timeout (issue #214).
pub fn record_rpc_timeout() {
    counter!(RPC_TIMEOUTS_TOTAL).increment(1);
}

/// Publish which endpoint of the configured pool is currently serving traffic
/// (0 = primary), so a silent, sustained failover is visible (issue #213).
pub fn set_rpc_active_endpoint(index: usize) {
    gauge!(RPC_ACTIVE_ENDPOINT).set(index as f64);
}

/// Count a switch to a different RPC endpoint (issue #213).
/// Called only by rpc::endpoints, which is currently unreferenced.
#[allow(dead_code)]
pub fn record_rpc_failover() {
    counter!(RPC_FAILOVERS_TOTAL).increment(1);
}

/// Publish the number of committed-but-unpublished events. A backlog that keeps
/// climbing means live subscribers are missing data (issue #200).
pub fn set_outbox_backlog(backlog: i64) {
    gauge!(OUTBOX_BACKLOG).set(backlog as f64);
}

/// Count an event delivered to the Redis stream by the relay (issue #200).
pub fn record_outbox_published() {
    counter!(OUTBOX_PUBLISHED_TOTAL).increment(1);
}

/// Count a failed relay publish attempt (issue #200).
pub fn record_outbox_publish_failure() {
    counter!(OUTBOX_PUBLISH_FAILURES_TOTAL).increment(1);
}

/// Record one RPC call's latency, labelled by method and the endpoint pool
/// index that served it (0 = primary). Recorded for every call regardless of
/// outcome, so a degraded-but-not-yet-failing provider (rising latency, no
/// errors yet) is visible before it starts timing out (issue #294).
pub fn record_rpc_call_duration(method: &'static str, endpoint_index: usize, seconds: f64) {
    histogram!(RPC_CALL_DURATION_SECONDS, "method" => method, "endpoint" => endpoint_index.to_string())
        .record(seconds);
}

/// Count an RPC failure labelled by method and a coarse error type, so ops
/// can distinguish "chain is quiet" from "RPC is degraded" and see which
/// failure mode is driving it (issue #294).
pub fn record_rpc_error(method: &'static str, error_type: &'static str) {
    counter!(RPC_ERRORS_TOTAL, "method" => method, "error_type" => error_type).increment(1);
}

/// Increment the per-contract event counter. `contract_id` must be either an
/// allowlisted contract ID or the sentinel `"other"` — never an unbounded value.
pub fn record_events_by_contract(contract_id: &str, count: u64) {
    if count > 0 {
        counter!(EVENTS_BY_CONTRACT_TOTAL, "contract" => contract_id.to_string()).increment(count);
    }
}

pub fn record_decode_duration(seconds: f64) {
    histogram!(EVENT_DECODE_DURATION_SECONDS).record(seconds);
}

/// Set the health score for a specific RPC endpoint.
///
/// Called by the health scorer after each score update so operators can see
/// which endpoints are degraded and whether failover is working.
pub fn set_rpc_health_score(endpoint: &str, score: u8) {
    gauge!(RPC_HEALTH_SCORE, "endpoint" => endpoint.to_string()).set(score as f64);
}
