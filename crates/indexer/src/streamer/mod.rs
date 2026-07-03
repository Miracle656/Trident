//! # Streamer
//!
//! Owns the RPC polling loop. Responsibilities:
//!
//! - Maintaining the ledger cursor: reading the last processed sequence from
//!   `system_state` on startup, advancing it after each successful batch, and
//!   persisting it atomically with the events it covers.
//! - Calling `getEvents` on the Stellar Soroban RPC node on a configurable
//!   interval (`POLL_INTERVAL_MS`), following the `pagingToken` cursor field
//!   to paginate across large ledger ranges within a single poll cycle.
//! - Fault tolerance and retry logic: transient RPC failures are retried with
//!   exponential backoff; persistent failures are logged without crashing the
//!   process or losing cursor position so the next poll cycle can recover.
//! - Handing each raw event to the `Parser` and forwarding normalised
//!   `SorobanEvent` values to both PostgreSQL (via `db`) and Redis Streams
//!   (via `redis_stream`).

use std::collections::HashSet;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio_retry::{strategy::ExponentialBackoff, Retry};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use trident_common::TridentError;

use crate::{
    alerting::{AlertContext, Alerter},
    config::Config,
    db, metrics,
    parser::Parser,
    redis_stream,
    rpc::RpcClient,
};
/// How often (in poll loop iterations) we re-query `indexed_contracts`.
/// At the default 5 s poll interval this is ≈ 60 s — matches the env-var default.
const FILTER_REFRESH_EVERY_N_POLLS: u32 = 12;

pub struct Streamer {
    config: Config,
    db: PgPool,
    redis: redis::aio::MultiplexedConnection,
    rpc: RpcClient,
    parser: Parser,
    /// `None`  → index all contracts (empty `indexed_contracts` table).
    /// `Some`  → allowlist; events from unlisted contracts are skipped.
    contract_filter: Option<HashSet<String>>,
    /// Counts poll cycles so we know when to refresh the filter.
    poll_count: u32,
    /// Outbound webhook alerter (issue #75). No-op when URL is not configured.
    alerter: Alerter,
    /// Chain tip ledger from the most recent RPC response (issue #75).
    last_chain_tip: u64,
}

impl Streamer {
    pub async fn new(
        config: Config,
        db: PgPool,
        redis: redis::aio::MultiplexedConnection,
    ) -> Result<Self, TridentError> {
        let rpc = RpcClient::new(config.stellar_rpc_url.clone());
        let parser = Parser::new(config.index_diagnostic);
        let contract_filter = Self::load_filter(&db, &config.network).await?;
        let alerter = Alerter::from_config(
            config.alert_webhook_url.clone(),
            config.alert_lag_threshold,
            config.alert_cooldown_minutes,
        )?;

        Ok(Self {
            config,
            db,
            redis,
            rpc,
            parser,
            contract_filter,
            poll_count: 0,
            alerter,
            last_chain_tip: 0,
        })
    }

    /// Load (or reload) the contract allowlist from DB.
    /// Returns `None` if the table is empty (index-all mode).
    async fn load_filter(
        pool: &PgPool,
        network: &str,
    ) -> Result<Option<HashSet<String>>, TridentError> {
        let set = db::load_indexed_contracts(pool, network).await?;
        if set.is_empty() {
            Ok(None)
        } else {
            tracing::info!(count = set.len(), "Contract allowlist loaded");
            Ok(Some(set))
        }
    }

    /// Reload the contract filter from DB. Called periodically inside `run`.
    pub async fn refresh_contract_filter(&mut self) -> Result<(), TridentError> {
        match Self::load_filter(&self.db, &self.config.network).await {
            Ok(filter) => {
                self.contract_filter = filter;
                Ok(())
            }
            Err(e) => {
                // Non-fatal: keep the existing filter, log the error.
                tracing::warn!(error = %e, "Failed to refresh contract filter; keeping existing");
                Ok(())
            }
        }
    }

    /// Start the polling loop. Runs until `shutdown` is cancelled, always
    /// finishing the current `poll_once` before stopping (never mid-batch).
    pub async fn run(&mut self, shutdown: CancellationToken) -> Result<(), TridentError> {
        tracing::info!(network = %self.config.network, "Streamer started");
        tracing::info!(
            "[indexer] poll interval: {}ms",
            self.config.poll_interval.as_millis()
        );
        tracing::info!(
            "[indexer] max events per poll: {}",
            self.config.max_events_per_poll
        );

        let mut cursor = db::get_cursor(&self.db).await?;
        tracing::info!(cursor, "Resuming from ledger cursor");

        loop {
            // Check for shutdown before starting a new poll so we never begin
            // a batch we can't finish atomically.
            if shutdown.is_cancelled() {
                break;
            }

            // Periodically refresh the contract allowlist so new contracts
            // become active without a restart (issue #47).
            self.poll_count = self.poll_count.wrapping_add(1);
            if self.poll_count.is_multiple_of(FILTER_REFRESH_EVERY_N_POLLS) {
                self.refresh_contract_filter().await?;
            }

            let poll_span = tracing::info_span!("poll_cycle", cursor = cursor);
            match self.poll_once(&mut cursor).instrument(poll_span).await {
                Ok(events_processed) => {
                    if events_processed > 0 {
                        tracing::info!(events_processed, cursor, "Batch processed");
                    } else {
                        tracing::debug!(cursor, "No new events");
                    }
                }
                Err(e) => {
                    // Log but do not crash — the cursor is safe, next poll will retry.
                    metrics::record_poll_error();
                    tracing::error!(error = %e, "Poll cycle failed, will retry next interval");
                }
            }

            // Sleep until the next poll interval, waking immediately on shutdown.
            tokio::select! {
                _ = tokio::time::sleep(self.config.poll_interval) => {}
                _ = shutdown.cancelled() => {
                    tracing::info!("Shutdown signal received, stopping after current poll");
                    break;
                }
            }
        }

        tracing::info!("Streamer stopped cleanly");
        Ok(())
    }

    /// Execute a single poll cycle. Fetches all available pages from the RPC
    /// starting at `cursor`, persists each event, and advances the cursor.
    /// Returns the total number of events processed in this cycle.
    async fn poll_once(&mut self, cursor: &mut u64) -> Result<usize, TridentError> {
        let poll_start = Instant::now();
        let retry_strategy = ExponentialBackoff::from_millis(200)
            .max_delay(Duration::from_secs(2))
            .take(5);

        // First-ever run: anchor to ledger 1 via start_ledger.
        // All subsequent calls use paging_token cursors so the RPC can resume
        // exactly where we left off without re-scanning from genesis.
        let (start_ledger, initial_cursor) = if *cursor == 0 {
            (Some(1u64), None)
        } else {
            (None, Some(cursor.to_string()))
        };

        let mut page_cursor = initial_cursor;
        let mut total = 0;

        loop {
            let pc = page_cursor.clone();
            let sl = start_ledger;
            let mut attempt = 0u32;
            let limit = self.config.max_events_per_poll;
            let page = Retry::start(retry_strategy.clone(), || {
                attempt += 1;
                if attempt > 1 {
                    metrics::record_rpc_retry();
                }
                async { self.rpc.get_events(sl, pc.clone(), limit).await }
                    .instrument(tracing::info_span!("rpc_get_events"))
            })
            .await?;

            tracing::debug!(
                latest_ledger = page.latest_ledger,
                cursor = *cursor,
                "RPC page received"
            );

            metrics::set_ledger_lag(page.latest_ledger.saturating_sub(*cursor) as i64);
            self.last_chain_tip = page.latest_ledger;

            if page.events.is_empty() {
                break;
            }

            let last_paging_token = page.events.last().map(|e| e.paging_token.clone());

            let mut events_in_page: i32 = 0;
            let mut skipped_in_page: u64 = 0;
            for raw in &page.events {
                let parse_result = {
                    let _span = tracing::info_span!("parse_events").entered();
                    self.parser.parse_event(raw)
                };
                match parse_result {
                    Ok(Some(event)) => {
                        // Contract allowlist filtering (issue #47).
                        // None → index all; Some(set) → only listed contracts.
                        if let Some(ref filter) = self.contract_filter {
                            if !filter.contains(&event.contract_id) {
                                tracing::trace!(
                                    contract_id = %event.contract_id,
                                    "Skipping event from unlisted contract"
                                );
                                skipped_in_page += 1;
                                continue;
                            }
                        }
                        db::insert_event(&self.db, &event)
                            .instrument(tracing::info_span!(
                                "db_insert_events",
                                contract_id = %event.contract_id
                            ))
                            .await?;
                        redis_stream::publish_event(
                            &mut self.redis,
                            &event,
                            self.config.redis_stream_maxlen,
                        )
                        .instrument(tracing::info_span!("redis_xadd"))
                        .await?;
                        total += 1;
                        events_in_page += 1;
                    }
                    Ok(None) => {
                        // diagnostic or failed-call event — intentionally skipped
                        skipped_in_page += 1;
                    }
                    Err(e) => {
                        tracing::warn!(
                            tx_hash = %raw.tx_hash,
                            error = %e,
                            "Skipping unparseable event"
                        );
                        metrics::record_parse_error();
                        let ledger_seq: u64 = raw.ledger.parse().unwrap_or(0);
                        let event_idx: u32 = raw
                            .id
                            .split('-')
                            .next_back()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        let raw_payload = serde_json::to_string(&serde_json::json!({
                            "type": &raw.event_type,
                            "ledger": &raw.ledger,
                            "ledgerClosedAt": &raw.ledger_closed_at,
                            "contractId": &raw.contract_id,
                            "id": &raw.id,
                            "topic": &raw.topic,
                            "value": &raw.value,
                        }))
                        .unwrap_or_else(|_| "{}".to_string());
                        if let Err(db_err) = db::insert_parse_error(
                            &self.db,
                            ledger_seq,
                            event_idx,
                            &raw_payload,
                            &e.to_string(),
                        )
                        .await
                        {
                            tracing::error!(
                                error = %db_err,
                                "Failed to record parse error in database"
                            );
                        }
                        skipped_in_page += 1;
                    }
                }
            }

            metrics::record_events_processed(events_in_page as u64);
            metrics::record_events_skipped(skipped_in_page);

            // Advance the persistent cursor and record ledger metadata.
            if let Some(last) = page.events.last() {
                let seq: u64 = last.ledger.parse().unwrap_or(*cursor);
                if seq > *cursor {
                    *cursor = seq;
                    db::set_cursor(&self.db, *cursor).await?;

                    // Fetch the real ledger hash from getLedgers RPC.
                    // Non-critical: log a warning on failure, store empty string.
                    let ledger_hash = match self.rpc.get_ledger(seq).await {
                        Ok(Some(h)) => h,
                        Ok(None) => {
                            tracing::warn!(seq, "getLedgers returned no ledger for sequence");
                            String::new()
                        }
                        Err(e) => {
                            tracing::warn!(seq, error = %e, "getLedgers failed, storing empty hash");
                            String::new()
                        }
                    };

                    db::insert_ledger_metadata(
                        &self.db,
                        seq,
                        &ledger_hash,
                        &last.ledger_closed_at,
                        events_in_page,
                    )
                    .await?;
                }
            }

            // An incomplete page means we have caught up to the chain tip.
            if page.events.len() < self.config.max_events_per_poll as usize {
                break;
            }

            page_cursor = last_paging_token;
        }

        // Recompute lag once the loop settles so it reflects the final cursor
        // relative to the chain tip (zero once we have caught up).
        metrics::set_ledger_lag(self.last_chain_tip.saturating_sub(*cursor) as i64);

        // Write health stats after every successful cycle (issue #62).
        // Non-fatal: log on failure so a bad health write doesn't stop indexing.
        let poll_duration = poll_start.elapsed();
        metrics::record_poll_duration(poll_duration.as_secs_f64());
        if let Err(e) =
            db::update_health_stats(&self.db, *cursor as i64, total as i32, poll_duration).await
        {
            tracing::warn!(error = %e, "Failed to update health stats");
        }

        // Alerting (issue #75) — best-effort, never aborts the poll cycle.
        if self.alerter.is_enabled() {
            match db::get_alert_state(&self.db).await {
                Ok(mut alert_state) => {
                    let ctx = AlertContext {
                        last_ledger_indexed: *cursor,
                        chain_tip_ledger: self.last_chain_tip,
                        lag_threshold: self.config.alert_lag_threshold,
                        network: self.config.network.clone(),
                    };
                    self.alerter.evaluate(&ctx, &mut alert_state).await;
                    if let Err(e) = db::set_alert_state(&self.db, &alert_state).await {
                        tracing::warn!(error = %e, "Failed to persist alert state");
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to read alert state");
                }
            }
        }

        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine};
    use stellar_xdr::curr::{Limited, Limits, ScSymbol, ScVal, WriteXdr};
    use wiremock::matchers::{body_partial_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // Return the (db, redis) URLs, or skip the test when they are absent.
    // When REQUIRE_TEST_SERVICES is set (the rust-integration CI job sets it),
    // a missing URL is a hard failure instead of a silent skip — otherwise a
    // misconfigured integration job would go green without running anything.
    macro_rules! require_services {
        () => {{
            let required = std::env::var("REQUIRE_TEST_SERVICES").is_ok();
            match (
                std::env::var("TEST_DATABASE_URL"),
                std::env::var("TEST_REDIS_URL"),
            ) {
                (Ok(db), Ok(rd)) => (db, rd),
                _ if required => panic!(
                    "TEST_DATABASE_URL and TEST_REDIS_URL must be set when REQUIRE_TEST_SERVICES is set"
                ),
                _ => {
                    eprintln!("SKIP: TEST_DATABASE_URL / TEST_REDIS_URL not set");
                    return;
                }
            }
        }};
    }

    fn sym_xdr(s: &str) -> String {
        let val = ScVal::Symbol(ScSymbol::try_from(s.to_string()).unwrap());
        let mut buf = vec![];
        val.write_xdr(&mut Limited::new(&mut buf, Limits::none()))
            .unwrap();
        STANDARD.encode(buf)
    }

    fn void_xdr() -> String {
        let val = ScVal::Void;
        let mut buf = vec![];
        val.write_xdr(&mut Limited::new(&mut buf, Limits::none()))
            .unwrap();
        STANDARD.encode(buf)
    }

    fn events_page(ledger: u64, count: usize) -> serde_json::Value {
        let events: Vec<serde_json::Value> = (0..count)
            .map(|i| {
                serde_json::json!({
                    "type": "contract",
                    "ledger": ledger.to_string(),
                    "ledgerClosedAt": "2024-01-01T00:00:00Z",
                    "contractId": "CTEST",
                    "id": format!("{:016}-{}", ledger, i),
                    "pagingToken": format!("{}-{}", ledger, i),
                    "txHash": format!("hash{}{}", ledger, i),
                    "topic": [sym_xdr("transfer")],
                    "value": void_xdr(),
                    "inSuccessfulContractCall": true
                })
            })
            .collect();

        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "events": events,
                "latestLedger": ledger
            }
        })
    }

    fn error_500() -> ResponseTemplate {
        ResponseTemplate::new(500).set_body_string("Internal Server Error")
    }

    fn rpc_ok(body: serde_json::Value) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(body)
    }

    async fn make_streamer(db_url: &str, redis_url: &str, rpc_url: String) -> Streamer {
        let db = sqlx::PgPool::connect(db_url).await.unwrap();
        let redis = redis::Client::open(redis_url)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let config = Config {
            stellar_rpc_url: rpc_url,
            database_url: db_url.to_string(),
            db_pool_size: 3,
            redis_url: redis_url.to_string(),
            network: "testnet".to_string(),
            poll_interval: Duration::from_millis(50),
            index_diagnostic: false,
            max_events_per_poll: 200,
            redis_stream_maxlen: 10_000,
            metrics_port: 0,
            alert_webhook_url: None,
            alert_lag_threshold: 200,
            alert_cooldown_minutes: 30,
        };

        Streamer::new(config, db, redis).await.unwrap()
    }

    async fn reset_db(pool: &sqlx::PgPool) {
        sqlx::query("DELETE FROM soroban_events")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM ledger_metadata")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("UPDATE system_state SET value = '0' WHERE key = 'latest_ledger_cursor'")
            .execute(pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn events_written_to_postgres_after_poll() {
        let (db_url, redis_url) = require_services!();
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(rpc_ok(events_page(100, 3)))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(rpc_ok(events_page(100, 0)))
            .mount(&server)
            .await;

        let mut s = make_streamer(&db_url, &redis_url, server.uri()).await;
        reset_db(&s.db).await;

        let mut cursor = db::get_cursor(&s.db).await.unwrap();
        s.poll_once(&mut cursor).await.unwrap();

        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM soroban_events")
            .fetch_one(&s.db)
            .await
            .unwrap();
        assert_eq!(count.0, 3, "expected 3 events in soroban_events");
    }

    #[tokio::test]
    async fn cursor_advances_in_system_state_after_poll() {
        let (db_url, redis_url) = require_services!();
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(rpc_ok(events_page(200, 2)))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(rpc_ok(events_page(200, 0)))
            .mount(&server)
            .await;

        let mut s = make_streamer(&db_url, &redis_url, server.uri()).await;
        reset_db(&s.db).await;

        let mut cursor = 0u64;
        s.poll_once(&mut cursor).await.unwrap();

        let stored = db::get_cursor(&s.db).await.unwrap();
        assert_eq!(stored, 200, "cursor should advance to ledger 200");
        assert_eq!(cursor, 200);
    }

    #[tokio::test]
    async fn events_published_to_redis_stream_after_poll() {
        let (db_url, redis_url) = require_services!();
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(rpc_ok(events_page(300, 2)))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(rpc_ok(events_page(300, 0)))
            .mount(&server)
            .await;

        let mut s = make_streamer(&db_url, &redis_url, server.uri()).await;
        reset_db(&s.db).await;

        // Trim the stream so we start fresh.
        let _: () = redis::cmd("XTRIM")
            .arg("trident:events")
            .arg("MAXLEN")
            .arg(0)
            .query_async(&mut s.redis)
            .await
            .unwrap_or(());

        let mut cursor = 0u64;
        s.poll_once(&mut cursor).await.unwrap();

        let len: i64 = redis::cmd("XLEN")
            .arg("trident:events")
            .query_async(&mut s.redis)
            .await
            .unwrap();
        assert_eq!(len, 2, "expected 2 events in Redis stream");
    }

    #[tokio::test]
    async fn poll_returns_error_when_rpc_consistently_fails() {
        let (db_url, redis_url) = require_services!();
        let server = MockServer::start().await;

        // Always return 500 so all retries exhaust.
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(error_500())
            .mount(&server)
            .await;

        let mut s = make_streamer(&db_url, &redis_url, server.uri()).await;
        reset_db(&s.db).await;

        let mut cursor = 0u64;
        // tokio-retry with max 5 retries and 200ms base — allow up to 10s
        let result = tokio::time::timeout(Duration::from_secs(10), s.poll_once(&mut cursor))
            .await
            .expect("poll_once timed out");
        assert!(
            result.is_err(),
            "poll_once should fail after retries exhausted"
        );
    }

    #[tokio::test]
    async fn poll_once_increments_metrics_counters() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        use metrics_util::MetricKind;

        let (db_url, redis_url) = require_services!();
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(rpc_ok(events_page(500, 3)))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(rpc_ok(events_page(500, 0)))
            .mount(&server)
            .await;

        let mut s = make_streamer(&db_url, &redis_url, server.uri()).await;
        reset_db(&s.db).await;

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let guard = ::metrics::set_default_local_recorder(&recorder);

        let mut cursor = 0u64;
        let total = s.poll_once(&mut cursor).await.unwrap();
        drop(guard);

        assert_eq!(total, 3);

        let snapshot = snapshotter.snapshot().into_vec();
        let counter_value = |name: &str| {
            snapshot
                .iter()
                .find(|(key, _, _, _)| {
                    key.kind() == MetricKind::Counter && key.key().name() == name
                })
                .and_then(|(_, _, _, value)| match value {
                    DebugValue::Counter(n) => Some(*n),
                    _ => None,
                })
                .unwrap_or(0)
        };
        let gauge_value = |name: &str| {
            snapshot
                .iter()
                .find(|(key, _, _, _)| key.kind() == MetricKind::Gauge && key.key().name() == name)
                .and_then(|(_, _, _, value)| match value {
                    DebugValue::Gauge(n) => Some(n.into_inner()),
                    _ => None,
                })
        };

        assert_eq!(
            counter_value(metrics::EVENTS_TOTAL),
            3,
            "events_total should increment by the number of events processed"
        );
        assert_eq!(
            counter_value(metrics::POLL_ERRORS_TOTAL),
            0,
            "no poll errors occurred"
        );
        assert_eq!(
            gauge_value(metrics::LEDGER_LAG),
            Some(0.0),
            "lag should be zero once the cursor catches up to the chain tip"
        );
    }

    #[tokio::test]
    async fn full_page_triggers_followup_poll_partial_page_stops() {
        let (db_url, redis_url) = require_services!();
        let server = MockServer::start().await;

        // getLedgers calls (made after each cursor advance) must not consume the
        // getEvents page mocks, so match them separately.
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_partial_json(
                serde_json::json!({ "method": "getLedgers" }),
            ))
            .respond_with(rpc_ok(serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": { "ledgers": [] }
            })))
            .mount(&server)
            .await;
        // First getEvents call returns 200 events (full page) → triggers follow-up
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_partial_json(
                serde_json::json!({ "method": "getEvents" }),
            ))
            .respond_with(rpc_ok(events_page(400, 200)))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // Second getEvents call returns 5 events (partial page) → stops pagination
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_partial_json(
                serde_json::json!({ "method": "getEvents" }),
            ))
            .respond_with(rpc_ok(events_page(401, 5)))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // Any further getEvents calls return empty
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_partial_json(
                serde_json::json!({ "method": "getEvents" }),
            ))
            .respond_with(rpc_ok(events_page(401, 0)))
            .mount(&server)
            .await;

        let mut s = make_streamer(&db_url, &redis_url, server.uri()).await;
        reset_db(&s.db).await;

        let mut cursor = 0u64;
        let total = s.poll_once(&mut cursor).await.unwrap();

        assert_eq!(
            total, 205,
            "should process 200 + 5 = 205 events across two pages"
        );
    }
}
