use std::time::Duration;
use trident_common::TridentError;

#[derive(Debug)]
pub struct Config {
    pub database_url: String,
    pub db_pool_size: u32,
    pub redis_url: String,
    /// Primary RPC endpoint — always the first entry of `stellar_rpc_urls`.
    pub stellar_rpc_url: String,
    /// Prioritised RPC endpoints used for health-based failover (issue #213).
    pub stellar_rpc_urls: Vec<String>,
    /// Consecutive failures on the active endpoint before failing over (issue #213).
    /// Read only by rpc::endpoints, which is currently unreferenced — see the
    /// note at the top of that module.
    #[allow(dead_code)]
    pub rpc_failover_threshold: u32,
    /// How long a failed endpoint is parked before it is probed again (issue #213).
    #[allow(dead_code)]
    pub rpc_endpoint_cooldown: Duration,
    pub network: String,
    pub poll_interval: Duration,
    /// Shortest adaptive poll interval, applied when lag >= `lag_high_watermark`.
    pub poll_interval_floor: Duration,
    /// Longest adaptive poll interval, applied when the indexer is caught up.
    pub poll_interval_ceiling: Duration,
    /// Lag (ledgers) at or above which the floor interval applies.
    pub lag_high_watermark: u64,
    /// Hysteresis deadband (ledgers) suppressing interval churn on lag jitter.
    pub poll_hysteresis_ledgers: u64,
    /// TCP connect timeout for RPC HTTP requests (issue #214).
    pub rpc_connect_timeout: Duration,
    /// Overall request timeout (connect, headers and body) for RPC calls (issue #214).
    pub rpc_request_timeout: Duration,
    /// How long an idle pooled connection is kept before it is dropped (issue #214).
    pub rpc_pool_idle_timeout: Duration,
    /// Maximum idle keep-alive connections retained per RPC host (issue #214).
    pub rpc_pool_max_idle_per_host: usize,
    /// TCP keep-alive probe interval for pooled RPC sockets (issue #214).
    pub rpc_tcp_keepalive: Duration,
    /// How often the outbox relay scans for unpublished events (issue #200).
    pub outbox_poll_interval: Duration,
    /// Maximum events published per relay pass (issue #200).
    pub outbox_batch_size: i64,
    /// Backlog size at which the relay warns that delivery is falling behind.
    pub outbox_backlog_alert_threshold: i64,
    pub index_diagnostic: bool,
    /// Topic patterns pushed into the `getEvents` RPC filter alongside the
    /// contract allowlist (issue #203). Empty means "no topic narrowing".
    pub topic_filters: Vec<Vec<String>>,
    pub max_events_per_poll: u32,
    /// Maximum rows per batched INSERT when committing a page (issue #199).
    pub db_batch_size: usize,
    pub redis_stream_maxlen: u64,
    pub metrics_port: u16,
    pub health_port: u16,
    pub alert_webhook_url: Option<String>,
    pub alert_lag_threshold: u64,
    pub alert_cooldown_minutes: u64,
    /// statement_timeout for every DB connection (ms). Prevents runaway queries
    /// from holding the pool indefinitely (#249).
    pub statement_timeout_ms: u64,
    /// idle_in_transaction_session_timeout (ms). Reclaims connections leaked by
    /// open transactions (#249).
    pub idle_in_transaction_timeout_ms: u64,
    /// How long a cached `token_metadata` row is considered fresh before the
    /// indexer re-simulates name()/symbol()/decimals() for that contract
    /// (issue #263). Applies to both positive and negative (non-token)
    /// results.
    pub token_metadata_refresh_interval: Duration,
    /// Network passphrase used to derive Stellar Asset Contract ids (issue
    /// #262). Defaults from `network` for the two well-known networks.
    pub network_passphrase: String,
    /// Operator-configured classic assets to resolve SAC events for (issue
    /// #262). Each is a `code:issuer` pair, or the literal `native` for XLM.
    pub tracked_sac_assets: Vec<crate::parser::sac::TrackedAsset>,
}

/// Default Postgres pool size for the indexer. It is a single writer with low
/// write concurrency, so a small pool is correct (issue #87).
const DEFAULT_DB_POOL_SIZE: u32 = 3;

impl Config {
    pub fn from_env() -> Result<Self, TridentError> {
        let mut missing: Vec<&str> = Vec::new();

        let database_url = collect_required("DATABASE_URL", &mut missing);
        let redis_url = collect_required("REDIS_URL", &mut missing);

        // Endpoint list for failover (issue #213). STELLAR_RPC_URLS is the
        // prioritised, comma-separated form; STELLAR_RPC_URL remains valid as a
        // single-value alias so existing deployments keep working unchanged.
        let stellar_rpc_urls = parse_endpoint_list(
            std::env::var("STELLAR_RPC_URLS").ok(),
            std::env::var("STELLAR_RPC_URL").ok(),
        )?;
        if stellar_rpc_urls.is_empty() {
            missing.push("STELLAR_RPC_URL (or STELLAR_RPC_URLS)");
        }

        if !missing.is_empty() {
            return Err(TridentError::config(anyhow::anyhow!(
                "[trident-indexer] missing required env vars:\n{}",
                missing.join("\n")
            )));
        }

        let network = std::env::var("NETWORK").unwrap_or_else(|_| "testnet".into());

        // Network passphrase for SAC contract id derivation (issue #262).
        // NETWORK_PASSPHRASE always wins when set; otherwise it is inferred
        // from the well-known "testnet"/"mainnet" friendly names, and any
        // other `network` value must supply an explicit passphrase.
        let network_passphrase = match std::env::var("NETWORK_PASSPHRASE") {
            Ok(v) if !v.is_empty() => v,
            _ => default_network_passphrase(&network)?,
        };

        // Tracked classic assets whose SAC events should carry asset context
        // (issue #262), e.g. TRACKED_SAC_ASSETS="USDC:GA5Z...,native".
        let tracked_sac_assets = match std::env::var("TRACKED_SAC_ASSETS") {
            Ok(spec) if !spec.trim().is_empty() => parse_tracked_sac_assets(&spec)?,
            _ => Vec::new(),
        };

        let poll_interval_ms = parse_bounded_u64("POLL_INTERVAL_MS", 1000, 100, 60_000)?;
        let max_events_per_poll = parse_bounded_u64("MAX_EVENTS_PER_POLL", 200, 1, 10_000)?;
        // Rows per batched INSERT (issue #199). Large enough that a default
        // 200-event page commits in one statement, bounded so a huge page
        // cannot build an unbounded statement.
        let db_batch_size = parse_bounded_u64("DB_BATCH_SIZE", 1_000, 1, 10_000)?;

        // Adaptive poll interval bounds (issue #198). Defaults: poll every 250ms
        // while far behind, back off to 5s once caught up, cross over at 100
        // ledgers of lag, with a 10-ledger hysteresis deadband.
        let poll_interval_floor_ms = parse_bounded_u64("POLL_INTERVAL_FLOOR_MS", 250, 50, 60_000)?;
        let poll_interval_ceiling_ms =
            parse_bounded_u64("POLL_INTERVAL_CEILING_MS", 5000, 100, 600_000)?;
        if poll_interval_ceiling_ms <= poll_interval_floor_ms {
            return Err(TridentError::config(anyhow::anyhow!(
                "[indexer] POLL_INTERVAL_CEILING_MS ({poll_interval_ceiling_ms}) must exceed POLL_INTERVAL_FLOOR_MS ({poll_interval_floor_ms})"
            )));
        }
        let lag_high_watermark = parse_bounded_u64("LAG_HIGH_WATERMARK", 100, 1, 100_000_000)?;
        let poll_hysteresis_ledgers =
            parse_bounded_u64("POLL_HYSTERESIS_LEDGERS", 10, 0, 1_000_000)?;

        // RPC HTTP client timeouts and connection reuse (issue #214). Without an
        // explicit timeout a hung TCP connection blocks a poll forever: the retry
        // wrapper only reacts to returned errors, never to a call that never
        // returns. Defaults: 5s connect, 30s overall request.
        let rpc_connect_timeout_ms =
            parse_bounded_u64("RPC_CONNECT_TIMEOUT_MS", 5_000, 100, 60_000)?;
        let rpc_request_timeout_ms =
            parse_bounded_u64("RPC_REQUEST_TIMEOUT_MS", 30_000, 500, 600_000)?;
        if rpc_request_timeout_ms < rpc_connect_timeout_ms {
            return Err(TridentError::config(anyhow::anyhow!(
                "[indexer] RPC_REQUEST_TIMEOUT_MS ({rpc_request_timeout_ms}) must be >= RPC_CONNECT_TIMEOUT_MS ({rpc_connect_timeout_ms})"
            )));
        }
        let rpc_pool_idle_timeout_ms =
            parse_bounded_u64("RPC_POOL_IDLE_TIMEOUT_MS", 90_000, 1_000, 600_000)?;
        let rpc_pool_max_idle_per_host =
            parse_bounded_u64("RPC_POOL_MAX_IDLE_PER_HOST", 8, 1, 1_024)? as usize;
        let rpc_tcp_keepalive_ms =
            parse_bounded_u64("RPC_TCP_KEEPALIVE_MS", 60_000, 1_000, 600_000)?;

        // Failover tuning (issue #213): park an endpoint after this many
        // consecutive failures, and probe it again after the cooldown.
        let rpc_failover_threshold = parse_bounded_u64("RPC_FAILOVER_THRESHOLD", 3, 1, 100)? as u32;
        let rpc_endpoint_cooldown_ms =
            parse_bounded_u64("RPC_ENDPOINT_COOLDOWN_MS", 30_000, 1_000, 3_600_000)?;

        // Outbox relay tuning (issue #200). The default 100ms interval keeps
        // live delivery latency close to the direct-publish path while the
        // bounded batch stops the relay starving the poll loop.
        let outbox_poll_interval_ms =
            parse_bounded_u64("OUTBOX_POLL_INTERVAL_MS", 100, 10, 60_000)?;
        let outbox_batch_size = parse_bounded_u64("OUTBOX_BATCH_SIZE", 500, 1, 10_000)? as i64;
        let outbox_backlog_alert_threshold =
            parse_bounded_u64("OUTBOX_BACKLOG_ALERT_THRESHOLD", 10_000, 1, 10_000_000)? as i64;

        let index_diagnostic = std::env::var("INDEX_DIAGNOSTIC")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        // Optional server-side topic narrowing (issue #203), e.g.
        // INDEX_TOPIC_FILTERS="transfer/*/*,mint/*/*". Only applied when a
        // contract allowlist is configured; an invalid spec is a hard error
        // rather than a silent fallback to unfiltered indexing.
        let topic_filters = match std::env::var("INDEX_TOPIC_FILTERS") {
            Ok(spec) => crate::rpc::filters::parse_topic_filters(&spec).map_err(|e| {
                TridentError::config(anyhow::anyhow!("[indexer] INDEX_TOPIC_FILTERS: {e}"))
            })?,
            Err(_) => Vec::new(),
        };

        let alert_webhook_url = std::env::var("ALERT_WEBHOOK_URL")
            .ok()
            .filter(|s| !s.is_empty());
        let alert_lag_threshold = parse_bounded_u64("ALERT_LAG_THRESHOLD", 200, 1, 1_000_000)?;
        let alert_cooldown_minutes = parse_bounded_u64("ALERT_COOLDOWN_MINUTES", 30, 1, 10_080)?;

        Ok(Self {
            database_url: database_url.unwrap(),
            db_pool_size: parse_pool_size("INDEXER_DB_POOL_SIZE", DEFAULT_DB_POOL_SIZE)?,
            redis_url: redis_url.unwrap(),
            stellar_rpc_url: stellar_rpc_urls[0].clone(),
            stellar_rpc_urls,
            rpc_failover_threshold,
            rpc_endpoint_cooldown: Duration::from_millis(rpc_endpoint_cooldown_ms),
            network,
            poll_interval: Duration::from_millis(poll_interval_ms),
            poll_interval_floor: Duration::from_millis(poll_interval_floor_ms),
            poll_interval_ceiling: Duration::from_millis(poll_interval_ceiling_ms),
            lag_high_watermark,
            poll_hysteresis_ledgers,
            rpc_connect_timeout: Duration::from_millis(rpc_connect_timeout_ms),
            rpc_request_timeout: Duration::from_millis(rpc_request_timeout_ms),
            rpc_pool_idle_timeout: Duration::from_millis(rpc_pool_idle_timeout_ms),
            rpc_pool_max_idle_per_host,
            rpc_tcp_keepalive: Duration::from_millis(rpc_tcp_keepalive_ms),
            outbox_poll_interval: Duration::from_millis(outbox_poll_interval_ms),
            outbox_batch_size,
            outbox_backlog_alert_threshold,
            index_diagnostic,
            topic_filters,
            max_events_per_poll: max_events_per_poll as u32,
            db_batch_size: db_batch_size as usize,
            redis_stream_maxlen: std::env::var("REDIS_STREAM_MAXLEN")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10_000),
            metrics_port: std::env::var("METRICS_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(9090),
            health_port: std::env::var("HEALTH_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8080),
            alert_webhook_url,
            alert_lag_threshold,
            alert_cooldown_minutes,
            statement_timeout_ms: parse_bounded_u64(
                "DB_STATEMENT_TIMEOUT_MS",
                30_000,
                100,
                3_600_000,
            )?,
            idle_in_transaction_timeout_ms: parse_bounded_u64(
                "DB_IDLE_IN_TRANSACTION_TIMEOUT_MS",
                10_000,
                100,
                3_600_000,
            )?,
            token_metadata_refresh_interval: Duration::from_secs(parse_bounded_u64(
                "TOKEN_METADATA_REFRESH_INTERVAL_SECS",
                86_400,
                60,
                2_592_000,
            )?),
            network_passphrase,
            tracked_sac_assets,
        })
    }
}

/// Well-known Stellar network passphrases (issue #262). Any network name
/// other than these two must set `NETWORK_PASSPHRASE` explicitly — guessing
/// would silently derive wrong SAC contract ids.
fn default_network_passphrase(network: &str) -> Result<String, TridentError> {
    match network {
        "testnet" => Ok("Test SDF Network ; September 2015".to_string()),
        "mainnet" | "pubnet" => Ok("Public Global Stellar Network ; September 2015".to_string()),
        other => Err(TridentError::config(anyhow::anyhow!(
            "[indexer] NETWORK={other:?} has no well-known passphrase; set NETWORK_PASSPHRASE explicitly"
        ))),
    }
}

/// Parse `TRACKED_SAC_ASSETS` as a comma-separated list of `CODE:ISSUER`
/// pairs, or the bare literal `native` for XLM (issue #262).
fn parse_tracked_sac_assets(
    spec: &str,
) -> Result<Vec<crate::parser::sac::TrackedAsset>, TridentError> {
    let mut assets = Vec::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if part.eq_ignore_ascii_case("native") {
            assets.push(crate::parser::sac::TrackedAsset {
                code: "native".to_string(),
                issuer: String::new(),
            });
            continue;
        }
        let (code, issuer) = part.split_once(':').ok_or_else(|| {
            TridentError::config(anyhow::anyhow!(
                "[indexer] TRACKED_SAC_ASSETS entry {part:?} must be CODE:ISSUER or 'native'"
            ))
        })?;
        if code.is_empty() || issuer.is_empty() {
            return Err(TridentError::config(anyhow::anyhow!(
                "[indexer] TRACKED_SAC_ASSETS entry {part:?} must be CODE:ISSUER or 'native'"
            )));
        }
        assets.push(crate::parser::sac::TrackedAsset {
            code: code.to_string(),
            issuer: issuer.to_string(),
        });
    }
    Ok(assets)
}

/// Read a required env var; on absence push its name to `missing` and return None.
fn collect_required<'a>(key: &'a str, missing: &mut Vec<&'a str>) -> Option<String> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            missing.push(key);
            None
        }
    }
}

/// Build the prioritised endpoint list from `STELLAR_RPC_URLS` (comma-separated)
/// falling back to the single-value `STELLAR_RPC_URL` alias (issue #213).
///
/// Duplicates are dropped so a repeated URL cannot mask a real failover target,
/// and a list that contains only blanks is rejected rather than silently
/// collapsing to "no endpoints".
fn parse_endpoint_list(
    list: Option<String>,
    single: Option<String>,
) -> Result<Vec<String>, TridentError> {
    let raw = match list.filter(|s| !s.trim().is_empty()) {
        Some(s) => s,
        None => single.unwrap_or_default(),
    };

    let mut urls: Vec<String> = Vec::new();
    for part in raw.split(',') {
        let url = part.trim();
        if url.is_empty() {
            continue;
        }
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(TridentError::config(anyhow::anyhow!(
                "[indexer] Stellar RPC endpoint {url:?} must start with http:// or https://"
            )));
        }
        if !urls.iter().any(|existing| existing == url) {
            urls.push(url.to_string());
        }
    }

    Ok(urls)
}

/// Parse an env var as u64 with a default and inclusive [min, max] bounds.
fn parse_bounded_u64(key: &str, default: u64, min: u64, max: u64) -> Result<u64, TridentError> {
    match std::env::var(key) {
        Err(_) => Ok(default),
        Ok(raw) => {
            let v: u64 = raw.parse().map_err(|_| {
                TridentError::config(anyhow::anyhow!(
                    "[indexer] {key} must be a positive integer, got {raw:?}"
                ))
            })?;
            if v < min || v > max {
                return Err(TridentError::config(anyhow::anyhow!(
                    "[indexer] {key} must be between {min} and {max}, got {v}"
                )));
            }
            Ok(v)
        }
    }
}

/// Parse an optional positive pool-size env var, falling back to `default`.
/// A present-but-invalid value (non-numeric or zero) is a hard configuration
/// error rather than a silent fallback.
fn parse_pool_size(key: &str, default: u32) -> Result<u32, TridentError> {
    match std::env::var(key) {
        Err(_) => Ok(default),
        Ok(raw) => raw.parse::<u32>().ok().filter(|&n| n > 0).ok_or_else(|| {
            TridentError::config(anyhow::anyhow!("{key} must be a positive integer"))
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    /// Process environment is global state shared by every test thread, so all
    /// env-mutating tests serialise on this lock. Without it one test clearing
    /// `REDIS_URL` can fail an unrelated test mid-`from_env`.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn env_guard() -> std::sync::MutexGuard<'static, ()> {
        // A panicking test must not poison the lock for the rest of the suite.
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn with_env<F: FnOnce()>(pairs: &[(&str, &str)], f: F) {
        let _guard = env_guard();
        for (k, v) in pairs {
            env::set_var(k, v);
        }
        f();
        for (k, _) in pairs {
            env::remove_var(k);
        }
    }

    fn required_vars() -> Vec<(&'static str, &'static str)> {
        vec![
            ("DATABASE_URL", "postgres://localhost/test"),
            ("REDIS_URL", "redis://localhost:6379"),
            ("STELLAR_RPC_URL", "https://soroban-testnet.stellar.org"),
        ]
    }

    #[test]
    fn missing_all_required_vars_lists_all_in_error() {
        let _guard = env_guard();
        env::remove_var("DATABASE_URL");
        env::remove_var("REDIS_URL");
        env::remove_var("STELLAR_RPC_URL");
        env::remove_var("POLL_INTERVAL_MS");
        env::remove_var("MAX_EVENTS_PER_POLL");

        let err = Config::from_env().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("DATABASE_URL"),
            "error should mention DATABASE_URL"
        );
        assert!(msg.contains("REDIS_URL"), "error should mention REDIS_URL");
        assert!(
            msg.contains("STELLAR_RPC_URL"),
            "error should mention STELLAR_RPC_URL"
        );
    }

    #[test]
    fn missing_single_required_var_names_it() {
        let _guard = env_guard();
        env::set_var("DATABASE_URL", "postgres://localhost/test");
        env::set_var("STELLAR_RPC_URL", "https://soroban-testnet.stellar.org");
        env::remove_var("REDIS_URL");
        env::remove_var("POLL_INTERVAL_MS");
        env::remove_var("MAX_EVENTS_PER_POLL");

        let err = Config::from_env().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("REDIS_URL"));
        assert!(
            !msg.contains("DATABASE_URL"),
            "DATABASE_URL should not appear"
        );

        env::remove_var("DATABASE_URL");
        env::remove_var("STELLAR_RPC_URL");
    }

    #[test]
    fn poll_interval_default_is_1000ms() {
        let vars = required_vars();
        with_env(&vars, || {
            env::remove_var("POLL_INTERVAL_MS");
            env::remove_var("MAX_EVENTS_PER_POLL");
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.poll_interval.as_millis(), 1000);
        });
    }

    #[test]
    fn poll_interval_custom_value() {
        let mut vars = required_vars();
        vars.push(("POLL_INTERVAL_MS", "500"));
        with_env(&vars, || {
            env::remove_var("MAX_EVENTS_PER_POLL");
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.poll_interval.as_millis(), 500);
        });
    }

    #[test]
    fn poll_interval_below_minimum_is_rejected() {
        let mut vars = required_vars();
        vars.push(("POLL_INTERVAL_MS", "50"));
        with_env(&vars, || {
            env::remove_var("MAX_EVENTS_PER_POLL");
            let err = Config::from_env().unwrap_err();
            assert!(err.to_string().contains("POLL_INTERVAL_MS"));
        });
    }

    #[test]
    fn poll_interval_above_maximum_is_rejected() {
        let mut vars = required_vars();
        vars.push(("POLL_INTERVAL_MS", "90000"));
        with_env(&vars, || {
            env::remove_var("MAX_EVENTS_PER_POLL");
            let err = Config::from_env().unwrap_err();
            assert!(err.to_string().contains("POLL_INTERVAL_MS"));
        });
    }

    #[test]
    fn poll_interval_non_integer_is_rejected() {
        let mut vars = required_vars();
        vars.push(("POLL_INTERVAL_MS", "abc"));
        with_env(&vars, || {
            env::remove_var("MAX_EVENTS_PER_POLL");
            let err = Config::from_env().unwrap_err();
            assert!(err.to_string().contains("POLL_INTERVAL_MS"));
        });
    }

    #[test]
    fn poll_interval_boundary_min_accepted() {
        let mut vars = required_vars();
        vars.push(("POLL_INTERVAL_MS", "100"));
        with_env(&vars, || {
            env::remove_var("MAX_EVENTS_PER_POLL");
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.poll_interval.as_millis(), 100);
        });
    }

    #[test]
    fn poll_interval_boundary_max_accepted() {
        let mut vars = required_vars();
        vars.push(("POLL_INTERVAL_MS", "60000"));
        with_env(&vars, || {
            env::remove_var("MAX_EVENTS_PER_POLL");
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.poll_interval.as_millis(), 60000);
        });
    }

    #[test]
    fn max_events_per_poll_default_is_200() {
        let vars = required_vars();
        with_env(&vars, || {
            env::remove_var("POLL_INTERVAL_MS");
            env::remove_var("MAX_EVENTS_PER_POLL");
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.max_events_per_poll, 200);
        });
    }

    #[test]
    fn max_events_per_poll_custom_value() {
        let mut vars = required_vars();
        vars.push(("MAX_EVENTS_PER_POLL", "500"));
        with_env(&vars, || {
            env::remove_var("POLL_INTERVAL_MS");
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.max_events_per_poll, 500);
        });
    }

    #[test]
    fn max_events_per_poll_below_minimum_is_rejected() {
        let mut vars = required_vars();
        vars.push(("MAX_EVENTS_PER_POLL", "0"));
        with_env(&vars, || {
            env::remove_var("POLL_INTERVAL_MS");
            let err = Config::from_env().unwrap_err();
            assert!(err.to_string().contains("MAX_EVENTS_PER_POLL"));
        });
    }

    #[test]
    fn max_events_per_poll_above_maximum_is_rejected() {
        let mut vars = required_vars();
        vars.push(("MAX_EVENTS_PER_POLL", "10001"));
        with_env(&vars, || {
            env::remove_var("POLL_INTERVAL_MS");
            let err = Config::from_env().unwrap_err();
            assert!(err.to_string().contains("MAX_EVENTS_PER_POLL"));
        });
    }

    #[test]
    fn max_events_per_poll_invalid_string_is_rejected() {
        let mut vars = required_vars();
        vars.push(("MAX_EVENTS_PER_POLL", "not-a-number"));
        with_env(&vars, || {
            env::remove_var("POLL_INTERVAL_MS");
            let err = Config::from_env().unwrap_err();
            assert!(err.to_string().contains("MAX_EVENTS_PER_POLL"));
        });
    }

    #[test]
    fn max_events_per_poll_boundary_min_accepted() {
        let mut vars = required_vars();
        vars.push(("MAX_EVENTS_PER_POLL", "1"));
        with_env(&vars, || {
            env::remove_var("POLL_INTERVAL_MS");
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.max_events_per_poll, 1);
        });
    }

    #[test]
    fn max_events_per_poll_boundary_max_accepted() {
        let mut vars = required_vars();
        vars.push(("MAX_EVENTS_PER_POLL", "10000"));
        with_env(&vars, || {
            env::remove_var("POLL_INTERVAL_MS");
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.max_events_per_poll, 10000);
        });
    }

    #[test]
    fn endpoint_list_prefers_the_multi_value_form() {
        let urls = parse_endpoint_list(
            Some("https://a.example, https://b.example".into()),
            Some("https://legacy.example".into()),
        )
        .unwrap();
        assert_eq!(urls, vec!["https://a.example", "https://b.example"]);
    }

    #[test]
    fn endpoint_list_falls_back_to_single_value_alias() {
        let urls = parse_endpoint_list(None, Some("https://legacy.example".into())).unwrap();
        assert_eq!(urls, vec!["https://legacy.example"]);
    }

    #[test]
    fn endpoint_list_drops_duplicates_and_blanks() {
        let urls = parse_endpoint_list(
            Some("https://a.example,,  ,https://a.example,https://b.example".into()),
            None,
        )
        .unwrap();
        assert_eq!(urls, vec!["https://a.example", "https://b.example"]);
    }

    #[test]
    fn endpoint_list_rejects_non_http_scheme() {
        let err = parse_endpoint_list(Some("ftp://a.example".into()), None).unwrap_err();
        assert!(err.to_string().contains("http://"));
    }

    #[test]
    fn endpoint_list_is_empty_when_nothing_is_configured() {
        assert!(parse_endpoint_list(None, None).unwrap().is_empty());
    }

    #[test]
    fn multiple_endpoints_are_read_from_env() {
        let mut vars = required_vars();
        vars.push((
            "STELLAR_RPC_URLS",
            "https://primary.example,https://secondary.example",
        ));
        with_env(&vars, || {
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.stellar_rpc_urls.len(), 2);
            assert_eq!(cfg.stellar_rpc_url, "https://primary.example");
            env::remove_var("STELLAR_RPC_URLS");
        });
    }

    #[test]
    fn failover_knobs_have_defaults() {
        let vars = required_vars();
        with_env(&vars, || {
            env::remove_var("RPC_FAILOVER_THRESHOLD");
            env::remove_var("RPC_ENDPOINT_COOLDOWN_MS");
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.rpc_failover_threshold, 3);
            assert_eq!(cfg.rpc_endpoint_cooldown.as_millis(), 30_000);
        });
    }

    #[test]
    fn db_batch_size_defaults_to_1000() {
        let vars = required_vars();
        with_env(&vars, || {
            env::remove_var("DB_BATCH_SIZE");
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.db_batch_size, 1_000);
        });
    }

    #[test]
    fn outbox_relay_knobs_have_defaults() {
        let vars = required_vars();
        with_env(&vars, || {
            for key in [
                "OUTBOX_POLL_INTERVAL_MS",
                "OUTBOX_BATCH_SIZE",
                "OUTBOX_BACKLOG_ALERT_THRESHOLD",
            ] {
                env::remove_var(key);
            }
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.outbox_poll_interval.as_millis(), 100);
            assert_eq!(cfg.outbox_batch_size, 500);
            assert_eq!(cfg.outbox_backlog_alert_threshold, 10_000);
        });
    }

    #[test]
    fn db_batch_size_custom_value() {
        let mut vars = required_vars();
        vars.push(("DB_BATCH_SIZE", "250"));
        with_env(&vars, || {
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.db_batch_size, 250);
        });
    }

    #[test]
    fn db_batch_size_zero_is_rejected() {
        let mut vars = required_vars();
        vars.push(("DB_BATCH_SIZE", "0"));
        with_env(&vars, || {
            let err = Config::from_env().unwrap_err();
            assert!(err.to_string().contains("DB_BATCH_SIZE"));
        });
    }

    #[test]
    fn topic_filters_default_to_empty() {
        let vars = required_vars();
        with_env(&vars, || {
            env::remove_var("INDEX_TOPIC_FILTERS");
            let cfg = Config::from_env().unwrap();
            assert!(cfg.topic_filters.is_empty());
        });
    }

    #[test]
    fn outbox_batch_size_is_bounded() {
        let mut vars = required_vars();
        vars.push(("OUTBOX_BATCH_SIZE", "0"));
        with_env(&vars, || {
            let err = Config::from_env().unwrap_err();
            assert!(err.to_string().contains("OUTBOX_BATCH_SIZE"));
        });
    }

    #[test]
    fn rpc_timeouts_have_sensible_defaults() {
        let vars = required_vars();
        with_env(&vars, || {
            for key in [
                "RPC_CONNECT_TIMEOUT_MS",
                "RPC_REQUEST_TIMEOUT_MS",
                "RPC_POOL_IDLE_TIMEOUT_MS",
                "RPC_POOL_MAX_IDLE_PER_HOST",
                "RPC_TCP_KEEPALIVE_MS",
            ] {
                env::remove_var(key);
            }
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.rpc_connect_timeout.as_millis(), 5_000);
            assert_eq!(cfg.rpc_request_timeout.as_millis(), 30_000);
            assert_eq!(cfg.rpc_pool_idle_timeout.as_millis(), 90_000);
            assert_eq!(cfg.rpc_pool_max_idle_per_host, 8);
            assert_eq!(cfg.rpc_tcp_keepalive.as_millis(), 60_000);
        });
    }

    #[test]
    fn rpc_timeouts_are_env_configurable() {
        let mut vars = required_vars();
        vars.push(("RPC_CONNECT_TIMEOUT_MS", "1500"));
        vars.push(("RPC_REQUEST_TIMEOUT_MS", "9000"));
        vars.push(("RPC_POOL_MAX_IDLE_PER_HOST", "32"));
        with_env(&vars, || {
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.rpc_connect_timeout.as_millis(), 1_500);
            assert_eq!(cfg.rpc_request_timeout.as_millis(), 9_000);
            assert_eq!(cfg.rpc_pool_max_idle_per_host, 32);
        });
    }

    #[test]
    fn topic_filters_parsed_from_spec() {
        let mut vars = required_vars();
        vars.push(("INDEX_TOPIC_FILTERS", "transfer/*/*"));
        with_env(&vars, || {
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.topic_filters.len(), 1);
            assert_eq!(cfg.topic_filters[0].len(), 3);
        });
    }

    #[test]
    fn rpc_request_timeout_below_connect_timeout_is_rejected() {
        let mut vars = required_vars();
        vars.push(("RPC_CONNECT_TIMEOUT_MS", "10000"));
        vars.push(("RPC_REQUEST_TIMEOUT_MS", "1000"));
        with_env(&vars, || {
            let err = Config::from_env().unwrap_err();
            assert!(err.to_string().contains("RPC_REQUEST_TIMEOUT_MS"));
        });
    }

    #[test]
    fn rpc_timeout_out_of_bounds_is_rejected() {
        let mut vars = required_vars();
        vars.push(("RPC_CONNECT_TIMEOUT_MS", "0"));
        with_env(&vars, || {
            let err = Config::from_env().unwrap_err();
            assert!(err.to_string().contains("RPC_CONNECT_TIMEOUT_MS"));
        });
    }

    #[test]
    fn invalid_topic_filter_spec_is_rejected() {
        let mut vars = required_vars();
        vars.push(("INDEX_TOPIC_FILTERS", "a/b/c/d/e"));
        with_env(&vars, || {
            let err = Config::from_env().unwrap_err();
            assert!(err.to_string().contains("INDEX_TOPIC_FILTERS"));
        });
    }

    #[test]
    fn token_metadata_refresh_interval_defaults_to_24h() {
        let vars = required_vars();
        with_env(&vars, || {
            env::remove_var("TOKEN_METADATA_REFRESH_INTERVAL_SECS");
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.token_metadata_refresh_interval.as_secs(), 86_400);
        });
    }

    #[test]
    fn parse_pool_size_uses_default_when_unset() {
        std::env::remove_var("TEST_POOL_UNSET");
        assert_eq!(parse_pool_size("TEST_POOL_UNSET", 7).unwrap(), 7);
    }

    #[test]
    fn parse_pool_size_reads_valid_value() {
        std::env::set_var("TEST_POOL_VALID", "12");
        assert_eq!(parse_pool_size("TEST_POOL_VALID", 3).unwrap(), 12);
        std::env::remove_var("TEST_POOL_VALID");
    }

    #[test]
    fn parse_pool_size_rejects_zero_and_garbage() {
        std::env::set_var("TEST_POOL_BAD", "0");
        assert!(parse_pool_size("TEST_POOL_BAD", 3).is_err());
        std::env::set_var("TEST_POOL_BAD", "abc");
        assert!(parse_pool_size("TEST_POOL_BAD", 3).is_err());
        std::env::remove_var("TEST_POOL_BAD");
    }
}
