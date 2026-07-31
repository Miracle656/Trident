use std::net::SocketAddr;

use opentelemetry_otlp::WithExportConfig;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

mod alerting;
mod config;
mod db;
mod health;
mod metrics;
mod parser;
mod poll;
mod redis_stream;
mod rpc;
mod spec;
mod storage;
mod streamer;
mod token_metadata;

fn init_tracer() -> Option<opentelemetry_sdk::trace::Tracer> {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok()?;
    let sampling_ratio = std::env::var("OTEL_SAMPLING_RATIO")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.1);

    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    match opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint),
        )
        .with_trace_config(
            opentelemetry_sdk::trace::Config::default()
                .with_sampler(opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(
                    sampling_ratio,
                ))
                .with_resource(opentelemetry_sdk::Resource::new(vec![
                    opentelemetry::KeyValue::new("service.name", "trident-indexer"),
                ])),
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)
    {
        Ok(tracer) => Some(tracer),
        Err(e) => {
            eprintln!("Failed to initialise OpenTelemetry tracer: {e}");
            None
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing(init_tracer());

    tracing::info!("Trident indexer starting");

    let cfg = config::Config::from_env().unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(1);
    });

    metrics::install(cfg.metrics_port)?;

    // Set statement_timeout and idle_in_transaction_session_timeout on every
    // new connection so a pathological query or leaked transaction cannot hold
    // the pool indefinitely (#249).
    let stmt_timeout = cfg.statement_timeout_ms;
    let idle_timeout = cfg.idle_in_transaction_timeout_ms;
    let db_pool = PgPoolOptions::new()
        .max_connections(cfg.db_pool_size)
        .after_connect(move |conn, _| {
            Box::pin(async move {
                sqlx::query(&format!("SET statement_timeout = '{stmt_timeout}ms'"))
                    .execute(&mut *conn)
                    .await?;
                sqlx::query(&format!(
                    "SET idle_in_transaction_session_timeout = '{idle_timeout}ms'"
                ))
                .execute(&mut *conn)
                .await?;
                Ok(())
            })
        })
        .connect(&cfg.database_url)
        .await?;
    tracing::info!(
        pool_size = cfg.db_pool_size,
        statement_timeout_ms = stmt_timeout,
        idle_in_transaction_timeout_ms = idle_timeout,
        "Database connected with timeout defaults"
    );

    let redis_client = redis::Client::open(cfg.redis_url.as_str())?;
    let redis_conn = redis_client.get_multiplexed_async_connection().await?;
    tracing::info!("Redis connected");

    // Spawn health and readiness endpoints on HEALTH_PORT (default 8080, separate
    // from the Prometheus /metrics listener on METRICS_PORT) (#206).
    let health_addr: SocketAddr = ([0, 0, 0, 0], cfg.health_port).into();
    let health_db = db_pool.clone();
    let health_redis_url = cfg.redis_url.clone();
    tokio::spawn(async move {
        health::serve(health_addr, health_db, health_redis_url).await;
    });

    let shutdown = CancellationToken::new();

    // Spawn signal watcher — cancels the token on SIGTERM or SIGINT.
    let shutdown_trigger = shutdown.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm =
                signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Received SIGINT, initiating graceful shutdown");
                }
                _ = sigterm.recv() => {
                    tracing::info!("Received SIGTERM, initiating graceful shutdown");
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("Received SIGINT, initiating graceful shutdown");
        }

        shutdown_trigger.cancel();
    });

    // Outbox relay (issue #200): the poll loop only commits events; this task
    // delivers them to Redis. It runs alongside the streamer and stops on the
    // same shutdown signal.
    let mut relay = redis_stream::relay::OutboxRelay::new(
        db_pool.clone(),
        redis_conn.clone(),
        redis_stream::relay::RelayConfig {
            interval: cfg.outbox_poll_interval,
            batch_size: cfg.outbox_batch_size,
            backlog_alert_threshold: cfg.outbox_backlog_alert_threshold,
            stream_maxlen: cfg.redis_stream_maxlen,
        },
    );
    let relay_shutdown = shutdown.clone();
    let relay_handle = tokio::spawn(async move { relay.run(relay_shutdown).await });

    // Allow the shutdown drain to finish its in-flight work before the process
    // is killed. Kubernetes/Fly terminationGracePeriodSeconds should be ≥ this
    // value + a small buffer (recommended: SHUTDOWN_GRACE_SECS + 5).
    const SHUTDOWN_GRACE_SECS: u64 = 30;

    let mut s = streamer::Streamer::new(cfg, db_pool).await?;
    let result = s.run(shutdown).await;

    // Let the relay drain its current pass before the process exits, bounded
    // by the shutdown grace period (issue #205).
    match tokio::time::timeout(Duration::from_secs(SHUTDOWN_GRACE_SECS), relay_handle).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "Outbox relay task did not shut down cleanly");
        }
        Err(_) => {
            tracing::warn!(
                grace_seconds = SHUTDOWN_GRACE_SECS,
                "Graceful shutdown exceeded grace period; forcing exit"
            );
        }
    }
    result?;

    tracing::info!("Trident indexer stopped");
    Ok(())
}

/// Initialise tracing/logging, optionally enabling the tokio-console async
/// profiler.
///
/// tokio-console is opt-in on two levels so it can never be reached in a normal
/// deployment: it is compiled out unless the `tokio-console` cargo feature is
/// built, and even then it stays off unless `TOKIO_CONSOLE_ENABLED=true`. The
/// console server binds to `127.0.0.1:6669` by default (see console-subscriber
/// docs / `TOKIO_CONSOLE_BIND`), so it is not publicly reachable.
fn init_tracing(tracer: Option<opentelemetry_sdk::trace::Tracer>) {
    // Shared JSON log schema (#294) composed with OpenTelemetry trace export
    // (#290) so log lines and traces share the same correlation ids.
    let otel_layer = tracer.map(|t| tracing_opentelemetry::layer().with_tracer(t));

    #[cfg(feature = "tokio-console")]
    if std::env::var("TOKIO_CONSOLE_ENABLED").as_deref() == Ok("true") {
        use tracing_subscriber::Layer;
        let console_layer = console_subscriber::spawn();
        let json_layer =
            trident_common::logging::JsonLayer::new("trident-indexer", std::io::stdout)
                .with_filter(default_filter());
        // OTel export is intentionally omitted while attached to tokio-console —
        // this is a local debug mode, not a production tracing path.
        tracing_subscriber::registry()
            .with(console_layer)
            .with(json_layer)
            .init();
        tracing::warn!(
            "tokio-console ENABLED on 127.0.0.1:6669 — internal/debug use only, never expose publicly"
        );
        return;
    }

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(trident_common::logging::JsonLayer::new(
            "trident-indexer",
            std::io::stdout,
        ))
        .with(otel_layer)
        .init();
}

#[cfg(feature = "tokio-console")]
fn default_filter() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
}
