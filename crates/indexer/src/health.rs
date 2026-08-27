/// Minimal HTTP server providing /healthz (liveness) and /readyz (readiness)
/// endpoints on HEALTH_PORT (default 8080) (#206).
///
/// Prometheus /metrics are served separately by metrics-exporter-prometheus on
/// METRICS_PORT (default 9090). This server handles only Kubernetes probes.
///
/// The server runs in its own tokio task and is never intentionally stopped —
/// the process exiting is the shutdown mechanism. This is intentional for an
/// internal Kubernetes probe server: there is no value in a graceful drain.
use std::net::SocketAddr;

use sqlx::PgPool;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

pub async fn serve(addr: SocketAddr, db: PgPool, redis_url: String) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("health server failed to bind {addr}: {e}");
            return;
        }
    };
    tracing::info!("health server listening on {addr}");

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("health server accept error: {e}");
                continue;
            }
        };
        let db = db.clone();
        let redis_url = redis_url.clone();
        tokio::spawn(handle_conn(stream, db, redis_url));
    }
}

async fn handle_conn(stream: tokio::net::TcpStream, db: PgPool, redis_url: String) {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let mut first_line = String::new();
    if reader.read_line(&mut first_line).await.is_err() {
        return;
    }

    // Drain the remaining HTTP headers so the client doesn't get a RST.
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(0) | Err(_) => break,
            _ if line == "\r\n" || line.trim().is_empty() => break,
            _ => {}
        }
    }

    let path = first_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .split('?')
        .next()
        .unwrap_or("/");

    let (code, phrase, content_type, body): (u16, &str, &str, String) = match path {
        "/healthz" => (200, "OK", "text/plain", "ok\n".into()),
        "/readyz" => {
            let (code, phrase, body) = readyz(&db, &redis_url).await;
            (code, phrase, "text/plain", body)
        }
        // Admin endpoint: return recent dead-lettered (parse error) events
        // for operational inspection (issue #414).
        "/admin/dead-letter" => {
            let (code, phrase, body) = dead_letter_summary(&db).await;
            (code, phrase, "application/json", body)
        }
        _ => (404, "Not Found", "text/plain", "not found\n".into()),
    };

    let response = format!(
        "HTTP/1.1 {code} {phrase}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    );
    let _ = write_half.write_all(response.as_bytes()).await;
}

async fn readyz(db: &PgPool, redis_url: &str) -> (u16, &'static str, String) {
    let db_ok = sqlx::query("SELECT 1").execute(db).await.is_ok();
    let redis_ok = check_redis(redis_url).await;

    if db_ok && redis_ok {
        (200, "OK", "ready\n".into())
    } else {
        let mut reasons = Vec::new();
        if !db_ok {
            reasons.push("db");
        }
        if !redis_ok {
            reasons.push("redis");
        }
        (
            503,
            "Service Unavailable",
            format!("not ready: {}\n", reasons.join(", ")),
        )
    }
}

async fn check_redis(redis_url: &str) -> bool {
    let client = match redis::Client::open(redis_url) {
        Ok(c) => c,
        Err(_) => return false,
    };
    match client.get_multiplexed_async_connection().await {
        Ok(mut conn) => redis::cmd("PING")
            .query_async::<_, redis::Value>(&mut conn)
            .await
            .is_ok(),
        Err(_) => false,
    }
}

/// Return the 20 most recent dead-lettered events as a JSON array.
async fn dead_letter_summary(db: &PgPool) -> (u16, &'static str, String) {
    match sqlx::query_as::<_, DeadLetterRow>(
        r#"SELECT id, ledger_sequence, event_index, error_message, created_at
           FROM parse_errors
           ORDER BY id DESC
           LIMIT 20"#,
    )
    .fetch_all(db)
    .await
    {
        Ok(rows) => {
            let entries: Vec<serde_json::Value> = rows
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "id": r.id,
                        "ledger_sequence": r.ledger_sequence,
                        "event_index": r.event_index,
                        "error_message": r.error_message,
                        "created_at": r.created_at,
                    })
                })
                .collect();
            let body = serde_json::to_string(&entries).unwrap_or_else(|_| "[]".into());
            (200, "OK", body)
        }
        Err(e) => (
            500,
            "Internal Server Error",
            format!("{{\"error\": \"{e}\"}}"),
        ),
    }
}

#[derive(sqlx::FromRow)]
struct DeadLetterRow {
    id: i64,
    ledger_sequence: i64,
    event_index: i32,
    error_message: String,
    created_at: chrono::NaiveDateTime,
}
