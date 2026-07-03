use std::time::Duration;

use chrono::{DateTime, Utc};
use opentelemetry::propagation::Extractor;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use sqlx::PgPool;
use tonic::{Request, Response, Status};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::trident::{
    events_server::Events, Event, GetEventRequest, ListEventsRequest, ListEventsResponse,
    StreamEventsRequest,
};

// ---------------------------------------------------------------------------
// W3C TraceContext extraction from tonic metadata
// ---------------------------------------------------------------------------

struct MetadataCarrier<'a>(&'a tonic::metadata::MetadataMap);

impl<'a> Extractor for MetadataCarrier<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .filter_map(|k| {
                if let tonic::metadata::KeyRef::Ascii(k) = k {
                    Some(k.as_str())
                } else {
                    None
                }
            })
            .collect()
    }
}

fn extract_context(metadata: &tonic::metadata::MetadataMap) -> opentelemetry::Context {
    opentelemetry::global::get_text_map_propagator(|prop| prop.extract(&MetadataCarrier(metadata)))
}

const REDIS_STREAM_KEY: &str = "trident:events";
const STREAM_CHANNEL_BUF: usize = 128;
const DEFAULT_NETWORK: &str = "testnet";

pub struct EventsServiceImpl {
    pub db: PgPool,
    pub redis: redis::aio::ConnectionManager,
}

impl EventsServiceImpl {
    pub fn new(db: PgPool, redis: redis::aio::ConnectionManager) -> Self {
        Self { db, redis }
    }
}

// ---------------------------------------------------------------------------
// DB row → proto conversion
// ---------------------------------------------------------------------------

#[derive(sqlx::FromRow)]
struct EventRow {
    id: Uuid,
    contract_id: String,
    ledger_sequence: i64,
    ledger_timestamp: DateTime<Utc>,
    transaction_hash: String,
    event_index: i32,
    event_type: String,
    topics: serde_json::Value,
    data: serde_json::Value,
    created_at: DateTime<Utc>,
}

fn row_to_event(row: EventRow) -> Event {
    let topics: Vec<String> = serde_json::from_value(row.topics).unwrap_or_default();
    Event {
        id: row.id.to_string(),
        contract_id: row.contract_id,
        ledger_sequence: row.ledger_sequence as u64,
        ledger_timestamp: row.ledger_timestamp.to_rfc3339(),
        transaction_hash: row.transaction_hash,
        event_index: row.event_index as u32,
        event_type: row.event_type,
        topics,
        data: row.data.to_string(),
        created_at: row.created_at.to_rfc3339(),
    }
}

fn db_err(e: sqlx::Error) -> Status {
    tracing::error!(error = %e, "database error");
    Status::internal("internal error")
}

/// Normalise a network string — defaults to "testnet" when empty.
fn resolve_network(network: &str) -> &str {
    if network.is_empty() {
        DEFAULT_NETWORK
    } else {
        network
    }
}

// ---------------------------------------------------------------------------
// gRPC service implementation
// ---------------------------------------------------------------------------

#[tonic::async_trait]
impl Events for EventsServiceImpl {
    async fn list_events(
        &self,
        request: Request<ListEventsRequest>,
    ) -> Result<Response<ListEventsResponse>, Status> {
        let parent_cx = extract_context(request.metadata());
        let span = tracing::info_span!("list_events", "rpc.system" = "grpc");
        span.set_parent(parent_cx);

        let db = self.db.clone();
        let req = request.into_inner();
        let contract_id_attr = req.contract_id.clone();
        span.record("contract_id", contract_id_attr.as_str());

        async move {
            let limit = req.limit.clamp(1, 200) as i64;
            let network = resolve_network(&req.network);

            let (cursor_seq, cursor_idx): (Option<i64>, Option<i32>) = if req.cursor.is_empty() {
                (None, None)
            } else {
                let cursor_id = Uuid::parse_str(&req.cursor)
                    .map_err(|_| Status::invalid_argument("cursor must be a valid UUID"))?;

                let row: Option<(i64, i32)> = sqlx::query_as(
                    "SELECT ledger_sequence, event_index FROM soroban_events WHERE id = $1",
                )
                .bind(cursor_id)
                .fetch_optional(&db)
                .await
                .map_err(db_err)?;

                match row {
                    Some((seq, idx)) => (Some(seq), Some(idx)),
                    None => {
                        return Err(Status::invalid_argument("cursor references unknown event"))
                    }
                }
            };

            let rows: Vec<EventRow> = sqlx::query_as(
                r#"
                SELECT id, contract_id, ledger_sequence, ledger_timestamp,
                       transaction_hash, event_index, event_type, topics, data, created_at
                FROM soroban_events
                WHERE
                    network = $1
                    AND ($2::text = '' OR contract_id = $2)
                    AND ($3::text = '' OR topic_0 = $3)
                    AND ($4::text = '' OR topic_1 = $4)
                    AND ($5::bigint = 0 OR ledger_sequence >= $5)
                    AND ($6::bigint = 0 OR ledger_sequence <= $6)
                    AND (
                        $7::bigint IS NULL
                        OR (ledger_sequence, event_index) > ($7, $8)
                    )
                ORDER BY ledger_sequence ASC, event_index ASC
                LIMIT $9
                "#,
            )
            .bind(network)
            .bind(&req.contract_id)
            .bind(&req.topic_0)
            .bind(&req.topic_1)
            .bind(req.ledger_from as i64)
            .bind(req.ledger_to as i64)
            .bind(cursor_seq)
            .bind(cursor_idx)
            .bind(limit)
            .fetch_all(&db)
            .await
            .map_err(db_err)?;

            let has_more = rows.len() as i64 == limit;
            let next_cursor = if has_more {
                rows.last().map(|r| r.id.to_string()).unwrap_or_default()
            } else {
                String::new()
            };

            let events: Vec<Event> = rows.into_iter().map(row_to_event).collect();

            Ok(Response::new(ListEventsResponse {
                events,
                next_cursor,
                has_more,
            }))
        }
        .instrument(span)
        .await
    }

    async fn get_event(
        &self,
        request: Request<GetEventRequest>,
    ) -> Result<Response<Event>, Status> {
        let parent_cx = extract_context(request.metadata());
        let span = tracing::info_span!("get_event", "rpc.system" = "grpc");
        span.set_parent(parent_cx);

        let db = self.db.clone();
        let req = request.into_inner();

        async move {
            let id = Uuid::parse_str(&req.id)
                .map_err(|_| Status::invalid_argument("id must be a valid UUID"))?;

            let network = resolve_network(&req.network);

            let row: Option<EventRow> = sqlx::query_as(
                r#"
            SELECT id, contract_id, ledger_sequence, ledger_timestamp,
                   transaction_hash, event_index, event_type, topics, data, created_at
            FROM soroban_events
            WHERE id = $1
              AND network = $2
            "#,
            )
            .bind(id)
            .bind(network)
            .fetch_optional(&db)
            .await
            .map_err(db_err)?;

            match row {
                Some(r) => Ok(Response::new(row_to_event(r))),
                None => Err(Status::not_found(format!("event {id} not found"))),
            }
        }
        .instrument(span)
        .await
    }

    type StreamEventsStream = tokio_stream::wrappers::ReceiverStream<Result<Event, Status>>;

    async fn stream_events(
        &self,
        request: Request<StreamEventsRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        let parent_cx = extract_context(request.metadata());
        let span = tracing::info_span!("stream_events", "rpc.system" = "grpc");
        span.set_parent(parent_cx);
        let _entered = span.entered();

        let req = request.into_inner();

        if req.contract_id.is_empty() {
            return Err(Status::invalid_argument("contract_id is required"));
        }

        let contract_id = req.contract_id;
        let topic_0_filter = if req.topic_0.is_empty() {
            None
        } else {
            Some(req.topic_0)
        };

        let (tx, rx) = tokio::sync::mpsc::channel(STREAM_CHANNEL_BUF);
        let mut redis = self.redis.clone();

        tokio::spawn(async move {
            let mut last_id = "$".to_string();
            let opts = StreamReadOptions::default().block(5_000).count(100);

            loop {
                let reply: redis::RedisResult<StreamReadReply> = redis
                    .xread_options(&[REDIS_STREAM_KEY], &[&last_id], &opts)
                    .await;

                match reply {
                    Ok(StreamReadReply { keys }) => {
                        for stream_key in keys {
                            for entry in stream_key.ids {
                                last_id = entry.id.clone();

                                let get = |field: &str| -> String {
                                    entry
                                        .map
                                        .get(field)
                                        .and_then(|v| {
                                            if let redis::Value::Data(b) = v {
                                                String::from_utf8(b.clone()).ok()
                                            } else {
                                                None
                                            }
                                        })
                                        .unwrap_or_default()
                                };

                                if get("contract_id") != contract_id {
                                    continue;
                                }

                                if let Some(ref t0) = topic_0_filter {
                                    let topics_json = get("topics");
                                    let topics: Vec<String> =
                                        serde_json::from_str(&topics_json).unwrap_or_default();
                                    if topics.first().map(String::as_str) != Some(t0.as_str()) {
                                        continue;
                                    }
                                }

                                let topics_json = get("topics");
                                let topics: Vec<String> =
                                    serde_json::from_str(&topics_json).unwrap_or_default();

                                let event = Event {
                                    id: String::new(),
                                    contract_id: get("contract_id"),
                                    ledger_sequence: get("ledger_sequence").parse().unwrap_or(0),
                                    ledger_timestamp: get("ledger_timestamp"),
                                    transaction_hash: get("transaction_hash"),
                                    event_index: get("event_index").parse().unwrap_or(0),
                                    event_type: get("event_type"),
                                    topics,
                                    data: get("data"),
                                    created_at: String::new(),
                                };

                                if tx.send(Ok(event)).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Redis XREAD error in stream_events, retrying");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        if tx.is_closed() {
                            return;
                        }
                    }
                }

                if tx.is_closed() {
                    return;
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
}

#[cfg(test)]
mod tests {
    use tokio_stream::StreamExt as _;

    use super::*;

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

    async fn make_svc(db_url: &str, redis_url: &str) -> EventsServiceImpl {
        let db = PgPool::connect(db_url).await.unwrap();
        let redis = redis::Client::open(redis_url)
            .unwrap()
            .get_connection_manager()
            .await
            .unwrap();
        EventsServiceImpl::new(db, redis)
    }

    async fn seed_events(pool: &PgPool, contract_id: &str, network: &str, count: usize) {
        for i in 0..count {
            sqlx::query(
                r#"
                INSERT INTO soroban_events
                    (contract_id, network, ledger_sequence, ledger_timestamp, transaction_hash,
                     event_index, event_type, topics, data)
                VALUES ($1, $2, $3, NOW(), $4, $5, 'contract', '["transfer"]', '{}')
                ON CONFLICT DO NOTHING
                "#,
            )
            .bind(contract_id)
            .bind(network)
            .bind((100 + i) as i64)
            .bind(format!("txhash_{contract_id}_{network}_{i}"))
            .bind(i as i32)
            .execute(pool)
            .await
            .unwrap();
        }
    }

    async fn insert_one_event(pool: &PgPool, network: &str) -> Uuid {
        let id = Uuid::new_v4();
        let tx_hash = format!("txhashtest-{id}");
        sqlx::query(
            r#"
            INSERT INTO soroban_events
                (id, contract_id, network, ledger_sequence, ledger_timestamp, transaction_hash,
                 event_index, event_type, topics, data)
            VALUES ($1, 'CTEST', $2, 999, NOW(), $3, 0, 'contract', '["transfer"]', '{}')
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(id)
        .bind(network)
        .bind(tx_hash)
        .execute(pool)
        .await
        .unwrap();
        id
    }

    #[tokio::test]
    async fn list_events_filters_by_contract_id() {
        let (db_url, redis_url) = require_services!();
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_a = format!("CONTRACT_A_{}", uuid::Uuid::new_v4());
        let contract_b = format!("CONTRACT_B_{}", uuid::Uuid::new_v4());

        seed_events(&pool, &contract_a, "testnet", 3).await;
        seed_events(&pool, &contract_b, "testnet", 2).await;

        let svc = make_svc(&db_url, &redis_url).await;
        let req = Request::new(ListEventsRequest {
            contract_id: contract_a.clone(),
            network: "testnet".to_string(),
            limit: 200,
            ..Default::default()
        });
        let resp = svc.list_events(req).await.unwrap().into_inner();

        assert_eq!(resp.events.len(), 3);
        assert!(resp.events.iter().all(|e| e.contract_id == contract_a));
        assert!(!resp.has_more);
    }

    #[tokio::test]
    async fn list_events_cursor_pagination() {
        let (db_url, redis_url) = require_services!();
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_id = format!("CONTRACT_PAGE_{}", uuid::Uuid::new_v4());
        seed_events(&pool, &contract_id, "testnet", 5).await;

        let svc = make_svc(&db_url, &redis_url).await;

        let first_page = svc
            .list_events(Request::new(ListEventsRequest {
                contract_id: contract_id.clone(),
                network: "testnet".to_string(),
                limit: 2,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(first_page.events.len(), 2);
        assert!(first_page.has_more);
        assert!(!first_page.next_cursor.is_empty());

        let second_page = svc
            .list_events(Request::new(ListEventsRequest {
                contract_id: contract_id.clone(),
                network: "testnet".to_string(),
                limit: 200,
                cursor: first_page.next_cursor,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(second_page.events.len(), 3);
        assert!(!second_page.has_more);
    }

    #[tokio::test]
    async fn list_events_isolated_by_network() {
        let (db_url, redis_url) = require_services!();
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_id = format!("CONTRACT_NET_{}", uuid::Uuid::new_v4());
        seed_events(&pool, &contract_id, "testnet", 3).await;
        seed_events(&pool, &contract_id, "mainnet", 2).await;

        let svc = make_svc(&db_url, &redis_url).await;

        let testnet_resp = svc
            .list_events(Request::new(ListEventsRequest {
                contract_id: contract_id.clone(),
                network: "testnet".to_string(),
                limit: 200,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        let mainnet_resp = svc
            .list_events(Request::new(ListEventsRequest {
                contract_id: contract_id.clone(),
                network: "mainnet".to_string(),
                limit: 200,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(testnet_resp.events.len(), 3);
        assert_eq!(mainnet_resp.events.len(), 2);
    }

    #[tokio::test]
    async fn get_existing_event_returns_correct_fields() {
        let (db_url, redis_url) = require_services!();
        let pool = PgPool::connect(&db_url).await.unwrap();

        let event_id = insert_one_event(&pool, "testnet").await;

        let svc = make_svc(&db_url, &redis_url).await;
        let req = Request::new(GetEventRequest {
            id: event_id.to_string(),
            network: "testnet".to_string(),
        });
        let event = svc.get_event(req).await.unwrap().into_inner();

        assert_eq!(event.id, event_id.to_string());
        assert_eq!(event.contract_id, "CTEST");
        assert_eq!(event.event_type, "contract");
        assert_eq!(event.topics, vec!["transfer".to_string()]);
    }

    #[tokio::test]
    async fn get_event_wrong_network_returns_not_found() {
        let (db_url, redis_url) = require_services!();
        let pool = PgPool::connect(&db_url).await.unwrap();

        let event_id = insert_one_event(&pool, "testnet").await;

        let svc = make_svc(&db_url, &redis_url).await;
        let req = Request::new(GetEventRequest {
            id: event_id.to_string(),
            network: "mainnet".to_string(),
        });
        let err = svc.get_event(req).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn get_unknown_uuid_returns_not_found() {
        let (db_url, redis_url) = require_services!();
        let svc = make_svc(&db_url, &redis_url).await;

        let req = Request::new(GetEventRequest {
            id: Uuid::new_v4().to_string(),
            network: "testnet".to_string(),
        });
        let err = svc.get_event(req).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn get_malformed_uuid_returns_invalid_argument() {
        let (db_url, redis_url) = require_services!();
        let svc = make_svc(&db_url, &redis_url).await;

        let req = Request::new(GetEventRequest {
            id: "not-a-uuid".to_string(),
            network: "testnet".to_string(),
        });
        let err = svc.get_event(req).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn stream_events_delivers_published_event() {
        let (db_url, redis_url) = require_services!();
        let svc = make_svc(&db_url, &redis_url).await;

        let req = Request::new(StreamEventsRequest {
            contract_id: "CTEST_STREAM".to_string(),
            topic_0: String::new(),
        });

        let mut stream = svc.stream_events(req).await.unwrap().into_inner();

        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut pub_conn = redis::Client::open(redis_url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let _: String = redis::cmd("XADD")
            .arg(REDIS_STREAM_KEY)
            .arg("*")
            .arg("contract_id")
            .arg("CTEST_STREAM")
            .arg("ledger_sequence")
            .arg("777")
            .arg("ledger_timestamp")
            .arg("2024-01-01T00:00:00Z")
            .arg("transaction_hash")
            .arg("txhashstream")
            .arg("event_index")
            .arg("0")
            .arg("event_type")
            .arg("contract")
            .arg("topics")
            .arg(r#"["transfer"]"#)
            .arg("data")
            .arg("null")
            .query_async(&mut pub_conn)
            .await
            .unwrap();

        let event: Event = tokio::time::timeout(Duration::from_secs(8), stream.next())
            .await
            .expect("timed out waiting for streamed event")
            .expect("stream ended unexpectedly")
            .expect("stream returned error");

        assert_eq!(event.contract_id, "CTEST_STREAM");
        assert_eq!(event.ledger_sequence, 777);
    }
}
