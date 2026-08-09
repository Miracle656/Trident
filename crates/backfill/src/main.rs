use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use sqlx::PgPool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{sleep, Duration, Instant};
use tracing_subscriber::EnvFilter;

mod db;
mod parser;
mod rpc_client;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    from_ledger: u64,

    #[arg(long)]
    to_ledger: u64,

    #[arg(long)]
    contract: Option<String>,

    #[arg(long, default_value_t = 4)]
    workers: usize,

    #[arg(long, default_value_t = 0)]
    rpc_delay_ms: u64,

    #[arg(long)]
    dry_run: bool,

    #[arg(long, default_value_t = String::from("testnet"))]
    network: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    tracing::info!(
        from = args.from_ledger,
        to = args.to_ledger,
        workers = args.workers,
        "Starting backfill"
    );

    let db = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;

    let total_ledgers = args.to_ledger - args.from_ledger + 1;
    let pb = ProgressBar::new(total_ledgers);
    pb.set_style(
        ProgressStyle::with_template("{msg} {bar:40.cyan/blue} {pos}/{len} ({percent}%) {eta}")?
            .progress_chars("=> "),
    );
    pb.set_message(format!(
        "Backfilling ledgers {}–{}:",
        args.from_ledger, args.to_ledger
    ));

    let (tx, rx) = mpsc::channel::<(u64, u64)>(args.workers * 2);

    // Split range into chunks for workers
    let chunk_size = (total_ledgers as usize).div_ceil(args.workers);
    let mut start = args.from_ledger;
    while start <= args.to_ledger {
        let end = std::cmp::min(start + chunk_size as u64 - 1, args.to_ledger);
        tx.send((start, end)).await?;
        start = end + 1;
    }
    drop(tx);

    let events_indexed = Arc::new(AtomicU64::new(0));
    let duplicates_skipped = Arc::new(AtomicU64::new(0));

    let rpc = Arc::new(rpc_client::RpcClient::new(std::env::var(
        "STELLAR_RPC_URL",
    )?));

    let rx = Arc::new(Mutex::new(rx));
    let mut handles = vec![];

    // Spawn worker tasks
    for _ in 0..args.workers {
        let rx = Arc::clone(&rx);
        let rpc = rpc.clone();
        let db = db.clone();
        let parser = parser::Parser::new(false);
        let contract = args.contract.clone();
        let dry_run = args.dry_run;
        let events_indexed = events_indexed.clone();
        let duplicates_skipped = duplicates_skipped.clone();
        let pb = pb.clone();
        let rpc_delay = args.rpc_delay_ms;
        let network = args.network.clone();

        let handle = tokio::spawn(async move {
            while let Some((s, e)) = rx.lock().await.recv().await {
                tracing::info!(start = s, end = e, "Worker got range");
                let mut page_cursor: Option<String> = None;
                let mut seq = s;
                while seq <= e {
                    match rpc.get_events(Some(seq), page_cursor.clone()).await {
                        Ok(page) => {
                            if page.events.is_empty() {
                                break;
                            }
                            for raw in &page.events {
                                match parser.parse_event(raw) {
                                    Ok(Some(ev)) => {
                                        if let Some(ref c) = contract {
                                            if &ev.contract_id != c {
                                                continue;
                                            }
                                        }
                                        if dry_run {
                                            // count only
                                            events_indexed.fetch_add(1, Ordering::Relaxed);
                                            if events_indexed.load(Ordering::Relaxed) <= 10 {
                                                println!("DRY: event {:?}", ev);
                                            }
                                        } else {
                                            match db::insert_event(&db, &ev, &network).await {
                                                Ok(_) => {
                                                    events_indexed.fetch_add(1, Ordering::Relaxed);
                                                }
                                                Err(_) => {
                                                    duplicates_skipped
                                                        .fetch_add(1, Ordering::Relaxed);
                                                }
                                            }
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(e) => tracing::warn!(error = %e, "parse error"),
                                }
                            }

                            // advance to last event ledger
                            if let Some(last) = page.events.last() {
                                if let Ok(last_seq) = last.ledger.parse::<u64>() {
                                    seq = last_seq + 1;
                                    pb.inc(last_seq - s + 1);
                                } else {
                                    seq += 1;
                                    pb.inc(1);
                                }
                            } else {
                                break;
                            }

                            if page.events.len() < 200 {
                                break;
                            }

                            page_cursor = page.events.last().map(|e| e.page_cursor());

                            if rpc_delay > 0 {
                                sleep(Duration::from_millis(rpc_delay)).await;
                            }
                        }
                        Err(err) => {
                            if let trident_common::TridentError::RpcError { source, .. } = &err {
                                tracing::warn!(error = %source, "RPC error");
                            } else {
                                tracing::warn!(error = %err, "RPC error");
                            }
                            sleep(Duration::from_millis(500)).await;
                        }
                    }
                }
            }
        });

        handles.push(handle);
    }

    let start_time = Instant::now();
    for h in handles {
        let _ = h.await;
    }

    pb.finish_and_clear();

    let duration = start_time.elapsed();
    let events = events_indexed.load(Ordering::Relaxed);
    let dups = duplicates_skipped.load(Ordering::Relaxed);

    let summary = serde_json::json!({
        "ledgers_processed": total_ledgers,
        "events_indexed": events,
        "duplicates_skipped": dups,
        "duration_seconds": duration.as_secs()
    });

    println!("{}", serde_json::to_string_pretty(&summary)?);

    Ok(())
}
