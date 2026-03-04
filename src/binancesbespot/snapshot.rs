use bytes::Bytes;
use jiff::Timestamp;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, warn};

use crate::file::{Tag, WriteRecord};
use crate::throttler::Throttler;

/// Fetch the full depth snapshot for `symbol` from the REST API.
pub async fn fetch_snapshot(
    client: &reqwest::Client,
    symbol: &str,
) -> Result<Bytes, reqwest::Error> {
    let url = format!(
        "https://api.binance.com/api/v3/depth?symbol={}&limit=5000",
        symbol.to_uppercase()
    );
    let bytes = client
        .get(&url)
        .header("Accept", "application/json")
        .send()
        .await?
        .bytes()
        .await?;
    Ok(bytes)
}

/// Background task: fetch a REST depth snapshot for every symbol at startup and
/// then every `interval_secs` seconds (default 3600 = 1 hour). Writes tagged
/// `Rest` records into the same per-symbol zstd file as the SBE stream frames.
///
/// `client` is passed in from `run_collection` so the same connection pool
/// is shared with gap-triggered snapshot fetches — no duplicate Client.
pub async fn snapshot_loop(
    symbols: Vec<String>,
    writer_tx: UnboundedSender<WriteRecord>,
    client: reqwest::Client,
    throttler: Throttler,
    interval_secs: u64,
) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
    // tick() fires immediately on the first call, so the first snapshot runs at startup.
    loop {
        ticker.tick().await;

        for symbol in &symbols {
            // Symbols are stored lowercase in the writer; normalise here.
            let symbol_lower = symbol.to_lowercase();

            let result = throttler.execute(fetch_snapshot(&client, symbol)).await;

            match result {
                Some(Ok(data)) => {
                    let recv_time = Timestamp::now();
                    let record = WriteRecord {
                        recv_time,
                        symbol: symbol_lower,
                        tag: Tag::Rest,
                        data,
                    };
                    if writer_tx.send(record).is_err() {
                        return; // channel closed — collector is shutting down
                    }
                }
                Some(Err(e)) => {
                    error!(symbol = %symbol, error = %e, "failed to fetch depth snapshot");
                }
                None => {
                    warn!(symbol = %symbol, "snapshot fetch rate-limited, skipping");
                }
            }
        }
    }
}
