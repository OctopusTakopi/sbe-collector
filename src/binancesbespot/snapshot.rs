use bytes::Bytes;
use jiff::Timestamp;
use tokio::sync::mpsc::Sender;
use tracing::{error, warn};

use crate::file::{Symbol, Tag, WriteRecord};
use crate::throttler::Throttler;

/// Depth levels requested per snapshot.
///
/// Must move together with [`SNAPSHOT_WEIGHT`]: Binance charges `/api/v3/depth`
/// 5 at a limit up to 100, 25 to 500, 50 to 1000 and 250 to 5000. Dropping to
/// 1000 would cost a fifth of the budget per fetch, at the price of a shallower
/// book in the recording.
const SNAPSHOT_LIMIT: u32 = 5_000;

/// What Binance charges for a [`SNAPSHOT_LIMIT`]-deep snapshot.
pub const SNAPSHOT_WEIGHT: u32 = 250;

/// Fetch the full depth snapshot for `symbol` from the REST API.
pub async fn fetch_snapshot(
    client: &reqwest::Client,
    symbol: &str,
) -> Result<Bytes, anyhow::Error> {
    let url = format!(
        "https://api.binance.com/api/v3/depth?symbol={}&limit={SNAPSHOT_LIMIT}",
        symbol.to_uppercase()
    );
    let response = client
        .get(&url)
        .header("Accept", "application/json")
        .send()
        .await?;
    let status = response.status();
    let body = response.bytes().await?;
    if !status.is_success() {
        let preview = &body[..body.len().min(1024)];
        anyhow::bail!(
            "Binance depth snapshot returned {status}: {}",
            String::from_utf8_lossy(preview)
        );
    }
    Ok(body)
}

/// Background task: fetch a REST depth snapshot for every symbol at startup and
/// then every `interval_secs` seconds (default 3600 = 1 hour). Writes tagged
/// `Rest` records into the same per-symbol zstd file as the SBE stream frames.
///
/// `client` is passed in from `run_collection` so the same connection pool
/// is shared with gap-triggered snapshot fetches — no duplicate Client.
pub async fn snapshot_loop(
    symbols: Vec<Symbol>,
    writer_tx: Sender<WriteRecord>,
    client: reqwest::Client,
    throttler: Throttler,
    interval_secs: u64,
) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
    // tick() fires immediately on the first call, so the first snapshot runs at startup.
    loop {
        ticker.tick().await;

        for symbol in &symbols {
            let result = throttler
                .execute(SNAPSHOT_WEIGHT, fetch_snapshot(&client, symbol))
                .await;

            match result {
                Some(Ok(data)) => {
                    let recv_time = Timestamp::now();
                    let record = WriteRecord {
                        recv_time,
                        symbol: Symbol::clone(symbol),
                        tag: Tag::Rest,
                        data,
                    };
                    if writer_tx.send(record).await.is_err() {
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
