//! Binance spot's REST depth snapshot, 5000 levels a side, recorded as the
//! venue sent it.

use std::future::Future;

use bytes::Bytes;
use jiff::Timestamp;

use super::ws::Endpoint;
use crate::{
    file::Symbol,
    session::Protocol,
    snapshot::Depth,
    throttler::{self, Throttler},
};

/// Depth levels requested per snapshot.
///
/// Moves with [`SNAPSHOT_WEIGHT`]: Binance charges `/api/v3/depth` 5 at a limit
/// up to 100, 25 to 500, 50 to 1000 and 250 to 5000.
const SNAPSHOT_LIMIT: u32 = 5_000;

/// What Binance charges for a [`SNAPSHOT_LIMIT`]-deep snapshot.
const SNAPSHOT_WEIGHT: u32 = 250;

/// Spot request weight allowed per IP per minute, as Binance reports it in
/// `exchangeInfo` under `REQUEST_WEIGHT`.
const SPOT_WEIGHT_PER_MINUTE: u32 = 6_000;

/// What snapshots may spend of it, 12 full books a minute: half, because
/// anything else on the host draws from the same per-IP pool, and being
/// wrong costs a ban.
pub(crate) const SNAPSHOT_WEIGHT_BUDGET: u32 = SPOT_WEIGHT_PER_MINUTE / 2;

// Collector's spot snapshots, up to 2000 a minute, fit beside these.
const _: () = assert!(SNAPSHOT_WEIGHT_BUDGET + 2_000 < SPOT_WEIGHT_PER_MINUTE);

/// Whether `body` is a depth snapshot: it opens with the book's update id.
fn is_book(body: &[u8]) -> bool {
    body.starts_with(br#"{"lastUpdateId":"#)
}

fn preview(body: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(&body[..body.len().min(1024)])
}

/// The REST book of a spot symbol.
pub(crate) struct Book;

impl Depth for Book {
    fn label(&self) -> &'static str {
        Endpoint::SOURCE
    }

    fn weight(&self) -> u32 {
        SNAPSHOT_WEIGHT
    }

    fn fetch(
        &self,
        client: reqwest::Client,
        throttler: Throttler,
        symbol: Symbol,
    ) -> impl Future<Output = anyhow::Result<Bytes>> + Send + use<> {
        async move {
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
            // Headers must be read before the body consumes the response.
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let body = response.bytes().await?;
            if !status.is_success() {
                // 418 is Binance's ban; 429 is the warning shot before one.
                if matches!(status.as_u16(), 418 | 429) {
                    throttler.note_ban(throttler::ban_expiry(
                        retry_after.as_deref(),
                        &body,
                        Timestamp::now(),
                    ));
                }
                anyhow::bail!(
                    "Binance depth snapshot returned {status}: {}",
                    preview(&body)
                );
            }
            // Recorded as a book, an error or a proxy's page under 200 would
            // read as one to every tool downstream.
            if !is_book(&body) {
                anyhow::bail!("Binance depth snapshot is not a book: {}", preview(&body));
            }
            Ok(body)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_book_passes_for_a_snapshot() {
        assert!(is_book(
            br#"{"lastUpdateId":1027024,"bids":[["4.0","431.0"]],"asks":[]}"#
        ));
        assert!(!is_book(br#"{"code":-1121,"msg":"Invalid symbol."}"#));
        assert!(!is_book(b"<html>502 Bad Gateway</html>"));
    }
}
