//! Hourly REST book snapshots, recorded beside the stream: 400 levels a side
//! where `bbo-tbt` shows one.

use std::time::Duration;

use anyhow::bail;
use bytes::Bytes;
use jiff::Timestamp;
use reqwest::{StatusCode, header::CONTENT_TYPE};
use sbe_collector::{
    record::Tag,
    sbe::{
        MessageHeader,
        okx::{SnapshotDepthBlock, TEMPLATE_SNAPSHOT_DEPTH},
    },
};
use tokio::sync::mpsc::Sender;
use tracing::error;
use zerocopy::FromBytes;

use crate::file::{Symbol, WriteRecord};

const BOOKS_URL: &str = "https://www.okx.com/api/v5/market/books-sbe";

/// How often every instrument's book is recorded.
const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Held back before the first request, so a crash-looping collector sends
/// none.
const SNAPSHOT_START_DELAY: Duration = Duration::from_secs(5);

/// Spacing between requests. The limit is 10 per 10 s per instrument, and a
/// round asks each instrument once.
const SNAPSHOT_PACE: Duration = Duration::from_secs(2);

/// Whether a reply is a whole snapshot of instrument `code`. OKX answers an
/// unknown code, or a rate limit, with JSON, sometimes under HTTP 200; that
/// must never land in the recording as a book.
fn check(code: i64, status: StatusCode, is_sbe: bool, body: &[u8]) -> anyhow::Result<()> {
    let preview = || String::from_utf8_lossy(&body[..body.len().min(512)]).into_owned();
    if !status.is_success() || !is_sbe {
        bail!("returned {status}: {}", preview());
    }
    let Ok((header, rest)) = MessageHeader::read_from_prefix(body) else {
        bail!("returned {} bytes, too short for an SBE header", body.len());
    };
    if header.template_id.get() != TEMPLATE_SNAPSHOT_DEPTH {
        bail!(
            "returned template {} instead of a book snapshot",
            header.template_id.get()
        );
    }
    let Ok((block, _)) = SnapshotDepthBlock::read_from_prefix(rest) else {
        bail!("returned a book snapshot cut short at {} bytes", body.len());
    };
    if block.inst_id_code.get() != code {
        bail!(
            "returned the book of instIdCode {}",
            block.inst_id_code.get()
        );
    }
    Ok(())
}

pub(crate) struct Snapshots {
    client: reqwest::Client,
}

impl Snapshots {
    pub(crate) fn new() -> reqwest::Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self { client })
    }

    async fn fetch(&self, code: i64) -> anyhow::Result<Bytes> {
        let response = self
            .client
            .get(format!("{BOOKS_URL}?instIdCode={code}&source=0"))
            .send()
            .await?;
        let status = response.status();
        let is_sbe = response
            .headers()
            .get(CONTENT_TYPE)
            .is_some_and(|value| value.as_bytes().starts_with(b"application/sbe"));
        let body = response.bytes().await?;
        check(code, status, is_sbe, &body)?;
        Ok(body)
    }

    /// Snapshot every instrument after [`SNAPSHOT_START_DELAY`] and then every
    /// [`SNAPSHOT_INTERVAL`], one request per [`SNAPSHOT_PACE`]. Returns once
    /// the writer is gone.
    pub(crate) async fn run_periodic(
        self,
        books: Vec<(i64, Symbol)>,
        writer_tx: Sender<WriteRecord>,
    ) {
        tokio::time::sleep(SNAPSHOT_START_DELAY).await;
        // The first tick is immediate.
        let mut ticker = tokio::time::interval(SNAPSHOT_INTERVAL);
        let mut pacer = tokio::time::interval(SNAPSHOT_PACE);
        // An overrun must not fire the backlog at once.
        pacer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            for (code, symbol) in &books {
                pacer.tick().await;
                let data = match self.fetch(*code).await {
                    Ok(data) => data,
                    Err(error) => {
                        error!(%symbol, %error, "failed to fetch an OKX book snapshot");
                        continue;
                    }
                };
                let record = WriteRecord {
                    recv_time: Timestamp::now(),
                    symbol: Symbol::clone(symbol),
                    tag: Tag::Rest,
                    data,
                };
                if writer_tx.send(record).await.is_err() {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 1006 snapshot of `code` with one level a side.
    fn snapshot(template_id: u16, code: i64) -> Vec<u8> {
        let mut body = Vec::new();
        for field in [26u16, template_id, 1, 0] {
            body.extend_from_slice(&field.to_le_bytes());
        }
        for field in [code, 1_791_179_472_009_000, 81_991_082_079] {
            body.extend_from_slice(&field.to_le_bytes());
        }
        body.extend_from_slice(&[(-1i8) as u8, (-8i8) as u8]);
        for _ in 0..2 {
            body.extend_from_slice(&20u16.to_le_bytes());
            body.extend_from_slice(&1u16.to_le_bytes());
            body.extend_from_slice(&857_653i64.to_le_bytes());
            body.extend_from_slice(&16_694_228i64.to_le_bytes());
            body.extend_from_slice(&19i32.to_le_bytes());
        }
        body
    }

    #[test]
    fn a_whole_snapshot_of_the_instrument_passes() {
        check(3, StatusCode::OK, true, &snapshot(1006, 3)).unwrap();
    }

    /// What OKX really sent for an unknown code.
    #[test]
    fn a_json_error_under_http_200_is_refused() {
        let body = br#"{"code":"51001","msg":"Instrument ID, Instrument ID code or Spread ID doesn't exist."}"#;
        let error = check(999_999_999, StatusCode::OK, false, body).unwrap_err();
        assert!(error.to_string().contains("51001"), "{error}");
    }

    #[test]
    fn anything_but_this_instruments_snapshot_is_refused() {
        let rate_limited = br#"{"code":"50011","msg":"Too Many Requests"}"#;
        assert!(check(3, StatusCode::TOO_MANY_REQUESTS, false, rate_limited).is_err());
        assert!(check(3, StatusCode::OK, true, &snapshot(1006, 4)).is_err());
        assert!(check(3, StatusCode::OK, true, &snapshot(1000, 3)).is_err());
        assert!(check(3, StatusCode::OK, true, &snapshot(1006, 3)[..20]).is_err());
    }
}
