//! OKX's REST book snapshot, 400 levels a side where `bbo-tbt` shows one,
//! recorded as the venue sent it.

use std::collections::HashMap;

use anyhow::bail;
use bytes::Bytes;
use reqwest::{StatusCode, header::CONTENT_TYPE};
use sbe_collector::sbe::{
    MessageHeader,
    okx::{SnapshotDepthBlock, TEMPLATE_SNAPSHOT_DEPTH},
};
use zerocopy::FromBytes;

use super::{instruments::Instrument, protocol::Okx};
use crate::{
    file::Symbol,
    session::Protocol,
    snapshot::{Depth, FetchError},
};

const BOOKS_URL: &str = "https://www.okx.com/api/v5/market/books-sbe";

/// Book snapshots a minute. A round asks each instrument once, paced, which
/// keeps within OKX's 10 requests per 10 s per instrument; this only bounds
/// what the pace allows.
pub(crate) const BOOKS_PER_MINUTE: u32 = 60;

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

/// The REST books of the subscribed instruments, asked for by the
/// `instIdCode` the lookup resolved each one to.
pub(crate) struct Books {
    codes: HashMap<Symbol, i64>,
}

impl Books {
    pub(crate) fn new(instruments: &[Instrument]) -> Self {
        Self {
            codes: instruments
                .iter()
                .map(|instrument| (instrument.symbol(), instrument.code))
                .collect(),
        }
    }
}

impl Depth for Books {
    fn label(&self) -> &'static str {
        Okx::SOURCE
    }

    /// OKX limits requests, not weight.
    fn weight(&self) -> u32 {
        1
    }

    async fn fetch(&self, client: &reqwest::Client, symbol: &Symbol) -> Result<Bytes, FetchError> {
        let Some(&code) = self.codes.get(symbol) else {
            return Err(anyhow::anyhow!("{symbol} is not a subscribed instrument").into());
        };
        let response = client
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
