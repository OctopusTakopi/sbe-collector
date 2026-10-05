//! Resolving the command line's instIds to the `instIdCode`s OKX's SBE
//! stream carries instead.

use std::time::Duration;

use anyhow::{Context, bail};
use serde::Deserialize;
use tracing::warn;

use crate::file::Symbol;

const INSTRUMENTS_URL: &str = "https://www.okx.com/api/v5/public/instruments";

/// An instrument to record.
pub(crate) struct Instrument {
    /// As OKX spells it: `BTC-USDT`, `BTC-USDT-SWAP`.
    pub(crate) inst_id: String,
    /// What the stream calls it. Changes if the instrument is relisted.
    pub(crate) code: i64,
}

impl Instrument {
    /// The name of its files: the instId, lowercase.
    pub(crate) fn symbol(&self) -> Symbol {
        Symbol::from(self.inst_id.to_ascii_lowercase())
    }
}

/// Look every instId up, failing on the first OKX does not know. One small
/// filtered request each: a whole SPOT listing is over a megabyte.
pub(crate) async fn resolve(inst_ids: &[String]) -> anyhow::Result<Vec<Instrument>> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()?;
    let mut instruments = Vec::with_capacity(inst_ids.len());
    for inst_id in inst_ids {
        // OKX rejects a lowercase instId outright.
        let inst_id = inst_id.to_ascii_uppercase();
        let url = format!("{INSTRUMENTS_URL}?{}", query(&inst_id)?);
        let body = client
            .get(&url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .with_context(|| format!("cannot look up {inst_id} at {url}"))?
            .bytes()
            .await?;
        let code = parse(&inst_id, &body)?;
        instruments.push(Instrument { inst_id, code });
    }
    Ok(instruments)
}

/// The instruments query for `inst_id`, its type read off its shape. Options
/// need their family even alongside the instId.
fn query(inst_id: &str) -> anyhow::Result<String> {
    let parts: Vec<&str> = inst_id.split('-').collect();
    let is_expiry = |part: &str| part.len() == 6 && part.bytes().all(|byte| byte.is_ascii_digit());
    Ok(match parts.as_slice() {
        [_, _] => format!("instType=SPOT&instId={inst_id}"),
        [_, _, "SWAP"] => format!("instType=SWAP&instId={inst_id}"),
        [_, _, expiry] if is_expiry(expiry) => format!("instType=FUTURES&instId={inst_id}"),
        [base, quote, expiry, _, "C" | "P"] if is_expiry(expiry) => {
            format!("instType=OPTION&instFamily={base}-{quote}&instId={inst_id}")
        }
        _ => bail!(
            "{inst_id} is not an OKX instId: expected a shape like BTC-USDT, BTC-USDT-SWAP, \
             BTC-USD-251226 or BTC-USD-251226-100000-C"
        ),
    })
}

#[derive(Deserialize)]
struct Response {
    code: String,
    msg: String,
    data: Vec<Row>,
}

#[derive(Deserialize)]
struct Row {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "instIdCode")]
    code: Option<i64>,
    state: String,
}

/// The `instIdCode` in an instruments reply for exactly `inst_id`.
fn parse(inst_id: &str, body: &[u8]) -> anyhow::Result<i64> {
    let response: Response = serde_json::from_slice(body).with_context(|| {
        format!(
            "unreadable instruments reply for {inst_id}: {}",
            String::from_utf8_lossy(&body[..body.len().min(512)])
        )
    })?;
    if response.code != "0" {
        bail!(
            "OKX does not know {inst_id}: {} (code {})",
            response.msg,
            response.code
        );
    }
    let [row] = response.data.as_slice() else {
        bail!(
            "OKX listed {} instruments for {inst_id}",
            response.data.len()
        );
    };
    if row.inst_id != inst_id {
        bail!("OKX answered {} for {inst_id}", row.inst_id);
    }
    let Some(code) = row.code else {
        bail!("OKX gave {inst_id} no instIdCode");
    };
    if row.state != "live" {
        warn!(inst_id, state = %row.state, "the instrument is not live; it streams nothing until it is");
    }
    Ok(code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_instrument_type_follows_the_inst_id_shape() {
        for (inst_id, expected) in [
            ("BTC-USDT", "instType=SPOT&instId=BTC-USDT"),
            ("BTC-USDT-SWAP", "instType=SWAP&instId=BTC-USDT-SWAP"),
            ("BTC-USD-261225", "instType=FUTURES&instId=BTC-USD-261225"),
            (
                "BTC-USD-261030-100000-C",
                "instType=OPTION&instFamily=BTC-USD&instId=BTC-USD-261030-100000-C",
            ),
        ] {
            assert_eq!(query(inst_id).unwrap(), expected);
        }
        for malformed in ["BTCUSDT", "BTC-USDT-PERP", "BTC-USD-2612-1-C"] {
            assert!(query(malformed).is_err(), "{malformed}");
        }
    }

    /// Trimmed from a live reply.
    #[test]
    fn a_listed_instrument_yields_its_code() {
        let body = br#"{"code":"0","data":[{"alias":"","baseCcy":"BTC","instId":"BTC-USDT","instIdCode":3,"instType":"SPOT","state":"live","tickSz":"0.1"}],"msg":""}"#;
        assert_eq!(parse("BTC-USDT", body).unwrap(), 3);
    }

    #[test]
    fn an_unknown_instrument_fails_by_name() {
        let body = br#"{"code":"51001","data":[],"msg":"Instrument ID, Instrument ID code, or Spread ID doesn't exist."}"#;
        let error = parse("NOPE-USDT", body).unwrap_err().to_string();
        assert!(error.contains("NOPE-USDT"), "{error}");
        assert!(error.contains("51001"), "{error}");
    }

    #[test]
    fn a_reply_without_a_code_or_for_another_instrument_fails() {
        let no_code = br#"{"code":"0","data":[{"instId":"BTC-USDT","state":"live"}],"msg":""}"#;
        assert!(parse("BTC-USDT", no_code).is_err());
        let other = br#"{"code":"0","data":[{"instId":"ETH-USDT","instIdCode":4,"state":"live"}],"msg":""}"#;
        assert!(parse("BTC-USDT", other).is_err());
        assert!(parse("BTC-USDT", b"<html>").is_err());
    }
}
