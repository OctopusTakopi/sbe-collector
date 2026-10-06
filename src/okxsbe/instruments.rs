//! Resolving the command line's instIds to the `instIdCode`s OKX's SBE
//! stream carries instead.

use anyhow::{Context, bail};
use serde::Deserialize;
use tracing::warn;

use crate::{
    file::Symbol,
    listing::{self, Unlisted},
};

const INSTRUMENTS_URL: &str = "https://www.okx.com/api/v5/public/instruments";

/// OKX's answer for an instId it does not list.
const UNKNOWN_INSTRUMENT: &str = "51001";

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

/// Look every instId up, asking again while OKX cannot answer, and failing
/// with every instId it does not list. One small filtered request each: a
/// whole SPOT listing is over a megabyte.
pub(crate) async fn resolve(inst_ids: &[String]) -> anyhow::Result<Vec<Instrument>> {
    let queries = inst_ids
        .iter()
        .map(|inst_id| {
            // OKX rejects a lowercase instId outright.
            let inst_id = inst_id.to_ascii_uppercase();
            let query = query(&inst_id)?;
            anyhow::Ok((inst_id, query))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let client = listing::client()?;
    let codes = listing::patiently(listing::PATIENCE, || look_up(&client, &queries)).await??;
    Ok(queries
        .into_iter()
        .zip(codes)
        .map(|((inst_id, _), code)| Instrument { inst_id, code })
        .collect())
}

/// Each instId's code, or why OKX does not list the ones it does not.
async fn look_up(
    client: &reqwest::Client,
    queries: &[(String, String)],
) -> anyhow::Result<Result<Vec<i64>, Unlisted>> {
    let mut codes = Vec::with_capacity(queries.len());
    let mut unlisted = Vec::new();
    for (inst_id, query) in queries {
        let url = format!("{INSTRUMENTS_URL}?{query}");
        let body = client
            .get(&url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .with_context(|| format!("cannot look up {inst_id} at {url}"))?
            .bytes()
            .await?;
        match parse(inst_id, &body)? {
            Ok(code) => codes.push(code),
            Err(reason) => unlisted.push(format!("{inst_id} ({reason})")),
        }
    }
    Ok(if unlisted.is_empty() {
        Ok(codes)
    } else {
        Err(Unlisted(unlisted))
    })
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

/// The `instIdCode` in an instruments reply for exactly `inst_id`, or why
/// OKX does not list it. An unreadable reply, or any error but
/// [`UNKNOWN_INSTRUMENT`] (a rate limit, say), is no answer.
fn parse(inst_id: &str, body: &[u8]) -> anyhow::Result<Result<i64, String>> {
    let response: Response = serde_json::from_slice(body).with_context(|| {
        format!(
            "unreadable instruments reply for {inst_id}: {}",
            String::from_utf8_lossy(&body[..body.len().min(512)])
        )
    })?;
    match response.code.as_str() {
        "0" => {}
        UNKNOWN_INSTRUMENT => {
            return Ok(Err(format!("{} (code {})", response.msg, response.code)));
        }
        code => bail!("OKX answered {code} for {inst_id}: {}", response.msg),
    }
    let [row] = response.data.as_slice() else {
        return Ok(Err(format!("{} instruments listed", response.data.len())));
    };
    if row.inst_id != inst_id {
        return Ok(Err(format!("OKX answered {}", row.inst_id)));
    }
    let Some(code) = row.code else {
        return Ok(Err("no instIdCode".to_owned()));
    };
    if row.state != "live" {
        warn!(inst_id, state = %row.state, "the instrument is not live; it streams nothing until it is");
    }
    Ok(Ok(code))
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
        assert_eq!(parse("BTC-USDT", body).unwrap(), Ok(3));
    }

    /// What OKX really answers for an instId it does not list.
    #[test]
    fn an_unknown_instrument_is_unlisted() {
        let body = br#"{"code":"51001","data":[],"msg":"Instrument ID, Instrument ID code, or Spread ID doesn't exist."}"#;
        let reason = parse("NOPE-USDT", body).unwrap().unwrap_err();
        assert!(reason.contains("51001"), "{reason}");
    }

    #[test]
    fn a_reply_without_a_code_or_for_another_instrument_is_unlisted() {
        let no_code = br#"{"code":"0","data":[{"instId":"BTC-USDT","state":"live"}],"msg":""}"#;
        assert!(parse("BTC-USDT", no_code).unwrap().is_err());
        let other = br#"{"code":"0","data":[{"instId":"ETH-USDT","instIdCode":4,"state":"live"}],"msg":""}"#;
        assert!(parse("BTC-USDT", other).unwrap().is_err());
    }

    /// A rate limit or an unreadable page is no answer: asked again.
    #[test]
    fn a_rate_limit_or_an_unreadable_reply_is_no_answer() {
        let rate_limited = br#"{"code":"50011","data":[],"msg":"Too Many Requests"}"#;
        assert!(parse("BTC-USDT", rate_limited).is_err());
        assert!(parse("BTC-USDT", b"<html>").is_err());
    }
}
