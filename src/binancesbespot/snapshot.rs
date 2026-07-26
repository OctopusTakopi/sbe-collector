use std::time::Duration;

use bytes::Bytes;
use jiff::{Span, Timestamp};
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

/// Minimum spacing between snapshot requests.
///
/// The round used to issue every symbol back to back — seven full-depth
/// snapshots inside 50 ms, 1,750 request weight in one burst. The weight window
/// admits that, but it only knows what this process has spent, so four restarts
/// inside a minute stack four bursts past Binance's 6,000 allowance with
/// nothing in a position to notice. Spacing the requests means a restart costs
/// one snapshot rather than a whole round.
const SNAPSHOT_PACE: Duration = Duration::from_secs(2);

/// How long to wait before the first request of the process.
///
/// A collector that crash-loops then spends no weight at all, instead of
/// burning a round every time it comes up.
const SNAPSHOT_START_DELAY: Duration = Duration::from_secs(5);

/// Extract when a ban lifts from a 418/429 reply.
///
/// Binance answers with `Retry-After` in seconds and, for `-1003`, repeats the
/// expiry as a millisecond epoch in the message: `IP banned until 1785050129644`.
/// The header is preferred as the more structured of the two.
fn ban_expiry(retry_after: Option<&str>, body: &[u8], now: Timestamp) -> Option<Timestamp> {
    if let Some(seconds) = retry_after.and_then(|value| value.trim().parse::<i64>().ok()) {
        return now.checked_add(Span::new().try_seconds(seconds).ok()?).ok();
    }
    let body = std::str::from_utf8(body).ok()?;
    let rest = body.split("banned until ").nth(1)?;
    let millis: i64 = rest
        .trim_start()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()?;
    Timestamp::from_millisecond(millis).ok()
}

/// Fetch the full depth snapshot for `symbol` from the REST API.
///
/// Takes the throttler so a ban in the reply gates every later request rather
/// than only failing this one — see [`Throttler::note_ban`].
pub async fn fetch_snapshot(
    client: &reqwest::Client,
    throttler: &Throttler,
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
    // Headers must be read before the body consumes the response.
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = response.bytes().await?;
    if !status.is_success() {
        // 418 is Binance's ban; 429 is the warning shot before one.
        if status.as_u16() == 418 || status.as_u16() == 429 {
            match ban_expiry(retry_after.as_deref(), &body, Timestamp::now()) {
                Some(until) => throttler.note_ban(until),
                None => {
                    // Better to sit out a fixed spell than keep sending into a
                    // ban whose length could not be read.
                    warn!(%status, "rate-limited with no readable expiry; backing off");
                    if let Ok(span) = Span::new().try_minutes(2)
                        && let Ok(until) = Timestamp::now().checked_add(span)
                    {
                        throttler.note_ban(until);
                    }
                }
            }
        }
        let preview = &body[..body.len().min(1024)];
        anyhow::bail!(
            "Binance depth snapshot returned {status}: {}",
            String::from_utf8_lossy(preview)
        );
    }
    Ok(body)
}

/// Background task: fetch a REST depth snapshot for every symbol shortly after
/// startup and then every `interval_secs` seconds (default 3600 = 1 hour).
/// Writes tagged `Rest` records into the same per-symbol zstd file as the SBE
/// stream frames.
///
/// Requests within a round are spaced by [`SNAPSHOT_PACE`] and the first is held
/// back by [`SNAPSHOT_START_DELAY`], so neither a restart nor a crash-loop can
/// dump a whole round's request weight at once.
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
    tokio::time::sleep(SNAPSHOT_START_DELAY).await;

    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
    let mut pacer = tokio::time::interval(SNAPSHOT_PACE);
    // A round that overruns its spacing must not then fire the backlog at once.
    pacer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // tick() fires immediately on the first call, so the first round runs as
    // soon as the start delay is over.
    loop {
        ticker.tick().await;

        for symbol in &symbols {
            pacer.tick().await;
            let result = throttler
                .execute(SNAPSHOT_WEIGHT, fetch_snapshot(&client, &throttler, symbol))
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The body Binance actually returned when this collector was banned.
    const BAN_BODY: &[u8] = br#"{"code":-1003,"msg":"Way too much request weight used; IP banned until 1785050129644. Please use WebSocket Streams for live updates to avoid bans."}"#;

    #[test]
    fn the_ban_expiry_is_read_from_the_message() {
        let until = ban_expiry(None, BAN_BODY, Timestamp::now()).expect("expiry in the body");

        assert_eq!(until.as_millisecond(), 1_785_050_129_644);
    }

    /// `Retry-After` is the structured answer and wins over the prose.
    #[test]
    fn retry_after_is_preferred() {
        let now = Timestamp::from_millisecond(1_785_050_000_000).unwrap();

        let until = ban_expiry(Some("120"), BAN_BODY, now).expect("expiry from the header");

        assert_eq!(until.as_millisecond(), 1_785_050_000_000 + 120_000);
    }

    /// A reply that says nothing usable must not be read as "not banned".
    #[test]
    fn an_unreadable_reply_yields_no_expiry() {
        assert!(ban_expiry(None, b"{}", Timestamp::now()).is_none());
        assert!(ban_expiry(Some("soon"), b"nothing here", Timestamp::now()).is_none());
        assert!(ban_expiry(None, b"banned until soon", Timestamp::now()).is_none());
    }

    /// Once a ban is recorded, nothing is sent until it lifts — the behaviour
    /// that keeps Binance's two minutes from escalating into days.
    #[tokio::test]
    async fn a_recorded_ban_stops_every_request() {
        let throttler = Throttler::new(crate::throttler::SNAPSHOT_WEIGHT_BUDGET);
        assert_eq!(
            throttler.execute(SNAPSHOT_WEIGHT, async { 1 }).await,
            Some(1)
        );

        let until = ban_expiry(None, BAN_BODY, Timestamp::now()).unwrap();
        throttler.note_ban(until.checked_add(Span::new().hours(24)).unwrap());

        assert_eq!(
            throttler.execute(SNAPSHOT_WEIGHT, async { 2 }).await,
            None,
            "a banned IP must not be sent to, however much budget is left"
        );
    }

    /// A second ban while one is in force is an escalation, not a reprieve.
    #[tokio::test]
    async fn a_ban_is_never_shortened() {
        let throttler = Throttler::new(crate::throttler::SNAPSHOT_WEIGHT_BUDGET);
        let now = Timestamp::now();
        let far = now.checked_add(Span::new().hours(24)).unwrap();
        let near = now.checked_add(Span::new().minutes(1)).unwrap();

        throttler.note_ban(far);
        throttler.note_ban(near);

        assert_eq!(throttler.execute(SNAPSHOT_WEIGHT, async { 1 }).await, None);
    }
}
