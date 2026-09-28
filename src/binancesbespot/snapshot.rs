use std::time::Duration;

use bytes::Bytes;
use jiff::{SignedDuration, Timestamp};
use tokio::sync::mpsc::Sender;
use tracing::{error, warn};

use crate::{
    error::ConnectorError,
    file::{Symbol, Tag, WriteRecord},
    throttler::Throttler,
};

/// Depth levels requested per snapshot.
///
/// Moves with [`SNAPSHOT_WEIGHT`]: Binance charges `/api/v3/depth` 5 at a limit
/// up to 100, 25 to 500, 50 to 1000 and 250 to 5000.
const SNAPSHOT_LIMIT: u32 = 5_000;

/// What Binance charges for a [`SNAPSHOT_LIMIT`]-deep snapshot.
pub const SNAPSHOT_WEIGHT: u32 = 250;

/// Minimum spacing between snapshot requests.
///
/// The weight window only knows what this process spent, so back-to-back
/// rounds from a few quick restarts would stack past Binance's 6,000
/// allowance. Spaced out, a restart costs one snapshot rather than a round.
const SNAPSHOT_PACE: Duration = Duration::from_secs(2);

/// How often every symbol's book is recorded.
const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Held back before the first request, so a crash-looping collector spends
/// no weight at all.
const SNAPSHOT_START_DELAY: Duration = Duration::from_secs(5);

/// How long to stand down after a 418/429 whose expiry cannot be read.
const UNREADABLE_BAN: SignedDuration = SignedDuration::from_mins(2);

/// When the ban announced by a 418/429 reply lifts.
///
/// Binance sends `Retry-After` in seconds and, for `-1003`, repeats the expiry
/// as a millisecond epoch in the message: `IP banned until 1785050129644`. The
/// header wins. A reply stating neither still bans for [`UNREADABLE_BAN`]:
/// unreadable must never mean "not banned".
fn ban_expiry(retry_after: Option<&str>, body: &[u8], now: Timestamp) -> Timestamp {
    retry_after
        .and_then(|value| value.trim().parse::<u32>().ok())
        .and_then(|seconds| {
            now.checked_add(SignedDuration::from_secs(seconds.into()))
                .ok()
        })
        .or_else(|| stated_expiry(body))
        .unwrap_or_else(|| {
            warn!("rate-limited with no readable ban expiry; backing off");
            now.checked_add(UNREADABLE_BAN).unwrap_or(Timestamp::MAX)
        })
}

/// The epoch-millisecond expiry in a `-1003` message.
fn stated_expiry(body: &[u8]) -> Option<Timestamp> {
    let rest = std::str::from_utf8(body)
        .ok()?
        .split("banned until ")
        .nth(1)?
        .trim_start();
    let digits = rest
        .find(|c: char| !c.is_ascii_digit())
        .map_or(rest, |end| &rest[..end]);
    Timestamp::from_millisecond(digits.parse().ok()?).ok()
}

/// Fetch the full depth snapshot for `symbol` from the REST API. A ban in the
/// reply is recorded on `throttler`, so it gates every later request.
async fn fetch_snapshot(
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
        if matches!(status.as_u16(), 418 | 429) {
            throttler.note_ban(ban_expiry(retry_after.as_deref(), &body, Timestamp::now()));
        }
        let preview = &body[..body.len().min(1024)];
        anyhow::bail!(
            "Binance depth snapshot returned {status}: {}",
            String::from_utf8_lossy(preview)
        );
    }
    Ok(body)
}

/// REST depth snapshots, rationed by one weight budget shared by the
/// periodic round and gap recovery.
#[derive(Clone)]
pub struct Snapshots {
    client: reqwest::Client,
    throttler: Throttler,
}

impl Snapshots {
    pub fn new(throttler: Throttler) -> reqwest::Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self { client, throttler })
    }

    /// Fetch `symbol`'s book within the budget and queue it for the writer.
    /// Fails only when the writer is gone.
    pub async fn record(
        &self,
        symbol: &Symbol,
        writer_tx: &Sender<WriteRecord>,
    ) -> Result<(), ConnectorError> {
        let fetched = self
            .throttler
            .execute(
                SNAPSHOT_WEIGHT,
                fetch_snapshot(&self.client, &self.throttler, symbol),
            )
            .await;
        match fetched {
            Some(Ok(data)) => writer_tx
                .send(WriteRecord {
                    recv_time: Timestamp::now(),
                    symbol: Symbol::clone(symbol),
                    tag: Tag::Rest,
                    data,
                })
                .await
                .map_err(|_| ConnectorError::WriterClosed),
            Some(Err(error)) => {
                error!(%symbol, %error, "failed to fetch depth snapshot");
                Ok(())
            }
            None => {
                warn!(%symbol, "depth snapshot rate-limited, skipping");
                Ok(())
            }
        }
    }

    /// Snapshot every symbol after [`SNAPSHOT_START_DELAY`] and then every
    /// [`SNAPSHOT_INTERVAL`], one request per [`SNAPSHOT_PACE`]. Returns once
    /// the writer is gone.
    pub async fn run_periodic(self, symbols: Vec<Symbol>, writer_tx: Sender<WriteRecord>) {
        tokio::time::sleep(SNAPSHOT_START_DELAY).await;
        // The first tick is immediate.
        let mut ticker = tokio::time::interval(SNAPSHOT_INTERVAL);
        let mut pacer = tokio::time::interval(SNAPSHOT_PACE);
        // An overrun must not fire the backlog at once.
        pacer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            for symbol in &symbols {
                pacer.tick().await;
                if self.record(symbol, &writer_tx).await.is_err() {
                    return;
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
        let until = ban_expiry(None, BAN_BODY, Timestamp::now());

        assert_eq!(until.as_millisecond(), 1_785_050_129_644);
    }

    #[test]
    fn retry_after_is_preferred() {
        let now = Timestamp::from_millisecond(1_785_050_000_000).unwrap();

        let until = ban_expiry(Some("120"), BAN_BODY, now);

        assert_eq!(until.as_millisecond(), 1_785_050_000_000 + 120_000);
    }

    #[test]
    fn an_unreadable_reply_still_bans() {
        let now = Timestamp::from_millisecond(1_785_050_000_000).unwrap();
        for (retry_after, body) in [
            (None, &b"{}"[..]),
            (Some("soon"), b"nothing here"),
            (None, b"banned until soon"),
            (Some("-5"), b"{}"),
        ] {
            assert_eq!(
                ban_expiry(retry_after, body, now).as_millisecond(),
                1_785_050_000_000 + 120_000,
                "{retry_after:?} {body:?}"
            );
        }
    }

    /// Once a ban is recorded, nothing is sent until it lifts: what keeps
    /// Binance's two minutes from escalating into days.
    #[tokio::test]
    async fn a_recorded_ban_stops_every_request() {
        let throttler = Throttler::new(crate::throttler::SNAPSHOT_WEIGHT_BUDGET);
        assert_eq!(
            throttler.execute(SNAPSHOT_WEIGHT, async { 1 }).await,
            Some(1)
        );

        // Relative to now: `BAN_BODY`'s real expiry has long passed.
        let until = Timestamp::now()
            .checked_add(SignedDuration::from_hours(24))
            .unwrap();
        throttler.note_ban(until);

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
        let far = now.checked_add(SignedDuration::from_hours(24)).unwrap();
        let near = now.checked_add(SignedDuration::from_mins(1)).unwrap();

        throttler.note_ban(far);
        throttler.note_ban(near);

        assert_eq!(throttler.execute(SNAPSHOT_WEIGHT, async { 1 }).await, None);
    }
}
