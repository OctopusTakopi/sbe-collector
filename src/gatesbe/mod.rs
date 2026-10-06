mod protocol;

use std::collections::{HashMap, HashSet};

use anyhow::{Context as _, bail};
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::task::JoinSet;
use tracing::warn;

use sbe_collector::{
    exchange::Exchange,
    sbe::{
        MessageHeader,
        gate::{self, Market, ObuLink},
    },
};
use zerocopy::FromBytes;

use crate::{
    collection::{self, Consumer, Context, Recorder},
    dedup::{DEDUP_MAX_ENTRIES, DEDUP_WINDOW, Dedup},
    error::ConnectorError,
    file::Symbol,
    listing::{self, Unlisted},
    quality::{QualityEvent, QualityReporter},
    session::{Frame, Protocol},
};
use protocol::Gate;

const REST_URL: &str = "https://api.gateio.ws/api/v4";

/// What to collect: checked against Gate's listings before anything starts,
/// so an unknown pair stops the process cleanly.
pub struct Subscription {
    market: Market,
    /// As Gate spells them, `BTC_USDT`.
    pairs: Vec<String>,
}

impl Subscription {
    pub async fn prepare(market: Market, symbols: &[String]) -> anyhow::Result<Self> {
        let pairs: Vec<String> = symbols
            .iter()
            .map(|symbol| symbol.to_ascii_uppercase())
            .collect();
        let client = listing::client()?;
        let unlisted =
            listing::patiently(listing::PATIENCE, || unlisted(&client, market, &pairs)).await?;
        if !unlisted.is_empty() {
            return Err(Unlisted(unlisted).into());
        }
        Ok(Self { market, pairs })
    }
}

/// Why Gate does not list each of `pairs` it does not.
async fn unlisted(
    client: &reqwest::Client,
    market: Market,
    pairs: &[String],
) -> anyhow::Result<Vec<String>> {
    let mut unlisted = Vec::new();
    for pair in pairs {
        let url = match market {
            Market::Spot => format!("{REST_URL}/spot/currency_pairs/{pair}"),
            Market::Futures => format!("{REST_URL}/futures/usdt/contracts/{pair}"),
        };
        let response = client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("cannot look up {pair} at {url}"))?;
        let status = response.status();
        let body = response.bytes().await?;
        if let Some(reason) = check_listing(market, pair, status, &body)? {
            unlisted.push(format!("{pair} ({reason})"));
        }
    }
    Ok(unlisted)
}

/// How Gate refuses a pair it does not list: `{"label":"INVALID_CURRENCY",
/// "message":"Invalid currency FOOX"}`, `{"label":"CONTRACT_NOT_FOUND"}`.
#[derive(Deserialize)]
struct Refusal {
    label: String,
    message: Option<String>,
}

/// A spot pair's or a futures contract's listing; the fields this reads.
#[derive(Deserialize)]
struct Listing {
    /// A spot pair's name.
    id: Option<String>,
    /// A contract's name.
    name: Option<String>,
    trade_status: Option<String>,
    in_delisting: Option<bool>,
}

/// Why Gate does not list `pair` in `market`, if it does not: it answers an
/// unknown pair 400, with an error label. A 400 refuses the request itself,
/// which asking again cannot change; anything else, a rate limit or an
/// outage, is no answer. A pair it lists but does not trade is recorded
/// anyway, with a warning, since it streams nothing until it does.
fn check_listing(
    market: Market,
    pair: &str,
    status: StatusCode,
    body: &[u8],
) -> anyhow::Result<Option<String>> {
    let preview = || String::from_utf8_lossy(&body[..body.len().min(512)]).into_owned();
    if status == StatusCode::BAD_REQUEST {
        return Ok(Some(match serde_json::from_slice::<Refusal>(body) {
            Ok(Refusal {
                label,
                message: Some(message),
            }) => format!("{label}: {message}"),
            Ok(Refusal { label, .. }) => label,
            Err(_) => preview(),
        }));
    }
    if !status.is_success() {
        bail!("Gate answered {status} for {pair}: {}", preview());
    }
    let listing: Listing = serde_json::from_slice(body)
        .with_context(|| format!("unreadable listing of {pair}: {}", preview()))?;
    let listed = match market {
        Market::Spot => listing.id,
        Market::Futures => listing.name,
    };
    if listed.as_deref() != Some(pair) {
        return Ok(Some(format!("Gate answered {listed:?}")));
    }
    if listing
        .trade_status
        .as_deref()
        .is_some_and(|status| status != "tradable")
        || listing.in_delisting == Some(true)
    {
        warn!(pair, "Gate lists the pair as not trading");
    }
    Ok(None)
}

/// The single consumer of every connection's frames: duplicate suppression,
/// routing by the instrument each frame names, and the merged book chain.
struct Ingest {
    market: Market,
    recorder: Recorder,
    /// Gate's spelling, `BTC_USDT`, to the file key, `btc_usdt`.
    symbols: HashMap<Box<[u8]>, Symbol>,
    /// Names already reported as unsubscribed: the quality log syncs each
    /// event.
    unknown: HashSet<Box<[u8]>>,
    /// The highest `obu` id recorded per symbol, across every connection.
    marks: HashMap<Symbol, i64>,
    quality: QualityReporter,
}

/// Filtered even on one connection: it overlaps two sockets every hour, when
/// its session is replaced, and Gate repeats no frame unchanged, so filtering
/// a single connection loses nothing.
fn dedup(exchange: Exchange) -> Dedup {
    Dedup::new(exchange, DEDUP_WINDOW, DEDUP_MAX_ENTRIES)
}

impl Ingest {
    fn new(market: Market, recorder: Recorder, pairs: &[String], quality: QualityReporter) -> Self {
        Self {
            market,
            recorder,
            symbols: pairs
                .iter()
                .map(|pair| {
                    (
                        Box::from(pair.as_bytes()),
                        Symbol::from(pair.to_ascii_lowercase()),
                    )
                })
                .collect(),
            unknown: HashSet::new(),
            marks: HashMap::new(),
            quality,
        }
    }
}

impl Consumer for Ingest {
    async fn handle(&mut self, frame: Frame) -> Result<(), ConnectorError> {
        let Frame { recv_time, data } = frame;
        if self.recorder.dedup.is_duplicate(&data) {
            return Ok(());
        }
        let Ok((header, _)) = MessageHeader::read_from_prefix(&data) else {
            return Err(ConnectorError::ShortFrame { len: data.len() });
        };
        let route = gate::route(self.market, &data);
        let symbol = match route.and_then(|route| self.symbols.get(route.instrument)) {
            // Only a subscribed pair's book has one chain to follow.
            Some(symbol) => {
                let symbol = Symbol::clone(symbol);
                if let Some(link) = route.and_then(|route| route.obu) {
                    self.check_book(&symbol, link);
                }
                symbol
            }
            None => {
                let named = route.map_or(&[][..], |route| route.instrument);
                self.report_unknown(named, header.template_id.get());
                Symbol::from("unknown")
            }
        };
        self.recorder.write(recv_time, symbol, data).await
    }
}

impl Ingest {
    /// Report a hole in `symbol`'s merged book stream when a delta jumps past
    /// the ids recorded so far. A lagging connection may still deliver them:
    /// as with Binance's depth check, this records the moment the hole was
    /// real, and `gap_detector`'s count of the file says whether it stayed.
    /// Each connection that missed ids replaces itself from a fresh snapshot.
    ///
    /// A delta ending at or below the mark is covered ground, a straggler
    /// from a lagging connection. A snapshot never lowers the mark: one from
    /// a fresh connection can trail deltas already recorded.
    fn check_book(&mut self, symbol: &Symbol, link: ObuLink<'_>) {
        let Some(mark) = self.marks.get_mut(symbol) else {
            self.marks.insert(Symbol::clone(symbol), link.last_id);
            return;
        };
        let prev = *mark;
        *mark = prev.max(link.last_id);
        if link.full || link.first_id <= prev.saturating_add(1) {
            return;
        }
        warn!(
            %symbol,
            "book gap: expected first id {} but got {} (last {prev})",
            prev + 1,
            link.first_id
        );
        self.quality.report(QualityEvent::FeedDegraded {
            at_ns: QualityEvent::now_ns(),
            source: format!("{}/{symbol}", Gate::SOURCE),
            detail: format!(
                "book gap: expected first id {} but got {} (last {prev})",
                prev + 1,
                link.first_id
            ),
        });
    }

    fn report_unknown(&mut self, name: &[u8], template_id: u16) {
        if !self.unknown.insert(Box::from(name)) {
            return;
        }
        let name = String::from_utf8_lossy(name);
        warn!(%name, template_id, "a frame for an unsubscribed instrument; writing to 'unknown'");
        self.quality.report(QualityEvent::FeedDegraded {
            at_ns: QualityEvent::now_ns(),
            source: Gate::SOURCE.to_owned(),
            detail: format!("frames naming {name:?} (template_id={template_id}) go to 'unknown'"),
        });
    }
}

/// Records `subscription` until shutdown or a critical failure.
pub async fn collect(subscription: Subscription, cx: Context) -> anyhow::Result<()> {
    let Subscription { market, pairs } = subscription;
    let exchange = match market {
        Market::Spot => Exchange::GateSbeSpot,
        Market::Futures => Exchange::GateSbeFutures,
    };
    let gate = Gate::new(market, &pairs, cx.quality.clone());
    let recorder = Recorder {
        dedup: dedup(exchange),
        writer_tx: cx.writer_tx.clone(),
    };
    let ingest = Ingest::new(market, recorder, &pairs, cx.quality.clone());
    collection::run_slots(gate, ingest, cx, JoinSet::new()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::WriteRecord;
    use bytes::Bytes;
    use tokio::sync::mpsc::{Receiver, channel};

    fn names(frame: &mut Vec<u8>, channel: &str, name: &str) {
        for text in [channel, name] {
            frame.push(text.len() as u8);
            frame.extend_from_slice(text.as_bytes());
        }
    }

    /// A spot `obu` frame for `pair`, sent at `time`.
    fn obu(pair: &str, full: bool, first_id: i64, last_id: i64, time: i64) -> Bytes {
        let mut frame = Vec::new();
        for field in [36u16, gate::TEMPLATE_OBU, 1, 1] {
            frame.extend_from_slice(&field.to_le_bytes());
        }
        frame.extend_from_slice(&time.to_le_bytes());
        frame.push(2);
        frame.extend_from_slice(&1_791_182_069_963_000i64.to_le_bytes());
        frame.push(u8::from(full));
        frame.extend_from_slice(&first_id.to_le_bytes());
        frame.extend_from_slice(&last_id.to_le_bytes());
        frame.extend_from_slice(&[(-1i8) as u8, (-6i8) as u8]);
        for _ in 0..2 {
            frame.extend_from_slice(&16u16.to_le_bytes());
            frame.extend_from_slice(&1u16.to_le_bytes());
            frame.extend_from_slice(&[7; 16]);
        }
        names(&mut frame, "spot.obu", &format!("ob.{pair}.50"));
        Bytes::from(frame)
    }

    fn ingest(quality: QualityReporter) -> (Ingest, Receiver<WriteRecord>) {
        let (writer_tx, writer_rx) = channel(32);
        let recorder = Recorder {
            dedup: dedup(Exchange::GateSbeSpot),
            writer_tx,
        };
        (
            Ingest::new(Market::Spot, recorder, &["BTC_USDT".to_owned()], quality),
            writer_rx,
        )
    }

    fn written(writer_rx: &mut Receiver<WriteRecord>) -> Vec<WriteRecord> {
        std::iter::from_fn(|| writer_rx.try_recv().ok()).collect()
    }

    /// Spot copies of one event differ in the send `time` alone, whether
    /// they come from two connections or from one connection's hourly
    /// handover.
    #[tokio::test]
    async fn a_second_sockets_copy_is_written_once() {
        let (mut ingest, mut writer_rx) = ingest(QualityReporter::disabled());

        for time in [100, 117] {
            ingest
                .handle(Frame::now(obu("BTC_USDT", true, 0, 9, time)))
                .await
                .unwrap();
        }

        let records = written(&mut writer_rx);
        assert_eq!(records.len(), 1);
        assert_eq!(&*records[0].symbol, "btc_usdt");
    }

    #[tokio::test]
    async fn the_merged_book_chain_reports_only_real_holes() {
        let (quality, events) = QualityReporter::test_channel();
        let (mut ingest, _writer_rx) = ingest(quality);
        let feed = [
            obu("BTC_USDT", true, 0, 100, 1),
            obu("BTC_USDT", false, 101, 105, 2),
            // A lagging connection's straggler, then an overlap.
            obu("BTC_USDT", false, 101, 105, 3),
            obu("BTC_USDT", false, 104, 108, 4),
            // A fresh connection's snapshot trailing what is recorded.
            obu("BTC_USDT", true, 0, 106, 5),
            obu("BTC_USDT", false, 109, 110, 6),
        ];
        for frame in feed {
            ingest.handle(Frame::now(frame)).await.unwrap();
        }
        assert!(
            events.try_recv().is_err(),
            "no hole yet: {:?}",
            ingest.marks
        );

        ingest
            .handle(Frame::now(obu("BTC_USDT", false, 115, 120, 7)))
            .await
            .unwrap();
        let QualityEvent::FeedDegraded { source, detail, .. } = events.try_recv().unwrap() else {
            panic!("expected a degraded feed");
        };
        assert_eq!(source, "gate-sbe/btc_usdt");
        assert_eq!(
            detail,
            "book gap: expected first id 111 but got 115 (last 110)"
        );
        assert_eq!(ingest.marks["btc_usdt"], 120);
    }

    /// Unsubscribed streams share the `unknown` file, not a book chain: their
    /// unrelated ids must not read as holes in one another.
    #[tokio::test]
    async fn an_unsubscribed_pair_goes_to_unknown_and_is_reported_once() {
        let (quality, events) = QualityReporter::test_channel();
        let (mut ingest, mut writer_rx) = ingest(quality);

        for (pair, first_id, last_id) in [
            ("XRP_USDT", 9, 10),
            ("XRP_USDT", 11, 12),
            ("SOL_USDT", 500, 501),
            ("XRP_USDT", 13, 13),
        ] {
            ingest
                .handle(Frame::now(obu(pair, false, first_id, last_id, 1)))
                .await
                .unwrap();
        }

        assert!(
            written(&mut writer_rx)
                .iter()
                .all(|record| &*record.symbol == "unknown")
        );
        let reports: Vec<_> = std::iter::from_fn(|| events.try_recv().ok()).collect();
        assert_eq!(reports.len(), 2, "one report per name, no book gaps");
        assert!(ingest.marks.is_empty());
    }

    #[test]
    fn a_listing_must_name_the_pair() {
        let spot = br#"{"id":"BTC_USDT","base":"BTC","quote":"USDT","trade_status":"tradable"}"#;
        assert_eq!(
            check_listing(Market::Spot, "BTC_USDT", StatusCode::OK, spot).unwrap(),
            None
        );
        let futures = br#"{"name":"BTC_USDT","status":"trading","in_delisting":false}"#;
        assert_eq!(
            check_listing(Market::Futures, "BTC_USDT", StatusCode::OK, futures).unwrap(),
            None
        );

        let listed = |pair, body| check_listing(Market::Spot, pair, StatusCode::OK, body).unwrap();
        assert!(listed("ETH_USDT", spot).is_some());
        assert!(listed("BTC_USDT", futures).is_some());
    }

    /// What Gate really answers for pairs it does not list.
    #[test]
    fn an_unknown_pair_is_unlisted_by_gates_label() {
        let unknown = br#"{"label":"CONTRACT_NOT_FOUND"}"#;
        let reason = check_listing(
            Market::Futures,
            "NOPE_USDT",
            StatusCode::BAD_REQUEST,
            unknown,
        );
        assert_eq!(reason.unwrap().as_deref(), Some("CONTRACT_NOT_FOUND"));
        let unknown = br#"{"label":"INVALID_CURRENCY","message":"Invalid currency FOOX"}"#;
        let reason = check_listing(Market::Spot, "FOOX_USDT", StatusCode::BAD_REQUEST, unknown);
        assert_eq!(
            reason.unwrap().as_deref(),
            Some("INVALID_CURRENCY: Invalid currency FOOX")
        );
    }

    /// A rate limit or an outage is no answer: asked again.
    #[test]
    fn a_rate_limit_or_an_outage_is_no_answer() {
        let rate_limited =
            br#"{"label":"TOO_MANY_REQUESTS","message":"Request Rate limit Exceeded"}"#;
        for status in [StatusCode::TOO_MANY_REQUESTS, StatusCode::BAD_GATEWAY] {
            assert!(check_listing(Market::Spot, "BTC_USDT", status, rate_limited).is_err());
        }
    }
}
