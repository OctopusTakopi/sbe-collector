mod protocol;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, bail};
use bytes::Bytes;
use jiff::Timestamp;
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::{
    sync::{mpsc::Sender, watch},
    task::JoinSet,
};
use tracing::{error, warn};

use sbe_collector::{
    exchange::Exchange,
    record::Tag,
    sbe::{
        MessageHeader,
        gate::{self, Market, ObuLink},
    },
};
use zerocopy::FromBytes;

use crate::{
    dedup::{DEDUP_MAX_ENTRIES, DEDUP_WINDOW, Dedup},
    error::ConnectorError,
    feed::{BackgroundResult, Feed},
    file::{Symbol, WriteRecord},
    quality::{QualityEvent, QualityReporter},
    readiness::Readiness,
    session::{self, Protocol},
};
use protocol::Gate;

const REST_URL: &str = "https://api.gateio.ws/api/v4";

/// What to collect: checked against Gate's listings before anything starts,
/// so an unknown pair stops the process cleanly.
pub struct Subscription {
    market: Market,
    /// As Gate spells them, `BTC_USDT`.
    pairs: Vec<String>,
    connections: usize,
}

impl Subscription {
    pub async fn prepare(
        market: Market,
        symbols: &[String],
        connections: usize,
    ) -> anyhow::Result<Self> {
        let pairs: Vec<String> = symbols
            .iter()
            .map(|symbol| symbol.to_ascii_uppercase())
            .collect();
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()?;
        for pair in &pairs {
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
            check_listing(market, pair, status, &body)?;
        }
        Ok(Self {
            market,
            pairs,
            connections,
        })
    }
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

/// Whether Gate lists `pair` in `market`; a pair it lists but does not trade
/// is recorded anyway, with a warning, since it streams nothing until it does.
fn check_listing(
    market: Market,
    pair: &str,
    status: StatusCode,
    body: &[u8],
) -> anyhow::Result<()> {
    let preview = || String::from_utf8_lossy(&body[..body.len().min(512)]).into_owned();
    if !status.is_success() {
        bail!("Gate does not list {pair}: {status} {}", preview());
    }
    let listing: Listing = serde_json::from_slice(body)
        .with_context(|| format!("unreadable listing of {pair}: {}", preview()))?;
    let listed = match market {
        Market::Spot => listing.id,
        Market::Futures => listing.name,
    };
    if listed.as_deref() != Some(pair) {
        bail!("Gate answered {listed:?} for {pair}");
    }
    if listing
        .trade_status
        .as_deref()
        .is_some_and(|status| status != "tradable")
        || listing.in_delisting == Some(true)
    {
        warn!(pair, "Gate lists the pair as not trading");
    }
    Ok(())
}

/// The single consumer of every connection's frames: duplicate suppression,
/// routing by the instrument each frame names, and the merged book chain.
struct Ingest {
    market: Market,
    writer_tx: Sender<WriteRecord>,
    /// Gate's spelling, `BTC_USDT`, to the file key, `btc_usdt`.
    symbols: HashMap<Box<[u8]>, Symbol>,
    /// Names already reported as unsubscribed: the quality log syncs each
    /// event.
    unknown: HashSet<Box<[u8]>>,
    /// The highest `obu` id recorded per symbol, across every connection.
    marks: HashMap<Symbol, i64>,
    dedup: Dedup,
    quality: QualityReporter,
    /// Connection supervisors.
    tasks: JoinSet<BackgroundResult>,
}

impl Ingest {
    fn new(
        market: Market,
        exchange: Exchange,
        writer_tx: Sender<WriteRecord>,
        pairs: &[String],
        quality: QualityReporter,
    ) -> Self {
        Self {
            market,
            writer_tx,
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
            // Even one connection overlaps two sockets every hour, when its
            // session is replaced, and Gate repeats no frame unchanged, so
            // filtering a single connection loses nothing.
            dedup: Dedup::new(exchange, DEDUP_WINDOW, DEDUP_MAX_ENTRIES),
            quality,
            tasks: JoinSet::new(),
        }
    }

    async fn handle(&mut self, recv_time: Timestamp, data: Bytes) -> Result<(), ConnectorError> {
        if self.dedup.is_duplicate(&data) {
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
        self.write(recv_time, symbol, data).await
    }

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

    async fn write(
        &self,
        recv_time: Timestamp,
        symbol: Symbol,
        data: Bytes,
    ) -> Result<(), ConnectorError> {
        self.writer_tx
            .send(WriteRecord {
                recv_time,
                symbol,
                tag: Tag::Sbe,
                data,
            })
            .await
            .map_err(|_| ConnectorError::WriterClosed)
    }
}

pub async fn run_collection(
    subscription: Subscription,
    writer_tx: Sender<WriteRecord>,
    shutdown: watch::Receiver<bool>,
    quality: QualityReporter,
    readiness: Readiness,
) -> anyhow::Result<()> {
    let Subscription {
        market,
        pairs,
        connections,
    } = subscription;
    let exchange = match market {
        Market::Spot => Exchange::GateSbeSpot,
        Market::Futures => Exchange::GateSbeFutures,
    };
    let gate = Arc::new(Gate::new(market, &pairs, quality.clone()));
    let mut ingest = Ingest::new(market, exchange, writer_tx, &pairs, quality.clone());
    let ws_rx = session::spawn_slots(&mut ingest.tasks, gate, connections, &quality, &readiness);
    let mut feed = Feed::new(ws_rx, shutdown);

    while let Some((recv_time, data)) = feed.recv(&mut ingest.tasks).await? {
        match ingest.handle(recv_time, data).await {
            Ok(()) => {}
            Err(ConnectorError::WriterClosed) => return Err(ConnectorError::WriterClosed.into()),
            Err(error) => error!(%error, "couldn't handle the received data"),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
        (
            Ingest::new(
                Market::Spot,
                Exchange::GateSbeSpot,
                writer_tx,
                &["BTC_USDT".to_owned()],
                quality,
            ),
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
                .handle(Timestamp::now(), obu("BTC_USDT", true, 0, 9, time))
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
            ingest.handle(Timestamp::now(), frame).await.unwrap();
        }
        assert!(
            events.try_recv().is_err(),
            "no hole yet: {:?}",
            ingest.marks
        );

        ingest
            .handle(Timestamp::now(), obu("BTC_USDT", false, 115, 120, 7))
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
                .handle(Timestamp::now(), obu(pair, false, first_id, last_id, 1))
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
        check_listing(Market::Spot, "BTC_USDT", StatusCode::OK, spot).unwrap();
        let futures = br#"{"name":"BTC_USDT","status":"trading","in_delisting":false}"#;
        check_listing(Market::Futures, "BTC_USDT", StatusCode::OK, futures).unwrap();

        assert!(check_listing(Market::Spot, "ETH_USDT", StatusCode::OK, spot).is_err());
        assert!(check_listing(Market::Spot, "BTC_USDT", StatusCode::OK, futures).is_err());
        let unknown = br#"{"label":"CONTRACT_NOT_FOUND"}"#;
        let error = check_listing(
            Market::Futures,
            "NOPE_USDT",
            StatusCode::BAD_REQUEST,
            unknown,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("CONTRACT_NOT_FOUND"), "{error}");
    }
}
