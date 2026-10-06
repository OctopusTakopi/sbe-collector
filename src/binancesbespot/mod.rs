pub(crate) mod snapshot;
mod ws;

use std::collections::{HashMap, HashSet};

use anyhow::bail;
use serde::Deserialize;
use tokio::task::JoinSet;
use tracing::warn;
use zerocopy::FromBytes;

use sbe_collector::{
    exchange::Exchange,
    sbe::{
        MessageHeader,
        binance::{DepthDiffBlock, TEMPLATE_DEPTH_DIFF},
    },
};

use crate::{
    collection::{self, Consumer, Context, Recorder},
    dedup::Dedup,
    error::ConnectorError,
    file::Symbol,
    listing::{self, Unlisted},
    quality::{QualityEvent, QualityReporter},
    session::{Frame, Protocol},
    snapshot::{Repairs, Snapshots},
};
use snapshot::{Book, SNAPSHOT_WEIGHT_BUDGET};
use ws::Endpoint;

/// Streams subscribed per symbol; `$symbol` becomes the lowercase symbol.
const STREAMS: [&str; 4] = [
    "$symbol@trade",
    "$symbol@bestBidAsk",
    "$symbol@depth",
    "$symbol@depth20",
];

const EXCHANGE_INFO: &str = "https://api.binance.com/api/v3/exchangeInfo?symbol=";

/// How far a book update id may fall below the last one seen before the
/// stream is treated as restarted rather than reordered.
///
/// Redundant connections reorder by at most the dedup window's worth of
/// updates, thousands on the busiest symbol; a book that re-bases drops by
/// orders of magnitude more, since Binance's ids run in the billions.
const RESYNC_BACKSTEP: i64 = 1_000_000;

/// Whether `data` ends with `symbol` as the `VarString8` (length byte, then
/// ASCII) that the stream bridge appends to every SBE frame. Case-insensitive.
fn frame_has_symbol(data: &[u8], symbol: &str) -> bool {
    let n = data.len();
    let symbol_len = symbol.len();
    if n < symbol_len + 1 {
        return false;
    }
    let len_pos = n - symbol_len - 1;
    data[len_pos] as usize == symbol_len
        && data[n - symbol_len..].eq_ignore_ascii_case(symbol.as_bytes())
}

/// The subscribed symbol `data` belongs to, as the entry in `symbols`, so the
/// caller clones an `Arc` rather than allocating. Matching the known set keeps
/// both the length byte and the content tight.
fn find_symbol<'a>(data: &[u8], symbols: &'a [Symbol]) -> Option<&'a Symbol> {
    symbols.iter().find(|symbol| frame_has_symbol(data, symbol))
}

/// Resolves frames to subscribed symbols, trying the last match first.
struct SymbolMatcher {
    symbols: Vec<Symbol>,
    last: Option<Symbol>,
}

impl SymbolMatcher {
    fn new(symbols: Vec<Symbol>) -> Self {
        Self {
            symbols,
            last: None,
        }
    }

    fn resolve(&mut self, data: &[u8]) -> Option<Symbol> {
        if let Some(symbol) = &self.last
            && frame_has_symbol(data, symbol)
        {
            return Some(Symbol::clone(symbol));
        }
        let symbol = Symbol::clone(find_symbol(data, &self.symbols)?);
        self.last = Some(Symbol::clone(&symbol));
        Some(symbol)
    }
}

/// The single consumer of every connection's frames: duplicate suppression,
/// symbol routing and depth continuity.
struct Ingest {
    recorder: Recorder,
    symbols: SymbolMatcher,
    /// Highest `lastBookUpdateId` seen per symbol.
    last_update_ids: HashMap<Symbol, i64>,
    /// Templates whose frames already went to 'unknown': the quality log
    /// syncs each event.
    unknown: HashSet<u16>,
    /// Asks for the snapshots that repair a depth gap.
    repairs: Repairs,
    quality: QualityReporter,
}

impl Ingest {
    fn new(
        recorder: Recorder,
        symbols: Vec<Symbol>,
        repairs: Repairs,
        quality: QualityReporter,
    ) -> Self {
        Self {
            recorder,
            symbols: SymbolMatcher::new(symbols),
            last_update_ids: HashMap::new(),
            unknown: HashSet::new(),
            repairs,
            quality,
        }
    }
}

impl Consumer for Ingest {
    async fn handle(&mut self, frame: Frame) -> Result<(), ConnectorError> {
        let Frame { recv_time, data } = frame;
        // Before anything reads update ids: a second copy of a depth diff
        // starts where the first already ended, so it would read as a gap.
        if self.recorder.dedup.is_duplicate(&data) {
            return Ok(());
        }
        let Ok((header, body)) = MessageHeader::read_from_prefix(&data) else {
            return Err(ConnectorError::ShortFrame { len: data.len() });
        };
        let template_id = header.template_id.get();

        let Some(symbol) = self.symbols.resolve(&data) else {
            self.report_unknown(template_id, data.len());
            return self
                .recorder
                .write(recv_time, Symbol::from("unknown"), data)
                .await;
        };

        if template_id == TEMPLATE_DEPTH_DIFF {
            let Ok((block, _)) = DepthDiffBlock::read_from_prefix(body) else {
                return Err(ConnectorError::ShortBlock {
                    name: "depth diff",
                    len: data.len(),
                });
            };
            self.check_continuity(
                &symbol,
                block.first_book_update_id.get(),
                block.last_book_update_id.get(),
            );
        }
        self.recorder.write(recv_time, symbol, data).await
    }
}

impl Ingest {
    /// Every frame ends with a subscribed symbol, so one that does not is
    /// worth a quality event: once per template, not once per frame.
    fn report_unknown(&mut self, template_id: u16, len: usize) {
        if !self.unknown.insert(template_id) {
            return;
        }
        warn!(
            len,
            template_id, "could not identify the symbol of an SBE frame; writing to 'unknown'"
        );
        self.quality.report(QualityEvent::FeedDegraded {
            at_ns: QualityEvent::now_ns(),
            source: Endpoint::SOURCE.to_owned(),
            detail: format!(
                "frames of template_id={template_id} name no subscribed symbol; they go to 'unknown'"
            ),
        });
    }

    /// Report a forward hole in `symbol`'s depth diffs and fetch a snapshot to
    /// repair it.
    ///
    /// A diff starting at or below the next expected id is already covered: a
    /// straggler from a lagging connection, or the frame that fills an earlier
    /// hole. The mark only moves forward, so one reorder cannot make the next
    /// in-order diff a gap as well.
    fn check_continuity(&mut self, symbol: &Symbol, first_id: i64, last_id: i64) {
        let Some(mark) = self.last_update_ids.get_mut(symbol) else {
            self.last_update_ids.insert(Symbol::clone(symbol), last_id);
            return;
        };
        let prev = *mark;
        if last_id < prev.saturating_sub(RESYNC_BACKSTEP) {
            // A relist or maintenance restarts the ids; a mark left above them
            // would read every later diff as a hole.
            warn!(
                %symbol,
                prev_u = prev,
                last_u = last_id,
                "book update ids restarted well below the last seen; resyncing"
            );
            *mark = last_id;
            return;
        }
        *mark = prev.max(last_id);
        let expected = prev.saturating_add(1);
        if first_id <= expected {
            return;
        }
        warn!(
            %symbol,
            "depth gap detected: expected first_u={expected} but got {first_id} (prev_u={prev})"
        );
        self.quality.report(QualityEvent::FeedDegraded {
            at_ns: QualityEvent::now_ns(),
            source: format!("{}/{symbol}", Endpoint::SOURCE),
            detail: format!(
                "depth gap: expected first_u={expected} but got {first_id} (prev_u={prev})"
            ),
        });
        self.repairs.request(symbol);
    }
}

/// What to collect, from the command line. Built before anything starts, so
/// a missing or malformed key, or a symbol Binance does not list, stops the
/// process cleanly.
pub struct Subscription {
    endpoint: Endpoint,
    symbols: Vec<String>,
}

impl Subscription {
    pub async fn prepare(symbols: &[String]) -> anyhow::Result<Self> {
        let api_key = std::env::var("BINANCE_API_KEY").unwrap_or_default();
        if api_key.is_empty() {
            bail!("BINANCE_API_KEY env var must be set for the SBE stream endpoint");
        }
        let endpoint = Endpoint::new(&STREAMS, symbols, &api_key)?;
        let client = listing::client()?;
        let unlisted = listing::patiently(listing::PATIENCE, || unlisted(&client, symbols)).await?;
        if !unlisted.is_empty() {
            return Err(Unlisted(unlisted).into());
        }
        Ok(Self {
            endpoint,
            symbols: symbols.to_vec(),
        })
    }
}

/// Why Binance does not list each of `symbols` it does not.
async fn unlisted(client: &reqwest::Client, symbols: &[String]) -> anyhow::Result<Vec<String>> {
    let mut unlisted = Vec::new();
    for symbol in symbols {
        let symbol = symbol.to_ascii_uppercase();
        let response = client
            .get(format!("{EXCHANGE_INFO}{symbol}"))
            .send()
            .await?;
        let status = response.status().as_u16();
        let body = response.bytes().await?;
        if let Some(reason) = exchange_info(status, &body)? {
            unlisted.push(format!("{symbol} ({reason})"));
        }
    }
    Ok(unlisted)
}

/// Why `exchangeInfo?symbol=` says Binance does not list a symbol, if it
/// does not. A 400 refuses the request itself, which asking again cannot
/// change: `-1121 Invalid symbol.`, or `-1100` for one with characters no
/// symbol has. Anything else, a rate limit or an outage, is no answer.
fn exchange_info(status: u16, body: &[u8]) -> anyhow::Result<Option<String>> {
    #[derive(Deserialize)]
    struct Refusal {
        code: i64,
        msg: String,
    }
    let preview = || String::from_utf8_lossy(&body[..body.len().min(256)]).into_owned();
    match status {
        200..=299 => Ok(None),
        400 => Ok(Some(match serde_json::from_slice::<Refusal>(body) {
            Ok(refusal) => format!("{} {}", refusal.code, refusal.msg),
            Err(_) => preview(),
        })),
        _ => bail!("exchangeInfo answered {status}: {}", preview()),
    }
}

/// Records `subscription` until shutdown or a critical failure.
pub async fn collect(subscription: Subscription, cx: Context) -> anyhow::Result<()> {
    let Subscription { endpoint, symbols } = subscription;
    let symbols: Vec<Symbol> = symbols
        .iter()
        .map(|symbol| Symbol::from(symbol.to_ascii_lowercase()))
        .collect();
    let (snapshots, repairs) = Snapshots::new(
        Book,
        SNAPSHOT_WEIGHT_BUDGET,
        symbols.clone(),
        cx.writer_tx.clone(),
    )?;
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        snapshots.run().await;
        Err(anyhow::anyhow!("depth-snapshot task exited"))
    });
    let recorder = Recorder {
        dedup: Dedup::for_connections(cx.connections, Exchange::BinanceSbeSpot),
        writer_tx: cx.writer_tx.clone(),
    };
    let ingest = Ingest::new(recorder, symbols, repairs, cx.quality.clone());
    collection::run_slots(endpoint, ingest, cx, tasks).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::WriteRecord;
    use bytes::Bytes;
    use sbe_collector::sbe::binance::EVENT_TIME;
    use std::sync::Arc;
    use tokio::sync::mpsc::{Receiver, channel};

    #[test]
    fn finds_canonical_symbol_without_allocating() {
        let symbols = vec![Symbol::from("btcusdt"), Symbol::from("ethusdt")];
        let data = b"\x00\x00\x00\x00\x00\x00\x00\x00\x07BTCUSDT";

        let found = find_symbol(data, &symbols).unwrap();

        assert!(Arc::ptr_eq(found, &symbols[0]));
    }

    /// A `DepthDiffStreamEvent` for BTCUSDT with empty bid and ask groups:
    /// 8-byte header, the 26-byte fixed block, two `groupSize16Encoding`
    /// dimensions, then the symbol the stream bridge appends. 50 bytes.
    fn depth_diff(first_u: i64, last_u: i64) -> Bytes {
        let mut frame = Vec::with_capacity(50);
        frame.extend_from_slice(&26u16.to_le_bytes()); // blockLength
        frame.extend_from_slice(&TEMPLATE_DEPTH_DIFF.to_le_bytes());
        frame.extend_from_slice(&1u16.to_le_bytes()); // schemaId
        frame.extend_from_slice(&0u16.to_le_bytes()); // version
        // eventTime, unrelated to the update ids: tying them would make the
        // "other connection" copies byte-identical and let a whole-frame key
        // pass these tests.
        frame.extend_from_slice(&1_785_046_950_114_506i64.to_le_bytes());
        frame.extend_from_slice(&first_u.to_le_bytes());
        frame.extend_from_slice(&last_u.to_le_bytes());
        frame.push(0); // priceExponent
        frame.push(0); // qtyExponent
        for _ in 0..2 {
            frame.extend_from_slice(&16u16.to_le_bytes()); // group blockLength
            frame.extend_from_slice(&0u16.to_le_bytes()); // numInGroup
        }
        frame.push(7); // VarString8 length
        frame.extend_from_slice(b"BTCUSDT");
        Bytes::from(frame)
    }

    /// The same event as a redundant connection delivers it: identical but for
    /// the per-connection `eventTime`.
    fn as_seen_by_another_connection(frame: &Bytes) -> Bytes {
        let mut copy = frame.to_vec();
        let stamp = i64::from_le_bytes(copy[EVENT_TIME].try_into().unwrap());
        copy[EVENT_TIME].copy_from_slice(&(stamp - 23).to_le_bytes());
        assert_ne!(&copy[..], &frame[..], "the copies must differ on the wire");
        Bytes::from(copy)
    }

    /// An ingest for BTCUSDT. Its weight budget is zero, so a gap's recovery
    /// snapshot can never reach Binance from a unit test.
    fn ingest(connections: usize) -> (Ingest, Snapshots<Book>, Receiver<WriteRecord>) {
        ingest_reporting(connections, QualityReporter::disabled())
    }

    /// An ingest for BTCUSDT, its snapshot task, never run, and the writer
    /// queue it records into.
    fn ingest_reporting(
        connections: usize,
        quality: QualityReporter,
    ) -> (Ingest, Snapshots<Book>, Receiver<WriteRecord>) {
        let (writer_tx, writer_rx) = channel(16);
        let btcusdt = vec![Symbol::from("btcusdt")];
        // No budget: a repair must never reach Binance from a unit test.
        let (snapshots, repairs) =
            Snapshots::new(Book, 0, btcusdt.clone(), writer_tx.clone()).unwrap();
        let recorder = Recorder {
            dedup: Dedup::for_connections(connections, Exchange::BinanceSbeSpot),
            writer_tx,
        };
        let ingest = Ingest::new(recorder, btcusdt, repairs, quality);
        (ingest, snapshots, writer_rx)
    }

    impl Ingest {
        async fn feed(&mut self, data: Bytes) {
            self.handle(Frame::now(data)).await.unwrap();
        }
    }

    fn written(writer_rx: &mut Receiver<WriteRecord>) -> usize {
        std::iter::from_fn(|| writer_rx.try_recv().ok()).count()
    }

    /// Depth diffs, in order: each starts at the id after the previous one's
    /// last.
    fn depth_sequence() -> [Bytes; 4] {
        [
            depth_diff(2, 3),
            depth_diff(4, 5),
            depth_diff(6, 7),
            depth_diff(8, 9),
        ]
    }

    #[tokio::test]
    async fn a_second_connections_copy_is_neither_written_nor_read_as_a_gap() {
        let (mut ingest, snapshots, mut writer_rx) = ingest(2);
        let sequence = depth_sequence();

        for frame in &sequence {
            ingest.feed(frame.clone()).await;
            ingest.feed(as_seen_by_another_connection(frame)).await;
        }

        assert_eq!(
            written(&mut writer_rx),
            sequence.len(),
            "each frame is recorded once"
        );
        assert_eq!(snapshots.asked(), 0, "no gap should have been reported");
        assert_eq!(ingest.last_update_ids["btcusdt"], 9);
    }

    /// One connection dropping mid-stream leaves no hole: the other covers
    /// the frames it missed.
    #[tokio::test]
    async fn a_reconnect_on_one_connection_leaves_no_gap() {
        let (mut ingest, snapshots, mut writer_rx) = ingest(2);
        let sequence = depth_sequence();

        for frame in &sequence[..2] {
            ingest.feed(frame.clone()).await;
            ingest.feed(as_seen_by_another_connection(frame)).await;
        }
        // Connection 0 is down for the third frame.
        ingest
            .feed(as_seen_by_another_connection(&sequence[2]))
            .await;
        ingest.feed(sequence[3].clone()).await;
        ingest
            .feed(as_seen_by_another_connection(&sequence[3]))
            .await;

        assert_eq!(
            written(&mut writer_rx),
            sequence.len(),
            "the stream is still complete"
        );
        assert_eq!(
            snapshots.asked(),
            0,
            "no gap, so no recovery snapshot to fetch"
        );
    }

    /// A frame arriving after a later one costs one gap, for the moment the
    /// hole was real; the straggler filling it is not a second one.
    #[tokio::test]
    async fn a_straggler_fills_a_hole_rather_than_reporting_another() {
        let (mut ingest, snapshots, _writer_rx) = ingest(2);
        let sequence = depth_sequence();

        ingest.feed(sequence[0].clone()).await;
        ingest.feed(sequence[2].clone()).await;
        assert_eq!(ingest.last_update_ids["btcusdt"], 7);
        assert_eq!(snapshots.asked(), 1, "the skipped frame is a real hole");

        ingest.feed(sequence[1].clone()).await;
        assert_eq!(
            ingest.last_update_ids["btcusdt"], 7,
            "sequence only moves forward"
        );

        ingest.feed(sequence[3].clone()).await;
        assert_eq!(
            snapshots.asked(),
            1,
            "already-covered frames must not each fetch a snapshot"
        );
    }

    #[tokio::test]
    async fn a_restarted_book_resyncs_instead_of_wedging() {
        let (mut ingest, snapshots, _writer_rx) = ingest(2);
        // Parked in the billions, as Binance ids really are.
        ingest
            .last_update_ids
            .insert(Symbol::from("btcusdt"), 5_000_000_000);

        for frame in depth_sequence() {
            ingest.feed(frame).await;
        }

        assert_eq!(
            ingest.last_update_ids["btcusdt"], 9,
            "the mark follows the restarted stream"
        );
        assert!(
            snapshots.asked() <= 1,
            "one resync, not a snapshot fetch per frame: {}",
            snapshots.asked()
        );
    }

    /// Unfiltered with one connection, so a session handover's overlap is
    /// recorded twice (see [`Dedup::for_connections`]), but the repeat is
    /// covered ground, not a gap.
    #[tokio::test]
    async fn a_single_connection_records_every_frame_it_receives() {
        let (mut ingest, snapshots, mut writer_rx) = ingest(1);
        let sequence = depth_sequence();

        for frame in &sequence {
            ingest.feed(frame.clone()).await;
        }
        ingest
            .feed(as_seen_by_another_connection(&sequence[3]))
            .await;

        assert_eq!(written(&mut writer_rx), sequence.len() + 1);
        assert_eq!(
            snapshots.asked(),
            0,
            "a repeat is already-covered ground, not a hole to refetch"
        );
    }

    #[tokio::test]
    async fn malformed_frames_say_what_is_missing() {
        let (mut ingest, _snapshots, _writer_rx) = ingest(1);

        let runt = ingest.handle(Frame::now(Bytes::from_static(b"\x01\x02")));
        assert_eq!(
            runt.await.unwrap_err().to_string(),
            "2-byte frame is shorter than the SBE message header"
        );
        let mut truncated = depth_diff(2, 3).to_vec();
        truncated.drain(20..42);
        let truncated = ingest.handle(Frame::now(Bytes::from(truncated)));
        assert_eq!(
            truncated.await.unwrap_err().to_string(),
            "28-byte depth diff is shorter than its fixed block"
        );
    }

    #[test]
    fn exchange_info_names_an_invalid_symbol_and_retries_anything_else() {
        assert_eq!(exchange_info(200, br#"{"symbols":[]}"#).unwrap(), None);
        assert_eq!(
            exchange_info(400, br#"{"code":-1121,"msg":"Invalid symbol."}"#)
                .unwrap()
                .as_deref(),
            Some("-1121 Invalid symbol.")
        );
        assert_eq!(
            exchange_info(400, br#"{"code":-1100,"msg":"Illegal characters found."}"#)
                .unwrap()
                .as_deref(),
            Some("-1100 Illegal characters found.")
        );
        assert!(exchange_info(418, br#"{"code":-1003}"#).is_err());
        assert!(exchange_info(502, b"<html>").is_err());
    }

    #[tokio::test]
    async fn an_unidentified_frame_is_written_to_unknown_and_reported_once() {
        let (quality, events) = QualityReporter::test_channel();
        let (mut ingest, _snapshots, mut writer_rx) = ingest_reporting(1, quality);
        for first_u in [2, 4] {
            let mut frame = depth_diff(first_u, first_u + 1).to_vec();
            let len = frame.len();
            frame[len - 7..].copy_from_slice(b"XRPUSDT");
            ingest.feed(Bytes::from(frame)).await;
        }

        for _ in 0..2 {
            assert_eq!(&*writer_rx.try_recv().unwrap().symbol, "unknown");
        }
        assert!(ingest.last_update_ids.is_empty());
        assert!(matches!(
            events.try_recv(),
            Ok(QualityEvent::FeedDegraded { .. })
        ));
        assert!(events.try_recv().is_err(), "one report per template");
    }
}
