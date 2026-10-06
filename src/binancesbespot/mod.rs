pub(crate) mod snapshot;
mod ws;

use std::collections::HashMap;

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
    quality::{QualityEvent, QualityReporter},
    session::{Frame, Protocol},
    throttler::{SNAPSHOT_WEIGHT_BUDGET, Throttler},
};
use snapshot::Snapshots;
use ws::Endpoint;

/// Streams subscribed per symbol; `$symbol` becomes the lowercase symbol.
const STREAMS: [&str; 4] = [
    "$symbol@trade",
    "$symbol@bestBidAsk",
    "$symbol@depth",
    "$symbol@depth20",
];

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
    snapshots: Snapshots,
    /// Snapshots fetched to repair a depth gap.
    recoveries: JoinSet<()>,
    quality: QualityReporter,
}

impl Ingest {
    fn new(
        recorder: Recorder,
        symbols: Vec<Symbol>,
        snapshots: Snapshots,
        quality: QualityReporter,
    ) -> Self {
        Self {
            recorder,
            symbols: SymbolMatcher::new(symbols),
            last_update_ids: HashMap::new(),
            snapshots,
            recoveries: JoinSet::new(),
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
            warn!(
                len = data.len(),
                template_id, "could not identify symbol in SBE frame; writing to 'unknown'"
            );
            self.quality.report(QualityEvent::FeedDegraded {
                at_ns: QualityEvent::now_ns(),
                source: Endpoint::SOURCE.to_owned(),
                detail: format!(
                    "could not identify symbol in SBE frame (template_id={template_id}, len={})",
                    data.len()
                ),
            });
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
        while self.recoveries.try_join_next().is_some() {}
        let snapshots = self.snapshots.clone();
        let writer_tx = self.recorder.writer_tx.clone();
        let symbol = Symbol::clone(symbol);
        self.recoveries.spawn(async move {
            // A closed writer is the collection's own failure to report.
            let _ = snapshots.record(&symbol, &writer_tx).await;
        });
    }
}

/// What to collect, from the command line. Built before anything starts, so
/// a missing or malformed key stops the process cleanly.
pub struct Subscription {
    endpoint: Endpoint,
    symbols: Vec<String>,
}

impl Subscription {
    pub fn from_env(symbols: &[String]) -> anyhow::Result<Self> {
        let api_key = std::env::var("BINANCE_API_KEY").unwrap_or_default();
        if api_key.is_empty() {
            anyhow::bail!("BINANCE_API_KEY env var must be set for the SBE stream endpoint");
        }
        Ok(Self {
            endpoint: Endpoint::new(&STREAMS, symbols, &api_key)?,
            symbols: symbols.to_vec(),
        })
    }
}

/// Records `subscription` until shutdown or a critical failure.
pub async fn collect(subscription: Subscription, cx: Context) -> anyhow::Result<()> {
    let Subscription { endpoint, symbols } = subscription;
    let symbols: Vec<Symbol> = symbols
        .iter()
        .map(|symbol| Symbol::from(symbol.to_ascii_lowercase()))
        .collect();
    let snapshots = Snapshots::new(Throttler::new(SNAPSHOT_WEIGHT_BUDGET))?;
    let mut tasks = JoinSet::new();
    {
        let snapshots = snapshots.clone();
        let symbols = symbols.clone();
        let writer_tx = cx.writer_tx.clone();
        tasks.spawn(async move {
            snapshots.run_periodic(symbols, writer_tx).await;
            Err(anyhow::anyhow!("periodic depth-snapshot task exited"))
        });
    }
    let recorder = Recorder {
        dedup: Dedup::for_connections(cx.connections, Exchange::BinanceSbeSpot),
        writer_tx: cx.writer_tx.clone(),
    };
    let ingest = Ingest::new(recorder, symbols, snapshots, cx.quality.clone());
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
    fn ingest(connections: usize) -> (Ingest, Receiver<WriteRecord>) {
        let (writer_tx, writer_rx) = channel(16);
        let recorder = Recorder {
            dedup: Dedup::for_connections(connections, Exchange::BinanceSbeSpot),
            writer_tx,
        };
        let ingest = Ingest::new(
            recorder,
            vec![Symbol::from("btcusdt")],
            Snapshots::new(Throttler::new(0)).unwrap(),
            QualityReporter::disabled(),
        );
        (ingest, writer_rx)
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
        let (mut ingest, mut writer_rx) = ingest(2);
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
        assert!(
            ingest.recoveries.is_empty(),
            "no gap should have been reported"
        );
        assert_eq!(ingest.last_update_ids["btcusdt"], 9);
    }

    /// One connection dropping mid-stream leaves no hole: the other covers
    /// the frames it missed.
    #[tokio::test]
    async fn a_reconnect_on_one_connection_leaves_no_gap() {
        let (mut ingest, mut writer_rx) = ingest(2);
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
        assert!(
            ingest.recoveries.is_empty(),
            "no gap, so no recovery snapshot to fetch"
        );
    }

    /// A frame arriving after a later one costs one gap, for the moment the
    /// hole was real; the straggler filling it is not a second one.
    #[tokio::test]
    async fn a_straggler_fills_a_hole_rather_than_reporting_another() {
        let (mut ingest, _writer_rx) = ingest(2);
        let sequence = depth_sequence();

        ingest.feed(sequence[0].clone()).await;
        ingest.feed(sequence[2].clone()).await;
        assert_eq!(ingest.last_update_ids["btcusdt"], 7);
        assert_eq!(
            ingest.recoveries.len(),
            1,
            "the skipped frame is a real hole"
        );

        ingest.feed(sequence[1].clone()).await;
        assert_eq!(
            ingest.last_update_ids["btcusdt"], 7,
            "sequence only moves forward"
        );

        ingest.feed(sequence[3].clone()).await;
        assert_eq!(
            ingest.recoveries.len(),
            1,
            "already-covered frames must not each fetch a snapshot"
        );
    }

    #[tokio::test]
    async fn a_restarted_book_resyncs_instead_of_wedging() {
        let (mut ingest, _writer_rx) = ingest(2);
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
            ingest.recoveries.len() <= 1,
            "one resync, not a snapshot fetch per frame: {}",
            ingest.recoveries.len()
        );
    }

    /// Unfiltered with one connection, so a session handover's overlap is
    /// recorded twice (see [`Dedup::for_connections`]), but the repeat is
    /// covered ground, not a gap.
    #[tokio::test]
    async fn a_single_connection_records_every_frame_it_receives() {
        let (mut ingest, mut writer_rx) = ingest(1);
        let sequence = depth_sequence();

        for frame in &sequence {
            ingest.feed(frame.clone()).await;
        }
        ingest
            .feed(as_seen_by_another_connection(&sequence[3]))
            .await;

        assert_eq!(written(&mut writer_rx), sequence.len() + 1);
        assert!(
            ingest.recoveries.is_empty(),
            "a repeat is already-covered ground, not a hole to refetch"
        );
    }

    #[tokio::test]
    async fn malformed_frames_say_what_is_missing() {
        let (mut ingest, _writer_rx) = ingest(1);

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

    #[tokio::test]
    async fn an_unidentified_frame_is_written_to_unknown() {
        let (mut ingest, mut writer_rx) = ingest(1);
        let mut frame = depth_diff(2, 3).to_vec();
        let len = frame.len();
        frame[len - 7..].copy_from_slice(b"XRPUSDT");

        ingest.feed(Bytes::from(frame)).await;

        let record = writer_rx.try_recv().unwrap();
        assert_eq!(&*record.symbol, "unknown");
        assert!(ingest.last_update_ids.is_empty());
    }
}
