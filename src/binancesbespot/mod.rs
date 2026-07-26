// dead_code allow removed: unused items are now tracked explicitly.
mod snapshot;
mod ws;

use crate::dedup::Dedup;
use crate::error::ConnectorError;
use crate::feed::Feed;
use crate::file::{Symbol, Tag, WriteRecord};
use crate::sbe_types::{DepthDiffBlock, MessageHeader, TEMPLATE_DEPTH_DIFF};
use crate::throttler::Throttler;
use bytes::Bytes;
use jiff::Timestamp;
use std::collections::HashMap;
use tokio::{
    sync::{
        mpsc::{Sender, channel},
        watch,
    },
    task::JoinSet,
};
use zerocopy::FromBytes;

use ws::keep_connection;

/// Match the tail of the buffer against every known subscribed symbol to extract
/// the symbol appended by the Binance stream bridge as a `VarString8`
/// (1-byte length prefix + ASCII chars) at the very end of each SBE frame.
/// Checking against known symbols makes both the length and the byte content
/// constraints tight — an accidental match is essentially impossible.
///
/// Returns a reference to the matching entry in `symbols` (already-owned,
/// canonical form), so the caller pays zero extra allocations.
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

pub fn find_symbol<'a>(data: &[u8], symbols: &'a [Symbol]) -> Option<&'a Symbol> {
    symbols.iter().find(|symbol| frame_has_symbol(data, symbol))
}

pub struct SymbolMatcher {
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

        let symbol = find_symbol(data, &self.symbols)?;
        let symbol = Symbol::clone(symbol);
        self.last = Some(Symbol::clone(&symbol));
        Some(symbol)
    }

    fn symbols(&self) -> &[Symbol] {
        &self.symbols
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn handle(
    data: Bytes,
    writer_tx: &Sender<WriteRecord>,
    recv_time: Timestamp,
    symbols: &mut SymbolMatcher,
    prev_u_map: &mut HashMap<Symbol, i64>,
    dedup: &mut Dedup,
    client: &reqwest::Client,
    throttler: &Throttler,
    tasks: &mut JoinSet<()>,
) -> Result<(), ConnectorError> {
    // Before anything reads book update ids. A second copy of a depth diff
    // starts at the update id after the one the first copy already consumed, so
    // letting it through would report a gap on every single frame.
    if dedup.is_duplicate(&data) {
        return Ok(());
    }

    if data.len() < 8 {
        return Err(ConnectorError::Format);
    }

    let Ok((header, _)) = MessageHeader::read_from_prefix(&data) else {
        return Err(ConnectorError::Sbe);
    };

    let template_id = header.template_id.get();

    let Some(symbol) = symbols.resolve(&data) else {
        // Log unrecognised frames so silent data-loss is observable.
        tracing::warn!(
            len = data.len(),
            template_id,
            "could not identify symbol in SBE frame — writing to 'unknown'"
        );
        writer_tx
            .send(WriteRecord {
                recv_time,
                symbol: Symbol::from("unknown"),
                tag: Tag::Sbe,
                data,
            })
            .await
            .map_err(|_| ConnectorError::WriterClosed)?;
        return Ok(());
    };

    // Data-loss detection for depth diffs.
    if template_id == TEMPLATE_DEPTH_DIFF {
        if let Ok((blk, _)) = DepthDiffBlock::read_from_prefix(&data[8..]) {
            let u = blk.last_book_update_id.get();
            let first_u = blk.first_book_update_id.get();

            // Only trigger the gap alarm when we already have a previous update id.
            // On the very first message prev_u is None — normal startup, not a gap.
            if let Some(prev) = prev_u_map.get_mut(symbol.as_ref()) {
                if first_u != *prev + 1 {
                    tracing::warn!(
                        symbol = %symbol,
                        "depth gap detected: expected first_u={} but got {} (prev_u={})",
                        *prev + 1,
                        first_u,
                        *prev
                    );

                    let sym_for_spawn = Symbol::clone(&symbol);
                    let writer_tx_ = writer_tx.clone();
                    let client_ = client.clone();
                    let throttler_ = throttler.clone();

                    tasks.spawn(async move {
                        use crate::binancesbespot::snapshot::fetch_snapshot;
                        match throttler_
                            .execute(fetch_snapshot(&client_, &sym_for_spawn))
                            .await
                        {
                            Some(Ok(snap_data)) => {
                                let _ = writer_tx_
                                    .send(WriteRecord {
                                        recv_time: Timestamp::now(),
                                        symbol: sym_for_spawn,
                                        tag: Tag::Rest,
                                        data: snap_data,
                                    })
                                    .await;
                            }
                            Some(Err(e)) => {
                                tracing::error!(
                                    symbol = %sym_for_spawn,
                                    error = %e,
                                    "failed to fetch recovery snapshot"
                                );
                            }
                            None => {
                                tracing::warn!(
                                    symbol = %sym_for_spawn,
                                    "recovery snapshot rate-limited"
                                );
                            }
                        }
                    });
                }
                // Only ever forwards. Redundant connections can deliver an
                // event whose predecessor has not arrived yet (see `dedup`), and
                // rewinding here would make the *next* in-order frame look like
                // a gap too, turning one reorder into a snapshot fetch per frame
                // until the streams realign.
                *prev = u.max(*prev);
            } else {
                prev_u_map.insert(Symbol::clone(&symbol), u);
            }
        } else {
            return Err(ConnectorError::Sbe);
        }
    }

    writer_tx
        .send(WriteRecord {
            recv_time,
            symbol,
            tag: Tag::Sbe,
            data,
        })
        .await
        .map_err(|_| ConnectorError::WriterClosed)?;
    Ok(())
}

pub async fn run_collection(
    streams: Vec<String>,
    symbols: Vec<String>,
    writer_tx: Sender<WriteRecord>,
    api_key: String,
    shutdown: watch::Receiver<bool>,
    connections: usize,
) -> Result<(), anyhow::Error> {
    let connections = connections.max(1);
    let mut dedup = Dedup::for_connections(connections);
    // All connections share the queue, so it is sized per connection to keep
    // the burst each one can absorb independent of how many there are.
    let (ws_tx, ws_rx) =
        channel::<(Timestamp, Bytes)>(crate::WS_QUEUE_CAPACITY.saturating_mul(connections));
    let mut feed = Feed::new(ws_rx, shutdown);
    let mut tasks = JoinSet::new();
    let canonical_symbols: Vec<Symbol> = symbols
        .iter()
        .map(|symbol| Symbol::from(symbol.to_ascii_lowercase()))
        .collect();
    let mut symbol_matcher = SymbolMatcher::new(canonical_symbols);

    for connection in 0..connections {
        let streams = streams.clone();
        let symbols = symbols.clone();
        let api_key = api_key.clone();
        let ws_tx = ws_tx.clone();
        tasks.spawn(async move {
            tokio::time::sleep(crate::CONNECT_STAGGER * connection as u32).await;
            keep_connection(streams, symbols, api_key, connection, ws_tx).await;
            tracing::error!(connection, "the websocket connection task exited");
        });
    }
    // The clones above are the only senders that should keep the feed open;
    // holding this one would stop `Feed` from ever seeing the queue close.
    drop(ws_tx);

    // One shared reqwest::Client for both the periodic snapshot loop and
    // gap-triggered snapshot fetches.
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    let throttler = Throttler::new(100);
    {
        let symbols = symbol_matcher.symbols().to_vec();
        let writer_tx = writer_tx.clone();
        let client = client.clone();
        let throttler = throttler.clone();
        tasks.spawn(async move {
            snapshot::snapshot_loop(symbols, writer_tx, client, throttler, 3600).await;
            tracing::error!("the periodic depth-snapshot task exited");
        });
    }

    let mut prev_u_map = HashMap::new();

    let mut messages_before_reap = 1_024;
    while let Some((recv_time, data)) = feed.recv(&mut tasks).await {
        messages_before_reap -= 1;
        if messages_before_reap == 0 {
            while let Some(result) = tasks.try_join_next() {
                // Cancellation is how shutdown stops these tasks; only a panic
                // is worth reporting.
                if let Err(error) = result
                    && !error.is_cancelled()
                {
                    tracing::error!(?error, "background task failed");
                }
            }
            messages_before_reap = 1_024;
        }
        if let Err(error) = handle(
            data,
            &writer_tx,
            recv_time,
            &mut symbol_matcher,
            &mut prev_u_map,
            &mut dedup,
            &client,
            &throttler,
            &mut tasks,
        )
        .await
        {
            if matches!(&error, ConnectorError::WriterClosed) {
                return Err(error.into());
            }
            tracing::error!(?error, "couldn't handle the received data.");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::channel;

    #[test]
    fn finds_canonical_symbol_without_allocating() {
        let symbols = vec![Symbol::from("btcusdt"), Symbol::from("ethusdt")];
        let data = b"\x00\x00\x00\x00\x00\x00\x00\x00\x07BTCUSDT";

        let found = find_symbol(data, &symbols).unwrap();

        assert!(std::sync::Arc::ptr_eq(found, &symbols[0]));
    }

    /// A `DepthDiffStreamEvent` for BTCUSDT with empty bid and ask groups:
    /// 8-byte header, the 26-byte fixed block, two `groupSize16Encoding`
    /// dimensions, then the symbol the stream bridge appends.
    fn depth_diff(first_u: i64, last_u: i64) -> Bytes {
        let mut frame = Vec::with_capacity(44);
        frame.extend_from_slice(&26u16.to_le_bytes()); // blockLength
        frame.extend_from_slice(&TEMPLATE_DEPTH_DIFF.to_le_bytes());
        frame.extend_from_slice(&1u16.to_le_bytes()); // schemaId
        frame.extend_from_slice(&0u16.to_le_bytes()); // version
        frame.extend_from_slice(&first_u.to_le_bytes()); // eventTime
        frame.extend_from_slice(&first_u.to_le_bytes());
        frame.extend_from_slice(&last_u.to_le_bytes());
        frame.push(0u8 as i8 as u8); // priceExponent
        frame.push(0u8 as i8 as u8); // qtyExponent
        for _ in 0..2 {
            frame.extend_from_slice(&16u16.to_le_bytes()); // group blockLength
            frame.extend_from_slice(&0u16.to_le_bytes()); // numInGroup
        }
        frame.push(7); // VarString8 length
        frame.extend_from_slice(b"BTCUSDT");
        Bytes::from(frame)
    }

    struct Harness {
        symbols: SymbolMatcher,
        prev_u_map: HashMap<Symbol, i64>,
        dedup: Dedup,
        client: reqwest::Client,
        throttler: Throttler,
        tasks: JoinSet<()>,
    }

    impl Harness {
        fn with_connections(connections: usize) -> Self {
            Self {
                symbols: SymbolMatcher::new(vec![Symbol::from("btcusdt")]),
                prev_u_map: HashMap::new(),
                dedup: Dedup::for_connections(connections),
                client: reqwest::Client::new(),
                throttler: Throttler::new(1),
                tasks: JoinSet::new(),
            }
        }

        async fn feed(
            &mut self,
            writer_tx: &Sender<WriteRecord>,
            data: Bytes,
        ) -> Result<(), ConnectorError> {
            handle(
                data,
                writer_tx,
                Timestamp::now(),
                &mut self.symbols,
                &mut self.prev_u_map,
                &mut self.dedup,
                &self.client,
                &self.throttler,
                &mut self.tasks,
            )
            .await
        }
    }

    /// Depth diffs, in order: each one starts at the update id after the
    /// previous one's last.
    fn depth_sequence() -> [Bytes; 4] {
        [
            depth_diff(2, 3),
            depth_diff(4, 5),
            depth_diff(6, 7),
            depth_diff(8, 9),
        ]
    }

    /// The second connection's copy must be dropped *before* the continuity
    /// check. Its `firstBookUpdateId` is the one the first copy already
    /// consumed, so letting it through would report a gap on every frame.
    #[tokio::test]
    async fn a_second_connections_copy_is_neither_written_nor_read_as_a_gap() {
        let (writer_tx, mut writer_rx) = channel(16);
        let mut harness = Harness::with_connections(2);
        let sequence = depth_sequence();

        for frame in &sequence {
            // Both connections deliver every frame.
            harness.feed(&writer_tx, frame.clone()).await.unwrap();
            harness.feed(&writer_tx, frame.clone()).await.unwrap();
        }

        let mut written = 0;
        while writer_rx.try_recv().is_ok() {
            written += 1;
        }
        assert_eq!(written, sequence.len(), "each frame is recorded once");
        assert!(harness.tasks.is_empty(), "no gap should have been reported");
        assert_eq!(harness.prev_u_map["btcusdt"], 9);
    }

    /// The point of the whole feature: one connection dropping mid-stream
    /// leaves no hole, because the other one covers the frames it missed.
    #[tokio::test]
    async fn a_reconnect_on_one_connection_leaves_no_gap() {
        let (writer_tx, mut writer_rx) = channel(16);
        let mut harness = Harness::with_connections(2);
        let sequence = depth_sequence();

        // Both connections are up for the first two frames.
        for frame in &sequence[..2] {
            harness.feed(&writer_tx, frame.clone()).await.unwrap();
            harness.feed(&writer_tx, frame.clone()).await.unwrap();
        }
        // Connection 0 drops here and misses the third frame entirely; only
        // connection 1 delivers it.
        harness.feed(&writer_tx, sequence[2].clone()).await.unwrap();
        // Connection 0 is back, and both deliver the next frame.
        harness.feed(&writer_tx, sequence[3].clone()).await.unwrap();
        harness.feed(&writer_tx, sequence[3].clone()).await.unwrap();

        let mut written = 0;
        while writer_rx.try_recv().is_ok() {
            written += 1;
        }
        assert_eq!(written, sequence.len(), "the stream is still complete");
        assert!(
            harness.tasks.is_empty(),
            "the surviving connection covered the reconnect, so there is no gap \
             and no recovery snapshot to fetch"
        );
    }

    /// A frame that arrives after a later one — possible once redundant
    /// connections can be skewed past the dedup window — costs one gap, not a
    /// gap on every frame after it. Rewinding the sequence would make the next
    /// in-order diff mismatch too, and so on until the streams realign.
    #[tokio::test]
    async fn an_out_of_order_frame_does_not_rewind_the_sequence() {
        let (writer_tx, _writer_rx) = channel(16);
        let mut harness = Harness::with_connections(2);
        let sequence = depth_sequence();

        harness.feed(&writer_tx, sequence[0].clone()).await.unwrap();
        harness.feed(&writer_tx, sequence[2].clone()).await.unwrap();
        assert_eq!(harness.prev_u_map["btcusdt"], 7);
        let after_skip = harness.tasks.len();
        assert_eq!(after_skip, 1, "the skipped frame is a genuine gap");

        // The straggler arrives late. It is a gap too, but it must not drag the
        // sequence back to 5 and make the *next* frame look like one as well.
        harness.feed(&writer_tx, sequence[1].clone()).await.unwrap();
        assert_eq!(
            harness.prev_u_map["btcusdt"], 7,
            "sequence only moves forward"
        );

        harness.feed(&writer_tx, sequence[3].clone()).await.unwrap();
        assert_eq!(
            harness.tasks.len(),
            after_skip + 1,
            "the reorder costs one gap, not one per frame afterwards"
        );
    }

    /// With redundancy off, nothing is filtered — a single connection cannot
    /// produce a duplicate, and paying for the filter would be pure overhead.
    #[tokio::test]
    async fn a_single_connection_records_every_frame_it_receives() {
        let (writer_tx, mut writer_rx) = channel(16);
        let mut harness = Harness::with_connections(1);
        let sequence = depth_sequence();

        for frame in &sequence {
            harness.feed(&writer_tx, frame.clone()).await.unwrap();
        }

        let mut written = 0;
        while writer_rx.try_recv().is_ok() {
            written += 1;
        }
        assert_eq!(written, sequence.len());
    }
}
