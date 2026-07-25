// dead_code allow removed: unused items are now tracked explicitly.
mod snapshot;
mod ws;

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
    client: &reqwest::Client,
    throttler: &Throttler,
    tasks: &mut JoinSet<()>,
) -> Result<(), ConnectorError> {
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
                *prev = u;
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
) -> Result<(), anyhow::Error> {
    let (ws_tx, ws_rx) = channel::<(Timestamp, Bytes)>(crate::WS_QUEUE_CAPACITY);
    let mut feed = Feed::new(ws_rx, shutdown);
    let mut tasks = JoinSet::new();
    let canonical_symbols: Vec<Symbol> = symbols
        .iter()
        .map(|symbol| Symbol::from(symbol.to_ascii_lowercase()))
        .collect();
    let mut symbol_matcher = SymbolMatcher::new(canonical_symbols);

    tasks.spawn(async move {
        keep_connection(streams, symbols, api_key, ws_tx).await;
        tracing::error!("the websocket connection task exited");
    });

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

    #[test]
    fn finds_canonical_symbol_without_allocating() {
        let symbols = vec![Symbol::from("btcusdt"), Symbol::from("ethusdt")];
        let data = b"\x00\x00\x00\x00\x00\x00\x00\x00\x07BTCUSDT";

        let found = find_symbol(data, &symbols).unwrap();

        assert!(std::sync::Arc::ptr_eq(found, &symbols[0]));
    }
}
