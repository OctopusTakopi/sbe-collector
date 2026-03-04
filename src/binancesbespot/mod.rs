// dead_code allow removed: unused items are now tracked explicitly.
mod snapshot;
mod ws;

use crate::error::ConnectorError;
use crate::file::{Tag, WriteRecord};
use crate::sbe_types::{DepthDiffBlock, MessageHeader, TEMPLATE_DEPTH_DIFF};
use crate::throttler::Throttler;
use bytes::Bytes;
use jiff::Timestamp;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;
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
pub fn find_symbol<'a>(data: &[u8], symbols: &'a [String]) -> Option<&'a String> {
    let n = data.len();
    for sym in symbols {
        let sym_len = sym.len();
        if n < sym_len + 1 {
            continue;
        }
        let len_pos = n - sym_len - 1;
        // Check the length prefix byte.
        if data[len_pos] as usize != sym_len {
            continue;
        }
        // Case-insensitive byte comparison against the known symbol.
        if data[n - sym_len..].eq_ignore_ascii_case(sym.as_bytes()) {
            return Some(sym);
        }
    }
    None
}

pub fn handle(
    data: Bytes,
    writer_tx: &UnboundedSender<WriteRecord>,
    recv_time: Timestamp,
    symbols: &[String],
    prev_u_map: &mut HashMap<String, i64>,
    client: &reqwest::Client,
    throttler: &Throttler,
) -> Result<(), ConnectorError> {
    if data.len() < 8 {
        return Err(ConnectorError::Format);
    }

    let Ok((header, _)) = MessageHeader::read_from_prefix(&data) else {
        return Err(ConnectorError::Sbe);
    };

    let template_id = header.template_id.get();

    let Some(sym) = find_symbol(&data, symbols) else {
        // Log unrecognised frames so silent data-loss is observable.
        tracing::warn!(
            len = data.len(),
            template_id,
            "could not identify symbol in SBE frame — writing to 'unknown'"
        );
        let _ = writer_tx.send(WriteRecord {
            recv_time,
            symbol: "unknown".to_string(),
            tag: Tag::Sbe,
            data,
        });
        return Ok(());
    };

    // Normalise to lowercase once; used as the writer HashMap key and file-name.
    let symbol_str = sym.to_lowercase();

    // Data-loss detection for depth diffs.
    if template_id == TEMPLATE_DEPTH_DIFF {
        if let Ok((blk, _)) = DepthDiffBlock::read_from_prefix(&data[8..]) {
            let u = blk.last_book_update_id.get();
            let first_u = blk.first_book_update_id.get();

            // Only trigger the gap alarm when we already have a previous update id.
            // On the very first message prev_u is None — normal startup, not a gap.
            if let Some(&prev) = prev_u_map.get(&symbol_str)
                && first_u != prev + 1
            {
                tracing::warn!(
                    symbol = %symbol_str,
                    "depth gap detected: expected first_u={} but got {} (prev_u={})",
                    prev + 1,
                    first_u,
                    prev
                );

                // Clone only what the spawn needs; symbol_str is moved into the
                // insert below, so no second clone is needed.
                let sym_for_spawn = symbol_str.clone();
                let writer_tx_ = writer_tx.clone();
                let client_ = client.clone();
                let throttler_ = throttler.clone();

                tokio::spawn(async move {
                    use crate::binancesbespot::snapshot::fetch_snapshot;
                    match throttler_
                        .execute(fetch_snapshot(&client_, &sym_for_spawn))
                        .await
                    {
                        Some(Ok(snap_data)) => {
                            let _ = writer_tx_.send(WriteRecord {
                                recv_time: Timestamp::now(),
                                symbol: sym_for_spawn,
                                tag: Tag::Rest,
                                data: snap_data,
                            });
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
            // Always update prev_u regardless of whether a gap was detected.
            prev_u_map.insert(symbol_str.clone(), u);
        } else {
            return Err(ConnectorError::Sbe);
        }
    }

    let _ = writer_tx.send(WriteRecord {
        recv_time,
        symbol: symbol_str,
        tag: Tag::Sbe,
        data,
    });
    Ok(())
}

pub async fn run_collection(
    streams: Vec<String>,
    symbols: Vec<String>,
    writer_tx: UnboundedSender<WriteRecord>,
    api_key: String,
) -> Result<(), anyhow::Error> {
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::task::JoinSet;

    let (ws_tx, mut ws_rx) = unbounded_channel::<(Timestamp, Bytes)>();
    let mut tasks = JoinSet::new();

    tasks.spawn(keep_connection(streams, symbols.clone(), api_key, ws_tx));

    // One shared reqwest::Client for both the periodic snapshot loop and
    // gap-triggered snapshot fetches.
    let client = reqwest::Client::new();

    let throttler = Throttler::new(100);
    tasks.spawn(snapshot::snapshot_loop(
        symbols.clone(),
        writer_tx.clone(),
        client.clone(),
        throttler.clone(),
        3600,
    ));

    let mut prev_u_map = HashMap::new();

    while let Some((recv_time, data)) = ws_rx.recv().await {
        if let Err(error) = handle(
            data,
            &writer_tx,
            recv_time,
            &symbols,
            &mut prev_u_map,
            &client,
            &throttler,
        ) {
            tracing::error!(?error, "couldn't handle the received data.");
        }
    }

    Ok(())
}
