//! Example: read and print records from a .zst data file produced by sbe-collector.
//!
//! Usage:
//!   cargo run --bin reader -- <file.zst> [<file2.zst> ...]

#[path = "../sbe_types.rs"]
mod sbe_types;

use std::{
    env,
    fs::File,
    io::{self, BufReader, Read},
    path::Path,
};

use sbe_types::{
    DepthEntry, GroupSize16Encoding, GroupSizeEncoding, MessageHeader, TradeEntry, TradesBlock,
};
use zerocopy::FromBytes;
use zstd::stream::read::Decoder as ZstdDecoder;

const TAG_SBE: u8 = b'S';
const TAG_REST: u8 = b'R';
const FRAME_HEADER_LEN: usize = 13; // 8 (nanos) + 1 (tag) + 4 (payload_len)

// Message IDs
const TEMPLATE_TRADES: u16 = 10000;
const TEMPLATE_BEST_BID_ASK: u16 = 10001;
const TEMPLATE_DEPTH_SNAPSHOT: u16 = 10002;
const TEMPLATE_DEPTH_DIFF: u16 = 10003;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let files: Vec<String> = env::args().skip(1).collect();
    if files.is_empty() {
        eprintln!("Usage: reader <file.zst> [<file2.zst> ...]");
        std::process::exit(1);
    }

    for path in &files {
        println!("=== {} ===", path);
        if let Err(e) = read_file(Path::new(path)) {
            eprintln!("Error reading {}: {}", path, e);
        }
    }
    Ok(())
}

fn read_file(path: &Path) -> io::Result<()> {
    let file = File::open(path)?;
    let zstd_reader = ZstdDecoder::new(BufReader::new(file))?;
    let mut reader = BufReader::new(zstd_reader);

    let mut header_buf = [0u8; FRAME_HEADER_LEN];
    let mut record_count = 0usize;
    let mut sbe_count = 0usize;
    let mut rest_count = 0usize;

    loop {
        match reader.read_exact(&mut header_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }

        let nanos = i64::from_le_bytes(header_buf[0..8].try_into().unwrap());
        let tag = header_buf[8];
        let payload_len = u32::from_le_bytes(header_buf[9..13].try_into().unwrap()) as usize;

        let mut payload = vec![0u8; payload_len];
        reader.read_exact(&mut payload)?;

        record_count += 1;

        match tag {
            TAG_REST => {
                rest_count += 1;
                print_rest_record(nanos, &payload);
            }
            TAG_SBE => {
                sbe_count += 1;
                print_sbe_record(nanos, &payload);
            }
            other => {
                println!("[{nanos}] unknown tag: 0x{other:02x}, payload_len={payload_len}");
            }
        }
    }

    println!(
        "--- total: {} records ({} SBE stream, {} REST snapshot) ---",
        record_count, sbe_count, rest_count
    );
    Ok(())
}

fn print_rest_record(nanos: i64, payload: &[u8]) {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) else {
        println!("[{nanos}] R <invalid JSON>");
        return;
    };

    let last_update_id = v["lastUpdateId"].as_u64().unwrap_or(0);
    let bids = v["bids"].as_array();
    let asks = v["asks"].as_array();

    println!(
        "[{nanos}] R SNAPSHOT lastUpdateId={} bids={} asks={}",
        last_update_id,
        bids.map_or(0, |a| a.len()),
        asks.map_or(0, |a| a.len()),
    );

    if let Some(bids) = bids {
        println!("  Bids:");
        for b in bids.iter().take(5) {
            if let Some(pair) = b.as_array() {
                println!(
                    "    {} @ {}",
                    pair[0].as_str().unwrap_or("?"),
                    pair[1].as_str().unwrap_or("?")
                );
            }
        }
    }
    if let Some(asks) = asks {
        println!("  Asks:");
        for a in asks.iter().take(5) {
            if let Some(pair) = a.as_array() {
                println!(
                    "    {} @ {}",
                    pair[0].as_str().unwrap_or("?"),
                    pair[1].as_str().unwrap_or("?")
                );
            }
        }
    }
}

fn print_sbe_record(nanos: i64, data: &[u8]) {
    if data.len() < 8 {
        println!("[{nanos}] S <too short: {} bytes>", data.len());
        return;
    }

    let Ok((header, _)) = MessageHeader::read_from_prefix(data) else {
        println!("[{nanos}] S <invalid SBE header>");
        return;
    };

    let template_id = header.template_id.get();
    let block_len = header.block_length.get() as usize;

    // Find symbol using the subscribed-set-free tail matcher (no known symbols
    // in the reader, so we fall back to the validated heuristic, but now using
    // the correct forward-reading approach).
    let symbol_str = extract_symbol(data);

    let msg_type = match template_id {
        TEMPLATE_TRADES => "TRADES",
        TEMPLATE_BEST_BID_ASK => "BEST_BID_ASK",
        TEMPLATE_DEPTH_SNAPSHOT => "DEPTH_SNAPSHOT",
        TEMPLATE_DEPTH_DIFF => "DEPTH_DIFF",
        other => {
            return println!(
                "[{nanos}] S template={other} {symbol_str} block_len={block_len} total={}b",
                data.len()
            );
        }
    };

    match template_id {
        // ── TradesStreamEvent ────────────────────────────────────────────────
        // Fixed block (blockLength=18): TradesBlock
        // Group header: GroupSizeEncoding (6 bytes, uint32 numInGroup)  ← 6 bytes!
        // Entries: TradeEntry (25 bytes each: id+price+qty+isBuyerMaker)
        TEMPLATE_TRADES if data.len() >= 8 + block_len => {
            if let Ok((blk, _)) = TradesBlock::read_from_prefix(&data[8..]) {
                let mut off = 8 + block_len;
                // GroupSizeEncoding for trades: 6 bytes, numInGroup is uint32.
                let count = read_trades_group_count(data, &mut off);
                println!(
                    "[{nanos}] S {msg_type} {symbol_str} trades={count} t={}",
                    blk.event_time.get()
                );
                // Print first few trade entries.
                let scale = |m: i64, e: i8| (m as f64) * 10f64.powi(e as i32);
                for i in 0..count {
                    if off + std::mem::size_of::<TradeEntry>() > data.len() {
                        break;
                    }
                    if let Ok((entry, _)) = TradeEntry::read_from_prefix(&data[off..]) {
                        if i < 5 {
                            println!(
                                "  trade id={} price={:.6} qty={:.6} buyer_maker={}",
                                entry.id.get(),
                                scale(entry.price.get(), blk.price_exponent),
                                scale(entry.qty.get(), blk.qty_exponent),
                                entry.is_buyer_maker,
                            );
                        }
                        off += std::mem::size_of::<TradeEntry>();
                    } else {
                        break;
                    }
                }
                if count > 5 {
                    println!("  ...");
                }
            }
            return;
        }

        // ── BestBidAskStreamEvent ────────────────────────────────────────────
        // Fixed block (blockLength=50): BestBidAskBlock. No groups.
        TEMPLATE_BEST_BID_ASK if data.len() >= 8 + block_len => {
            use sbe_types::BestBidAskBlock;
            if let Ok((bba, _)) = BestBidAskBlock::read_from_prefix(&data[8..]) {
                let scale = |m: i64, e: i8| (m as f64) * 10f64.powi(e as i32);
                println!(
                    "[{nanos}] S {msg_type} {symbol_str} bid={:.6} ask={:.6} t={}",
                    scale(bba.bid_price.get(), bba.price_exponent),
                    scale(bba.ask_price.get(), bba.price_exponent),
                    bba.event_time.get(),
                );
                return;
            }
        }

        // ── DepthDiffStreamEvent ──────────────────────────────────────────────
        // Fixed block (blockLength=26): DepthDiffBlock.
        // Groups: bids + asks, each with GroupSize16Encoding (4 bytes, uint16 numInGroup).
        TEMPLATE_DEPTH_DIFF if data.len() >= 8 + block_len => {
            use sbe_types::DepthDiffBlock;
            if let Ok((blk, _)) = DepthDiffBlock::read_from_prefix(&data[8..]) {
                let mut off = 8 + block_len;
                println!(
                    "[{nanos}] S {msg_type} {symbol_str} t={} uid={}-{}",
                    blk.event_time.get(),
                    blk.first_book_update_id.get(),
                    blk.last_book_update_id.get(),
                );
                print_depth_levels(
                    data,
                    &mut off,
                    blk.price_exponent,
                    blk.qty_exponent,
                    "diffBids",
                    "diffAsks",
                );
                return;
            }
        }

        // ── DepthSnapshotStreamEvent ──────────────────────────────────────────
        // Fixed block (blockLength=18): DepthSnapshotBlock.
        // Groups: bids + asks, each with GroupSize16Encoding (4 bytes, uint16 numInGroup).
        TEMPLATE_DEPTH_SNAPSHOT if data.len() >= 8 + block_len => {
            use sbe_types::DepthSnapshotBlock;
            if let Ok((blk, _)) = DepthSnapshotBlock::read_from_prefix(&data[8..]) {
                let mut off = 8 + block_len;
                println!(
                    "[{nanos}] S {msg_type} {symbol_str} t={} uid={}",
                    blk.event_time.get(),
                    blk.book_update_id.get(),
                );
                print_depth_levels(
                    data,
                    &mut off,
                    blk.price_exponent,
                    blk.qty_exponent,
                    "snapshotBids",
                    "snapshotAsks",
                );
                return;
            }
        }

        _ => {}
    }

    println!(
        "[{nanos}] S {msg_type} {symbol_str} block_len={block_len} total={}b",
        data.len()
    );
}

fn print_depth_levels(
    data: &[u8],
    offset: &mut usize,
    p_exp: i8,
    q_exp: i8,
    bid_label: &str,
    ask_label: &str,
) {
    let scale = |m: i64, e: i8| (m as f64) * 10f64.powi(e as i32);

    // Both bid and ask groups use GroupSize16Encoding (4 bytes, uint16 numInGroup).
    for label in [bid_label, ask_label] {
        if let Some((count, entry_len)) = read_depth_group_header(data, offset) {
            println!("  {} ({}):", label, count);
            for i in 0..count {
                if let Ok((entry, _)) = DepthEntry::read_from_prefix(&data[*offset..]) {
                    if i < 5 {
                        println!(
                            "    {:.6} @ {:.6}",
                            scale(entry.price.get(), p_exp),
                            scale(entry.qty.get(), q_exp)
                        );
                    }
                    *offset += entry_len;
                } else {
                    break;
                }
            }
            if count > 5 {
                println!("    ...");
            }
        }
    }
}

/// Read a `GroupSize16Encoding` header (4 bytes) used by Depth Snapshot/Diff groups.
/// Returns (numInGroup as usize, entry_block_length).
fn read_depth_group_header(data: &[u8], offset: &mut usize) -> Option<(usize, usize)> {
    const GSE_LEN: usize = std::mem::size_of::<GroupSize16Encoding>(); // 4
    if *offset + GSE_LEN > data.len() {
        return None;
    }
    let Ok((gse, _)) = GroupSize16Encoding::read_from_prefix(&data[*offset..]) else {
        return None;
    };
    *offset += GSE_LEN;
    Some((
        gse.num_in_group.get() as usize,
        gse.block_length.get() as usize,
    ))
}

/// Read a `GroupSizeEncoding` header (6 bytes) used by the TradesStreamEvent group.
/// Returns the numInGroup count (u32 truncated to usize).
fn read_trades_group_count(data: &[u8], offset: &mut usize) -> usize {
    const GSE_LEN: usize = std::mem::size_of::<GroupSizeEncoding>(); // 6
    if *offset + GSE_LEN > data.len() {
        return 0;
    }
    let Ok((gse, _)) = GroupSizeEncoding::read_from_prefix(&data[*offset..]) else {
        return 0;
    };
    *offset += GSE_LEN;
    gse.num_in_group.get() as usize
}

/// Extract the terminal `VarString8` symbol appended by the Binance stream bridge.
///
/// The reader has no list of known symbols, so we can only use a heuristic. We
/// try each possible length from largest (19) down and accept the first suffix
/// whose preceding byte equals the suffix length AND all bytes are ASCII
/// alphanumeric. This is the same heuristic as before, preserved because the
/// reader has no subscribed-symbol list to match against.
///
/// In production the collector (mod.rs) uses exact symbol matching which is
/// immune to false positives.
fn extract_symbol(data: &[u8]) -> &str {
    let n = data.len();
    for sym_len in (2..20).rev() {
        if n < sym_len + 1 {
            continue;
        }
        let len_pos = n - sym_len - 1;
        if data[len_pos] as usize == sym_len {
            let s = &data[n - sym_len..];
            if s.iter().all(|&b| b.is_ascii_alphanumeric() || b == b'_')
                && let Ok(sym) = std::str::from_utf8(s)
            {
                return sym;
            }
        }
    }
    "unknown"
}
