//! Print the records of `.zst` files written by sbe-collector.
//!
//! Usage: `reader [--exchange <name>] <file.zst> [<file2.zst> ...]`
//!
//! A file's exchange is its directory's `_exchange` marker, or Binance spot
//! for a directory recorded before markers existed; `--exchange` overrides.

use std::{
    fs::File,
    io::{self, BufReader, Read},
    path::{Path, PathBuf},
};

use clap::Parser;
use sbe_collector::{
    exchange::Exchange,
    record::{HEADER_LEN, Header, Tag},
    sbe::{
        GroupSize16Encoding, MessageHeader,
        binance::{
            BestBidAskBlock, DepthDiffBlock, DepthEntry, DepthSnapshotBlock, GroupSizeEncoding,
            TEMPLATE_BEST_BID_ASK, TEMPLATE_DEPTH_DIFF, TEMPLATE_DEPTH_SNAPSHOT, TEMPLATE_TRADES,
            TradeEntry, TradesBlock,
        },
        okx::{self, BboTbtBlock},
        read_group,
    },
};
use serde_json::Value;
use zerocopy::FromBytes;
use zstd::stream::read::Decoder as ZstdDecoder;

/// Entries printed per group before `...`.
const SHOWN: usize = 5;

#[derive(Parser)]
#[command(
    version,
    about = "Print the records of sbe-collector's recording files"
)]
struct Args {
    /// Decode as this exchange instead of the one the file's directory names.
    #[arg(long)]
    exchange: Option<Exchange>,

    /// Recording files to print.
    #[arg(required = true)]
    files: Vec<PathBuf>,
}

fn main() {
    let args = Args::parse();
    for path in &args.files {
        println!("=== {} ===", path.display());
        let exchange = match args.exchange {
            Some(exchange) => Ok(exchange),
            None => Exchange::of_recordings_in(path.parent().unwrap_or(Path::new("."))),
        };
        if let Err(error) = exchange.and_then(|exchange| read_file(path, exchange)) {
            eprintln!("Error reading {}: {error}", path.display());
        }
    }
}

#[derive(Default)]
struct Counts {
    records: usize,
    sbe: usize,
    rest: usize,
}

fn read_file(path: &Path, exchange: Exchange) -> io::Result<()> {
    let decoder = ZstdDecoder::new(BufReader::new(File::open(path)?))?;
    let mut reader = BufReader::new(decoder);
    let mut header = [0u8; HEADER_LEN];
    let mut payload = Vec::new();
    let mut counts = Counts::default();
    loop {
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        }
        let Header { recv_ns, tag, len } = Header::parse(&header);
        payload.resize(len as usize, 0);
        reader.read_exact(&mut payload)?;
        counts.records += 1;
        match Tag::from_byte(tag) {
            Some(Tag::Rest) => {
                counts.rest += 1;
                match exchange {
                    Exchange::BinanceSbeSpot => print_rest_record(recv_ns, &payload),
                    Exchange::OkxSbe => println!("[{recv_ns}] R {} bytes", payload.len()),
                }
            }
            Some(Tag::Sbe) => {
                counts.sbe += 1;
                match exchange {
                    Exchange::BinanceSbeSpot => print_sbe_record(recv_ns, &payload),
                    Exchange::OkxSbe => print_okx_record(recv_ns, &payload),
                }
            }
            None => println!("[{recv_ns}] unknown tag: 0x{tag:02x}, payload_len={len}"),
        }
    }
    println!(
        "--- total: {} records ({} SBE stream, {} REST snapshot) ---",
        counts.records, counts.sbe, counts.rest
    );
    Ok(())
}

fn print_rest_record(nanos: i64, payload: &[u8]) {
    let Ok(snapshot) = serde_json::from_slice::<Value>(payload) else {
        println!("[{nanos}] R <invalid JSON>");
        return;
    };
    let bids = snapshot["bids"].as_array();
    let asks = snapshot["asks"].as_array();
    println!(
        "[{nanos}] R SNAPSHOT lastUpdateId={} bids={} asks={}",
        snapshot["lastUpdateId"].as_u64().unwrap_or(0),
        bids.map_or(0, Vec::len),
        asks.map_or(0, Vec::len),
    );
    for (label, levels) in [("Bids", bids), ("Asks", asks)] {
        let Some(levels) = levels else {
            continue;
        };
        println!("  {label}:");
        for level in levels.iter().take(SHOWN).filter_map(Value::as_array) {
            let field = |index: usize| level.get(index).and_then(Value::as_str).unwrap_or("?");
            println!("    {} @ {}", field(0), field(1));
        }
    }
}

fn template_name(template_id: u16) -> Option<&'static str> {
    Some(match template_id {
        TEMPLATE_TRADES => "TRADES",
        TEMPLATE_BEST_BID_ASK => "BEST_BID_ASK",
        TEMPLATE_DEPTH_SNAPSHOT => "DEPTH_SNAPSHOT",
        TEMPLATE_DEPTH_DIFF => "DEPTH_DIFF",
        _ => return None,
    })
}

/// `mantissa × 10^exponent`; floating point is for display only.
fn decimal(mantissa: i64, exponent: i8) -> f64 {
    (mantissa as f64) * 10f64.powi(i32::from(exponent))
}

fn print_sbe_record(nanos: i64, data: &[u8]) {
    let Ok((header, body)) = MessageHeader::read_from_prefix(data) else {
        println!("[{nanos}] S <too short: {} bytes>", data.len());
        return;
    };
    let template_id = header.template_id.get();
    let block_len = header.block_length.get() as usize;
    let symbol = extract_symbol(data);
    let Some(name) = template_name(template_id) else {
        println!(
            "[{nanos}] S template={template_id} {symbol} block_len={block_len} total={}b",
            data.len()
        );
        return;
    };
    let groups = size_of::<MessageHeader>() + block_len;

    match template_id {
        // Unlike the others, an unreadable trades block prints nothing.
        TEMPLATE_TRADES if data.len() >= groups => {
            if let Ok((block, _)) = TradesBlock::read_from_prefix(body) {
                print_trades(nanos, symbol, data, groups, &block);
            }
            return;
        }
        TEMPLATE_BEST_BID_ASK if data.len() >= groups => {
            if let Ok((block, _)) = BestBidAskBlock::read_from_prefix(body) {
                println!(
                    "[{nanos}] S {name} {symbol} bid={:.6} ask={:.6} t={}",
                    decimal(block.bid_price.get(), block.price_exponent),
                    decimal(block.ask_price.get(), block.price_exponent),
                    block.event_time.get(),
                );
                return;
            }
        }
        TEMPLATE_DEPTH_DIFF if data.len() >= groups => {
            if let Ok((block, _)) = DepthDiffBlock::read_from_prefix(body) {
                println!(
                    "[{nanos}] S {name} {symbol} t={} uid={}-{}",
                    block.event_time.get(),
                    block.first_book_update_id.get(),
                    block.last_book_update_id.get(),
                );
                print_depth_levels(
                    data,
                    groups,
                    [block.price_exponent, block.qty_exponent],
                    ["diffBids", "diffAsks"],
                );
                return;
            }
        }
        TEMPLATE_DEPTH_SNAPSHOT if data.len() >= groups => {
            if let Ok((block, _)) = DepthSnapshotBlock::read_from_prefix(body) {
                println!(
                    "[{nanos}] S {name} {symbol} t={} uid={}",
                    block.event_time.get(),
                    block.book_update_id.get(),
                );
                print_depth_levels(
                    data,
                    groups,
                    [block.price_exponent, block.qty_exponent],
                    ["snapshotBids", "snapshotAsks"],
                );
                return;
            }
        }
        _ => {}
    }
    println!(
        "[{nanos}] S {name} {symbol} block_len={block_len} total={}b",
        data.len()
    );
}

/// An OKX stream frame. The stream carries no symbol, only the instrument's
/// code; the file name says which instrument it is.
fn print_okx_record(nanos: i64, data: &[u8]) {
    let Ok((header, body)) = MessageHeader::read_from_prefix(data) else {
        println!("[{nanos}] S <too short: {} bytes>", data.len());
        return;
    };
    let template_id = header.template_id.get();
    let code = okx::inst_id_code(data).map_or_else(|| "?".to_owned(), |code| code.to_string());
    if template_id == okx::TEMPLATE_BBO_TBT
        && let Ok((block, _)) = BboTbtBlock::read_from_prefix(body)
    {
        let [px, sz] = [block.px_exponent, block.sz_exponent];
        println!(
            "[{nanos}] S BBO_TBT code={code} bid={:.6}x{:.6} ask={:.6}x{:.6} orders={}/{} seq={} t={}",
            decimal(block.bid_px.get(), px),
            decimal(block.bid_sz.get(), sz),
            decimal(block.ask_px.get(), px),
            decimal(block.ask_sz.get(), sz),
            block.bid_ord_count.get(),
            block.ask_ord_count.get(),
            block.seq_id.get(),
            block.ts_us.get(),
        );
        return;
    }
    println!(
        "[{nanos}] S template={template_id} code={code} block_len={} total={}b",
        header.block_length.get(),
        data.len()
    );
}

fn print_trades(nanos: i64, symbol: &str, data: &[u8], mut offset: usize, block: &TradesBlock) {
    let group = read_group::<GroupSizeEncoding>(data, &mut offset).unwrap_or_default();
    println!(
        "[{nanos}] S TRADES {symbol} trades={} t={}",
        group.count,
        block.event_time.get()
    );
    for _ in 0..group.count.min(SHOWN) {
        if offset + group.stride.max(size_of::<TradeEntry>()) > data.len() {
            break;
        }
        let Ok((entry, _)) = TradeEntry::read_from_prefix(&data[offset..]) else {
            break;
        };
        println!(
            "  trade id={} price={:.6} qty={:.6} buyer_maker={}",
            entry.id.get(),
            decimal(entry.price.get(), block.price_exponent),
            decimal(entry.qty.get(), block.qty_exponent),
            entry.is_buyer_maker,
        );
        offset += group.stride;
    }
    if group.count > SHOWN {
        println!("  ...");
    }
}

/// Both depth groups, bids then asks, from `offset`.
fn print_depth_levels(
    data: &[u8],
    mut offset: usize,
    [price_exponent, qty_exponent]: [i8; 2],
    labels: [&str; 2],
) {
    for label in labels {
        let Some(group) = read_group::<GroupSize16Encoding>(data, &mut offset) else {
            continue;
        };
        println!("  {label} ({}):", group.count);
        for index in 0..group.count {
            let Some(Ok((entry, _))) = data.get(offset..).map(DepthEntry::read_from_prefix) else {
                break;
            };
            if index < SHOWN {
                println!(
                    "    {:.6} @ {:.6}",
                    decimal(entry.price.get(), price_exponent),
                    decimal(entry.qty.get(), qty_exponent)
                );
            }
            offset += group.stride;
        }
        if group.count > SHOWN {
            println!("    ...");
        }
    }
}

/// The symbol the stream bridge appends to every frame as a `VarString8`.
///
/// With no subscription list to match against, take the longest suffix of
/// 2 to 19 alphanumeric or `_` bytes whose preceding byte equals its length.
fn extract_symbol(data: &[u8]) -> &str {
    let n = data.len();
    for symbol_len in (2..20).rev() {
        if n < symbol_len + 1 || data[n - symbol_len - 1] as usize != symbol_len {
            continue;
        }
        let symbol = &data[n - symbol_len..];
        if symbol
            .iter()
            .all(|&byte| byte.is_ascii_alphanumeric() || byte == b'_')
            && let Ok(symbol) = std::str::from_utf8(symbol)
        {
            return symbol;
        }
    }
    "unknown"
}
