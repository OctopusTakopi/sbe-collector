//! Print the records of `.zst` files written by sbe-collector.
//!
//! Usage: `reader <file.zst> [<file2.zst> ...]`

use std::{
    env,
    fs::File,
    io::{self, BufReader, Read},
    path::Path,
};

use sbe_collector::{
    record::{HEADER_LEN, Header, Tag},
    sbe_types::{
        BestBidAskBlock, DepthDiffBlock, DepthEntry, DepthSnapshotBlock, GroupSize16Encoding,
        GroupSizeEncoding, MessageHeader, TEMPLATE_BEST_BID_ASK, TEMPLATE_DEPTH_DIFF,
        TEMPLATE_DEPTH_SNAPSHOT, TEMPLATE_TRADES, TradeEntry, TradesBlock,
    },
};
use serde_json::Value;
use zerocopy::FromBytes;
use zstd::stream::read::Decoder as ZstdDecoder;

/// Entries printed per group before `...`.
const SHOWN: usize = 5;

fn main() {
    let files: Vec<String> = env::args().skip(1).collect();
    if files.is_empty() {
        eprintln!("Usage: reader <file.zst> [<file2.zst> ...]");
        std::process::exit(1);
    }
    for path in &files {
        println!("=== {path} ===");
        if let Err(error) = read_file(Path::new(path)) {
            eprintln!("Error reading {path}: {error}");
        }
    }
}

#[derive(Default)]
struct Counts {
    records: usize,
    sbe: usize,
    rest: usize,
}

fn read_file(path: &Path) -> io::Result<()> {
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
                print_rest_record(recv_ns, &payload);
            }
            Some(Tag::Sbe) => {
                counts.sbe += 1;
                print_sbe_record(recv_ns, &payload);
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

/// A repeating group: `count` entries of `stride` bytes each. The stride is
/// the wire `blockLength`, not a struct size: a newer schema version appends
/// fields to entries.
#[derive(Default)]
struct Group {
    count: usize,
    stride: usize,
}

/// A group dimension composite.
trait Dimension: FromBytes {
    fn group(&self) -> Group;
}

impl Dimension for GroupSizeEncoding {
    fn group(&self) -> Group {
        Group {
            count: self.num_in_group.get() as usize,
            stride: usize::from(self.block_length.get()),
        }
    }
}

impl Dimension for GroupSize16Encoding {
    fn group(&self) -> Group {
        Group {
            count: usize::from(self.num_in_group.get()),
            stride: usize::from(self.block_length.get()),
        }
    }
}

/// The group dimension at `*offset`, stepping past it.
fn read_group<D: Dimension>(data: &[u8], offset: &mut usize) -> Option<Group> {
    let (dimension, _) = D::read_from_prefix(data.get(*offset..)?).ok()?;
    *offset += size_of::<D>();
    Some(dimension.group())
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
