#![allow(dead_code, clippy::identity_op)]
use zerocopy::byteorder::LittleEndian;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

type U16LE = zerocopy::byteorder::U16<LittleEndian>;
type U32LE = zerocopy::byteorder::U32<LittleEndian>;
type I64LE = zerocopy::byteorder::I64<LittleEndian>;

// ── SBE Wire Header (8 bytes) ────────────────────────────────────────────────
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct MessageHeader {
    pub block_length: U16LE,
    pub template_id: U16LE,
    pub schema_id: U16LE,
    pub version: U16LE,
}

// ── Group size encodings ──────────────────────────────────────────────────────
//
// The schema defines TWO distinct dimension composites:
//
//   groupSizeEncoding  (used by TradesStreamEvent):
//     blockLength : uint16  (2 bytes)
//     numInGroup  : uint32  (4 bytes)   ← 32-bit!
//     Total: 6 bytes
//
//   groupSize16Encoding  (used by Depth Snapshot + Diff):
//     blockLength : uint16  (2 bytes)
//     numInGroup  : uint16  (2 bytes)   ← 16-bit
//     Total: 4 bytes
//
// Using the wrong one shifts every subsequent parse by ±2 bytes — silent corruption.

/// `groupSizeEncoding` — used by `TradesStreamEvent` (id=10000).
/// Wire size: 6 bytes.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct GroupSizeEncoding {
    pub block_length: U16LE,
    pub num_in_group: U32LE, // uint32 per schema
}

/// `groupSize16Encoding` — used by `DepthSnapshotStreamEvent` (id=10002) and
/// `DepthDiffStreamEvent` (id=10003).
/// Wire size: 4 bytes.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct GroupSize16Encoding {
    pub block_length: U16LE,
    pub num_in_group: U16LE, // uint16 per schema
}

// ── Message IDs ───────────────────────────────────────────────────────────────
pub const TEMPLATE_TRADES: u16 = 10000;
pub const TEMPLATE_BEST_BID_ASK: u16 = 10001;
pub const TEMPLATE_DEPTH_SNAPSHOT: u16 = 10002;
pub const TEMPLATE_DEPTH_DIFF: u16 = 10003;

// ── TradesStreamEvent (id=10000) fixed block ──────────────────────────────────
//
// Schema fixed block (blockLength = 18):
//   eventTime     : utcTimestampUs (int64)
//   transactTime  : utcTimestampUs (int64)
//   priceExponent : exponent8 (int8)
//   qtyExponent   : exponent8 (int8)
//
// Followed by group "trades" with groupSizeEncoding (6 bytes), then N entries.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct TradesBlock {
    pub event_time: I64LE,
    pub transact_time: I64LE,
    pub price_exponent: i8,
    pub qty_exponent: i8,
}

// TradesStreamEvent group "trades" entry (25 bytes per entry):
//   id            : tradeId  (int64)        ← was MISSING in old code
//   price         : mantissa64 (int64)
//   qty           : mantissa64 (int64)
//   isBuyerMaker  : boolEnum (uint8)
//   isBestMatch   : constant True — NOT present on the wire
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct TradeEntry {
    pub id: I64LE, // tradeId — was phantom `time` field before
    pub price: I64LE,
    pub qty: I64LE,
    pub is_buyer_maker: u8, // boolEnum: 0=False, 1=True
}

// ── BestBidAskStreamEvent (id=10001) fixed block (blockLength = 50) ───────────
//
// Schema:
//   eventTime     : utcTimestampUs (int64)
//   bookUpdateId  : updateId (int64)
//   priceExponent : exponent8 (int8)
//   qtyExponent   : exponent8 (int8)
//   bidPrice      : mantissa64 (int64)
//   bidQty        : mantissa64 (int64)
//   askPrice      : mantissa64 (int64)
//   askQty        : mantissa64 (int64)
//
// No groups. Followed immediately by symbol VarString8.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct BestBidAskBlock {
    pub event_time: I64LE,
    pub book_update_id: I64LE,
    pub price_exponent: i8,
    pub qty_exponent: i8,
    pub bid_price: I64LE,
    pub bid_qty: I64LE,
    pub ask_price: I64LE,
    pub ask_qty: I64LE,
}

// ── DepthSnapshotStreamEvent (id=10002) fixed block (blockLength = 18) ────────
//
// Schema:
//   eventTime     : utcTimestampUs (int64)
//   bookUpdateId  : updateId (int64)
//   priceExponent : exponent8 (int8)
//   qtyExponent   : exponent8 (int8)
//
// Followed by group "bids" (groupSize16Encoding), group "asks" (groupSize16Encoding),
// then symbol VarString8.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct DepthSnapshotBlock {
    pub event_time: I64LE,
    pub book_update_id: I64LE,
    pub price_exponent: i8,
    pub qty_exponent: i8,
}

// ── DepthDiffStreamEvent (id=10003) fixed block (blockLength = 26) ─────────────
//
// Schema:
//   eventTime          : utcTimestampUs (int64)
//   firstBookUpdateId  : updateId (int64)
//   lastBookUpdateId   : updateId (int64)
//   priceExponent      : exponent8 (int8)
//   qtyExponent        : exponent8 (int8)
//
// Followed by group "bids" (groupSize16Encoding), group "asks" (groupSize16Encoding),
// then symbol VarString8.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct DepthDiffBlock {
    pub event_time: I64LE,
    pub first_book_update_id: I64LE,
    pub last_book_update_id: I64LE,
    pub price_exponent: i8,
    pub qty_exponent: i8,
}

// ── Shared depth level entry (16 bytes, used in both Snapshot and Diff) ────────
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct DepthEntry {
    pub price: I64LE,
    pub qty: I64LE,
}

// ── VarString8 decoder — 1-byte length prefix ─────────────────────────────────
#[inline(always)]
pub fn get_var_str_u8<'a>(data: &'a [u8], offset: &mut usize) -> &'a [u8] {
    if *offset + 1 > data.len() {
        return &[];
    }
    let len = data[*offset] as usize;
    *offset += 1;
    if *offset + len > data.len() {
        return &[];
    }
    let s = &data[*offset..*offset + len];
    *offset += len;
    s
}

#[inline(always)]
pub fn format_decimal(mantissa: i64, exponent: i8) -> f64 {
    (mantissa as f64) * 10f64.powi(exponent as i32)
}
