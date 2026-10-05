//! Fixed-layout parts of the Binance spot stream schema
//! (`schemas/binance_stream_1_0.xml`).

use std::ops::Range;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use super::{Dimension, Group, I64LE, U16LE, U32LE};

pub const TEMPLATE_TRADES: u16 = 10000;
pub const TEMPLATE_BEST_BID_ASK: u16 = 10001;
pub const TEMPLATE_DEPTH_SNAPSHOT: u16 = 10002;
pub const TEMPLATE_DEPTH_DIFF: u16 = 10003;

/// The bytes of `eventTime`: the first fixed-block field of every template
/// above, right behind the 8-byte message header.
pub const EVENT_TIME: Range<usize> = 8..16;

/// `groupSizeEncoding`, the trades group's dimension: a `uint32` count, so
/// 6 bytes. Mistaking it for [`super::GroupSize16Encoding`], the depth groups'
/// dimension, shifts every later field.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct GroupSizeEncoding {
    pub block_length: U16LE,
    pub num_in_group: U32LE,
}

impl Dimension for GroupSizeEncoding {
    fn group(&self) -> Group {
        Group {
            count: self.num_in_group.get() as usize,
            stride: usize::from(self.block_length.get()),
        }
    }
}

/// `TradesStreamEvent` fixed block (18 bytes), followed by the trades group
/// and the symbol.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct TradesBlock {
    pub event_time: I64LE,
    pub transact_time: I64LE,
    pub price_exponent: i8,
    pub qty_exponent: i8,
}

/// One trades group entry (25 bytes). `isBestMatch` is a schema constant and
/// not on the wire.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct TradeEntry {
    pub id: I64LE,
    pub price: I64LE,
    pub qty: I64LE,
    /// `boolEnum`: 0 false, 1 true.
    pub is_buyer_maker: u8,
}

/// `BestBidAskStreamEvent` fixed block (50 bytes), followed by the symbol.
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

/// `DepthSnapshotStreamEvent` fixed block (18 bytes), followed by the bids
/// and asks groups and the symbol.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct DepthSnapshotBlock {
    pub event_time: I64LE,
    pub book_update_id: I64LE,
    pub price_exponent: i8,
    pub qty_exponent: i8,
}

/// `DepthDiffStreamEvent` fixed block (26 bytes), followed by the bids and
/// asks groups and the symbol.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct DepthDiffBlock {
    pub event_time: I64LE,
    pub first_book_update_id: I64LE,
    pub last_book_update_id: I64LE,
    pub price_exponent: i8,
    pub qty_exponent: i8,
}

/// One bids or asks entry of a depth snapshot or diff (16 bytes).
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct DepthEntry {
    pub price: I64LE,
    pub qty: I64LE,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_sizes_match_the_schema() {
        assert_eq!(size_of::<GroupSizeEncoding>(), 6);
        assert_eq!(size_of::<TradesBlock>(), 18);
        assert_eq!(size_of::<TradeEntry>(), 25);
        assert_eq!(size_of::<BestBidAskBlock>(), 50);
        assert_eq!(size_of::<DepthSnapshotBlock>(), 18);
        assert_eq!(size_of::<DepthDiffBlock>(), 26);
        assert_eq!(size_of::<DepthEntry>(), 16);
        assert_eq!(EVENT_TIME.start, size_of::<crate::sbe::MessageHeader>());
    }
}
