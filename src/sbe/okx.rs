//! Fixed-layout parts of OKX's SBE market-data schema (`okx_sbe_1_0.xml`).
//!
//! Every stream template (1000-1005) starts `instIdCode`, `tsUs`, `outTime`,
//! `seqId`; the stream carries no symbol, only the code `GET
//! /api/v5/public/instruments` maps an `instId` to.

use std::ops::{Range, RangeInclusive};

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use super::{I32LE, I64LE};

/// `BboTbtChannelEvent`, the `bbo-tbt` channel.
pub const TEMPLATE_BBO_TBT: u16 = 1000;

/// The templates pushed on the websocket, as opposed to the REST snapshot's.
pub const STREAM_TEMPLATES: RangeInclusive<u16> = 1000..=1005;

/// `instIdCode`: which instrument a stream frame is about.
pub const INST_ID_CODE: Range<usize> = 8..16;

/// `outTime`, the gateway's send time, stamped per connection: copies of one
/// event from three live connections differed in these bytes alone.
pub const OUT_TIME: Range<usize> = 24..32;

/// `BboTbtChannelEvent` fixed block (74 bytes). Prices and sizes are
/// mantissas of `10^pxExponent` and `10^szExponent`.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct BboTbtBlock {
    pub inst_id_code: I64LE,
    /// Microseconds, accurate to the millisecond.
    pub ts_us: I64LE,
    pub out_time: I64LE,
    pub seq_id: I64LE,
    pub ask_px: I64LE,
    pub ask_sz: I64LE,
    pub bid_px: I64LE,
    pub bid_sz: I64LE,
    pub ask_ord_count: I32LE,
    pub bid_ord_count: I32LE,
    pub px_exponent: i8,
    pub sz_exponent: i8,
}

/// The `instIdCode` of a stream frame, if it is long enough to carry one.
pub fn inst_id_code(frame: &[u8]) -> Option<i64> {
    Some(i64::from_le_bytes(
        frame.get(INST_ID_CODE)?.try_into().ok()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_sizes_match_the_schema() {
        assert_eq!(size_of::<BboTbtBlock>(), 74);
    }

    /// The first 82-byte frame of a live `bbo-tbt` capture for BTC-USDT.
    #[test]
    fn a_live_frame_decodes() {
        let mut frame = Vec::new();
        for field in [74u16, TEMPLATE_BBO_TBT, 1, 0] {
            frame.extend_from_slice(&field.to_le_bytes());
        }
        for field in [
            3i64,
            1_791_179_527_312_000,
            1_791_179_527_313_636,
            81_991_106_440,
            857_758,
            1,
            857_757,
            2,
        ] {
            frame.extend_from_slice(&field.to_le_bytes());
        }
        frame.extend_from_slice(&4i32.to_le_bytes());
        frame.extend_from_slice(&5i32.to_le_bytes());
        frame.extend_from_slice(&[(-1i8) as u8, (-8i8) as u8]);
        assert_eq!(frame.len(), 82);

        let (block, rest) = BboTbtBlock::read_from_prefix(&frame[8..]).unwrap();
        assert!(rest.is_empty());
        assert_eq!(inst_id_code(&frame), Some(3));
        assert_eq!(block.seq_id.get(), 81_991_106_440);
        assert_eq!(block.ask_px.get(), 857_758);
        assert_eq!(block.bid_ord_count.get(), 5);
        assert_eq!((block.px_exponent, block.sz_exponent), (-1, -8));
        assert_eq!(&frame[OUT_TIME], &1_791_179_527_313_636i64.to_le_bytes());
        assert_eq!(inst_id_code(&frame[..15]), None);
    }
}
