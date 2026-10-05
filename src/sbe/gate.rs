//! Fixed-layout parts of Gate's SBE market-data schemas, schema id 1:
//! `gate_spot_ws_v1.0.0.xml` for spot and `gate_fex_ws_v1.0.0.xml` for USDT
//! futures (github.com/gate/gatews, `sbe/schemas/prod`).
//!
//! The two markets number their templates alike, with identical headers, over
//! different layouts: futures' best bid/offer puts the ask first and its
//! trades come in a group. Decoding a frame takes its [`Market`].

use std::ops::Range;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use super::{GroupSize16Encoding, I64LE, MessageHeader, U64LE, read_group, var_str8};

/// `bbo`, the `book_ticker` channel.
pub const TEMPLATE_BBO: u16 = 1;
/// `publicTrade`, the `trades` channel.
pub const TEMPLATE_PUBLIC_TRADE: u16 = 2;
/// `obu`, the order book v2 channel: a full snapshot on subscribing, then
/// deltas each starting at the previous one's last id plus one.
pub const TEMPLATE_OBU: u16 = 3;

/// `time`, the server's send time and the first field of every template.
pub const TIME: Range<usize> = 8..16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Market {
    Spot,
    Futures,
}

/// Spot `bbo` fixed block (59 bytes). Times are microseconds.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct SpotBboBlock {
    pub time: I64LE,
    pub event: i8,
    /// Engine time.
    pub t: I64LE,
    /// Order book update id.
    pub u: I64LE,
    pub px_exponent: i8,
    pub sz_exponent: i8,
    pub bid_px: I64LE,
    pub bid_sz: I64LE,
    pub ask_px: I64LE,
    pub ask_sz: I64LE,
}

/// Futures `bbo` fixed block (59 bytes): spot's fields with the ask first.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct FuturesBboBlock {
    pub time: I64LE,
    pub event: i8,
    pub t: I64LE,
    pub u: I64LE,
    pub px_exponent: i8,
    pub sz_exponent: i8,
    pub ask_px: I64LE,
    pub ask_sz: I64LE,
    pub bid_px: I64LE,
    pub bid_sz: I64LE,
}

/// Spot `publicTrade` fixed block (60 bytes): one trade per frame.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct SpotTradeBlock {
    pub time: I64LE,
    pub event: i8,
    pub px_exponent: i8,
    pub sz_exponent: i8,
    pub id: U64LE,
    /// Increments by one per trade of the pair.
    pub id_market: U64LE,
    pub create_time: I64LE,
    pub create_time_us: I64LE,
    /// The taker's: 0 sell, 1 buy.
    pub side: i8,
    pub price: I64LE,
    pub amount: I64LE,
}

/// Futures `publicTrade` fixed block (11 bytes), followed by a group of
/// [`FuturesTrade`]s.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct FuturesTradeBlock {
    pub time: I64LE,
    pub event: i8,
    pub px_exponent: i8,
    pub sz_exponent: i8,
}

/// One futures trade (32 bytes).
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct FuturesTrade {
    pub t: I64LE,
    /// Increments by one per trade of the contract.
    pub id: U64LE,
    /// Contracts, negative when the taker sold.
    pub size: I64LE,
    pub price: I64LE,
}

/// `obu` fixed block (36 bytes, both markets), followed by the bids and asks
/// groups of [`Level`]s.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct ObuBlock {
    pub time: I64LE,
    pub event: i8,
    pub t: I64LE,
    /// 1 for a full snapshot, 0 for a delta.
    pub full: u8,
    /// `U`: 0 in a full snapshot.
    pub first_id: I64LE,
    /// `u`.
    pub last_id: I64LE,
    pub px_exponent: i8,
    pub sz_exponent: i8,
}

/// One price level of an `obu` frame (16 bytes); size 0 deletes the level.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct Level {
    pub px: I64LE,
    pub sz: I64LE,
}

/// One `obu` frame's place in its stream's id chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObuLink<'a> {
    /// The stream's name, `ob.BTC_USDT.50`.
    pub stream: &'a [u8],
    pub full: bool,
    pub first_id: i64,
    pub last_id: i64,
}

/// What a frame says about itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Route<'a> {
    /// The pair or contract, as Gate spells it (`BTC_USDT`).
    pub instrument: &'a [u8],
    /// An `obu` frame's place in its stream's id chain.
    pub obu: Option<ObuLink<'a>>,
}

/// Route a frame in one walk over it. Past the root block and the groups
/// come the channel's name and then the instrument's; an `obu` frame names
/// its stream, `ob.BTC_USDT.50`, whose middle is the pair.
pub fn route(market: Market, frame: &[u8]) -> Option<Route<'_>> {
    let (header, body) = MessageHeader::read_from_prefix(frame).ok()?;
    let template_id = header.template_id.get();
    let groups = match (market, template_id) {
        (_, TEMPLATE_BBO) | (Market::Spot, TEMPLATE_PUBLIC_TRADE) => 0,
        (Market::Futures, TEMPLATE_PUBLIC_TRADE) => 1,
        (_, TEMPLATE_OBU) => 2,
        _ => return None,
    };
    let mut offset = size_of::<MessageHeader>() + usize::from(header.block_length.get());
    for _ in 0..groups {
        let group = read_group::<GroupSize16Encoding>(frame, &mut offset)?;
        offset = offset.checked_add(group.count.checked_mul(group.stride)?)?;
    }
    var_str8(frame, &mut offset)?;
    let name = var_str8(frame, &mut offset)?;
    if template_id != TEMPLATE_OBU {
        return Some(Route {
            instrument: name,
            obu: None,
        });
    }
    let (block, _) = ObuBlock::read_from_prefix(body).ok()?;
    let stream = name.strip_prefix(b"ob.")?;
    let level = stream.iter().rposition(|&byte| byte == b'.')?;
    Some(Route {
        instrument: &stream[..level],
        obu: Some(ObuLink {
            stream: name,
            full: block.full != 0,
            first_id: block.first_id.get(),
            last_id: block.last_id.get(),
        }),
    })
}

/// The chain link of an `obu` frame; `None` for any other frame.
pub fn obu_link(market: Market, frame: &[u8]) -> Option<ObuLink<'_>> {
    route(market, frame)?.obu
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instrument(market: Market, frame: &[u8]) -> Option<&[u8]> {
        Some(route(market, frame)?.instrument)
    }

    fn var(frame: &mut Vec<u8>, text: &str) {
        frame.push(text.len() as u8);
        frame.extend_from_slice(text.as_bytes());
    }

    fn header(block_length: u16, template_id: u16) -> Vec<u8> {
        [block_length, template_id, 1, 1]
            .iter()
            .flat_map(|field| field.to_le_bytes())
            .collect()
    }

    /// An `obu` frame with one bid and two asks.
    fn obu(full: bool, first_id: i64, last_id: i64, stream: &str) -> Vec<u8> {
        let mut frame = header(36, TEMPLATE_OBU);
        frame.extend_from_slice(&1_791_182_070_044_224i64.to_le_bytes());
        frame.push(2);
        frame.extend_from_slice(&1_791_182_069_963_000i64.to_le_bytes());
        frame.push(u8::from(full));
        frame.extend_from_slice(&first_id.to_le_bytes());
        frame.extend_from_slice(&last_id.to_le_bytes());
        frame.extend_from_slice(&[(-1i8) as u8, (-6i8) as u8]);
        for count in [1u16, 2] {
            frame.extend_from_slice(&16u16.to_le_bytes());
            frame.extend_from_slice(&count.to_le_bytes());
            frame.extend(std::iter::repeat_n(7u8, 16 * usize::from(count)));
        }
        var(&mut frame, "spot.obu");
        var(&mut frame, stream);
        frame
    }

    #[test]
    fn wire_sizes_match_the_schemas() {
        assert_eq!(size_of::<SpotBboBlock>(), 59);
        assert_eq!(size_of::<FuturesBboBlock>(), 59);
        assert_eq!(size_of::<SpotTradeBlock>(), 60);
        assert_eq!(size_of::<FuturesTradeBlock>(), 11);
        assert_eq!(size_of::<FuturesTrade>(), 32);
        assert_eq!(size_of::<ObuBlock>(), 36);
        assert_eq!(size_of::<Level>(), 16);
    }

    #[test]
    fn an_obu_frame_names_its_pair_and_place_in_the_chain() {
        let frame = obu(false, 41, 45, "ob.BTC_USDT.50");

        assert_eq!(instrument(Market::Spot, &frame), Some(&b"BTC_USDT"[..]));
        assert_eq!(
            obu_link(Market::Futures, &frame),
            Some(ObuLink {
                stream: b"ob.BTC_USDT.50",
                full: false,
                first_id: 41,
                last_id: 45,
            })
        );
    }

    /// A futures trade frame carries its trades in a group before the names.
    #[test]
    fn a_futures_trade_frame_names_its_contract_past_the_group() {
        let mut frame = header(11, TEMPLATE_PUBLIC_TRADE);
        frame.extend_from_slice(&[0; 11]);
        frame.extend_from_slice(&32u16.to_le_bytes());
        frame.extend_from_slice(&2u16.to_le_bytes());
        frame.extend_from_slice(&[0; 64]);
        var(&mut frame, "futures.trades");
        var(&mut frame, "ETH_USDT");

        assert_eq!(instrument(Market::Futures, &frame), Some(&b"ETH_USDT"[..]));
    }

    #[test]
    fn a_bbo_frame_names_its_pair() {
        let mut frame = header(59, TEMPLATE_BBO);
        frame.extend_from_slice(&[0; 59]);
        var(&mut frame, "spot.book_ticker");
        var(&mut frame, "SOL_USDT");

        assert_eq!(instrument(Market::Spot, &frame), Some(&b"SOL_USDT"[..]));
        assert_eq!(obu_link(Market::Spot, &frame), None);
    }

    #[test]
    fn truncated_or_unknown_frames_name_nothing() {
        let frame = obu(true, 0, 9, "ob.BTC_USDT.50");
        assert_eq!(instrument(Market::Spot, &frame[..frame.len() - 3]), None);
        assert_eq!(instrument(Market::Spot, &frame[..50]), None);
        let mut unknown = header(4, 99);
        unknown.extend_from_slice(&[0; 4]);
        assert_eq!(instrument(Market::Spot, &unknown), None);
    }
}
