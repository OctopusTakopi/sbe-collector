//! SBE framing every venue's schema shares, and each venue's fixed layouts.

pub mod binance;
pub mod okx;

use std::ops::Range;

use zerocopy::byteorder::LittleEndian;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

pub(crate) type U16LE = zerocopy::byteorder::U16<LittleEndian>;
pub(crate) type U32LE = zerocopy::byteorder::U32<LittleEndian>;
pub(crate) type I32LE = zerocopy::byteorder::I32<LittleEndian>;
pub(crate) type I64LE = zerocopy::byteorder::I64<LittleEndian>;

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct MessageHeader {
    pub block_length: U16LE,
    pub template_id: U16LE,
    pub schema_id: U16LE,
    pub version: U16LE,
}

/// `groupSize16Encoding`, a group dimension with a `uint16` count, so 4
/// bytes.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned, Debug, Copy, Clone)]
#[repr(C)]
pub struct GroupSize16Encoding {
    pub block_length: U16LE,
    pub num_in_group: U16LE,
}

/// A repeating group: `count` entries of `stride` bytes each. The stride is
/// the wire `blockLength`, not a struct size: a newer schema version appends
/// fields to entries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Group {
    pub count: usize,
    pub stride: usize,
}

/// A group dimension composite.
pub trait Dimension: FromBytes {
    fn group(&self) -> Group;
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
pub fn read_group<D: Dimension>(data: &[u8], offset: &mut usize) -> Option<Group> {
    let (dimension, _) = D::read_from_prefix(data.get(*offset..)?).ok()?;
    *offset += size_of::<D>();
    Some(dimension.group())
}

/// An event's identity across connections: the frame hashed without the
/// bytes its venue stamps per connection (see
/// [`crate::exchange::Exchange::stamp`]), so two connections' copies of one
/// event share a key while distinct events keep apart.
///
/// 128 bits: at a dedup generation's ceiling a collision inside one window
/// has probability around 2^-85, where 64 bits would be a coin flip over a
/// year, and a collision silently discards a real frame. A frame too short
/// to hold its stamp is hashed whole: keeping an unknown frame twice is
/// recoverable, dropping a real one is not.
pub fn event_key(frame: &[u8], stamp: Option<Range<usize>>) -> u128 {
    match stamp {
        Some(stamp) if frame.len() >= stamp.end => {
            let mut hasher = xxhash_rust::xxh3::Xxh3::new();
            hasher.update(&frame[..stamp.start]);
            hasher.update(&frame[stamp.end..]);
            hasher.digest128()
        }
        _ => xxhash_rust::xxh3::xxh3_128(frame),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_wire_sizes_match_the_schemas() {
        assert_eq!(size_of::<MessageHeader>(), 8);
        assert_eq!(size_of::<GroupSize16Encoding>(), 4);
    }

    /// Hashing around the stamp is hashing the frame with the stamp cut out.
    #[test]
    fn the_key_ignores_exactly_the_stamp() {
        let frame: Vec<u8> = (0u8..40).collect();
        let mut restamped = frame.clone();
        restamped[24..32].fill(0xEE);
        let mut cut = frame[..24].to_vec();
        cut.extend_from_slice(&frame[32..]);

        assert_eq!(
            event_key(&frame, Some(24..32)),
            event_key(&restamped, Some(24..32))
        );
        assert_eq!(
            event_key(&frame, Some(24..32)),
            xxhash_rust::xxh3::xxh3_128(&cut)
        );
        assert_ne!(event_key(&frame, None), event_key(&restamped, None));

        let mut other_event = frame.clone();
        other_event[33] ^= 1;
        assert_ne!(
            event_key(&frame, Some(24..32)),
            event_key(&other_event, Some(24..32))
        );
    }

    #[test]
    fn a_frame_too_short_for_its_stamp_is_hashed_whole() {
        let runt = [1u8, 2, 3];
        assert_eq!(
            event_key(&runt, Some(8..16)),
            xxhash_rust::xxh3::xxh3_128(&runt)
        );
    }

    #[test]
    fn a_group_dimension_is_read_and_stepped_past() {
        let data = [16u8, 0, 3, 0, 0xAA];
        let mut offset = 0;
        assert_eq!(
            read_group::<GroupSize16Encoding>(&data, &mut offset),
            Some(Group {
                count: 3,
                stride: 16
            })
        );
        assert_eq!(offset, 4);
        assert_eq!(read_group::<GroupSize16Encoding>(&data, &mut offset), None);
    }
}
