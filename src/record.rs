//! On-disk record framing: `[i64 recv_ns LE][u8 tag][u32 payload_len LE][payload]`.

pub const HEADER_LEN: usize = 13;

/// Sanity ceiling for one payload, far above any frame or snapshot the
/// collectors write; a header claiming more is corrupt, or the file is not a
/// recording.
pub const MAX_PAYLOAD_LEN: u32 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Tag {
    /// Raw SBE stream frame.
    Sbe = b'S',
    /// REST book snapshot as the venue returned it: JSON from Binance, SBE
    /// template 1006 from OKX.
    Rest = b'R',
}

impl Tag {
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            b'S' => Some(Self::Sbe),
            b'R' => Some(Self::Rest),
            _ => None,
        }
    }
}

/// A record's fixed header. The tag stays a raw byte: readers report what
/// they do not recognise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub recv_ns: i64,
    pub tag: u8,
    pub len: u32,
}

impl Header {
    pub fn parse(bytes: &[u8; HEADER_LEN]) -> Self {
        let [t0, t1, t2, t3, t4, t5, t6, t7, tag, l0, l1, l2, l3] = *bytes;
        Self {
            recv_ns: i64::from_le_bytes([t0, t1, t2, t3, t4, t5, t6, t7]),
            tag,
            len: u32::from_le_bytes([l0, l1, l2, l3]),
        }
    }

    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut bytes = [0; HEADER_LEN];
        bytes[..8].copy_from_slice(&self.recv_ns.to_le_bytes());
        bytes[8] = self.tag;
        bytes[9..].copy_from_slice(&self.len.to_le_bytes());
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headers_round_trip_little_endian() {
        let header = Header {
            recv_ns: 1_785_542_400_000_000_123,
            tag: Tag::Rest as u8,
            len: 0x0102_0304,
        };
        let bytes = header.encode();
        assert_eq!(bytes[8], b'R');
        assert_eq!(bytes[9..], [4, 3, 2, 1]);
        assert_eq!(Header::parse(&bytes), header);
        assert_eq!(Tag::from_byte(bytes[8]), Some(Tag::Rest));
        assert_eq!(Tag::from_byte(b'{'), None);
    }
}
