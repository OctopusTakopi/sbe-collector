//! The exchanges recorded, and the marker naming one per output directory.
//!
//! Decoding a record takes its exchange, which the record itself does not
//! carry: two of Gate's markets send identical SBE headers over different
//! layouts. Every collector writes to a directory of its own, so the
//! directory names the exchange once, for every file in it.

use std::{
    fs::File,
    io::{self, Read, Write},
    ops::Range,
    path::Path,
};

use clap::ValueEnum;

use crate::sbe::{binance, gate, okx};

/// An exchange's SBE feed. The default is the exchange of a directory
/// recorded before markers existed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum Exchange {
    #[default]
    #[value(name = "binancesbespot")]
    BinanceSbeSpot,
    /// OKX's `public-sbe` endpoint, every instrument type on one connection.
    #[value(name = "okxsbe")]
    OkxSbe,
    #[value(name = "gatesbespot")]
    GateSbeSpot,
    /// Gate's USDT-settled perpetual futures.
    #[value(name = "gatesbefutures")]
    GateSbeFutures,
}

/// The file naming a directory's exchange: one line, its command-line
/// spelling.
pub const MARKER: &str = "_exchange";

impl Exchange {
    /// As spelled on the command line; the handover identity compares it
    /// verbatim with the incumbent's.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BinanceSbeSpot => "binancesbespot",
            Self::OkxSbe => "okxsbe",
            Self::GateSbeSpot => "gatesbespot",
            Self::GateSbeFutures => "gatesbefutures",
        }
    }

    /// The bytes of a `template_id` stream frame that the venue stamps per
    /// connection, so that two connections' copies of one event differ only
    /// there; `None` hashes the frame whole.
    ///
    /// Binance stamps `eventTime` when it serialises for a particular
    /// connection: redundant connections receive one event with stamps
    /// 18-37 us apart. OKX stamps the gateway's `outTime`, and its REST
    /// snapshot has none. Every Gate template starts with the server's send
    /// `time`.
    pub fn stamp(self, template_id: u16) -> Option<Range<usize>> {
        match self {
            Self::BinanceSbeSpot => (binance::TEMPLATE_TRADES..=binance::TEMPLATE_DEPTH_DIFF)
                .contains(&template_id)
                .then_some(binance::EVENT_TIME),
            Self::OkxSbe => okx::STREAM_TEMPLATES
                .contains(&template_id)
                .then_some(okx::OUT_TIME),
            Self::GateSbeSpot | Self::GateSbeFutures => Some(gate::TIME),
        }
    }

    /// The stamp of the stream frame `frame`, from its SBE header.
    pub fn frame_stamp(self, frame: &[u8]) -> Option<Range<usize>> {
        let template_id = u16::from_le_bytes(frame.get(2..4)?.try_into().ok()?);
        self.stamp(template_id)
    }

    /// The exchange `dir`'s recordings were made from: its marker's, or
    /// Binance spot for a directory recorded before markers existed.
    pub fn of_recordings_in(dir: &Path) -> io::Result<Self> {
        Ok(Self::marked_in(dir)?.unwrap_or_default())
    }

    /// The exchange `dir`'s marker names, if it has one. An empty marker is
    /// a claim still being written, or one a crash cut short.
    pub fn marked_in(dir: &Path) -> io::Result<Option<Self>> {
        let mut name = String::new();
        match File::open(dir.join(MARKER)) {
            Ok(mut file) => file.read_to_string(&mut name)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        if name.trim().is_empty() {
            return Ok(None);
        }
        Self::from_str(name.trim(), false).map(Some).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} names an unknown exchange: {:?}",
                    dir.join(MARKER).display(),
                    name.trim()
                ),
            )
        })
    }

    /// Mark `dir` as this exchange's, or confirm it already is.
    ///
    /// Refuses a directory marked for another exchange: two exchanges writing
    /// one symbol's file would interleave payloads no decoder can tell apart.
    pub fn claim(self, dir: &Path) -> io::Result<()> {
        let path = dir.join(MARKER);
        let file = match File::options().write(true).create_new(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                match Self::marked_in(dir)? {
                    Some(marked) if marked == self => return Ok(()),
                    Some(marked) => {
                        return Err(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            format!(
                                "{} holds {} recordings, not {}",
                                dir.display(),
                                marked.as_str(),
                                self.as_str()
                            ),
                        ));
                    }
                    // Empty: finish the claim. A concurrent one, a handover's
                    // other process, writes the same line.
                    None => File::options().write(true).truncate(true).open(&path)?,
                }
            }
            Err(error) => return Err(error),
        };
        let mut file = file;
        writeln!(file, "{}", self.as_str())?;
        file.sync_all()?;
        // Persist the directory entry too.
        File::open(dir)?.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "sbe-exchange-{tag}-{}-{}",
            std::process::id(),
            jiff::Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// The handover identity carries the CLI spelling, so it must round-trip.
    #[test]
    fn exchange_names_are_the_cli_spelling() {
        for exchange in Exchange::value_variants() {
            assert_eq!(Exchange::from_str(exchange.as_str(), false), Ok(*exchange));
        }
    }

    #[test]
    fn binance_stamps_event_time_on_its_stream_templates_only() {
        let exchange = Exchange::BinanceSbeSpot;
        for template_id in 10_000..=10_003 {
            assert_eq!(exchange.stamp(template_id), Some(8..16));
        }
        assert_eq!(exchange.stamp(10_004), None);
        assert_eq!(exchange.frame_stamp(&[0, 0, 0x13, 0x27]), Some(8..16));
        assert_eq!(exchange.frame_stamp(&[0, 0, 0x13]), None);
    }

    #[test]
    fn okx_stamps_out_time_on_stream_templates_only() {
        let exchange = Exchange::OkxSbe;
        for template_id in 1_000..=1_005 {
            assert_eq!(exchange.stamp(template_id), Some(24..32));
        }
        assert_eq!(
            exchange.stamp(1_006),
            None,
            "the REST snapshot has no outTime"
        );
        assert_eq!(exchange.stamp(10_000), None);
    }

    #[test]
    fn a_directory_marked_for_another_exchange_is_refused() {
        let dir = test_dir("other");
        Exchange::OkxSbe.claim(&dir).unwrap();

        let error = Exchange::BinanceSbeSpot.claim(&dir).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "{} holds okxsbe recordings, not binancesbespot",
                dir.display()
            )
        );
        assert_eq!(Exchange::of_recordings_in(&dir).unwrap(), Exchange::OkxSbe);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_claim_marks_the_directory_and_is_confirmed_again() {
        let dir = test_dir("claim");
        assert_eq!(Exchange::marked_in(&dir).unwrap(), None);
        assert_eq!(
            Exchange::of_recordings_in(&dir).unwrap(),
            Exchange::BinanceSbeSpot,
            "an unmarked directory is a legacy Binance one"
        );

        Exchange::BinanceSbeSpot.claim(&dir).unwrap();
        Exchange::BinanceSbeSpot.claim(&dir).unwrap();

        assert_eq!(
            std::fs::read_to_string(dir.join(MARKER)).unwrap(),
            "binancesbespot\n"
        );
        assert_eq!(
            Exchange::marked_in(&dir).unwrap(),
            Some(Exchange::BinanceSbeSpot)
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_directory_marked_for_an_unknown_exchange_is_refused() {
        let dir = test_dir("unknown");
        std::fs::write(dir.join(MARKER), "bitmexsbe\n").unwrap();

        let error = Exchange::BinanceSbeSpot.claim(&dir).unwrap_err();
        assert!(error.to_string().contains("bitmexsbe"), "{error}");
        assert!(Exchange::of_recordings_in(&dir).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A claim a crash cut short is finished by the next one.
    #[test]
    fn an_empty_marker_is_completed() {
        let dir = test_dir("empty");
        std::fs::write(dir.join(MARKER), "").unwrap();
        assert_eq!(Exchange::marked_in(&dir).unwrap(), None);

        Exchange::BinanceSbeSpot.claim(&dir).unwrap();

        assert_eq!(
            Exchange::marked_in(&dir).unwrap(),
            Some(Exchange::BinanceSbeSpot)
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}
