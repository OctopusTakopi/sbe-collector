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
    /// Binance spot for a directory recorded before markers existed. `None`
    /// for a directory another collector recorded, which these tools cannot
    /// decode.
    pub fn of_recordings_in(dir: &Path) -> io::Result<Option<Self>> {
        Ok(match Marked::read(dir)? {
            Marked::Unmarked => Some(Self::default()),
            Marked::Exchange(exchange) => Some(exchange),
            Marked::Foreign(_) => None,
        })
    }

    /// Refuses `dir` if its marker names anything else. Only reads: an
    /// unmarked directory is marked by [`Self::claim`], once this process is
    /// its incumbent.
    pub fn check_marker(self, dir: &Path) -> io::Result<()> {
        self.marked_for(dir)?;
        Ok(())
    }

    /// Mark `dir` as this exchange's, or confirm it already is.
    ///
    /// Refuses a directory marked for another exchange: two exchanges writing
    /// one symbol's file would interleave payloads no decoder can tell apart.
    pub fn claim(self, dir: &Path) -> io::Result<()> {
        if self.marked_for(dir)? {
            return Ok(());
        }
        let staged = dir.join(format!(
            ".{MARKER}.{}.{}",
            std::process::id(),
            self.as_str()
        ));
        let linked = self.link_staged(dir, &staged);
        // Gone already where it replaced an empty marker; a leftover only
        // litters.
        let _ = std::fs::remove_file(&staged);
        linked?;
        // Persist the directory entry too.
        File::open(dir)?.sync_all()
    }

    /// Whether `dir` is marked for this exchange already; an error if it is
    /// marked for anything else.
    fn marked_for(self, dir: &Path) -> io::Result<bool> {
        match Marked::read(dir)? {
            Marked::Unmarked => Ok(false),
            Marked::Exchange(marked) if marked == self => Ok(true),
            Marked::Exchange(marked) => Err(refused(dir, marked.as_str(), self)),
            Marked::Foreign(name) => Err(refused(dir, &name, self)),
        }
    }

    /// Writes the name whole to `staged`, then links it into place as the
    /// marker: a reader never sees it half written, and of claims racing,
    /// only one links.
    fn link_staged(self, dir: &Path, staged: &Path) -> io::Result<()> {
        let mut file = File::create(staged)?;
        writeln!(file, "{}", self.as_str())?;
        file.sync_all()?;
        let path = dir.join(MARKER);
        match std::fs::hard_link(staged, &path) {
            // Claimed meanwhile, or empty: a claim an older binary's crash
            // cut short, replaced whole.
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if self.marked_for(dir)? {
                    Ok(())
                } else {
                    std::fs::rename(staged, &path)
                }
            }
            linked => linked,
        }
    }
}

/// What a directory's marker says.
#[derive(Debug, PartialEq, Eq)]
pub enum Marked {
    /// No marker, or an empty one: an older binary's claim still being
    /// written, or one a crash cut short.
    Unmarked,
    Exchange(Exchange),
    /// An exchange this collector does not record, such as collector's
    /// `binancespot`.
    Foreign(String),
}

impl Marked {
    pub fn read(dir: &Path) -> io::Result<Self> {
        let mut name = String::new();
        match File::open(dir.join(MARKER)) {
            Ok(mut file) => file.read_to_string(&mut name)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Self::Unmarked),
            Err(error) => return Err(error),
        };
        let name = name.trim();
        Ok(if name.is_empty() {
            Self::Unmarked
        } else {
            Exchange::from_str(name, false)
                .map_or_else(|_| Self::Foreign(name.to_owned()), Self::Exchange)
        })
    }
}

fn refused(dir: &Path, marked: &str, exchange: Exchange) -> io::Error {
    io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!(
            "{} holds {marked} recordings, not {}",
            dir.display(),
            exchange.as_str()
        ),
    )
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
        assert_eq!(
            Exchange::of_recordings_in(&dir).unwrap(),
            Some(Exchange::OkxSbe)
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_claim_marks_the_directory_and_is_confirmed_again() {
        let dir = test_dir("claim");
        assert_eq!(Marked::read(&dir).unwrap(), Marked::Unmarked);
        assert_eq!(
            Exchange::of_recordings_in(&dir).unwrap(),
            Some(Exchange::BinanceSbeSpot),
            "an unmarked directory is a legacy Binance one"
        );

        Exchange::BinanceSbeSpot.claim(&dir).unwrap();
        Exchange::BinanceSbeSpot.claim(&dir).unwrap();

        assert_eq!(
            std::fs::read_to_string(dir.join(MARKER)).unwrap(),
            "binancesbespot\n"
        );
        let names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(names, [MARKER], "nothing staged is left behind");
        assert_eq!(
            Marked::read(&dir).unwrap(),
            Marked::Exchange(Exchange::BinanceSbeSpot)
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// collector writes the same marker: its directories are foreign here,
    /// refused to the collector and skipped by the tools.
    #[test]
    fn a_collector_directory_is_foreign() {
        let dir = test_dir("foreign");
        std::fs::write(dir.join(MARKER), "binancespot\n").unwrap();

        assert_eq!(
            Marked::read(&dir).unwrap(),
            Marked::Foreign("binancespot".to_owned())
        );
        let error = Exchange::BinanceSbeSpot.claim(&dir).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "{} holds binancespot recordings, not binancesbespot",
                dir.display()
            )
        );
        assert_eq!(Exchange::of_recordings_in(&dir).unwrap(), None);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// The check up front only reads: it refuses a directory marked for
    /// another exchange, and leaves an unmarked one, or one not made yet,
    /// unmarked.
    #[test]
    fn the_check_refuses_another_exchange_and_writes_nothing() {
        let dir = test_dir("check");
        Exchange::GateSbeSpot.check_marker(&dir).unwrap();
        Exchange::GateSbeSpot
            .check_marker(&dir.join("not-yet"))
            .unwrap();
        assert_eq!(Marked::read(&dir).unwrap(), Marked::Unmarked);
        assert!(!dir.join("not-yet").exists());

        Exchange::OkxSbe.claim(&dir).unwrap();
        Exchange::OkxSbe.check_marker(&dir).unwrap();
        let error = Exchange::GateSbeSpot.check_marker(&dir).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "{} holds okxsbe recordings, not {}",
                dir.display(),
                Exchange::GateSbeSpot.as_str()
            )
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Two exchanges claiming one directory at once: exactly one wins, and
    /// the marker it leaves is whole.
    #[test]
    fn of_two_exchanges_claiming_at_once_only_one_wins() {
        for round in 0..10 {
            let dir = test_dir(&format!("race-{round}"));
            let barrier = std::sync::Barrier::new(2);
            let (dir_ref, barrier) = (&dir, &barrier);
            let won = std::thread::scope(|scope| {
                [Exchange::OkxSbe, Exchange::BinanceSbeSpot]
                    .map(|exchange| {
                        scope.spawn(move || {
                            barrier.wait();
                            exchange.claim(dir_ref).is_ok()
                        })
                    })
                    .map(|claim| claim.join().unwrap())
            });
            let marker = std::fs::read_to_string(dir.join(MARKER)).unwrap();
            match won {
                [true, false] => assert_eq!(marker, "okxsbe\n"),
                [false, true] => assert_eq!(marker, "binancesbespot\n"),
                both => panic!("round {round}: {both:?}, marker {marker:?}"),
            }
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    /// A claim a crash cut short is finished by the next one.
    #[test]
    fn an_empty_marker_is_completed() {
        let dir = test_dir("empty");
        std::fs::write(dir.join(MARKER), "").unwrap();
        assert_eq!(Marked::read(&dir).unwrap(), Marked::Unmarked);

        Exchange::BinanceSbeSpot.claim(&dir).unwrap();

        assert_eq!(
            Marked::read(&dir).unwrap(),
            Marked::Exchange(Exchange::BinanceSbeSpot)
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}
