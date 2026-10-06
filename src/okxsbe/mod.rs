mod instruments;
mod protocol;
mod snapshot;

use std::collections::{HashMap, HashSet};

use tokio::task::JoinSet;
use tracing::warn;

use sbe_collector::{
    exchange::Exchange,
    sbe::{MessageHeader, okx},
};
use zerocopy::FromBytes;

use crate::{
    collection::{self, Consumer, Context, Recorder},
    dedup::Dedup,
    error::ConnectorError,
    file::Symbol,
    quality::{QualityEvent, QualityReporter},
    session::{Frame, Protocol},
    snapshot::Snapshots,
};
use instruments::Instrument;
use protocol::{Credentials, Okx};
use snapshot::{BOOKS_PER_MINUTE, Books};

/// Channels subscribed per instrument.
///
/// `books-l2-tbt` and `trades` need trading fee tier VIP4 or above (`64003`
/// below it); `bbo-tbt` needs only the login.
const CHANNELS: [&str; 1] = ["bbo-tbt"];

/// OKX allows 480 subscribe requests an hour per connection, and every
/// (re)connect sends one per channel and instrument.
const MAX_SUBSCRIPTIONS: usize = 480;

/// What to collect: resolved from the command line before anything starts,
/// so an unknown instId or missing credentials stop the process cleanly.
pub struct Subscription {
    instruments: Vec<Instrument>,
    credentials: Credentials,
}

impl Subscription {
    /// Read the credentials and resolve `symbols`, OKX instIds, to the codes
    /// the stream carries.
    pub async fn prepare(symbols: &[String]) -> anyhow::Result<Self> {
        let credentials = Credentials::from_env()?;
        let subscriptions = symbols.len() * CHANNELS.len();
        if subscriptions > MAX_SUBSCRIPTIONS {
            anyhow::bail!(
                "{subscriptions} subscriptions per connection exceed OKX's {MAX_SUBSCRIPTIONS} an hour"
            );
        }
        Ok(Self {
            instruments: instruments::resolve(symbols).await?,
            credentials,
        })
    }
}

/// The single consumer of every connection's frames: duplicate suppression
/// and routing by `instIdCode`.
struct Ingest {
    recorder: Recorder,
    /// `instIdCode` to the file key, the lowercase instId.
    symbols: HashMap<i64, Symbol>,
    /// Codes already reported as unknown: the quality log syncs each event.
    unknown: HashSet<i64>,
    quality: QualityReporter,
}

impl Ingest {
    fn new(recorder: Recorder, instruments: &[Instrument], quality: QualityReporter) -> Self {
        Self {
            recorder,
            symbols: instruments
                .iter()
                .map(|instrument| (instrument.code, instrument.symbol()))
                .collect(),
            unknown: HashSet::new(),
            quality,
        }
    }
}

impl Consumer for Ingest {
    async fn handle(&mut self, frame: Frame) -> Result<(), ConnectorError> {
        let Frame { recv_time, data } = frame;
        if self.recorder.dedup.is_duplicate(&data) {
            return Ok(());
        }
        let Ok((header, _)) = MessageHeader::read_from_prefix(&data) else {
            return Err(ConnectorError::ShortFrame { len: data.len() });
        };
        let Some(code) = okx::inst_id_code(&data) else {
            return Err(ConnectorError::ShortBlock {
                name: "OKX stream frame",
                len: data.len(),
            });
        };
        let symbol = match self.symbols.get(&code) {
            Some(symbol) => Symbol::clone(symbol),
            None => {
                self.report_unknown(code, header.template_id.get());
                Symbol::from("unknown")
            }
        };
        self.recorder.write(recv_time, symbol, data).await
    }
}

impl Ingest {
    /// Only subscribed codes should ever arrive, so an unknown one is worth a
    /// quality event, once.
    fn report_unknown(&mut self, code: i64, template_id: u16) {
        if !self.unknown.insert(code) {
            return;
        }
        warn!(
            code,
            template_id, "a frame for an unsubscribed instIdCode; writing to 'unknown'"
        );
        self.quality.report(QualityEvent::FeedDegraded {
            at_ns: QualityEvent::now_ns(),
            source: Okx::SOURCE.to_owned(),
            detail: format!(
                "frames for unsubscribed instIdCode {code} (template_id={template_id}) go to 'unknown'"
            ),
        });
    }
}

/// Records `subscription` until shutdown or a critical failure.
pub async fn collect(subscription: Subscription, cx: Context) -> anyhow::Result<()> {
    let Subscription {
        instruments,
        credentials,
    } = subscription;
    let okx = Okx::new(credentials, &instruments, &CHANNELS, cx.quality.clone());
    let symbols = instruments.iter().map(Instrument::symbol).collect();
    // `bbo-tbt` has no chain to break, so nothing asks for repairs.
    let (snapshots, _) = Snapshots::new(
        Books::new(&instruments),
        BOOKS_PER_MINUTE,
        symbols,
        cx.writer_tx.clone(),
    )?;
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        snapshots.run().await;
        Err(anyhow::anyhow!("book-snapshot task exited"))
    });
    let recorder = Recorder {
        dedup: Dedup::for_connections(cx.connections, Exchange::OkxSbe),
        writer_tx: cx.writer_tx.clone(),
    };
    let ingest = Ingest::new(recorder, &instruments, cx.quality.clone());
    collection::run_slots(okx, ingest, cx, tasks).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::WriteRecord;
    use bytes::Bytes;
    use tokio::sync::mpsc::{Receiver, channel};

    /// A `bbo-tbt` frame for `code`, sent at `out_time`.
    fn bbo(code: i64, seq_id: i64, out_time: i64) -> Bytes {
        let mut frame = Vec::with_capacity(82);
        for field in [74u16, okx::TEMPLATE_BBO_TBT, 1, 0] {
            frame.extend_from_slice(&field.to_le_bytes());
        }
        for field in [code, 1_791_179_527_312_000, out_time, seq_id, 11, 1, 10, 2] {
            frame.extend_from_slice(&field.to_le_bytes());
        }
        frame.extend_from_slice(&[0; 8]);
        frame.extend_from_slice(&[(-1i8) as u8, (-8i8) as u8]);
        Bytes::from(frame)
    }

    fn ingest(connections: usize, quality: QualityReporter) -> (Ingest, Receiver<WriteRecord>) {
        let (writer_tx, writer_rx) = channel(16);
        let instruments = [Instrument {
            inst_id: "BTC-USDT".to_owned(),
            code: 3,
        }];
        let recorder = Recorder {
            dedup: Dedup::for_connections(connections, Exchange::OkxSbe),
            writer_tx,
        };
        (Ingest::new(recorder, &instruments, quality), writer_rx)
    }

    fn written(writer_rx: &mut Receiver<WriteRecord>) -> Vec<WriteRecord> {
        std::iter::from_fn(|| writer_rx.try_recv().ok()).collect()
    }

    /// Two connections deliver one event with different `outTime`s.
    #[tokio::test]
    async fn a_second_connections_copy_is_written_once() {
        let (mut ingest, mut writer_rx) = ingest(2, QualityReporter::disabled());

        for (seq_id, out_time) in [(1, 100), (1, 117), (2, 130), (2, 129)] {
            ingest
                .handle(Frame::now(bbo(3, seq_id, out_time)))
                .await
                .unwrap();
        }

        let records = written(&mut writer_rx);
        assert_eq!(records.len(), 2);
        assert!(records.iter().all(|record| &*record.symbol == "btc-usdt"));
    }

    #[tokio::test]
    async fn a_single_connection_keeps_every_frame() {
        let (mut ingest, mut writer_rx) = ingest(1, QualityReporter::disabled());

        for out_time in [100, 117] {
            ingest
                .handle(Frame::now(bbo(3, 1, out_time)))
                .await
                .unwrap();
        }

        assert_eq!(written(&mut writer_rx).len(), 2);
    }

    #[tokio::test]
    async fn an_unsubscribed_code_goes_to_unknown_and_is_reported_once() {
        let (quality, events) = QualityReporter::test_channel();
        let (mut ingest, mut writer_rx) = ingest(1, quality);

        for seq_id in [1, 2] {
            ingest
                .handle(Frame::now(bbo(42, seq_id, 100)))
                .await
                .unwrap();
        }

        let records = written(&mut writer_rx);
        assert!(records.iter().all(|record| &*record.symbol == "unknown"));
        assert!(matches!(
            events.try_recv(),
            Ok(QualityEvent::FeedDegraded { .. })
        ));
        assert!(events.try_recv().is_err(), "one report per code");
    }

    #[tokio::test]
    async fn malformed_frames_say_what_is_missing() {
        let (mut ingest, _writer_rx) = ingest(1, QualityReporter::disabled());

        let runt = ingest.handle(Frame::now(Bytes::from_static(b"\x01\x02")));
        assert_eq!(
            runt.await.unwrap_err().to_string(),
            "2-byte frame is shorter than the SBE message header"
        );
        let header_only = ingest.handle(Frame::now(bbo(3, 1, 1).slice(..12)));
        assert_eq!(
            header_only.await.unwrap_err().to_string(),
            "12-byte OKX stream frame is shorter than its fixed block"
        );
    }
}
