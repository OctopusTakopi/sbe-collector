mod instruments;
mod protocol;
mod snapshot;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bytes::Bytes;
use jiff::Timestamp;
use tokio::{
    sync::{mpsc::Sender, watch},
    task::JoinSet,
};
use tracing::{error, warn};

use sbe_collector::{
    exchange::Exchange,
    record::Tag,
    sbe::{MessageHeader, okx},
};
use zerocopy::FromBytes;

use crate::{
    dedup::Dedup,
    error::ConnectorError,
    feed::{BackgroundResult, Feed},
    file::{Symbol, WriteRecord},
    quality::{QualityEvent, QualityReporter},
    readiness::Readiness,
    session::{self, Frame, Protocol},
};
use instruments::Instrument;
use protocol::{Credentials, Okx};
use snapshot::Snapshots;

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
    connections: usize,
}

impl Subscription {
    /// Read the credentials and resolve `symbols`, OKX instIds, to the codes
    /// the stream carries.
    pub async fn prepare(symbols: &[String], connections: usize) -> anyhow::Result<Self> {
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
            connections,
        })
    }
}

/// The single consumer of every connection's frames: duplicate suppression
/// and routing by `instIdCode`.
struct Ingest {
    writer_tx: Sender<WriteRecord>,
    /// `instIdCode` to the file key, the lowercase instId.
    symbols: HashMap<i64, Symbol>,
    /// Codes already reported as unknown: the quality log syncs each event.
    unknown: HashSet<i64>,
    dedup: Dedup,
    quality: QualityReporter,
    /// Connection supervisors and the periodic book snapshots.
    tasks: JoinSet<BackgroundResult>,
}

impl Ingest {
    fn new(
        writer_tx: Sender<WriteRecord>,
        instruments: &[Instrument],
        connections: usize,
        quality: QualityReporter,
    ) -> Self {
        Self {
            writer_tx,
            symbols: instruments
                .iter()
                .map(|instrument| (instrument.code, instrument.symbol()))
                .collect(),
            unknown: HashSet::new(),
            dedup: Dedup::for_connections(connections, Exchange::OkxSbe),
            quality,
            tasks: JoinSet::new(),
        }
    }

    async fn handle(&mut self, recv_time: Timestamp, data: Bytes) -> Result<(), ConnectorError> {
        if self.dedup.is_duplicate(&data) {
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
        self.write(recv_time, symbol, data).await
    }

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

    async fn write(
        &self,
        recv_time: Timestamp,
        symbol: Symbol,
        data: Bytes,
    ) -> Result<(), ConnectorError> {
        self.writer_tx
            .send(WriteRecord {
                recv_time,
                symbol,
                tag: Tag::Sbe,
                data,
            })
            .await
            .map_err(|_| ConnectorError::WriterClosed)
    }
}

pub async fn run_collection(
    subscription: Subscription,
    writer_tx: Sender<WriteRecord>,
    shutdown: watch::Receiver<bool>,
    quality: QualityReporter,
    readiness: Readiness,
) -> anyhow::Result<()> {
    let Subscription {
        instruments,
        credentials,
        connections,
    } = subscription;
    let okx = Arc::new(Okx::new(
        credentials,
        &instruments,
        &CHANNELS,
        quality.clone(),
    ));
    let snapshots = Snapshots::new()?;
    let mut ingest = Ingest::new(
        writer_tx.clone(),
        &instruments,
        connections,
        quality.clone(),
    );
    let ws_rx = session::spawn_slots(&mut ingest.tasks, okx, connections, &quality, &readiness);
    let mut feed = Feed::new(ws_rx, shutdown);
    let books = instruments
        .iter()
        .map(|instrument| (instrument.code, instrument.symbol()))
        .collect();
    ingest.tasks.spawn(async move {
        snapshots.run_periodic(books, writer_tx).await;
        Err(anyhow::anyhow!("periodic book-snapshot task exited"))
    });

    while let Some(Frame {
        recv_time, data, ..
    }) = feed.recv(&mut ingest.tasks).await?
    {
        match ingest.handle(recv_time, data).await {
            Ok(()) => {}
            Err(ConnectorError::WriterClosed) => return Err(ConnectorError::WriterClosed.into()),
            Err(error) => error!(%error, "couldn't handle the received data"),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
        (
            Ingest::new(writer_tx, &instruments, connections, quality),
            writer_rx,
        )
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
                .handle(Timestamp::now(), bbo(3, seq_id, out_time))
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
                .handle(Timestamp::now(), bbo(3, 1, out_time))
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
                .handle(Timestamp::now(), bbo(42, seq_id, 100))
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

        let runt = ingest.handle(Timestamp::now(), Bytes::from_static(b"\x01\x02"));
        assert_eq!(
            runt.await.unwrap_err().to_string(),
            "2-byte frame is shorter than the SBE message header"
        );
        let header_only = ingest.handle(Timestamp::now(), bbo(3, 1, 1).slice(..12));
        assert_eq!(
            header_only.await.unwrap_err().to_string(),
            "12-byte OKX stream frame is shorter than its fixed block"
        );
    }
}
