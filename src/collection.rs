//! The one pipeline every venue runs: redundant connections feeding a shared
//! queue, drained by a single consumer into the writer.

use std::{future::Future, sync::Arc};

use bytes::Bytes;
use jiff::Timestamp;
use sbe_collector::record::Tag;
use tokio::{
    sync::{mpsc::Sender, watch},
    task::JoinSet,
};
use tracing::error;

use crate::{
    dedup::Dedup,
    error::ConnectorError,
    feed::{BackgroundResult, Feed},
    file::{Symbol, WriteRecord},
    quality::QualityReporter,
    readiness::Readiness,
    session::{self, Frame, Protocol},
};

/// What every collection shares with the rest of the process.
#[derive(Clone)]
pub struct Context {
    pub writer_tx: Sender<WriteRecord>,
    pub shutdown: watch::Receiver<bool>,
    pub connections: usize,
    pub quality: QualityReporter,
    pub readiness: Readiness,
}

/// The single owner of a collection's queue: records each frame, from
/// whichever connection it came.
pub(crate) trait Consumer {
    fn handle(&mut self, frame: Frame) -> impl Future<Output = Result<(), ConnectorError>> + Send;
}

/// Consumer state every venue shares: duplicate suppression across redundant
/// connections, and the writer queue.
pub(crate) struct Recorder {
    pub(crate) dedup: Dedup,
    pub(crate) writer_tx: Sender<WriteRecord>,
}

impl Recorder {
    /// Queues a stream frame for `symbol`'s file.
    pub(crate) async fn write(
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

/// Runs `protocol`'s connection slots, and `consumer` on the queue they
/// share, until shutdown has drained it, or until a task in `tasks` or a
/// slot supervisor fails.
pub(crate) async fn run_slots<P: Protocol, C: Consumer>(
    protocol: P,
    consumer: C,
    cx: Context,
    mut tasks: JoinSet<BackgroundResult>,
) -> anyhow::Result<()> {
    let rx = session::spawn_slots(
        &mut tasks,
        Arc::new(protocol),
        cx.connections,
        &cx.quality,
        &cx.readiness,
    );
    consume(consumer, P::SOURCE, Feed::new(rx, cx.shutdown), tasks).await
}

async fn consume<C: Consumer>(
    mut consumer: C,
    source: &'static str,
    mut feed: Feed<Frame>,
    mut tasks: JoinSet<BackgroundResult>,
) -> anyhow::Result<()> {
    while let Some(frame) = feed.recv(&mut tasks).await? {
        match consumer.handle(frame).await {
            Ok(()) => {}
            Err(ConnectorError::WriterClosed) => return Err(ConnectorError::WriterClosed.into()),
            Err(error) => error!(endpoint = source, %error, "could not record a frame"),
        }
    }
    Ok(())
}
