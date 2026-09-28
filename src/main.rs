mod binancesbespot;
mod dedup;
mod error;
mod feed;
mod file;
mod handover;
mod quality;
mod readiness;
mod throttler;
mod ws;

use std::{path::PathBuf, time::Duration};

use anyhow::{anyhow, bail};
use clap::{Parser, ValueEnum};
use tokio::{
    select, signal,
    sync::{mpsc, oneshot, watch},
    task::{JoinError, JoinHandle},
};
use tracing::{error, info};

use file::{WriteRecord, Writer};
use quality::{HandoverState, QualityEvent, QualityLog};
use readiness::Readiness;

const WRITER_QUEUE_CAPACITY: usize = 65_536;
/// Per connection: all of them share the queue, so redundancy does not shrink
/// the burst each one can absorb.
const WS_QUEUE_CAPACITY: usize = 16_384;
/// How long the collection task may spend handing what it already received
/// to the writer before it is aborted.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a drained incumbent waits for its drain report to reach the
/// challenger.
const DRAIN_REPORT_TIMEOUT: Duration = Duration::from_secs(5);
/// Redundant connections are opened this far apart. Simultaneous handshakes
/// are what a venue's connection rate limit notices, and connections opened
/// together are the ones recycled together.
const CONNECT_STAGGER: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Exchange {
    #[value(name = "binancesbespot")]
    BinanceSbeSpot,
}

impl Exchange {
    /// As spelled on the command line; the handover identity compares it
    /// verbatim with the incumbent's.
    fn as_str(self) -> &'static str {
        match self {
            Self::BinanceSbeSpot => "binancesbespot",
        }
    }
}

#[derive(Parser, Debug)]
#[command(version, about = "Binance SBE stream collector")]
struct Args {
    /// Directory where collected data files are written.
    path: PathBuf,

    /// Exchange to collect.
    exchange: Exchange,

    /// Symbols to subscribe to (e.g. btcusdt ethusdt).
    symbols: Vec<String>,

    /// Number of redundant websocket connections to the exchange.
    ///
    /// Every connection subscribes to the same streams and duplicate frames
    /// are discarded, so a disconnect on one connection leaves no hole in the
    /// recording. Costs one extra connection's bandwidth per step. 1 disables
    /// redundancy.
    #[arg(
        short = 'c',
        long,
        default_value_t = 1,
        value_parser = clap::value_parser!(u8).range(1..=8),
    )]
    connections: u8,

    /// Linux Unix-domain socket used to coordinate zero-gap rolling updates.
    /// Defaults to `<path>/.collector.sock`.
    #[arg(long)]
    handover_socket: Option<PathBuf>,
}

enum Shutdown {
    Signal,
    Writer,
    Quality,
    Takeover(handover::TakeoverRequest),
    HandoverFailed(anyhow::Error),
    Collection(anyhow::Result<()>),
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();
    let api_key = std::env::var("BINANCE_API_KEY").unwrap_or_default();
    if api_key.is_empty() {
        bail!("BINANCE_API_KEY env var must be set for the SBE stream endpoint");
    }

    let connections = usize::from(args.connections);
    if connections > 1 {
        info!(
            connections,
            "redundant collection enabled; duplicate frames will be discarded"
        );
    }

    std::fs::create_dir_all(&args.path)?;
    let run_id = format!(
        "{}-{}",
        jiff::Timestamp::now().as_nanosecond(),
        std::process::id()
    );
    let QualityLog {
        reporter: quality,
        writer: quality_writer,
        stopped: mut quality_stopped,
        path: quality_path,
    } = quality::start(&args.path, &run_id)?;
    info!(run_id, path = %quality_path.display(), "quality log started");

    let (readiness, ready_rx) = Readiness::new(connections);
    let identity = handover::Identity {
        exchange: args.exchange.as_str().to_owned(),
        symbols: args.symbols.clone(),
        connections,
        output: std::fs::canonicalize(&args.path)?.display().to_string(),
    };
    let socket = args
        .handover_socket
        .clone()
        .unwrap_or_else(|| args.path.join(".collector.sock"));
    let (takeover_tx, mut takeover_rx) = mpsc::channel(1);
    let mut handover_task = tokio::spawn(handover::run(
        socket,
        identity,
        run_id.clone(),
        ready_rx,
        takeover_tx,
        quality.clone(),
    ));

    let (writer_tx, mut writer_rx) = mpsc::channel::<WriteRecord>(WRITER_QUEUE_CAPACITY);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut collection = match args.exchange {
        Exchange::BinanceSbeSpot => tokio::spawn(binancesbespot::run_collection(
            binancesbespot::Subscription {
                symbols: args.symbols,
                api_key,
                connections,
            },
            writer_tx,
            shutdown_rx,
            quality.clone(),
            readiness.clone(),
        )),
    };

    let (writer_done_tx, writer_done_rx) = oneshot::channel();
    let writer_thread = {
        let mut writer = Writer::new(&args.path, &run_id, quality.clone());
        std::thread::spawn(move || -> anyhow::Result<()> {
            let mut writer_ready = false;
            let mut result = Ok(());
            while let Some(record) = writer_rx.blocking_recv() {
                result = writer.write(record);
                if result.is_err() {
                    break;
                }
                if !writer_ready {
                    readiness.mark_writer_ready();
                    writer_ready = true;
                }
            }
            let result = result.and(writer.close());
            let _ = writer_done_tx.send(());
            info!("writer thread finished");
            result
        })
    };

    let shutdown = select! {
        result = shutdown_signal() => {
            let signal = result?;
            info!(signal, "shutdown signal received");
            Shutdown::Signal
        }
        _ = writer_done_rx => {
            error!("writer stopped; shutting down collection");
            Shutdown::Writer
        }
        _ = &mut quality_stopped => {
            error!("quality log stopped; shutting down collection");
            Shutdown::Quality
        }
        Some(request) = takeover_rx.recv() => {
            info!(peer_run_id = %request.peer_run_id, "ready replacement requested takeover");
            Shutdown::Takeover(request)
        }
        result = &mut handover_task => Shutdown::HandoverFailed(match result {
            Ok(Ok(())) => anyhow!("UDS handover supervisor exited unexpectedly"),
            Ok(Err(error)) => error,
            Err(error) => anyhow!("UDS handover task failed: {error}"),
        }),
        result = &mut collection => Shutdown::Collection(joined("collection task", result)),
    };

    let (collection_result, takeover) = match shutdown {
        Shutdown::Collection(result) => (result, None),
        Shutdown::Takeover(request) => (
            stop_collection(&shutdown_tx, &mut collection).await,
            Some(request),
        ),
        Shutdown::HandoverFailed(error) => (
            stop_collection(&shutdown_tx, &mut collection)
                .await
                .and(Err(error)),
            None,
        ),
        Shutdown::Signal | Shutdown::Writer | Shutdown::Quality => {
            (stop_collection(&shutdown_tx, &mut collection).await, None)
        }
    };

    let writer_result = writer_thread
        .join()
        .unwrap_or_else(|_| Err(anyhow!("writer thread panicked")));

    if let Err(error) = &collection_result {
        quality.report(QualityEvent::CriticalTaskFailed {
            at_ns: QualityEvent::now_ns(),
            task: "collection".to_owned(),
            error: error.to_string(),
        });
    }
    if let Err(error) = &writer_result {
        quality.report(QualityEvent::StorageDegraded {
            at_ns: QualityEvent::now_ns(),
            target: args.path.display().to_string(),
            error: error.to_string(),
        });
    }

    let report_grace = match takeover {
        Some(request) => {
            let outcome = match collection_result.as_ref().and(writer_result.as_ref()) {
                Ok(()) => Ok(()),
                Err(error) => Err(error.to_string()),
            };
            quality.report(QualityEvent::Handover {
                at_ns: QualityEvent::now_ns(),
                state: HandoverState::drained(outcome.is_ok()),
                peer_run_id: Some(request.peer_run_id.clone()),
            });
            request.complete(outcome);
            DRAIN_REPORT_TIMEOUT
        }
        None => Duration::ZERO,
    };
    finish_or_abort(&mut handover_task, report_grace).await;

    drop(quality);
    let quality_result = quality_writer
        .join()
        .unwrap_or_else(|_| Err(std::io::Error::other("quality writer thread panicked")))
        .map_err(anyhow::Error::from);

    let result = root_cause(collection_result, writer_result, "the writer");
    root_cause(result, quality_result, "the quality log")
}

/// Logs go to the tmux pane, so once tmux is gone every write fails with EIO.
/// tracing-subscriber reports that with `eprintln!`, which panics on the same
/// dead terminal, and `panic = "abort"` would then kill the process
/// mid-shutdown. Failed log lines are dropped instead. Otherwise as
/// `fmt::init()`: `RUST_LOG` targets, `INFO` by default.
fn init_tracing() {
    use tracing_subscriber::{
        filter::{LevelFilter, Targets},
        layer::SubscriberExt,
        util::SubscriberInitExt,
    };
    let targets = std::env::var("RUST_LOG")
        .ok()
        .and_then(|spec| spec.parse::<Targets>().ok())
        .unwrap_or_else(|| Targets::new().with_default(LevelFilter::INFO));
    tracing_subscriber::fmt()
        .log_internal_errors(false)
        .with_max_level(LevelFilter::TRACE)
        .finish()
        .with(targets)
        .init();
}

fn joined(task: &str, result: Result<anyhow::Result<()>, JoinError>) -> anyhow::Result<()> {
    result.unwrap_or_else(|error| Err(anyhow!("{task} failed: {error}")))
}

/// Ask the collection task to stop reading and flush what it already has to
/// the writer, rather than aborting it with a full queue.
async fn stop_collection(
    shutdown: &watch::Sender<bool>,
    collection: &mut JoinHandle<anyhow::Result<()>>,
) -> anyhow::Result<()> {
    shutdown.send_replace(true);
    match tokio::time::timeout(DRAIN_TIMEOUT, &mut *collection).await {
        Ok(result) => joined("collection task", result),
        Err(_) => {
            collection.abort();
            let _ = collection.await;
            // Exiting 0 here would make an incomplete dump look clean.
            Err(anyhow!(
                "collection task did not finish draining within {DRAIN_TIMEOUT:?}; \
                 queued records were discarded"
            ))
        }
    }
}

/// Give `task` `grace` to finish, then abort it. A task whose output was
/// already taken is left alone: polling it again would panic.
async fn finish_or_abort<T>(task: &mut JoinHandle<T>, grace: Duration) {
    if task.is_finished() {
        return;
    }
    if tokio::time::timeout(grace, &mut *task).await.is_err() {
        task.abort();
        let _ = task.await;
    }
}

/// The first failure is the root cause; a later one is usually its symptom,
/// so it is logged rather than allowed to mask it.
fn root_cause(
    first: anyhow::Result<()>,
    second: anyhow::Result<()>,
    second_name: &str,
) -> anyhow::Result<()> {
    match (first, second) {
        (Err(first), Err(second)) => {
            error!(error = %second, "{second_name} also failed while shutting down");
            Err(first)
        }
        (first, second) => first.and(second),
    }
}

async fn shutdown_signal() -> std::io::Result<&'static str> {
    use signal::unix::{SignalKind, signal as unix_signal};
    let mut sigterm = unix_signal(SignalKind::terminate())?;
    // `tmux kill-session` delivers SIGHUP; unhandled, it kills the process
    // before buffered records are written.
    let mut sighup = unix_signal(SignalKind::hangup())?;
    select! {
        result = signal::ctrl_c() => {
            result?;
            Ok("SIGINT")
        }
        _ = sigterm.recv() => Ok("SIGTERM"),
        _ = sighup.recv() => Ok("SIGHUP"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The handover identity carries the CLI spelling, so it must round-trip.
    #[test]
    fn exchange_names_are_the_cli_spelling() {
        for exchange in Exchange::value_variants() {
            assert_eq!(Exchange::from_str(exchange.as_str(), false), Ok(*exchange));
        }
    }
}
