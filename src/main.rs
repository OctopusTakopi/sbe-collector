mod binancesbespot;
mod collection;
mod dedup;
mod error;
mod feed;
mod file;
mod gatesbe;
mod handover;
mod listing;
mod okxsbe;
mod quality;
mod readiness;
mod session;
mod snapshot;
mod subscriptions;
mod throttler;
mod ws;

use std::{path::PathBuf, time::Duration};

use anyhow::{Context as _, anyhow};
use clap::Parser;
use sbe_collector::{exchange::Exchange, sbe::gate::Market};
use tokio::{
    select, signal,
    sync::{mpsc, oneshot, watch},
    task::{JoinError, JoinHandle},
};
use tracing::{error, info};

use collection::Context;
use file::{WriteRecord, Writer};
use quality::{HandoverState, QualityEvent, QualityLog};
use readiness::Readiness;

const WRITER_QUEUE_CAPACITY: usize = 65_536;
/// How long the collection task may spend handing what it already received
/// to the writer before it is aborted.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the handover supervisor gets at exit: to deliver the drain
/// report after a takeover, or to release the socket otherwise.
const HANDOVER_REPLY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Parser, Debug)]
#[command(
    version,
    about = "SBE market-data collector for Binance spot, OKX and Gate"
)]
struct Args {
    /// Directory where collected data files are written.
    path: PathBuf,

    /// Exchange to collect.
    exchange: Exchange,

    /// Symbols to subscribe to, as the exchange names them: btcusdt for
    /// Binance, instIds such as BTC-USDT or BTC-USDT-SWAP for OKX, BTC_USDT
    /// for Gate.
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

/// What a hangup does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Hangup {
    /// Drain and exit, as for SIGTERM: `tmux kill-session` delivers SIGHUP,
    /// and unhandled it kills the process before buffered records are
    /// written.
    Drain,
    /// SIGHUP was ignored at startup (`nohup`). Installing a handler would
    /// replace that inherited ignore.
    Ignore,
}

impl Hangup {
    fn for_disposition(handler: libc::sighandler_t) -> Self {
        if handler == libc::SIG_IGN {
            Self::Ignore
        } else {
            Self::Drain
        }
    }

    /// From SIGHUP's disposition before any handler is installed.
    fn inherited() -> std::io::Result<Self> {
        // SAFETY: with a null new action `sigaction` only writes the current
        // one into `old`, a live local for which all-zero bytes are valid.
        let (status, old) = unsafe {
            let mut old: libc::sigaction = std::mem::zeroed();
            let status = libc::sigaction(libc::SIGHUP, std::ptr::null(), &mut old);
            (status, old)
        };
        if status != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self::for_disposition(old.sa_sigaction))
    }
}

/// A venue's collection, prepared before anything starts, so missing
/// credentials or an unknown symbol stop the process before it takes the
/// handover socket or opens a quality log.
enum Venue {
    BinanceSbeSpot(binancesbespot::Subscription),
    OkxSbe(okxsbe::Subscription),
    GateSbe(gatesbe::Subscription),
}

impl Venue {
    async fn prepare(exchange: Exchange, symbols: &[String]) -> anyhow::Result<Self> {
        Ok(match exchange {
            Exchange::BinanceSbeSpot => {
                Self::BinanceSbeSpot(binancesbespot::Subscription::prepare(symbols).await?)
            }
            Exchange::OkxSbe => Self::OkxSbe(okxsbe::Subscription::prepare(symbols).await?),
            Exchange::GateSbeSpot => {
                Self::GateSbe(gatesbe::Subscription::prepare(Market::Spot, symbols).await?)
            }
            Exchange::GateSbeFutures => {
                Self::GateSbe(gatesbe::Subscription::prepare(Market::Futures, symbols).await?)
            }
        })
    }

    fn spawn(self, cx: Context) -> JoinHandle<anyhow::Result<()>> {
        match self {
            Self::BinanceSbeSpot(subscription) => {
                tokio::spawn(binancesbespot::collect(subscription, cx))
            }
            Self::OkxSbe(subscription) => tokio::spawn(okxsbe::collect(subscription, cx)),
            Self::GateSbe(subscription) => tokio::spawn(gatesbe::collect(subscription, cx)),
        }
    }
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
    let hangup = Hangup::inherited()?;
    if hangup == Hangup::Ignore {
        info!("SIGHUP is ignored (inherited, e.g. from nohup); a hangup will not stop collection");
    }
    let connections = usize::from(args.connections);
    let venue = Venue::prepare(args.exchange, &args.symbols)
        .await
        .with_context(|| format!("cannot collect {}", args.exchange.as_str()))?;
    if connections > 1 {
        info!(
            connections,
            "redundant collection enabled; duplicate frames will be discarded"
        );
    }

    std::fs::create_dir_all(&args.path)?;
    args.exchange.claim(&args.path).with_context(|| {
        format!(
            "cannot record {} into {}",
            args.exchange.as_str(),
            args.path.display()
        )
    })?;
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
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (takeover_tx, mut takeover_rx) = mpsc::channel(1);
    let mut handover_task = tokio::spawn(handover::run(
        socket,
        identity,
        run_id.clone(),
        ready_rx,
        takeover_tx,
        quality.clone(),
        shutdown_rx.clone(),
    ));

    let (writer_tx, mut writer_rx) = mpsc::channel::<WriteRecord>(WRITER_QUEUE_CAPACITY);
    let mut collection = venue.spawn(Context {
        writer_tx,
        shutdown: shutdown_rx,
        connections,
        quality: quality.clone(),
        readiness: readiness.clone(),
    });

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
        result = shutdown_signal(hangup) => {
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
        // Shutdown is requested only after this select, so the supervisor
        // returning here is always a failure.
        result = &mut handover_task => Shutdown::HandoverFailed(match result {
            Ok(Ok(())) => anyhow!("UDS handover supervisor exited unexpectedly"),
            Ok(Err(error)) => error,
            Err(error) => anyhow!("UDS handover task failed: {error}"),
        }),
        result = &mut collection => Shutdown::Collection(joined("collection task", result)),
    };

    if !matches!(shutdown, Shutdown::Takeover(_)) {
        // Release the handover socket at once. A takeover request that raced
        // this exit is dropped, and its challenger chooses its role again.
        shutdown_tx.send_replace(true);
        drop(takeover_rx);
    }
    let mut handover_result = Ok(());
    let (collection_result, takeover) = match shutdown {
        Shutdown::Collection(result) => (result, None),
        Shutdown::Takeover(request) => (
            stop_collection(&shutdown_tx, &mut collection).await,
            Some(request),
        ),
        Shutdown::HandoverFailed(error) => {
            handover_result = Err(error);
            (stop_collection(&shutdown_tx, &mut collection).await, None)
        }
        Shutdown::Signal | Shutdown::Writer | Shutdown::Quality => {
            (stop_collection(&shutdown_tx, &mut collection).await, None)
        }
    };

    let writer_result = writer_thread
        .join()
        .unwrap_or_else(|_| Err(anyhow!("writer thread panicked")));

    for (task, result) in [
        ("handover", &handover_result),
        ("collection", &collection_result),
    ] {
        if let Err(error) = result {
            quality.report(QualityEvent::CriticalTaskFailed {
                at_ns: QualityEvent::now_ns(),
                task: task.to_owned(),
                error: error.to_string(),
            });
        }
    }
    if let Err(error) = &writer_result {
        quality.report(QualityEvent::StorageDegraded {
            at_ns: QualityEvent::now_ns(),
            target: args.path.display().to_string(),
            error: error.to_string(),
        });
    }

    if let Some(request) = takeover {
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
    }
    finish_or_abort(&mut handover_task, HANDOVER_REPLY_TIMEOUT).await;

    drop(quality);
    let quality_result = quality_writer
        .join()
        .unwrap_or_else(|_| Err(std::io::Error::other("quality writer thread panicked")))
        .map_err(anyhow::Error::from);

    // A failed handover is what stopped the collection, so it comes first.
    let result = root_cause(handover_result, collection_result, "the collection");
    let result = root_cause(result, writer_result, "the writer");
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

async fn shutdown_signal(hangup: Hangup) -> std::io::Result<&'static str> {
    use signal::unix::{SignalKind, signal as unix_signal};
    let mut sigterm = unix_signal(SignalKind::terminate())?;
    let mut sighup = match hangup {
        Hangup::Drain => Some(unix_signal(SignalKind::hangup())?),
        Hangup::Ignore => None,
    };
    let sighup = async {
        match &mut sighup {
            Some(sighup) => {
                sighup.recv().await;
            }
            None => std::future::pending().await,
        }
    };
    select! {
        result = signal::ctrl_c() => {
            result?;
            Ok("SIGINT")
        }
        _ = sigterm.recv() => Ok("SIGTERM"),
        () = sighup => Ok("SIGHUP"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_ignored_sighup_stays_ignored() {
        assert_eq!(Hangup::for_disposition(libc::SIG_IGN), Hangup::Ignore);
        assert_eq!(Hangup::for_disposition(libc::SIG_DFL), Hangup::Drain);
        let handler = only_an_ignored_sighup_stays_ignored as fn() as libc::sighandler_t;
        assert_eq!(Hangup::for_disposition(handler), Hangup::Drain);
    }
}
