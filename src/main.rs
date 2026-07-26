mod binancesbespot;
mod dedup;
mod error;
mod feed;
mod file;
mod sbe_types;
mod throttler;
mod ws;

const WRITER_QUEUE_CAPACITY: usize = 65_536;
/// Per connection: the queue is shared by all of them, so redundancy does not
/// shrink the burst each one can absorb.
const WS_QUEUE_CAPACITY: usize = 16_384;
/// How long the collection task is given to hand its already-received messages
/// to the writer before it is aborted outright.
const DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Redundant connections are opened this far apart.
///
/// Simultaneous handshakes are what a venue's connection rate limit notices,
/// and connections opened together are the ones most likely to be recycled
/// together — which is exactly the correlation redundancy is meant to avoid.
const CONNECT_STAGGER: std::time::Duration = std::time::Duration::from_secs(5);

use anyhow::anyhow;
use clap::Parser;
use file::{WriteRecord, Writer};
use tokio::{
    select, signal,
    sync::{mpsc::channel, oneshot, watch},
};
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(version, about = "Binance SBE stream collector")]
struct Args {
    /// Directory where collected data files are written.
    path: String,

    /// Exchange name (currently only `binancesbespot`).
    exchange: String,

    /// Symbols to subscribe to (e.g. btcusdt ethusdt).
    symbols: Vec<String>,

    /// Number of redundant websocket connections to the exchange.
    ///
    /// Every connection subscribes to the same streams and duplicate frames
    /// are discarded, so a disconnect on one connection no longer leaves a hole
    /// in the recording — the others keep delivering while it reconnects. Costs
    /// one extra connection's bandwidth per step. 1 disables redundancy.
    #[arg(
        short = 'c',
        long,
        default_value_t = 1,
        value_parser = clap::value_parser!(u8).range(1..=8),
    )]
    connections: u8,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let connections = usize::from(args.connections);
    if connections > 1 {
        info!(
            connections,
            "redundant collection enabled; duplicate frames will be discarded"
        );
    }

    std::fs::create_dir_all(&args.path)?;
    let (writer_tx, mut writer_rx) = channel::<WriteRecord>(WRITER_QUEUE_CAPACITY);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let api_key = std::env::var("BINANCE_API_KEY").unwrap_or_default();

    let mut handle = match args.exchange.as_str() {
        "binancesbespot" => {
            if api_key.is_empty() {
                return Err(anyhow!(
                    "BINANCE_API_KEY env var must be set for the SBE stream endpoint"
                ));
            }
            let streams = [
                "$symbol@trade",
                "$symbol@bestBidAsk",
                "$symbol@depth",
                "$symbol@depth20",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

            tokio::spawn(binancesbespot::run_collection(
                streams,
                args.symbols,
                writer_tx,
                api_key,
                shutdown_rx,
                connections,
            ))
        }
        exchange => {
            return Err(anyhow!("{exchange} is not supported"));
        }
    };

    let (writer_done_tx, writer_done_rx) = oneshot::channel();
    let writer_thread = {
        let path = args.path.clone();
        std::thread::spawn(move || -> Result<(), anyhow::Error> {
            let mut writer = Writer::new(&path);
            let result = loop {
                match writer_rx.blocking_recv() {
                    Some(record) => {
                        if let Err(error) = writer.write(record) {
                            break Err(error);
                        }
                    }
                    None => break Ok(()),
                }
            };
            let result = result.and(writer.close());
            let _ = writer_done_tx.send(());
            info!("writer thread finished");
            result
        })
    };

    enum Shutdown {
        Signal,
        Writer,
        Collection(Result<(), anyhow::Error>),
    }

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
        result = &mut handle => {
            Shutdown::Collection(match result {
                Ok(result) => result,
                Err(error) => Err(anyhow!("collection task failed: {error}")),
            })
        }
    };

    let collection_result = match shutdown {
        Shutdown::Signal | Shutdown::Writer => {
            // Ask the collection task to stop reading and flush what it already
            // has, rather than aborting it with a full queue.
            shutdown_tx.send_replace(true);
            match tokio::time::timeout(DRAIN_TIMEOUT, &mut handle).await {
                Ok(Ok(result)) => result,
                Ok(Err(error)) => Err(anyhow!("collection task failed: {error}")),
                Err(_) => {
                    handle.abort();
                    let _ = handle.await;
                    // Whatever was still queued is gone. Exiting 0 here would
                    // make an incomplete dump look like a clean shutdown.
                    Err(anyhow!(
                        "collection task did not finish draining within {DRAIN_TIMEOUT:?}; \
                         queued records were discarded"
                    ))
                }
            }
        }
        Shutdown::Collection(result) => result,
    };

    let writer_result = match writer_thread.join() {
        Ok(result) => result,
        Err(_) => Err(anyhow!("writer thread panicked")),
    };

    // The collection error is the root cause; a writer close failure is usually
    // a symptom of the same underlying problem, so it must not mask it.
    match (collection_result, writer_result) {
        (Err(collection), Err(writer)) => {
            error!(%writer, "the writer also failed while shutting down");
            Err(collection)
        }
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn shutdown_signal() -> std::io::Result<&'static str> {
    #[cfg(unix)]
    {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        select! {
            result = signal::ctrl_c() => {
                result?;
                Ok("SIGINT")
            }
            _ = sigterm.recv() => Ok("SIGTERM"),
        }
    }

    #[cfg(not(unix))]
    {
        signal::ctrl_c().await?;
        Ok("SIGINT")
    }
}
