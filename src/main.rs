mod binancesbespot;
mod error;
mod file;
mod sbe_types;
mod throttler;
mod ws;

use anyhow::anyhow;
use clap::Parser;
use file::{WriteRecord, Writer};
use tokio::{signal, sync::mpsc::unbounded_channel};
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
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // D2: channel carries typed WriteRecord instead of an anonymous 4-tuple.
    let (writer_tx, mut writer_rx) = unbounded_channel::<WriteRecord>();

    let api_key = std::env::var("BINANCE_API_KEY").unwrap_or_default();

    let handle = match args.exchange.as_str() {
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
            ))
        }
        exchange => {
            return Err(anyhow!("{exchange} is not supported"));
        }
    };

    // Create output directory if it doesn't exist.
    std::fs::create_dir_all(&args.path)?;

    // Run the writer in a dedicated thread to avoid blocking async ingestion with
    // synchronous zstd compression and disk I/O.
    let writer_thread = {
        let path = args.path.clone();
        std::thread::spawn(move || {
            let mut writer = Writer::new(&path);
            while let Some(record) = writer_rx.blocking_recv() {
                if let Err(err) = writer.write(record) {
                    error!(?err, "write error");
                    break;
                }
            }
            writer.close();
            info!("writer thread finished");
        })
    };

    signal::ctrl_c().await?;
    info!("ctrl-c received, shutting down");

    // Abort the collection task to stop WebSocket and snapshot background tasks.
    handle.abort();

    // Await the handle so Tokio finishes dropping the task and all `writer_tx`
    // clones held inside it. Without this, `writer_rx.blocking_recv()` could block
    // indefinitely because the sender is not yet dropped when `writer_thread.join()`
    // is called.
    let _ = handle.await; // returns Err(JoinError::Cancelled) — intentional

    let _ = writer_thread.join();
    Ok(())
}
