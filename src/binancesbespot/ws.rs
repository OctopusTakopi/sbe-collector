use anyhow::Error;
use bytes::Bytes;
use fastwebsockets::OpCode;
use jiff::Timestamp;
use std::{
    io,
    io::ErrorKind,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::mpsc::Sender, time::timeout};
use tracing::{error, warn};

use crate::ws::{Delivery, Overflow};

const WSS_HOST: &str = "stream-sbe.binance.com";
const WSS_PORT: u16 = 9443;
/// Binance server pings arrive every ~20 s, and market-data frames on active
/// symbols arrive in milliseconds, so 60 s of total silence means a dead socket.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) async fn connect(
    streams_str: &str,
    api_key: &str,
    connection: usize,
    tls: Arc<rustls::ClientConfig>,
    ws_tx: Sender<(Timestamp, Bytes)>,
) -> Result<(), anyhow::Error> {
    let url = format!(
        "wss://{}:{}/stream?streams={}",
        WSS_HOST, WSS_PORT, streams_str
    );
    // Reuse the pre-built TLS config (Arc clone is O(1)).
    let mut conn = crate::ws::connect(&url, Some(api_key), tls).await?;
    let sender = conn.sender();
    let mut overflow = Overflow::new("binance-sbe-spot");

    loop {
        // `Connection::read` is not cancel-safe, so it may only be raced against
        // terminal arms. Both timeouts below drop the connection, which is what
        // makes cancelling it here sound.
        let message = match timeout(IDLE_TIMEOUT, conn.read()).await {
            Ok(result) => result?,
            Err(_) => {
                warn!(
                    connection,
                    ?IDLE_TIMEOUT,
                    "no websocket frame received; reconnecting"
                );
                return Err(Error::from(io::Error::new(ErrorKind::TimedOut, "idle")));
            }
        };

        match message.opcode {
            OpCode::Binary => {
                let recv_time = Timestamp::now();
                // The stream list is in the connect URL, so there is no
                // subscription protocol here — every binary frame is market
                // data and may be shed if the writer falls behind.
                let delivery =
                    crate::ws::deliver(&ws_tx, &mut overflow, (recv_time, message.payload), |_| {
                        true
                    })
                    .await;
                match delivery {
                    Delivery::Sent | Delivery::Dropped => {}
                    // Receiver dropped: the collector is shutting down.
                    Delivery::Closed => return Ok(()),
                    Delivery::Undeliverable => {
                        return Err(Error::from(io::Error::new(
                            ErrorKind::TimedOut,
                            "frame could not be delivered",
                        )));
                    }
                }
            }
            OpCode::Text => {
                tracing::info!("WS text: {}", String::from_utf8_lossy(&message.payload));
            }
            OpCode::Ping => {
                sender.pong(message.payload.to_vec()).await?;
            }
            OpCode::Close => {
                warn!(connection, "WS closed by server");
                return Err(Error::from(io::Error::new(
                    ErrorKind::ConnectionAborted,
                    "server closed",
                )));
            }
            _ => {}
        }
    }
}

/// D5: truncated exponential back-off with ±25 % jitter.
/// Avoids the thundering-herd problem when multiple instances reconnect
/// simultaneously after a network outage.
fn jittered_backoff(error_count: u32) -> Duration {
    // Cap exponent at 7 → max base = 100 * 128 = 12 800 ms, clamped to 10 s.
    let base_ms: u64 = (100u64 * 2u64.pow(error_count.min(7))).min(10_000);
    let jitter_range = (base_ms / 4).max(1);
    // Use subsecond nanos of the current wall clock as a cheap pseudo-random seed.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let jitter = nanos % (jitter_range * 2 + 1);
    Duration::from_millis(base_ms - jitter_range + jitter)
}

pub(crate) async fn keep_connection(
    streams: Vec<String>,
    symbols: Vec<String>,
    api_key: String,
    connection: usize,
    ws_tx: Sender<(Timestamp, Bytes)>,
) {
    // Build TLS config once for the lifetime of this task.
    let tls = Arc::new(crate::ws::build_tls_config());

    let streams_str = symbols
        .iter()
        .flat_map(|sym| {
            let sym = sym.to_lowercase();
            streams.iter().map(move |s| s.replace("$symbol", &sym))
        })
        .collect::<Vec<_>>()
        .join("/");

    tracing::info!(connection, "Connecting to SBE stream: {}", streams_str);

    let mut error_count: u32 = 0;
    loop {
        let connect_time = Instant::now();
        if let Err(err) = connect(
            &streams_str,
            &api_key,
            connection,
            tls.clone(),
            ws_tx.clone(),
        )
        .await
        {
            let lifetime = connect_time.elapsed();
            // The lifetime is what separates a venue recycling a healthy
            // connection from this side failing: an `Unexpected EOF` after an
            // hour is the former, one after a few seconds is the latter.
            error!(connection, ?err, ?lifetime, "WS connection error");
            error_count += 1;
            // Reset the counter if the last session lived long enough — it was
            // a transient blip, not a persistent failure.
            if lifetime > Duration::from_secs(30) {
                error_count = 0;
            }
            tokio::time::sleep(jittered_backoff(error_count)).await;
        } else {
            // Clean disconnect (ws_tx dropped) — exit.
            break;
        }
    }
}
