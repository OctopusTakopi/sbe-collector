use anyhow::Error;
use bytes::Bytes;
use fastwebsockets::{Frame, OpCode, Payload};
use jiff::Timestamp;
use std::{
    io,
    io::ErrorKind,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{select, sync::mpsc::UnboundedSender, time::interval};
use tracing::{error, warn};

const WSS_HOST: &str = "stream-sbe.binance.com";
const WSS_PORT: u16 = 9443;

pub(crate) async fn connect(
    streams_str: &str,
    api_key: &str,
    tls: Arc<rustls::ClientConfig>,
    ws_tx: UnboundedSender<(Timestamp, Bytes)>,
) -> Result<(), anyhow::Error> {
    let url = format!(
        "wss://{}:{}/stream?streams={}",
        WSS_HOST, WSS_PORT, streams_str
    );
    // Reuse the pre-built TLS config (Arc clone is O(1)).
    let mut ws = crate::ws::connect(&url, Some(api_key), tls).await?;

    // Reset on any received frame. Binance server pings arrive every ~20 s,
    // but market-data frames on active symbols arrive in milliseconds.
    // 60 s of total silence reliably indicates a dead connection.
    let mut last_activity = Instant::now();
    let mut checker = interval(Duration::from_secs(10));

    loop {
        select! {
            frame_res = ws.read_frame() => {
                match frame_res {
                    Ok(frame) => {
                        match frame.opcode {
                            OpCode::Binary => {
                                last_activity = Instant::now();
                                let recv_time = Timestamp::now();
                                let data = match frame.payload {
                                    Payload::Owned(v)       => Bytes::from(v),
                                    Payload::Borrowed(v)    => Bytes::copy_from_slice(v),
                                    Payload::BorrowedMut(v) => Bytes::copy_from_slice(v),
                                    Payload::Bytes(v)       => v.freeze(),
                                };
                                if ws_tx.send((recv_time, data)).is_err() {
                                    break;
                                }
                            }
                            OpCode::Text => {
                                last_activity = Instant::now();
                                let msg = String::from_utf8_lossy(&frame.payload);
                                tracing::info!("WS text: {}", msg);
                            }
                            OpCode::Ping => {
                                last_activity = Instant::now();
                                // Reply immediately with the same payload as required by RFC 6455.
                                ws.write_frame(Frame::pong(frame.payload)).await?;
                            }
                            OpCode::Close => {
                                warn!("WS closed by server");
                                return Err(Error::from(io::Error::new(
                                    ErrorKind::ConnectionAborted,
                                    "server closed",
                                )));
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        return Err(Error::from(e));
                    }
                }
            }
            _ = checker.tick() => {
                // Binance sends Ping frames every ~20 s; we reply above (OpCode::Ping).
                // An unsolicited pong every 10 s adds a write syscall with no benefit.
                if last_activity.elapsed() > Duration::from_secs(60) {
                    warn!("idle timeout: no frames received for 60s");
                    return Err(Error::from(io::Error::new(ErrorKind::TimedOut, "idle")));
                }
            }
        }
    }
    Ok(())
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
    ws_tx: UnboundedSender<(Timestamp, Bytes)>,
) {
    // Build TLS config once for the lifetime of this task.
    let tls = Arc::new(crate::ws::build_tls_config());

    let mut error_count: u32 = 0;
    loop {
        let connect_time = Instant::now();
        let streams_str = symbols
            .iter()
            .flat_map(|sym| {
                streams
                    .iter()
                    .map(move |s| s.replace("$symbol", sym.to_lowercase().as_str()))
            })
            .collect::<Vec<_>>()
            .join("/");

        tracing::info!("Connecting to SBE stream: {}", streams_str);

        if let Err(err) = connect(&streams_str, &api_key, tls.clone(), ws_tx.clone()).await {
            error!(?err, "WS connection error");
            error_count += 1;
            // Reset the counter if the last session lived long enough — it was
            // a transient blip, not a persistent failure.
            if connect_time.elapsed() > Duration::from_secs(30) {
                error_count = 0;
            }
            tokio::time::sleep(jittered_backoff(error_count)).await;
        } else {
            // Clean disconnect (ws_tx dropped) — exit.
            break;
        }
    }
}
