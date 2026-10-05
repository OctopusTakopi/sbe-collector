use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use fastwebsockets::{
    FragmentCollectorRead, Frame, OpCode, Payload, WebSocket, WebSocketError, WebSocketWrite,
    handshake,
};
use http_body_util::Empty;
use hyper::Request;
use hyper::header::HeaderMap;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use rustls::ClientConfig;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
use tracing::{error, info, warn};
use url::Url;

use crate::quality::{QualityEvent, QualityReporter};

type Io = TokioIo<Upgraded>;

/// How long a frame may wait for room in the websocket queue before it is
/// dropped.
///
/// The wait blocks the read loop, which then cannot answer a server ping, so
/// it must stay far below the venue's pong deadline (Binance closes after
/// about a minute).
pub const QUEUE_FULL_GRACE: Duration = Duration::from_secs(1);

/// Outgoing frames go through a queue so control replies never race the read
/// loop.
const OUTGOING_QUEUE_CAPACITY: usize = 64;

/// Bound on TCP, TLS and the websocket upgrade together, so a black-holed
/// connection becomes a retry instead of a task parked forever.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

/// RFC 6455 caps a control frame's payload at 125 bytes.
const MAX_CONTROL_PAYLOAD: usize = 125;

/// How long [`Connection::flush_close`] waits for a queued close frame.
const CLOSE_FLUSH_GRACE: Duration = Duration::from_secs(1);

const OVERFLOW_REPORT_INTERVAL: Duration = Duration::from_secs(5);

struct SpawnExecutor;

impl<F> hyper::rt::Executor<F> for SpawnExecutor
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, fut: F) {
        tokio::spawn(fut);
    }
}

/// A TLS client config over the Mozilla root bundle. Build it once and share
/// the `Arc`: each build clones ~140 root certificates.
pub fn build_tls_config() -> ClientConfig {
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

/// Rate-limited accounting for frames dropped because the writer cannot keep
/// up.
///
/// Dropping beats stalling: a stalled read loop stops answering pings, and
/// the venue's disconnect costs far more than the overflow. Dropping silently
/// is what must never happen.
pub struct Overflow {
    label: String,
    quality: QualityReporter,
    dropped: u64,
    reported: u64,
    last_report: Instant,
    /// The consumer is known to be too slow: shed without waiting.
    shedding: bool,
}

impl Overflow {
    pub fn new(label: impl Into<String>, quality: QualityReporter) -> Self {
        Self {
            label: label.into(),
            quality,
            dropped: 0,
            reported: 0,
            last_report: Instant::now(),
            shedding: false,
        }
    }

    pub fn record_drop(&mut self) {
        self.dropped += 1;
        if self.dropped == 1 || self.last_report.elapsed() >= OVERFLOW_REPORT_INTERVAL {
            error!(
                endpoint = %self.label,
                dropped_total = self.dropped,
                dropped_since_last_report = self.dropped - self.reported,
                "writer cannot keep up; dropping frames"
            );
            self.quality.report(QualityEvent::MarketDataDropped {
                at_ns: QualityEvent::now_ns(),
                source: self.label.clone(),
                dropped_total: self.dropped,
                dropped_since_last_report: self.dropped - self.reported,
            });
            self.reported = self.dropped;
            self.last_report = Instant::now();
        }
    }

    /// Record one frame that made it through, reporting recovery once.
    pub fn record_sent(&mut self) {
        if self.dropped > 0 {
            info!(
                endpoint = %self.label,
                dropped_total = self.dropped,
                "writer caught up; no longer dropping frames"
            );
            self.quality.report(QualityEvent::MarketDataRecovered {
                at_ns: QualityEvent::now_ns(),
                source: self.label.clone(),
                dropped_total: self.dropped,
            });
            self.dropped = 0;
            self.reported = 0;
        }
        self.shedding = false;
    }
}

/// Outcome of handing a frame to the consumer.
#[derive(Debug, PartialEq, Eq)]
pub enum Delivery {
    Sent,
    /// Shed because the consumer is saturated. Counted and reported.
    Dropped,
    /// The consumer is gone; the collector is shutting down.
    Closed,
    /// A frame that must not be shed could not be delivered.
    Undeliverable,
}

/// Hand a frame to the consumer without ever starving control frames.
///
/// Waiting on every frame would throttle reads to one frame per grace period
/// and bury the server's ping behind market data, so the wait happens only
/// when the queue first fills; after that, frames are shed at once until the
/// consumer catches up.
///
/// `sheddable` is consulted only on that slow path. Subscription acks and
/// rejections must not be shed: losing one leaves topics unsubscribed with
/// nothing to trigger a retry.
pub async fn deliver<T, F>(
    tx: &mpsc::Sender<T>,
    overflow: &mut Overflow,
    value: T,
    sheddable: F,
) -> Delivery
where
    F: FnOnce(&T) -> bool,
{
    match tx.try_reserve() {
        Ok(permit) => {
            permit.send(value);
            overflow.record_sent();
            return Delivery::Sent;
        }
        Err(mpsc::error::TrySendError::Closed(())) => return Delivery::Closed,
        Err(mpsc::error::TrySendError::Full(())) => {}
    }

    let may_shed = sheddable(&value);
    if may_shed && overflow.shedding {
        overflow.record_drop();
        return Delivery::Dropped;
    }

    match timeout(QUEUE_FULL_GRACE, tx.reserve()).await {
        Ok(Ok(permit)) => {
            permit.send(value);
            overflow.record_sent();
            Delivery::Sent
        }
        Ok(Err(_)) => Delivery::Closed,
        Err(_) if may_shed => {
            overflow.shedding = true;
            overflow.record_drop();
            Delivery::Dropped
        }
        Err(_) => Delivery::Undeliverable,
    }
}

/// A frame handed to the writer task.
pub enum Outgoing {
    Pong(Vec<u8>),
    CloseRaw(Vec<u8>),
}

impl Outgoing {
    fn from_obligated(frame: &Frame<'_>) -> Self {
        let payload = frame.payload.to_vec();
        match frame.opcode {
            OpCode::Close => Outgoing::CloseRaw(payload),
            _ => Outgoing::Pong(payload),
        }
    }

    fn into_frame(self) -> Frame<'static> {
        match self {
            Outgoing::Pong(payload) => Frame::pong(Payload::Owned(payload)),
            Outgoing::CloseRaw(payload) => Frame::close_raw(Payload::Owned(payload)),
        }
    }
}

/// Write handle for a [`Connection`]. Cheap to clone, and safe to use from a
/// task running concurrently with the read loop.
#[derive(Clone)]
pub struct FrameSender(mpsc::Sender<Outgoing>);

impl FrameSender {
    async fn send(&self, outgoing: Outgoing) -> Result<()> {
        self.0
            .send(outgoing)
            .await
            .map_err(|_| anyhow!("websocket writer stopped"))
    }

    pub async fn pong(&self, payload: impl Into<Vec<u8>>) -> Result<()> {
        self.send(Outgoing::Pong(payload.into())).await
    }

    /// Start a close handshake from this side; pair with
    /// [`Connection::flush_close`] to wait for it to reach the wire.
    ///
    /// Dropping the socket instead leaves the venue holding a connection slot
    /// this IP is limited on. `reason` is truncated to the 125-byte control
    /// frame limit, past which the close becomes a protocol error.
    pub async fn close(&self, code: u16, reason: &str) -> Result<()> {
        let room = MAX_CONTROL_PAYLOAD - size_of::<u16>();
        let reason = &reason.as_bytes()[..reason.len().min(room)];
        let mut payload = code.to_be_bytes().to_vec();
        payload.extend_from_slice(reason);
        self.send(Outgoing::CloseRaw(payload)).await
    }
}

/// The status code and reason of a close frame's payload, for the log: a
/// venue's own codes (OKX 4004 "no data received in 30s", 4005 "buffer is
/// full") are what tell one disconnect from another.
pub fn close_reason(payload: &[u8]) -> (Option<u16>, std::borrow::Cow<'_, str>) {
    match payload {
        [high, low, reason @ ..] => (
            Some(u16::from_be_bytes([*high, *low])),
            String::from_utf8_lossy(reason),
        ),
        _ => (None, std::borrow::Cow::Borrowed("")),
    }
}

/// A received websocket frame with an owned payload.
pub struct Message {
    pub opcode: OpCode,
    pub payload: Bytes,
}

/// A split websocket connection.
///
/// The write half is owned by a task fed through [`FrameSender`], so the
/// caller's read loop never shares a `select!` with writes (see
/// [`Connection::read`]). Generic over the transport for in-memory tests.
pub struct Connection<S = Io> {
    read: FragmentCollectorRead<ReadHalf<S>>,
    out_tx: mpsc::Sender<Outgoing>,
    writer: JoinHandle<()>,
}

impl<S> Connection<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    /// Split an already-handshaken websocket and start its writer task.
    pub fn from_websocket(ws: WebSocket<S>) -> Self {
        let (read, write) = ws.split(tokio::io::split);
        let (out_tx, out_rx) = mpsc::channel(OUTGOING_QUEUE_CAPACITY);
        Self {
            read: FragmentCollectorRead::new(read),
            out_tx,
            writer: tokio::spawn(writer_task(write, out_rx)),
        }
    }
}

impl<S> Connection<S> {
    /// A clonable handle for writing frames from another task.
    pub fn sender(&self) -> FrameSender {
        FrameSender(self.out_tx.clone())
    }

    /// Read the next frame.
    ///
    /// **Not cancel-safe.** `fastwebsockets` consumes the frame header
    /// (`buffer.advance(2)`) before awaiting the payload, so dropping this
    /// future mid-frame makes the next read parse payload bytes as a header:
    /// `ReservedBitsNotZero`, `FrameTooLarge`, or silently corrupted frames.
    /// Cancel it only when discarding the connection; any `select!` arm racing
    /// it must end the session, never `continue`.
    pub async fn read(&mut self) -> Result<Message>
    where
        S: AsyncRead + Unpin,
    {
        let out_tx = self.out_tx.clone();
        let mut send_fn = |frame: Frame<'_>| {
            let out_tx = out_tx.clone();
            let outgoing = Outgoing::from_obligated(&frame);
            async move {
                out_tx
                    .send(outgoing)
                    .await
                    .map_err(|_| anyhow!("websocket writer stopped"))
            }
        };

        let frame = self.read.read_frame(&mut send_fn).await?;
        let payload = match frame.payload {
            Payload::Owned(v) => Bytes::from(v),
            Payload::Borrowed(v) => Bytes::copy_from_slice(v),
            Payload::BorrowedMut(v) => Bytes::copy_from_slice(v),
            Payload::Bytes(v) => v.freeze(),
        };
        Ok(Message {
            opcode: frame.opcode,
            payload,
        })
    }

    /// Wait for a queued close frame to reach the wire.
    ///
    /// [`read`](Self::read) and [`FrameSender::close`] only queue the close;
    /// without this, dropping the connection aborts the writer first and the
    /// close handshake becomes an abrupt teardown. The writer stops after a
    /// close frame, so this returns as soon as it is out. Safe to call twice:
    /// a finished writer is not polled again.
    pub async fn flush_close(&mut self) {
        if self.writer.is_finished() {
            return;
        }
        if timeout(CLOSE_FLUSH_GRACE, &mut self.writer).await.is_err() {
            warn!("close frame was not written within {CLOSE_FLUSH_GRACE:?}");
        }
    }
}

impl<S> Drop for Connection<S> {
    fn drop(&mut self) {
        self.writer.abort();
    }
}

async fn writer_task<W>(mut write: WebSocketWrite<W>, mut rx: mpsc::Receiver<Outgoing>)
where
    W: AsyncWrite + Unpin,
{
    while let Some(outgoing) = rx.recv().await {
        let closing = matches!(outgoing, Outgoing::CloseRaw(_));
        if let Err(error) = write.write_frame(outgoing.into_frame()).await {
            warn!(%error, "websocket write failed");
            return;
        }
        // Nothing may follow a close frame; `flush_close` waits on this exit.
        if closing {
            return;
        }
    }
}

/// Why a connection attempt failed.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// The venue rejected the upgrade's credentials. OKX also answers 401 to
    /// a signature whose timestamp is more than 30 s off, so the clock is a
    /// suspect too.
    #[error(
        "the venue refused the credentials (HTTP 401): check the API key, secret, passphrase and the system clock"
    )]
    Unauthorized,
    #[error("{0:#}")]
    Failed(#[from] anyhow::Error),
}

/// Open a TLS WebSocket to `url` with `headers` added to the upgrade request,
/// within [`CONNECT_TIMEOUT`]: none of the three handshakes has a deadline of
/// its own.
pub async fn connect(
    url: &str,
    headers: HeaderMap,
    tls: Arc<ClientConfig>,
) -> Result<Connection, ConnectError> {
    timeout(CONNECT_TIMEOUT, handshake_all(url, headers, tls))
        .await
        .map_err(|_| anyhow!("connection attempt timed out after {CONNECT_TIMEOUT:?}"))?
}

/// The websocket upgrade request for `url`, carrying `headers` as well.
fn upgrade_request(url: &Url, headers: HeaderMap) -> Result<Request<Empty<Bytes>>> {
    let host = url.host_str().ok_or_else(|| anyhow!("No host in url"))?;
    let mut request = Request::builder()
        .uri(url.as_str())
        .header("Host", host)
        .header("Upgrade", "websocket")
        .header("Connection", "Upgrade")
        .header("Sec-WebSocket-Key", handshake::generate_key())
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::<Bytes>::new())
        .context("Failed to build request")?;
    request.headers_mut().extend(headers);
    Ok(request)
}

async fn handshake_all(
    url: &str,
    headers: HeaderMap,
    tls: Arc<ClientConfig>,
) -> Result<Connection, ConnectError> {
    let url = Url::parse(url).context("Invalid URL")?;
    let host = url.host_str().ok_or_else(|| anyhow!("No host in url"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| anyhow!("No port in url"))?;
    let domain = rustls::pki_types::ServerName::try_from(host)
        .map_err(|e| anyhow!("Invalid domain: {}", e))?
        .to_owned();

    let connector = TlsConnector::from(tls);

    let addr = format!("{}:{}", host, port);
    let tcp_stream = TcpStream::connect(&addr)
        .await
        .context("Failed to connect via TCP")?;
    let tls_stream = connector
        .connect(domain, tcp_stream)
        .await
        .context("Failed to perform TLS handshake")?;

    let request = upgrade_request(&url, headers)?;
    let (mut ws, _) = handshake::client(&SpawnExecutor, request, tls_stream)
        .await
        .map_err(|error| match error {
            WebSocketError::InvalidStatusCode(401) => ConnectError::Unauthorized,
            error => anyhow!("WebSocket handshake failed: {error:?}").into(),
        })?;
    // Pings reach the caller, which answers them through the writer task.
    ws.set_auto_pong(false);

    Ok(Connection::from_websocket(ws))
}

/// Both ends of an in-memory websocket, with no handshake and no network.
#[cfg(test)]
pub fn duplex_pair() -> (
    Connection<tokio::io::DuplexStream>,
    WebSocket<tokio::io::DuplexStream>,
) {
    use fastwebsockets::Role;

    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let mut client = WebSocket::after_handshake(client_io, Role::Client);
    client.set_auto_pong(false);
    let mut server = WebSocket::after_handshake(server_io, Role::Server);
    // The stand-in venue replies only when told to: automatic replies to a
    // client the test already dropped would fail its read with a broken pipe.
    server.set_auto_pong(false);
    server.set_auto_close(false);
    (Connection::from_websocket(client), server)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overflow_emits_loss_and_recovery_quality_events() {
        let (quality, rx) = QualityReporter::test_channel();
        let mut overflow = Overflow::new("test", quality);
        overflow.record_drop();
        overflow.record_drop();
        overflow.record_sent();

        assert!(matches!(
            rx.recv().unwrap(),
            QualityEvent::MarketDataDropped {
                dropped_total: 1,
                dropped_since_last_report: 1,
                ..
            }
        ));
        assert!(matches!(
            rx.recv().unwrap(),
            QualityEvent::MarketDataRecovered {
                dropped_total: 2,
                ..
            }
        ));
    }

    /// Queuing the pong is not evidence that it reached the wire.
    #[tokio::test]
    async fn server_ping_is_answered_with_a_matching_pong() {
        let (mut conn, mut server) = duplex_pair();

        server
            .write_frame(Frame::new(
                true,
                OpCode::Ping,
                None,
                Payload::Owned(b"keepalive-42".to_vec()),
            ))
            .await
            .unwrap();

        let message = conn.read().await.unwrap();
        assert_eq!(message.opcode, OpCode::Ping);
        assert_eq!(&message.payload[..], b"keepalive-42");
        conn.sender().pong(message.payload.to_vec()).await.unwrap();

        let reply = server.read_frame().await.unwrap();
        assert_eq!(reply.opcode, OpCode::Pong);
        assert_eq!(&reply.payload[..], b"keepalive-42");
    }

    /// A sustained stall must not cost one grace period per frame: that would
    /// throttle socket reads and leave pings stuck behind market data.
    #[tokio::test(start_paused = true)]
    async fn sustained_backpressure_sheds_without_waiting_after_the_first_grace() {
        let (tx, _rx) = mpsc::channel::<u32>(1);
        tx.try_reserve().unwrap().send(0); // fill it
        let mut overflow = Overflow::new("test", QualityReporter::disabled());

        let before = tokio::time::Instant::now();
        assert_eq!(
            deliver(&tx, &mut overflow, 1, |_| true).await,
            Delivery::Dropped
        );
        let after_first = tokio::time::Instant::now();
        assert!(
            after_first - before >= QUEUE_FULL_GRACE,
            "the first frame should absorb a burst"
        );

        // Every subsequent frame is shed immediately.
        for value in 2..100 {
            assert_eq!(
                deliver(&tx, &mut overflow, value, |_| true).await,
                Delivery::Dropped
            );
        }
        assert_eq!(
            tokio::time::Instant::now(),
            after_first,
            "shedding must not wait"
        );
        assert_eq!(overflow.dropped, 99);
    }

    /// A brief burst is absorbed losslessly rather than shed.
    #[tokio::test(start_paused = true)]
    async fn a_short_burst_is_absorbed_without_dropping() {
        let (tx, mut rx) = mpsc::channel::<u32>(1);
        tx.try_reserve().unwrap().send(0);

        tokio::spawn(async move {
            tokio::time::sleep(QUEUE_FULL_GRACE / 2).await;
            let _ = rx.recv().await;
            std::future::pending::<()>().await;
        });

        let mut overflow = Overflow::new("test", QualityReporter::disabled());
        assert_eq!(
            deliver(&tx, &mut overflow, 1, |_| true).await,
            Delivery::Sent
        );
        assert_eq!(overflow.dropped, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn control_frames_are_never_shed() {
        let (tx, _rx) = mpsc::channel::<u32>(1);
        tx.try_reserve().unwrap().send(0);
        let mut overflow = Overflow::new("test", QualityReporter::disabled());

        // Get into the shedding state with a data frame.
        assert_eq!(
            deliver(&tx, &mut overflow, 1, |_| true).await,
            Delivery::Dropped
        );
        assert!(overflow.shedding);

        // A non-sheddable frame reports undeliverable instead of vanishing.
        assert_eq!(
            deliver(&tx, &mut overflow, 2, |_| false).await,
            Delivery::Undeliverable
        );
    }

    #[tokio::test]
    async fn a_server_close_is_echoed_before_the_connection_is_dropped() {
        let (mut conn, mut server) = duplex_pair();

        server
            .write_frame(Frame::close(1001, b"going away"))
            .await
            .unwrap();

        let message = conn.read().await.unwrap();
        assert_eq!(message.opcode, OpCode::Close);
        conn.flush_close().await;
        drop(conn);

        let reply = server.read_frame().await.unwrap();
        assert_eq!(reply.opcode, OpCode::Close);
        assert_eq!(&reply.payload[2..], b"going away");
    }

    #[test]
    fn the_upgrade_request_carries_the_venues_headers() {
        let url = Url::parse("wss://ws.okx.com:8443/ws/v5/public-sbe").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("ok-access-key", "key".parse().unwrap());
        headers.insert("ok-access-passphrase", "phrase".parse().unwrap());

        let request = upgrade_request(&url, headers).unwrap();

        let headers = request.headers();
        assert_eq!(headers["host"], "ws.okx.com");
        assert_eq!(headers["upgrade"], "websocket");
        assert_eq!(headers["ok-access-key"], "key");
        assert_eq!(headers["ok-access-passphrase"], "phrase");
        assert_eq!(request.uri(), "wss://ws.okx.com:8443/ws/v5/public-sbe");
    }

    #[test]
    fn an_unauthorized_upgrade_names_the_credentials() {
        let message = ConnectError::Unauthorized.to_string();
        assert!(message.contains("401"), "{message}");
        assert!(message.contains("clock"), "{message}");
    }

    #[test]
    fn a_close_payload_yields_its_code_and_reason() {
        let mut payload = 4004u16.to_be_bytes().to_vec();
        payload.extend_from_slice(b"No data received in 30s");
        assert_eq!(
            close_reason(&payload),
            (Some(4004), "No data received in 30s".into())
        );
        assert_eq!(close_reason(&[]), (None, "".into()));
    }

    /// SBE market data arrives as binary frames.
    #[tokio::test]
    async fn binary_frames_round_trip_with_their_payload() {
        let (mut conn, mut server) = duplex_pair();

        server
            .write_frame(Frame::new(
                true,
                OpCode::Binary,
                None,
                Payload::Owned(vec![0x12, 0x00, 0x10, 0x27]),
            ))
            .await
            .unwrap();

        let message = conn.read().await.unwrap();
        assert_eq!(message.opcode, OpCode::Binary);
        assert_eq!(&message.payload[..], &[0x12, 0x00, 0x10, 0x27]);
    }
}
