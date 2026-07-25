use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use fastwebsockets::{
    FragmentCollectorRead, Frame, OpCode, Payload, WebSocket, WebSocketWrite, handshake,
};
use http_body_util::Empty;
use hyper::Request;
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

type Io = TokioIo<Upgraded>;

/// How long a frame may wait for room in the websocket queue before it is
/// dropped.
///
/// This wait blocks the read loop, and a blocked read loop cannot see — let
/// alone answer — a server ping, so it must stay far below the shortest
/// server-side pong deadline (Binance closes after about a minute). One second
/// absorbs ordinary bursts losslessly while keeping the socket responsive.
pub const QUEUE_FULL_GRACE: Duration = Duration::from_secs(1);

/// Outgoing frames are queued rather than written inline so that control-frame
/// replies never have to race the read loop.
const OUTGOING_QUEUE_CAPACITY: usize = 64;

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

/// Build a TLS client config backed by the Mozilla root certificate bundle.
/// Call this ONCE, wrap in `Arc`, and reuse across every reconnect to avoid
/// cloning ~140 root certificates on each connection attempt.
pub fn build_tls_config() -> ClientConfig {
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

/// Rate-limited accounting for frames dropped because the writer cannot keep up.
///
/// Dropping is preferable to stalling: a stalled read loop stops answering
/// pings, so the exchange closes the connection and the resulting gap is far
/// larger than the overflow itself. The depth-diff gap detector additionally
/// notices the resulting sequence break and refetches a snapshot. What must
/// never happen is dropping *silently*.
pub struct Overflow {
    label: &'static str,
    dropped: u64,
    reported: u64,
    last_report: Instant,
    /// Once the consumer is established as too slow, frames are shed without
    /// waiting at all. See [`deliver`].
    shedding: bool,
}

impl Overflow {
    pub fn new(label: &'static str) -> Self {
        Self {
            label,
            dropped: 0,
            reported: 0,
            last_report: Instant::now(),
            shedding: false,
        }
    }

    /// Record one dropped frame.
    pub fn record_drop(&mut self) {
        self.dropped += 1;
        if self.dropped == 1 || self.last_report.elapsed() >= OVERFLOW_REPORT_INTERVAL {
            error!(
                endpoint = self.label,
                dropped_total = self.dropped,
                dropped_since_last_report = self.dropped - self.reported,
                "writer cannot keep up; dropping frames"
            );
            self.reported = self.dropped;
            self.last_report = Instant::now();
        }
    }

    /// Record one frame that made it through, reporting recovery once.
    pub fn record_sent(&mut self) {
        if self.dropped > 0 {
            info!(
                endpoint = self.label,
                dropped_total = self.dropped,
                "writer caught up; no longer dropping frames"
            );
            self.dropped = 0;
            self.reported = 0;
        }
        self.shedding = false;
    }

    fn is_shedding(&self) -> bool {
        self.shedding
    }

    fn begin_shedding(&mut self) {
        self.shedding = true;
    }

    #[cfg(test)]
    pub fn dropped(&self) -> u64 {
        self.dropped
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
/// Waiting for queue space also blocks the read loop, so waiting on *every*
/// frame would throttle socket reads to one frame per grace period. Market data
/// would then pile up ahead of the server's ping in the kernel buffer, the ping
/// would go unanswered, and the exchange would close the connection — a far
/// bigger gap than the overflow itself. So the wait happens only when the queue
/// *first* fills; after that frames are shed without waiting until the consumer
/// catches up.
///
/// `sheddable` decides whether a given frame may be dropped, and is consulted
/// only on the slow path, so classification costs nothing while the consumer
/// keeps up. Subscription acks and rejections must not be shed: losing one
/// silently leaves topics unsubscribed with nothing left to trigger a retry.
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
    if may_shed && overflow.is_shedding() {
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
            overflow.begin_shedding();
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
    pub async fn pong(&self, payload: impl Into<Vec<u8>>) -> Result<()> {
        self.0
            .send(Outgoing::Pong(payload.into()))
            .await
            .map_err(|_| anyhow!("websocket writer stopped"))
    }
}

/// A received websocket frame with an owned payload.
pub struct Message {
    pub opcode: OpCode,
    pub payload: Bytes,
}

/// A split websocket connection.
///
/// The write half is owned by a dedicated task fed through [`FrameSender`], so
/// the caller's read loop never has to share a `select!` with writes. That
/// separation is load-bearing — see [`Connection::read`].
///
/// Generic over the transport so the read/pong path can be exercised over an
/// in-memory duplex stream in tests; production always uses the default.
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
    /// (`buffer.advance(2)`) before awaiting the payload bytes, so dropping this
    /// future mid-frame loses the header and the next read re-parses the middle
    /// of a payload as a header — yielding `ReservedBitsNotZero`, `FrameTooLarge`
    /// or, worse, silently corrupted frames.
    ///
    /// Cancelling it is therefore only sound when the connection is being
    /// discarded. Any `select!` arm racing this future must be terminal: return
    /// an error, never `continue` the loop.
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
        if let Err(error) = write.write_frame(outgoing.into_frame()).await {
            warn!(%error, "websocket write failed");
            return;
        }
    }
}

/// Open a TLS WebSocket to `url`, optionally sending `X-MBX-APIKEY`.
///
/// Accepts a pre-built `Arc<ClientConfig>` so that certificate loading is not
/// repeated on every reconnect — just clone the Arc.
pub async fn connect(
    url: &str,
    api_key: Option<&str>,
    tls: Arc<ClientConfig>,
) -> Result<Connection> {
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

    let mut req_builder = Request::builder()
        .uri(url.as_str())
        .header("Host", host)
        .header("Upgrade", "websocket")
        .header("Connection", "Upgrade")
        .header("Sec-WebSocket-Key", handshake::generate_key())
        .header("Sec-WebSocket-Version", "13");

    if let Some(key) = api_key {
        req_builder = req_builder.header("X-MBX-APIKEY", key);
    }

    let req = req_builder
        .body(Empty::<Bytes>::new())
        .context("Failed to build request")?;

    let (mut ws, _) = handshake::client(&SpawnExecutor, req, tls_stream)
        .await
        .map_err(|e| anyhow!("WebSocket handshake failed: {:?}", e))?;
    // Ping frames are surfaced to the caller so the connector can track
    // liveness; the pong is queued explicitly on the writer task.
    ws.set_auto_pong(false);

    Ok(Connection::from_websocket(ws))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastwebsockets::Role;

    /// Both ends of an in-memory websocket, with no handshake and no network.
    fn duplex_pair() -> (
        Connection<tokio::io::DuplexStream>,
        WebSocket<tokio::io::DuplexStream>,
    ) {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let mut client = WebSocket::after_handshake(client_io, Role::Client);
        client.set_auto_pong(false);
        let mut server = WebSocket::after_handshake(server_io, Role::Server);
        server.set_auto_pong(false);
        (Connection::from_websocket(client), server)
    }

    /// The pong must actually reach the wire with the ping's exact payload.
    /// Queuing it on the writer task is not evidence that it was written.
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
        let mut overflow = Overflow::new("test");

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
        assert_eq!(overflow.dropped(), 99);
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

        let mut overflow = Overflow::new("test");
        assert_eq!(
            deliver(&tx, &mut overflow, 1, |_| true).await,
            Delivery::Sent
        );
        assert_eq!(overflow.dropped(), 0);
    }

    /// Control responses must never be shed: losing a subscription rejection
    /// leaves those topics unsubscribed with nothing left to trigger a retry.
    #[tokio::test(start_paused = true)]
    async fn control_frames_are_never_shed() {
        let (tx, _rx) = mpsc::channel::<u32>(1);
        tx.try_reserve().unwrap().send(0);
        let mut overflow = Overflow::new("test");

        // Get into the shedding state with a data frame.
        assert_eq!(
            deliver(&tx, &mut overflow, 1, |_| true).await,
            Delivery::Dropped
        );
        assert!(overflow.is_shedding());

        // A non-sheddable frame reports undeliverable instead of vanishing.
        assert_eq!(
            deliver(&tx, &mut overflow, 2, |_| false).await,
            Delivery::Undeliverable
        );
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
