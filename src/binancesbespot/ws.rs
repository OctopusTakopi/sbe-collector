use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use fastwebsockets::OpCode;
use hyper::header::{HeaderMap, HeaderValue};
use jiff::Timestamp;
use rustls::ClientConfig;
use tokio::{
    io::AsyncRead,
    select,
    sync::{mpsc, oneshot},
    task::JoinSet,
    time::timeout,
};
use tracing::{debug, error, info, warn};

use crate::{
    quality::QualityReporter,
    readiness::Readiness,
    ws::{Connection, Delivery, Overflow},
};

const WSS_HOST: &str = "stream-sbe.binance.com";
const WSS_PORT: u16 = 9443;
/// Binance pings every ~20 s and active symbols tick in milliseconds, so 60 s
/// of silence means a dead socket.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// When a session starts arranging its own replacement.
///
/// The venue: "a single connection to stream-sbe.binance.com is only valid for
/// 24 hours; expect to be disconnected at the 24 hour mark". [`IDLE_TIMEOUT`]
/// bounds how late the check can run.
const MAX_SESSION_AGE: Duration = Duration::from_secs(23 * 60 * 60);

/// Spreads redundant connections' handovers. A fixed age cap would
/// re-synchronise a day later what `CONNECT_STAGGER` spread out at startup.
const SESSION_AGE_STAGGER: Duration = Duration::from_secs(15 * 60);

fn max_session_age(connection: usize) -> Duration {
    MAX_SESSION_AGE.saturating_sub(SESSION_AGE_STAGGER * connection as u32)
}

/// A session that lived this long was healthy: whatever ended it was the
/// venue's doing, and the backoff starts over.
const SETTLED: Duration = Duration::from_secs(30);

/// Pushed unprompted before the venue closes a connection:
/// `{"e":"serverShutdown","E":...}`.
const SERVER_SHUTDOWN: &str = "serverShutdown";

/// RFC 6455 "going away".
const CLOSE_GOING_AWAY: u16 = 1001;

/// Each session sends at most a `Relieve` and a `Live`.
const EVENT_QUEUE_CAPACITY: usize = 16;

/// How long a retired session may spend delivering its backlog.
///
/// Separate from [`RETIRE_GRACE`]: one `deliver` can wait a whole
/// `QUEUE_FULL_GRACE`, and a shared budget would let a slow drain starve the
/// close that hands the connection slot back.
const DRAIN_GRACE: Duration = Duration::from_secs(2);

/// How long a retired session may spend closing politely before it is
/// dropped.
const RETIRE_GRACE: Duration = Duration::from_secs(2);

/// Why a session asked for its replacement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reason {
    ServerShutdown,
    Age,
}

impl Reason {
    fn as_str(self) -> &'static str {
        match self {
            Reason::ServerShutdown => "server shutdown announced",
            Reason::Age => "connection age limit",
        }
    }
}

/// A session within one connection slot, numbered in opening order.
type SessionId = u64;

/// What a session reports to its slot's supervisor.
#[derive(Debug)]
enum Event {
    /// This session's market data reached the queue.
    Live(SessionId),
    /// Open the replacement now; this session keeps delivering until it is
    /// retired or the venue closes it.
    Relieve(SessionId, Reason),
}

/// Why a session's socket was lost.
#[derive(Debug, thiserror::Error)]
enum Loss {
    #[error("no frame for {IDLE_TIMEOUT:?}")]
    Idle,
    #[error("closed by the server")]
    ClosedByServer,
    #[error("a frame that must not be shed could not be queued")]
    Undeliverable,
    #[error("{0:#}")]
    Io(anyhow::Error),
}

#[derive(Debug)]
enum SessionEnd {
    /// The consumer is gone; the collector is shutting down.
    Finished,
    /// Retired by the supervisor once its replacement went live.
    Retired,
    Lost(Loss),
}

/// What ended the supervisor's watch over the active session.
enum Step {
    /// The session wants to be replaced and is still running.
    Relieved(Reason),
    Ended(SessionEnd),
}

/// Relieved sessions that are still carrying the feed.
///
/// The rule the handover rests on: **nothing is retired except by a session
/// that has proved it carries the feed**. A replacement that has not
/// delivered has proved nothing, so relieving it must never close the session
/// it was opened for; a venue rolling its pool can announce a shutdown on a
/// fresh connection before its first frame. A relieved session that later
/// goes live supersedes the ones before it but not itself, so retirement is
/// scoped to strictly lower ids.
#[derive(Default)]
struct Handover {
    pending: Vec<(SessionId, oneshot::Sender<()>)>,
}

impl Handover {
    /// Session `live` is delivering: every session opened before it is now
    /// redundant.
    fn on_live(&mut self, live: SessionId) {
        for (id, retire) in std::mem::take(&mut self.pending) {
            if id < live {
                let _ = retire.send(());
            } else {
                self.pending.push((id, retire));
            }
        }
    }

    /// Hold a relieved session's handle until something proves it can go.
    fn on_relieved(&mut self, id: SessionId, retire: oneshot::Sender<()>) {
        // Sessions the venue already closed cannot be retired; forgetting them
        // bounds this through a long rollout.
        self.pending.retain(|(_, pending)| !pending.is_closed());
        self.pending.push((id, retire));
    }

    fn waiting(&self) -> usize {
        self.pending.len()
    }
}

/// True if `payload` is the venue's shutdown announcement.
///
/// Parsed rather than substring-matched, so an error quoting the word does not
/// trigger a handover. The event is documented as a bare object; an enveloped
/// combined-stream copy is accepted too.
fn is_server_shutdown(payload: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return false;
    };
    let event = value.get("data").unwrap_or(&value);
    event.get("e").and_then(serde_json::Value::as_str) == Some(SERVER_SHUTDOWN)
}

/// What the drain recovered from a socket being retired.
#[derive(Default, Debug)]
struct Drained {
    delivered: usize,
    /// Shed under backpressure, so a lossy handover is not reported lossless.
    shed: usize,
}

/// The one way a market-data frame is stamped and queued, shared by the read
/// loop and the retire drain. With the stream list in the URL there is no
/// subscription protocol, so every binary frame is sheddable data.
async fn deliver_frame(
    ws_tx: &mpsc::Sender<(Timestamp, Bytes)>,
    overflow: &mut Overflow,
    payload: Bytes,
) -> Delivery {
    let recv_time = Timestamp::now();
    crate::ws::deliver(ws_tx, overflow, (recv_time, payload), |_| true).await
}

/// Deliver what already arrived on a socket about to be closed.
///
/// The replacement only sees events from its own subscription on, so this
/// backlog exists nowhere else. The zero timeout takes what is buffered and
/// stops the instant the socket would block. Cancelling `read` mid-frame is
/// sound here only because the connection is closed next; a frame still
/// arriving now is one the live replacement has too.
///
/// `deadline` is checked rather than wrapped around the drain, so the counts
/// survive a saturated queue eating the budget.
async fn drain_buffered<S>(
    conn: &mut Connection<S>,
    ws_tx: &mpsc::Sender<(Timestamp, Bytes)>,
    overflow: &mut Overflow,
    deadline: Instant,
) -> Drained
where
    S: AsyncRead + Unpin,
{
    let mut drained = Drained::default();
    while Instant::now() < deadline {
        let Ok(Ok(message)) = timeout(Duration::ZERO, conn.read()).await else {
            break;
        };
        if message.opcode != OpCode::Binary {
            continue;
        }
        match deliver_frame(ws_tx, overflow, message.payload).await {
            Delivery::Sent => drained.delivered += 1,
            Delivery::Dropped => drained.shed += 1,
            Delivery::Closed | Delivery::Undeliverable => break,
        }
    }
    drained
}

/// Where every connection slot connects.
pub(crate) struct Endpoint {
    url: String,
    /// `X-MBX-APIKEY`, validated once.
    api_key: HeaderValue,
    tls: Arc<ClientConfig>,
}

impl Endpoint {
    /// The combined-stream URL for every `$symbol` stream of every symbol.
    pub(crate) fn new(
        streams: &[impl AsRef<str>],
        symbols: &[String],
        api_key: &str,
    ) -> anyhow::Result<Self> {
        let mut api_key = HeaderValue::from_str(api_key)
            .map_err(|_| anyhow::anyhow!("BINANCE_API_KEY is not a valid header value"))?;
        api_key.set_sensitive(true);
        let streams = symbols
            .iter()
            .flat_map(|symbol| {
                let symbol = symbol.to_lowercase();
                streams
                    .iter()
                    .map(move |stream| stream.as_ref().replace("$symbol", &symbol))
            })
            .collect::<Vec<_>>()
            .join("/");
        Ok(Self {
            url: format!("wss://{WSS_HOST}:{WSS_PORT}/stream?streams={streams}"),
            api_key,
            tls: Arc::new(crate::ws::build_tls_config()),
        })
    }

    fn headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-mbx-apikey", self.api_key.clone());
        headers
    }
}

/// What every session of one connection slot shares.
#[derive(Clone)]
struct Slot {
    connection: usize,
    /// Names the slot in quality events and readiness.
    source: String,
    ws_tx: mpsc::Sender<(Timestamp, Bytes)>,
    /// One channel for every session of the slot, so a session relieved
    /// before it went live can still report going live.
    events: mpsc::Sender<Event>,
    max_age: Duration,
    quality: QualityReporter,
    readiness: Readiness,
}

impl Slot {
    fn request_replacement(&self, id: SessionId, reason: Reason) {
        warn!(
            connection = self.connection,
            session = id,
            reason = reason.as_str(),
            "asking for a replacement connection"
        );
        // The supervisor drains this promptly; a full queue means it has
        // already moved on.
        let _ = self.events.try_send(Event::Relieve(id, reason));
    }
}

/// Read one session to its end, delivering market data into the slot's queue.
///
/// Deciding to replace a session does not end it: on a shutdown announcement
/// or at `max_age` it reports [`Event::Relieve`] and keeps reading, so the
/// replacement handshakes while this socket still delivers. It stops when
/// retired, when the venue closes it, or when it falls silent.
async fn run_session<S>(
    slot: Slot,
    mut conn: Connection<S>,
    id: SessionId,
    mut retire: oneshot::Receiver<()>,
) -> SessionEnd
where
    S: AsyncRead + Unpin,
{
    let connection = slot.connection;
    let sender = conn.sender();
    let mut overflow = Overflow::new(slot.source.clone(), slot.quality.clone());
    let opened = Instant::now();
    let mut liveness = None;
    let mut announced_live = false;
    let mut relieved = false;

    loop {
        // Both arms end the session, which is what makes racing the
        // non-cancel-safe `read` sound.
        let message = select! {
            biased;
            _ = &mut retire => {
                let drained = drain_buffered(
                    &mut conn,
                    &slot.ws_tx,
                    &mut overflow,
                    Instant::now() + DRAIN_GRACE,
                )
                .await;
                // Hand the slot back rather than dropping the socket: a
                // handover holds two of this IP's limited connections.
                let _ = timeout(RETIRE_GRACE, async {
                    if sender.close(CLOSE_GOING_AWAY, "replaced").await.is_ok() {
                        conn.flush_close().await;
                    }
                })
                .await;
                info!(
                    connection,
                    session = id,
                    lifetime = ?opened.elapsed(),
                    recovered = drained.delivered,
                    shed = drained.shed,
                    "the replacement is carrying the feed; retiring this session"
                );
                return SessionEnd::Retired;
            }
            result = timeout(IDLE_TIMEOUT, conn.read()) => match result {
                Ok(Ok(message)) => message,
                Ok(Err(error)) => return SessionEnd::Lost(Loss::Io(error)),
                Err(_) => return SessionEnd::Lost(Loss::Idle),
            },
        };

        // Checked between reads, not raced in the `select!`, which would
        // cancel `read` mid-frame.
        if !relieved && opened.elapsed() >= slot.max_age {
            relieved = true;
            slot.request_replacement(id, Reason::Age);
        }

        match message.opcode {
            OpCode::Binary => {
                match deliver_frame(&slot.ws_tx, &mut overflow, message.payload).await {
                    Delivery::Sent => {
                        if liveness.is_none() {
                            liveness = Some(slot.readiness.source_live(slot.source.as_str()));
                        }
                    }
                    // The socket is alive but this process is not carrying
                    // the feed: revoke handover readiness until delivery
                    // succeeds again.
                    Delivery::Dropped => liveness = None,
                    Delivery::Closed => return SessionEnd::Finished,
                    Delivery::Undeliverable => return SessionEnd::Lost(Loss::Undeliverable),
                }
                if !announced_live {
                    announced_live = true;
                    let _ = slot.events.try_send(Event::Live(id));
                }
            }
            OpCode::Text => {
                let text = String::from_utf8_lossy(&message.payload);
                if is_server_shutdown(&message.payload) {
                    warn!(connection, %text, "the venue announced a server shutdown");
                    if !relieved {
                        relieved = true;
                        slot.request_replacement(id, Reason::ServerShutdown);
                    }
                } else {
                    // No routine text traffic exists on this endpoint, so any
                    // other text frame is worth seeing, not least a shutdown
                    // announcement in a form this does not recognise.
                    warn!(connection, "unexpected WS text frame: {}", text);
                }
            }
            OpCode::Ping => {
                if let Err(error) = sender.pong(message.payload.to_vec()).await {
                    return SessionEnd::Lost(Loss::Io(error));
                }
            }
            OpCode::Close => {
                let (code, reason) = crate::ws::close_reason(&message.payload);
                warn!(connection, lifetime = ?opened.elapsed(), ?code, %reason, "WS closed by server");
                conn.flush_close().await;
                return SessionEnd::Lost(Loss::ClosedByServer);
            }
            _ => {}
        }
    }
}

/// Truncated exponential backoff with ±25 % jitter, so instances that lost
/// the network together do not reconnect together.
fn jittered_backoff(error_count: u32) -> Duration {
    // 100 ms doubling, capped at 10 s.
    let base_ms: u64 = (100u64 * 2u64.pow(error_count.min(7))).min(10_000);
    let jitter_range = (base_ms / 4).max(1);
    // Subsecond wall-clock nanos as a cheap pseudo-random seed.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let jitter = nanos % (jitter_range * 2 + 1);
    Duration::from_millis(base_ms - jitter_range + jitter)
}

/// Pace the next connection attempt.
///
/// `lifetime` is how long the session that just ended lasted; a failed
/// handshake has none. Past [`SETTLED`] the count starts over; anything
/// shorter escalates, so a rolling restart or a refused handshake cannot spend
/// this IP's connection-attempt budget in a burst.
async fn back_off(error_count: &mut u32, lifetime: Option<Duration>) {
    if lifetime.is_some_and(|lifetime| lifetime > SETTLED) {
        *error_count = 0;
    } else {
        *error_count += 1;
    }
    tokio::time::sleep(jittered_backoff(*error_count)).await;
}

/// Keep one connection slot filled, replacing sessions before they are cut.
///
/// A session that announces its end, or reaches the age limit, keeps
/// delivering while its replacement handshakes, and is retired only once the
/// replacement has data flowing. The overlap's duplicates are what
/// [`crate::dedup`] is for.
pub(crate) async fn keep_connection(
    endpoint: Arc<Endpoint>,
    connection: usize,
    ws_tx: mpsc::Sender<(Timestamp, Bytes)>,
    quality: QualityReporter,
    readiness: Readiness,
) {
    info!(connection, url = %endpoint.url, "connecting to the SBE stream");

    // Relieved sessions still delivering. A `JoinSet` so that aborting this
    // supervisor at shutdown also drops their `ws_tx` clones, which would
    // otherwise hold the feed open until the drain timed out.
    let mut lingering: JoinSet<()> = JoinSet::new();
    let mut handover = Handover::default();
    let mut error_count: u32 = 0;
    // `slot` holds `event_tx` for as long as this runs, so `recv` never
    // yields `None`.
    let (event_tx, mut event_rx) = mpsc::channel(EVENT_QUEUE_CAPACITY);
    let slot = Slot {
        connection,
        source: format!("binance-sbe-spot/connection-{connection}"),
        ws_tx,
        events: event_tx,
        max_age: max_session_age(connection),
        quality,
        readiness,
    };
    let mut next_session: SessionId = 0;

    loop {
        while lingering.try_join_next().is_some() {}

        let opened = Instant::now();
        let conn =
            match crate::ws::connect(&endpoint.url, endpoint.headers(), Arc::clone(&endpoint.tls))
                .await
            {
                Ok(conn) => conn,
                Err(err) => {
                    error!(
                        connection,
                        ?err,
                        attempt = error_count + 1,
                        still_delivering = handover.waiting(),
                        "WS handshake failed"
                    );
                    back_off(&mut error_count, None).await;
                    continue;
                }
            };

        let id = next_session;
        next_session += 1;
        let (retire_tx, retire_rx) = oneshot::channel();
        let mut session = Box::pin(run_session(slot.clone(), conn, id, retire_rx));

        let step = loop {
            select! {
                biased;
                event = event_rx.recv() => match event {
                    // Proof that a session carries the feed, and the only
                    // thing that retires older ones. It can come from a
                    // relieved session, hence the shared channel and the id.
                    Some(Event::Live(live)) => handover.on_live(live),
                    Some(Event::Relieve(from, reason)) if from == id => {
                        break Step::Relieved(reason);
                    }
                    // A session asks once, so this is a relieved one repeating.
                    Some(Event::Relieve(from, _)) => {
                        debug!(connection, session = from, "ignoring a stale relieve");
                    }
                    None => break Step::Ended(SessionEnd::Finished),
                },
                end = &mut session => break Step::Ended(end),
            }
        };

        match step {
            Step::Relieved(reason) => {
                let lifetime = opened.elapsed();
                // Not retiring anything here: asking to be replaced says
                // nothing about whether the replacement will work.
                handover.on_relieved(id, retire_tx);
                warn!(
                    connection,
                    reason = reason.as_str(),
                    ?lifetime,
                    still_delivering = handover.waiting(),
                    "replacing this session; it keeps delivering until the new one is live"
                );
                lingering.spawn(async move {
                    let end = session.await;
                    info!(connection, ?end, "the relieved session ended");
                });
                // With no promised lead time (withdrawn 2026-06-09) the
                // replacement opens at once. A short-lived session still pays
                // the backoff: a venue rolling its pool can announce shutdowns
                // seconds after each handshake.
                back_off(&mut error_count, Some(lifetime)).await;
            }
            // Dropping `lingering` stops the relieved sessions too.
            Step::Ended(SessionEnd::Finished) => return,
            Step::Ended(SessionEnd::Retired) => {
                // Unreachable: the active session's handle is not in
                // `Handover` until it is relieved. Reconnect, with backoff so
                // it cannot spin.
                warn!(
                    connection,
                    session = id,
                    "the active session was retired; this should not happen"
                );
                back_off(&mut error_count, None).await;
            }
            Step::Ended(SessionEnd::Lost(loss)) => {
                // The lifetime separates the venue recycling a healthy
                // connection from this side failing.
                let lifetime = opened.elapsed();
                error!(
                    connection,
                    %loss,
                    ?lifetime,
                    still_delivering = handover.waiting(),
                    "WS connection lost"
                );
                back_off(&mut error_count, Some(lifetime)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastwebsockets::{Frame, Payload, WebSocket};
    use tokio::io::DuplexStream;

    /// Long enough that the age cap never fires during a test.
    const NEVER: Duration = Duration::from_secs(3_600);

    const TEST_SESSION: SessionId = 7;

    const ANNOUNCEMENT: &str = r#"{"e":"serverShutdown","E":1770123456789}"#;

    /// One session, its peer, and the channels the supervisor would watch.
    struct Harness {
        server: WebSocket<DuplexStream>,
        events: mpsc::Receiver<Event>,
        data: mpsc::Receiver<(Timestamp, Bytes)>,
        retire: Option<oneshot::Sender<()>>,
        session: tokio::task::JoinHandle<SessionEnd>,
    }

    impl Harness {
        fn start(max_age: Duration) -> Self {
            let (conn, server) = crate::ws::duplex_pair();
            let (event_tx, events) = mpsc::channel(2);
            let (ws_tx, data) = mpsc::channel(16);
            let (retire_tx, retire_rx) = oneshot::channel();
            let slot = Slot {
                connection: 0,
                source: "test".to_owned(),
                ws_tx,
                events: event_tx,
                max_age,
                quality: QualityReporter::disabled(),
                readiness: Readiness::new(1).0,
            };
            Self {
                server,
                events,
                data,
                retire: Some(retire_tx),
                session: tokio::spawn(run_session(slot, conn, TEST_SESSION, retire_rx)),
            }
        }

        async fn send_text(&mut self, text: &str) {
            self.server
                .write_frame(Frame::text(Payload::Owned(text.as_bytes().to_vec())))
                .await
                .unwrap();
        }

        /// Any binary frame is market data to the session.
        async fn send_market_data(&mut self) {
            self.server
                .write_frame(Frame::binary(Payload::Owned(vec![0x12, 0x00, 0x10, 0x27])))
                .await
                .unwrap();
        }
    }

    /// Acting on the announcement must not end the session: it keeps
    /// delivering while the replacement is brought up.
    #[tokio::test]
    async fn a_shutdown_announcement_asks_for_a_replacement_and_keeps_delivering() {
        let mut harness = Harness::start(NEVER);

        harness.send_market_data().await;
        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Live(TEST_SESSION))
        ));
        assert!(harness.data.recv().await.is_some());

        harness.send_text(ANNOUNCEMENT).await;
        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(TEST_SESSION, Reason::ServerShutdown))
        ));

        harness.send_market_data().await;
        assert!(
            harness.data.recv().await.is_some(),
            "the announced session must go on delivering"
        );
        assert!(!harness.session.is_finished());
    }

    #[tokio::test]
    async fn a_repeated_announcement_asks_only_once() {
        let mut harness = Harness::start(NEVER);

        harness.send_text(ANNOUNCEMENT).await;
        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(TEST_SESSION, Reason::ServerShutdown))
        ));
        harness.send_text(ANNOUNCEMENT).await;

        // Frames are processed in order: `Live` next means nothing was
        // requested in between.
        harness.send_market_data().await;
        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Live(TEST_SESSION))
        ));
    }

    #[tokio::test]
    async fn an_ordinary_text_frame_asks_for_nothing() {
        let mut harness = Harness::start(NEVER);

        harness.send_text(r#"{"result":null,"id":1}"#).await;
        harness.send_market_data().await;

        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Live(TEST_SESSION))
        ));
    }

    #[tokio::test]
    async fn a_session_past_its_age_cap_asks_for_a_replacement() {
        let mut harness = Harness::start(Duration::ZERO);

        harness.send_market_data().await;

        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(TEST_SESSION, Reason::Age))
        ));
        assert!(
            !harness.session.is_finished(),
            "the aged session keeps delivering until it is retired"
        );
    }

    /// A retired session hands its connection slot back with a close frame
    /// rather than dropping the socket.
    #[tokio::test]
    async fn a_retired_session_closes_the_connection() {
        let mut harness = Harness::start(NEVER);

        harness.retire.take().unwrap().send(()).unwrap();

        let closed = harness.server.read_frame().await.unwrap();
        assert_eq!(closed.opcode, OpCode::Close);
        assert_eq!(
            u16::from_be_bytes(closed.payload[..2].try_into().unwrap()),
            CLOSE_GOING_AWAY
        );
        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Retired
        ));
    }

    /// The supervisor stops reconnecting instead of reopening sockets nothing
    /// reads.
    #[tokio::test]
    async fn a_closed_consumer_finishes_the_session() {
        let mut harness = Harness::start(NEVER);

        harness.data.close();
        harness.send_market_data().await;

        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Finished
        ));
    }

    #[tokio::test]
    async fn a_closed_socket_is_lost_as_closed_by_the_server() {
        let mut harness = Harness::start(NEVER);

        harness
            .server
            .write_frame(Frame::close(1001, b"going away"))
            .await
            .unwrap();

        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Lost(Loss::ClosedByServer)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_socket_is_lost_as_idle() {
        let harness = Harness::start(NEVER);

        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Lost(Loss::Idle)
        ));
    }

    #[tokio::test]
    async fn retiring_delivers_the_backlog_that_already_arrived() {
        let (mut conn, mut server) = crate::ws::duplex_pair();
        let (ws_tx, data) = mpsc::channel(16);
        let mut overflow = Overflow::new("test", QualityReporter::disabled());

        for _ in 0..3 {
            server
                .write_frame(Frame::binary(Payload::Owned(vec![1, 2, 3, 4])))
                .await
                .unwrap();
        }

        let drained = drain_buffered(
            &mut conn,
            &ws_tx,
            &mut overflow,
            Instant::now() + DRAIN_GRACE,
        )
        .await;

        assert_eq!(drained.delivered, 3, "the backlog must not be discarded");
        assert_eq!(drained.shed, 0);
        assert_eq!(data.len(), 3);
    }

    /// No latency and no overlap beyond what was already in flight.
    #[tokio::test(start_paused = true)]
    async fn draining_a_caught_up_socket_returns_at_once() {
        let (mut conn, _server) = crate::ws::duplex_pair();
        let (ws_tx, _data) = mpsc::channel(16);
        let mut overflow = Overflow::new("test", QualityReporter::disabled());

        let before = tokio::time::Instant::now();
        let drained = drain_buffered(
            &mut conn,
            &ws_tx,
            &mut overflow,
            Instant::now() + DRAIN_GRACE,
        )
        .await;
        assert_eq!(drained.delivered, 0);
        assert_eq!(
            tokio::time::Instant::now(),
            before,
            "draining must not wait for frames that have not arrived"
        );
    }

    #[test]
    fn a_relieve_never_retires_the_session_still_carrying_the_feed() {
        let mut handover = Handover::default();
        let (live_session, mut live_rx) = oneshot::channel();
        let (unproven_session, _unproven_rx) = oneshot::channel();

        // The delivering session is relieved and keeps going; its
        // replacement is relieved too, before ever going live.
        handover.on_relieved(0, live_session);
        handover.on_relieved(1, unproven_session);

        assert_eq!(handover.waiting(), 2);
        assert!(
            live_rx.try_recv().is_err(),
            "the delivering session must still be running"
        );
    }

    #[test]
    fn a_relieved_session_that_goes_live_retires_its_elders_but_not_itself() {
        let mut handover = Handover::default();
        let (elder, mut elder_rx) = oneshot::channel();
        let (relieved_but_delivering, mut own_rx) = oneshot::channel();

        handover.on_relieved(0, elder);
        handover.on_relieved(1, relieved_but_delivering);

        handover.on_live(1);

        assert!(elder_rx.try_recv().is_ok(), "the elder is now redundant");
        assert!(
            own_rx.try_recv().is_err(),
            "a session must not retire itself by proving it works"
        );
        assert_eq!(handover.waiting(), 1);
    }

    #[test]
    fn going_live_retires_every_older_session() {
        let mut handover = Handover::default();
        let (first, mut first_rx) = oneshot::channel();
        let (second, mut second_rx) = oneshot::channel();

        handover.on_relieved(0, first);
        handover.on_relieved(1, second);
        handover.on_live(2);

        assert!(first_rx.try_recv().is_ok());
        assert!(second_rx.try_recv().is_ok());
        assert_eq!(handover.waiting(), 0);
    }

    #[test]
    fn sessions_that_already_ended_are_forgotten() {
        let mut handover = Handover::default();
        let (ended, ended_rx) = oneshot::channel();
        handover.on_relieved(0, ended);
        drop(ended_rx); // the session ran to its end on its own

        let (current, _current_rx) = oneshot::channel();
        handover.on_relieved(1, current);

        assert_eq!(handover.waiting(), 1);
    }

    #[test]
    fn redundant_connections_hand_over_at_different_times() {
        assert_eq!(max_session_age(0), MAX_SESSION_AGE);
        assert_eq!(max_session_age(1), MAX_SESSION_AGE - SESSION_AGE_STAGGER);
        // Even the last of the eight the CLI allows stays well inside 24 h.
        assert!(max_session_age(7) > Duration::from_secs(20 * 60 * 60));
    }

    #[test]
    fn the_endpoint_url_lists_every_stream_of_every_symbol() {
        let endpoint = Endpoint::new(
            &["$symbol@trade", "$symbol@depth"],
            &["BTCUSDT".to_owned(), "ethusdt".to_owned()],
            "key",
        )
        .unwrap();
        assert_eq!(
            endpoint.url,
            "wss://stream-sbe.binance.com:9443/stream?streams=\
             btcusdt@trade/btcusdt@depth/ethusdt@trade/ethusdt@depth"
        );
    }

    #[test]
    fn the_announcement_is_recognised() {
        assert!(is_server_shutdown(ANNOUNCEMENT.as_bytes()));
        assert!(is_server_shutdown(
            br#"{"stream":"btcusdt@depth","data":{"e":"serverShutdown","E":1}}"#
        ));
    }

    /// Only the event type counts, not the word anywhere in a message.
    #[test]
    fn only_the_event_type_counts() {
        assert!(!is_server_shutdown(
            br#"{"code":-1130,"msg":"serverShutdown is not a valid stream"}"#
        ));
        assert!(!is_server_shutdown(br#"{"e":"trade","E":1}"#));
        assert!(!is_server_shutdown(b"serverShutdown"));
        assert!(!is_server_shutdown(b"not json at all"));
    }
}
