//! The connection-slot supervisor every venue shares.
//!
//! Each of `-c N` slots keeps one socket open, replacing it make-before-break
//! when it asks to be relieved and reconnecting with backoff when it is lost.
//! Every binary frame goes to one queue for the venue's ingest; text frames
//! are the venue's protocol and stay with the [`Session`] that owns them.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use fastwebsockets::OpCode;
use hyper::header::HeaderMap;
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
    feed::BackgroundResult,
    quality::QualityReporter,
    readiness::Readiness,
    ws::{ConnectError, Connection, Delivery, FrameSender, Overflow},
};

/// A binary frame and when it arrived.
pub(crate) type Frame = (Timestamp, Bytes);

/// Per connection: all of them share the queue, so redundancy does not shrink
/// the burst each one can absorb.
const WS_QUEUE_CAPACITY: usize = 16_384;

/// Redundant connections are opened this far apart. Simultaneous handshakes
/// are what a venue's connection rate limit notices, and connections opened
/// together are the ones recycled together.
const CONNECT_STAGGER: Duration = Duration::from_secs(5);

/// Every venue here either pings every ~20 s or is kept talking by a
/// keepalive, and active symbols tick in milliseconds, so 60 s of silence
/// means a dead socket.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// A session that lived this long was healthy: whatever ended it was the
/// venue's doing, and the backoff starts over.
const SETTLED: Duration = Duration::from_secs(30);

/// How long a session may wait for the answer to every subscription. Replies
/// come within milliseconds; one that never comes would otherwise leave the
/// slot un-ready, and the channel missing, with nothing to say so.
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(10);

/// The least spacing between any two connection attempts of the process.
///
/// OKX allows 3 a second per IP, and a venue-wide notice relieves every slot
/// at once, each reconnecting after a settled session's 100 ms backoff.
const CONNECT_SPACING: Duration = Duration::from_millis(400);

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

/// Where one connection attempt goes and what its upgrade request carries.
pub(crate) struct Handshake<'a> {
    pub(crate) url: &'a str,
    pub(crate) headers: HeaderMap,
}

/// What a venue adds to the shared slot supervisor: one value per process,
/// shared by every slot.
pub(crate) trait Protocol: Send + Sync + 'static {
    /// One socket's state in the venue's text protocol.
    type Session: Session;

    /// Names the venue in logs, quality events and readiness sources.
    const SOURCE: &'static str;

    /// The URL and upgrade headers for one connection attempt; built per
    /// attempt, since a venue may sign a timestamp.
    fn handshake(&self) -> anyhow::Result<Handshake<'_>>;

    /// When a session on `connection` arranges its own replacement, if ever.
    fn max_age(&self, connection: usize) -> Option<Duration>;

    /// How often to ping a venue that never pings first.
    fn keepalive(&self) -> Option<Duration> {
        None
    }

    /// A fresh session, and the text requests it sends once connected.
    fn open(&self) -> (Self::Session, Vec<String>);
}

/// One socket's state in the venue's text protocol.
pub(crate) trait Session: Send + 'static {
    /// A text frame from the venue: a subscription reply or a notice.
    fn on_text(&mut self, text: &[u8]) -> Verdict;

    /// A binary frame, before it is queued. Delivered whatever this returns.
    fn on_binary(&mut self, _frame: &[u8]) -> Verdict {
        Verdict::Continue
    }

    /// Whether every subscription has been answered. Until then the session
    /// cannot prove it carries the whole feed, so it neither retires an older
    /// session nor counts towards handover readiness.
    fn settled(&self) -> bool {
        true
    }
}

/// What a session asks of its slot after a frame.
pub(crate) enum Verdict {
    Continue,
    /// Open a replacement; this session keeps delivering meanwhile.
    Relieve(Reason),
    /// Drop this socket and reconnect.
    End(Loss),
}

/// Why a session asked for its replacement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Reason {
    /// The venue announced it will close this connection.
    ServerShutdown,
    Age,
    /// The venue reported an error on a session already carrying the feed;
    /// a fresh one resubscribes everything.
    VenueError,
}

impl Reason {
    fn as_str(self) -> &'static str {
        match self {
            Reason::ServerShutdown => "server shutdown announced",
            Reason::Age => "connection age limit",
            Reason::VenueError => "the venue reported an error",
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
pub(crate) enum Loss {
    #[error("no frame for {IDLE_TIMEOUT:?}")]
    Idle,
    #[error("closed by the server")]
    ClosedByServer,
    #[error("a frame that must not be shed could not be queued")]
    Undeliverable,
    /// The venue rejected every subscription: nothing will ever flow.
    #[error("every subscription was refused: {0}")]
    Refused(String),
    /// The venue rejected a request in a way a reconnect may cure.
    #[error("the venue rejected a request: {0}")]
    Rejected(String),
    #[error("not every subscription was answered within {SUBSCRIBE_TIMEOUT:?}")]
    Unanswered,
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

/// What the drain recovered from a socket being retired.
#[derive(Default, Debug)]
struct Drained {
    delivered: usize,
    /// Shed under backpressure, so a lossy handover is not reported lossless.
    shed: usize,
}

/// The one way a market-data frame is stamped and queued, shared by the read
/// loop and the retire drain. Text frames are the venue's protocol and are
/// handled by the session, so every binary frame is sheddable data.
async fn deliver_frame(
    ws_tx: &mpsc::Sender<Frame>,
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
    ws_tx: &mpsc::Sender<Frame>,
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

/// What every session of one connection slot shares.
#[derive(Clone)]
struct Slot {
    connection: usize,
    /// Names the slot in quality events and readiness.
    source: String,
    ws_tx: mpsc::Sender<Frame>,
    /// One channel for every session of the slot, so a session relieved
    /// before it went live can still report going live.
    events: mpsc::Sender<Event>,
    max_age: Option<Duration>,
    keepalive: Option<Duration>,
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

/// Ping every `every`, for a venue that never pings first, so its pongs keep
/// [`IDLE_TIMEOUT`] meaningful on a quiet feed. Never completes unless the
/// writer stopped, so racing it against `read` never cancels a read.
async fn keep_alive(sender: FrameSender, every: Option<Duration>) -> Loss {
    let Some(every) = every else {
        return std::future::pending().await;
    };
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    loop {
        ticker.tick().await;
        if let Err(error) = sender.ping(Vec::new()).await {
            return Loss::Io(error);
        }
    }
}

/// Read one session to its end, delivering market data into the slot's queue.
///
/// Deciding to replace a session does not end it: when the session asks, or
/// at `max_age`, it reports [`Event::Relieve`] and keeps reading, so the
/// replacement handshakes while this socket still delivers. It stops when
/// retired, when the venue closes it, when it falls silent, or when the
/// session gives up on it.
async fn run_session<T, S>(
    slot: Slot,
    mut conn: Connection<S>,
    id: SessionId,
    mut retire: oneshot::Receiver<()>,
    mut session: T,
    requests: Vec<String>,
) -> SessionEnd
where
    T: Session,
    S: AsyncRead + Unpin,
{
    let connection = slot.connection;
    let sender = conn.sender();
    for request in requests {
        if let Err(error) = sender.text(request).await {
            return SessionEnd::Lost(Loss::Io(error));
        }
    }
    let mut overflow = Overflow::new(slot.source.clone(), slot.quality.clone());
    let opened = tokio::time::Instant::now();
    let mut liveness = None;
    let mut announced_live = false;
    let mut relieved = false;
    let keepalive = keep_alive(sender.clone(), slot.keepalive);
    tokio::pin!(keepalive);

    loop {
        // Every arm that completes ends the session, which is what makes
        // racing the non-cancel-safe `read` sound.
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
            loss = &mut keepalive => return SessionEnd::Lost(loss),
            result = timeout(IDLE_TIMEOUT, conn.read()) => match result {
                Ok(Ok(message)) => message,
                Ok(Err(error)) => return SessionEnd::Lost(Loss::Io(error)),
                Err(_) => return SessionEnd::Lost(Loss::Idle),
            },
        };

        // Checked between reads, not raced in the `select!`, which would
        // cancel `read` mid-frame.
        if !relieved
            && slot
                .max_age
                .is_some_and(|max_age| opened.elapsed() >= max_age)
        {
            relieved = true;
            slot.request_replacement(id, Reason::Age);
        }
        if !session.settled() && opened.elapsed() >= SUBSCRIBE_TIMEOUT {
            return SessionEnd::Lost(Loss::Unanswered);
        }

        let verdict = match message.opcode {
            OpCode::Binary => {
                let verdict = session.on_binary(&message.payload);
                // Data before the last reply proves nothing about the
                // channels still unanswered.
                let settled = session.settled();
                match deliver_frame(&slot.ws_tx, &mut overflow, message.payload).await {
                    Delivery::Sent => {
                        if settled && liveness.is_none() {
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
                if settled && !announced_live {
                    announced_live = true;
                    let _ = slot.events.try_send(Event::Live(id));
                }
                verdict
            }
            OpCode::Text => session.on_text(&message.payload),
            OpCode::Ping => {
                if let Err(error) = sender.pong(message.payload.to_vec()).await {
                    return SessionEnd::Lost(Loss::Io(error));
                }
                Verdict::Continue
            }
            OpCode::Close => {
                let (code, reason) = crate::ws::close_reason(&message.payload);
                warn!(connection, lifetime = ?opened.elapsed(), ?code, %reason, "WS closed by server");
                conn.flush_close().await;
                return SessionEnd::Lost(Loss::ClosedByServer);
            }
            _ => Verdict::Continue,
        };

        match verdict {
            Verdict::Continue => {}
            Verdict::Relieve(reason) => {
                if !relieved {
                    relieved = true;
                    slot.request_replacement(id, reason);
                }
            }
            Verdict::End(loss) => return SessionEnd::Lost(loss),
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

/// Spaces the connection attempts of every slot in the process by
/// [`CONNECT_SPACING`].
#[derive(Default)]
struct Pacer {
    next: tokio::sync::Mutex<Option<tokio::time::Instant>>,
}

impl Pacer {
    /// Wait for this attempt's turn. Holding the lock through the sleep is
    /// what queues the slots behind one another.
    async fn wait(&self) {
        let mut next = self.next.lock().await;
        if let Some(at) = *next {
            tokio::time::sleep_until(at).await;
        }
        *next = Some(tokio::time::Instant::now() + CONNECT_SPACING);
    }
}

/// A refusal no retry can cure ends the slot, and with it the collection, but
/// only while the slot has never carried the feed: credentials or symbols
/// wrong from the start. A slot that was delivering keeps retrying rather than
/// take the process down mid-recording.
fn give_up(ever_live: bool, refusal: impl std::fmt::Display) -> Option<anyhow::Error> {
    (!ever_live).then(|| anyhow::anyhow!("{refusal}"))
}

/// Open one socket for the slot: its handshake, then the upgrade.
async fn connect<P: Protocol>(
    protocol: &P,
    tls: &Arc<ClientConfig>,
    connection: usize,
    first_attempt: &mut bool,
) -> Result<Connection, ConnectError> {
    let handshake = protocol.handshake()?;
    if std::mem::take(first_attempt) {
        info!(connection, url = %handshake.url, "connecting to {}", P::SOURCE);
    }
    crate::ws::connect(handshake.url, handshake.headers, Arc::clone(tls)).await
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
/// A session that asks to be relieved, or reaches the age limit, keeps
/// delivering while its replacement handshakes, and is retired only once the
/// replacement has data flowing. The overlap's duplicates are what
/// [`crate::dedup`] is for.
///
/// Returns once the consumer is gone, or with the refusal that ended a slot
/// which never went live.
async fn keep_connection<P: Protocol>(
    protocol: Arc<P>,
    tls: Arc<ClientConfig>,
    pacer: Arc<Pacer>,
    connection: usize,
    ws_tx: mpsc::Sender<Frame>,
    quality: QualityReporter,
    readiness: Readiness,
) -> anyhow::Result<()> {
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
        source: format!("{}/connection-{connection}", P::SOURCE),
        ws_tx,
        events: event_tx,
        max_age: protocol.max_age(connection),
        keepalive: protocol.keepalive(),
        quality,
        readiness,
    };
    let mut next_session: SessionId = 0;
    let mut first_attempt = true;
    let mut ever_live = false;

    loop {
        while lingering.try_join_next().is_some() {}

        pacer.wait().await;
        let opened = Instant::now();
        let conn = match connect(&*protocol, &tls, connection, &mut first_attempt).await {
            Ok(conn) => conn,
            Err(ConnectError::Unauthorized)
                if let Some(error) = give_up(
                    ever_live,
                    format!("{}: {}", P::SOURCE, ConnectError::Unauthorized),
                ) =>
            {
                return Err(error);
            }
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
        let (session, requests) = protocol.open();
        let mut session = Box::pin(run_session(
            slot.clone(),
            conn,
            id,
            retire_rx,
            session,
            requests,
        ));

        let step = loop {
            select! {
                biased;
                event = event_rx.recv() => match event {
                    // Proof that a session carries the feed, and the only
                    // thing that retires older ones. It can come from a
                    // relieved session, hence the shared channel and the id.
                    Some(Event::Live(live)) => {
                        ever_live = true;
                        handover.on_live(live);
                    }
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
                // With no promised lead time the replacement opens at once. A
                // short-lived session still pays the backoff: a venue rolling
                // its pool can announce shutdowns seconds after each handshake.
                back_off(&mut error_count, Some(lifetime)).await;
            }
            // Dropping `lingering` stops the relieved sessions too.
            Step::Ended(SessionEnd::Finished) => return Ok(()),
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
                if let Loss::Refused(_) = &loss
                    && let Some(error) = give_up(ever_live, format!("{}: {loss}", P::SOURCE))
                {
                    return Err(error);
                }
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

/// Start one supervisor per connection slot, [`CONNECT_STAGGER`] apart, and
/// return the queue they all deliver into.
///
/// Only the supervisors hold senders, so the queue closes once they are gone
/// and the consumer can drain it to the end.
pub(crate) fn spawn_slots<P: Protocol>(
    tasks: &mut JoinSet<BackgroundResult>,
    protocol: Arc<P>,
    connections: usize,
    quality: &QualityReporter,
    readiness: &Readiness,
) -> mpsc::Receiver<Frame> {
    let (ws_tx, ws_rx) = mpsc::channel(WS_QUEUE_CAPACITY.saturating_mul(connections));
    let tls = Arc::new(crate::ws::build_tls_config());
    let pacer = Arc::new(Pacer::default());
    for connection in 0..connections {
        let protocol = Arc::clone(&protocol);
        let tls = Arc::clone(&tls);
        let pacer = Arc::clone(&pacer);
        let ws_tx = ws_tx.clone();
        let quality = quality.clone();
        let readiness = readiness.clone();
        tasks.spawn(async move {
            tokio::time::sleep(CONNECT_STAGGER * connection as u32).await;
            keep_connection(protocol, tls, pacer, connection, ws_tx, quality, readiness).await?;
            Err(anyhow::anyhow!(
                "websocket connection supervisor {connection} exited"
            ))
        });
    }
    ws_rx
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::readiness::ReadinessState;
    use fastwebsockets::{Frame as WsFrame, Payload, WebSocket};
    use tokio::{io::DuplexStream, sync::watch};

    const TEST_SESSION: SessionId = 7;

    /// Asks for a replacement on the text `relieve` and gives up on `refuse`;
    /// each `ack` answers one of `pending` subscriptions.
    #[derive(Default)]
    struct Script {
        pending: usize,
    }

    impl Session for Script {
        fn on_text(&mut self, text: &[u8]) -> Verdict {
            match text {
                b"relieve" => Verdict::Relieve(Reason::ServerShutdown),
                b"refuse" => Verdict::End(Loss::Refused("all of them".to_owned())),
                b"ack" => {
                    self.pending = self.pending.saturating_sub(1);
                    Verdict::Continue
                }
                _ => Verdict::Continue,
            }
        }

        fn settled(&self) -> bool {
            self.pending == 0
        }
    }

    /// One session, its peer, and the channels the supervisor would watch.
    struct Harness {
        server: WebSocket<DuplexStream>,
        events: mpsc::Receiver<Event>,
        data: mpsc::Receiver<Frame>,
        /// The writer is marked ready, so this follows the session's lease.
        ready: watch::Receiver<ReadinessState>,
        retire: Option<oneshot::Sender<()>>,
        session: tokio::task::JoinHandle<SessionEnd>,
    }

    impl Harness {
        fn start(max_age: Option<Duration>) -> Self {
            Self::with(Script::default(), Vec::new(), max_age, None)
        }

        fn with(
            script: Script,
            requests: Vec<String>,
            max_age: Option<Duration>,
            keepalive: Option<Duration>,
        ) -> Self {
            let (conn, server) = crate::ws::duplex_pair();
            let (event_tx, events) = mpsc::channel(2);
            let (ws_tx, data) = mpsc::channel(16);
            let (retire_tx, retire_rx) = oneshot::channel();
            let (readiness, ready) = Readiness::new(1);
            readiness.mark_writer_ready();
            let slot = Slot {
                connection: 0,
                source: "test".to_owned(),
                ws_tx,
                events: event_tx,
                max_age,
                keepalive,
                quality: QualityReporter::disabled(),
                readiness,
            };
            Self {
                server,
                events,
                data,
                ready,
                retire: Some(retire_tx),
                session: tokio::spawn(run_session(
                    slot,
                    conn,
                    TEST_SESSION,
                    retire_rx,
                    script,
                    requests,
                )),
            }
        }

        async fn expect_text(&mut self, expected: &str) {
            let frame = self.server.read_frame().await.unwrap();
            assert_eq!(frame.opcode, OpCode::Text);
            assert_eq!(&frame.payload[..], expected.as_bytes());
        }

        async fn send_text(&mut self, text: &str) {
            self.server
                .write_frame(WsFrame::text(Payload::Owned(text.as_bytes().to_vec())))
                .await
                .unwrap();
        }

        /// Any binary frame is market data to the session.
        async fn send_market_data(&mut self) {
            self.server
                .write_frame(WsFrame::binary(Payload::Owned(vec![
                    0x12, 0x00, 0x10, 0x27,
                ])))
                .await
                .unwrap();
        }
    }

    /// Acting on the announcement must not end the session: it keeps
    /// delivering while the replacement is brought up.
    #[tokio::test]
    async fn a_shutdown_announcement_asks_for_a_replacement_and_keeps_delivering() {
        let mut harness = Harness::start(None);

        harness.send_market_data().await;
        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Live(TEST_SESSION))
        ));
        assert!(harness.data.recv().await.is_some());

        harness.send_text("relieve").await;
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
        let mut harness = Harness::start(None);

        harness.send_text("relieve").await;
        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(TEST_SESSION, Reason::ServerShutdown))
        ));
        harness.send_text("relieve").await;

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
        let mut harness = Harness::start(None);

        harness.send_text(r#"{"result":null,"id":1}"#).await;
        harness.send_market_data().await;

        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Live(TEST_SESSION))
        ));
    }

    #[tokio::test]
    async fn a_session_past_its_age_cap_asks_for_a_replacement() {
        let mut harness = Harness::start(Some(Duration::ZERO));

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
        let mut harness = Harness::start(None);

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
        let mut harness = Harness::start(None);

        harness.data.close();
        harness.send_market_data().await;

        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Finished
        ));
    }

    #[tokio::test]
    async fn a_closed_socket_is_lost_as_closed_by_the_server() {
        let mut harness = Harness::start(None);

        harness
            .server
            .write_frame(WsFrame::close(1001, b"going away"))
            .await
            .unwrap();

        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Lost(Loss::ClosedByServer)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_socket_is_lost_as_idle() {
        let harness = Harness::start(None);

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
                .write_frame(WsFrame::binary(Payload::Owned(vec![1, 2, 3, 4])))
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

    #[tokio::test]
    async fn requests_go_out_in_order_once_connected() {
        let mut harness = Harness::with(
            Script { pending: 2 },
            vec!["first".to_owned(), "second".to_owned()],
            None,
            None,
        );

        harness.expect_text("first").await;
        harness.expect_text("second").await;
    }

    /// Data that arrives before the last reply is recorded, but proves nothing
    /// about the channels still unanswered: it must neither retire an older
    /// session nor make the process ready for a handover.
    #[tokio::test]
    async fn a_session_is_not_live_until_every_subscription_is_answered() {
        let mut harness = Harness::with(Script { pending: 1 }, vec!["sub".to_owned()], None, None);
        harness.expect_text("sub").await;

        harness.send_market_data().await;
        assert!(harness.data.recv().await.is_some());
        assert!(
            harness.events.try_recv().is_err(),
            "no Live before the reply"
        );
        assert!(!harness.ready.borrow().is_ready());

        harness.send_text("ack").await;
        harness.send_market_data().await;
        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Live(TEST_SESSION))
        ));
        assert!(harness.data.recv().await.is_some());
        assert!(harness.ready.borrow().is_ready());
    }

    #[tokio::test]
    async fn a_session_that_gives_up_is_lost() {
        let mut harness = Harness::start(None);

        harness.send_text("refuse").await;

        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Lost(Loss::Refused(_))
        ));
    }

    #[test]
    fn only_a_slot_that_never_carried_the_feed_gives_up() {
        let error = give_up(false, "okx-sbe: every subscription was refused").unwrap();
        assert_eq!(error.to_string(), "okx-sbe: every subscription was refused");
        assert!(give_up(true, "refused").is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_subscription_left_unanswered_ends_the_session() {
        let mut harness = Harness::with(Script { pending: 1 }, vec!["sub".to_owned()], None, None);
        harness.expect_text("sub").await;

        tokio::time::sleep(SUBSCRIBE_TIMEOUT).await;
        harness.send_market_data().await;

        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Lost(Loss::Unanswered)
        ));
    }

    /// The keepalive races the read without cancelling it: data on either
    /// side of a ping arrives intact.
    #[tokio::test(start_paused = true)]
    async fn keepalive_pings_go_out_while_data_keeps_flowing() {
        let every = Duration::from_secs(20);
        let mut harness = Harness::with(Script::default(), Vec::new(), None, Some(every));

        harness.send_market_data().await;
        assert!(harness.data.recv().await.is_some());

        let before = tokio::time::Instant::now();
        let ping = harness.server.read_frame().await.unwrap();
        assert_eq!(ping.opcode, OpCode::Ping);
        assert!(tokio::time::Instant::now() - before <= every);

        harness.send_market_data().await;
        let (_, payload) = harness.data.recv().await.unwrap();
        assert_eq!(&payload[..], &[0x12, 0x00, 0x10, 0x27]);
        assert!(!harness.session.is_finished());
    }

    /// Slots wait their turn: a venue-wide notice relieves every slot at once,
    /// and their reconnects must not burst past a per-IP attempt limit.
    #[tokio::test(start_paused = true)]
    async fn connection_attempts_are_spaced_across_slots() {
        let pacer = Arc::new(Pacer::default());
        let start = tokio::time::Instant::now();

        let attempts = three_attempts(Arc::clone(&pacer)).await;

        let mut attempts = attempts.to_vec();
        attempts.sort();
        assert_eq!(attempts[0], start);
        assert_eq!(attempts[1] - attempts[0], CONNECT_SPACING);
        assert_eq!(attempts[2] - attempts[1], CONNECT_SPACING);
    }

    /// Three slots asking for their turn at once; when each got it.
    async fn three_attempts(pacer: Arc<Pacer>) -> [tokio::time::Instant; 3] {
        let attempt = |pacer: Arc<Pacer>| async move {
            pacer.wait().await;
            tokio::time::Instant::now()
        };
        let (a, b, c) = tokio::join!(
            attempt(Arc::clone(&pacer)),
            attempt(Arc::clone(&pacer)),
            attempt(pacer)
        );
        [a, b, c]
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
}
