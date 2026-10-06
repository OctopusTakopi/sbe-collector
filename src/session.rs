//! The connection-slot supervisor every venue shares.
//!
//! Each of `-c N` slots keeps one socket open. A session that asks to be
//! relieved, or reaches its age limit, keeps delivering while its replacement
//! connects, and is retired only once the replacement carries the feed; a
//! lost session is reconnected with backoff. Every binary frame goes to one
//! queue for the venue's ingest; text frames are the venue's protocol and stay
//! with the [`Session`] that owns them. A venue plugs in a [`Protocol`]: where
//! to connect, what to send, and how to read its replies.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
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
    time::{MissedTickBehavior, timeout},
};
use tracing::{debug, error, info, warn};

use crate::{
    feed::BackgroundResult,
    quality::{QualityEvent, QualityReporter},
    readiness::{Readiness, Source, SourceLease},
    ws::{self, ConnectError, Connection, Delivery, FrameSender, Overflow},
};

/// Queue room per connection: the queue is shared, so redundancy does not
/// shrink the burst each connection can absorb.
const WS_QUEUE_CAPACITY: usize = 16_384;

/// Redundant connections are opened this far apart. Simultaneous handshakes
/// are what a venue's connection rate limit notices, and connections opened
/// together tend to be recycled together.
const CONNECT_STAGGER: Duration = Duration::from_secs(5);

/// A session that lived this long was healthy, so whatever ended it was the
/// venue's doing rather than a persistent failure.
const SETTLED: Duration = Duration::from_secs(30);

/// How long a session may wait for the answer to every subscription; one
/// that never comes would otherwise leave the slot unready, and a channel
/// missing, with nothing to say so. Answers come within milliseconds, but
/// queue behind the book snapshots that follow earlier subscribes: on
/// 2026-10-07 a cold start over one congested link kept answers back more
/// than 10 s.
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(30);

/// The least spacing between any two connection attempts of the process.
/// OKX allows 3 a second per IP, and a venue-wide notice relieves every slot
/// at once, each reconnecting after a settled session's 100 ms backoff.
const CONNECT_SPACING: Duration = Duration::from_millis(400);

/// RFC 6455 "going away".
const CLOSE_GOING_AWAY: u16 = 1001;

/// Each session sends at most a `Relieve` and a `Live`.
const EVENT_QUEUE_CAPACITY: usize = 16;

/// How long a retired session may spend delivering its backlog. Separate
/// from [`RETIRE_GRACE`]: one delivery can wait a whole queue-full grace, and
/// a shared budget would let a slow drain starve the close that hands the
/// connection slot back.
const DRAIN_GRACE: Duration = Duration::from_secs(2);

/// How long a retired session may spend closing politely before it is
/// dropped.
const RETIRE_GRACE: Duration = Duration::from_secs(2);

/// How long a retired session waits for the frame it is reading to arrive
/// whole, so the backlog behind it can be drained. One cut off mid-frame is
/// closed without another read: the rest of its bytes would parse as frames.
const RETIRE_QUIET: Duration = Duration::from_millis(250);

/// A session within one connection slot, numbered in opening order.
type SessionId = u64;

/// A market frame on the collection queue: when it was read, and its bytes.
#[derive(Debug)]
pub(crate) struct Frame {
    pub(crate) recv_time: Timestamp,
    pub(crate) data: Bytes,
}

#[cfg(test)]
impl Frame {
    /// `data` as read now.
    pub(crate) fn now(data: impl Into<Bytes>) -> Self {
        Self {
            recv_time: Timestamp::now(),
            data: data.into(),
        }
    }
}

/// Where one connection attempt goes and what its upgrade request carries.
pub(crate) struct Handshake<'a> {
    pub(crate) url: &'a str,
    pub(crate) headers: HeaderMap,
}

/// What a venue adds to the slot supervisor: one value per process, shared
/// by every slot.
pub(crate) trait Protocol: Send + Sync + 'static {
    /// One socket's state in the venue's protocol.
    type Session: Session;

    /// Names the venue in logs, quality events and readiness sources.
    const SOURCE: &'static str;

    /// The URL and upgrade headers for one connection attempt; built per
    /// attempt, since a venue may sign a timestamp.
    fn handshake(&self) -> anyhow::Result<Handshake<'_>>;

    /// Whether the upgrade carries credentials. A venue refusing it then
    /// refuses them, a wrong key or one not whitelisted for this IP, and no
    /// retry cures that; refused otherwise, the trouble is the venue's.
    fn credentialed(&self) -> bool {
        false
    }

    /// Longest silence tolerated on a socket. The venue's pings, or the pongs
    /// a keepalive earns, come more often than this on a live one.
    fn idle_timeout(&self) -> Duration;

    /// When a session on `connection` arranges its own replacement, if ever.
    fn max_age(&self, _connection: usize) -> Option<Duration> {
        None
    }

    /// A fresh session, and what it writes.
    fn open(&self) -> (Self::Session, Requests);
}

/// One socket's state in the venue's protocol: every text frame is the
/// venue's protocol, every binary frame market data.
pub(crate) trait Session: Send + 'static {
    /// A protocol frame: a subscription reply or a notice.
    fn on_control(&mut self, text: &[u8]) -> Verdict;

    /// A market frame, before it is queued. Queued whatever this returns.
    fn on_data(&mut self, _data: &[u8]) -> Verdict {
        Verdict::Continue
    }

    /// Whether every subscription has had its answer, an acceptance or a
    /// refusal; never false again once true. Until then the session proves
    /// nothing about its channels, so it neither retires an older session
    /// nor counts towards handover readiness, and it is given
    /// [`SUBSCRIBE_TIMEOUT`].
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
    /// This socket's own book stream skipped an update; a fresh subscription
    /// starts from a full snapshot.
    Gap,
}

impl Reason {
    fn as_str(self) -> &'static str {
        match self {
            Reason::ServerShutdown => "server shutdown announced",
            Reason::Age => "connection age limit",
            Reason::VenueError => "the venue reported an error",
            Reason::Gap => "gap in this connection's book stream",
        }
    }
}

/// Why a session's socket was lost.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Loss {
    #[error("no frame within {0:?}")]
    Idle(Duration),
    #[error("closed by the server")]
    ClosedByServer,
    #[error("the websocket writer stopped")]
    WriterStopped,
    /// The venue refused every subscription: nothing will ever flow.
    #[error("every subscription was refused: {0}")]
    Refused(String),
    /// The venue rejected a request in a way a reconnect may cure.
    #[error("the venue rejected a request: {0}")]
    Rejected(String),
    #[error("not every subscription was answered within {0:?}")]
    Unanswered(Duration),
    #[error("{0:#}")]
    Io(anyhow::Error),
}

/// What a session writes: its subscriptions once connected, and a ping
/// every `keepalive`, for a venue that never pings first, so the pongs it
/// earns keep the idle timeout meaningful on a quiet feed.
#[derive(Default)]
pub(crate) struct Requests {
    pub(crate) initial: Vec<String>,
    pub(crate) keepalive: Option<Duration>,
}

/// What a session reports to its slot's supervisor.
#[derive(Debug)]
enum Event {
    /// This session carries the feed: every older session is redundant.
    Live(SessionId),
    /// Open the replacement now; this session keeps delivering until it is
    /// retired or the venue closes it.
    Relieve(SessionId, Reason),
}

#[derive(Debug)]
enum SessionEnd {
    /// The consumer is gone; the collector is shutting down.
    Finished,
    /// Retired by the supervisor once a newer session went live.
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

    /// Holds a relieved session's handle until something proves it can go.
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

/// What every session of one connection slot shares.
#[derive(Clone)]
struct Slot {
    /// Names the slot in logs, quality events and readiness.
    source: Source,
    ws_tx: mpsc::Sender<Frame>,
    /// One channel for every session of the slot, so a session relieved
    /// before it went live can still report going live.
    events: mpsc::Sender<Event>,
    idle_timeout: Duration,
    max_age: Option<Duration>,
    /// Set once any slot's frame has reached the queue.
    delivered: Arc<AtomicBool>,
    quality: QualityReporter,
    readiness: Readiness,
}

impl Slot {
    fn request_replacement(&self, id: SessionId, reason: Reason) {
        warn!(
            endpoint = %self.source,
            session = id,
            reason = reason.as_str(),
            "asking for a replacement connection"
        );
        // The supervisor drains this promptly; a full queue means it has
        // already moved on.
        let _ = self.events.try_send(Event::Relieve(id, reason));
    }

    /// Stamps `data` as read now and queues it.
    async fn deliver(&self, overflow: &mut Overflow, data: Bytes) -> Delivery {
        let frame = Frame {
            recv_time: Timestamp::now(),
            data,
        };
        ws::deliver(&self.ws_tx, overflow, frame).await
    }
}

/// Delivers what already arrived on a socket about to be closed. Call only
/// on a frame boundary: after a whole frame, never after a cancelled `read`.
///
/// The replacement only sees events from its own subscription on, so this
/// backlog may exist nowhere else. The zero timeout takes what is buffered and
/// stops the instant the socket would block; cancelling `read` mid-frame there
/// is sound only because the connection is closed next. `deadline` is checked
/// rather than wrapped around the drain, so the counts survive a saturated
/// queue eating the budget.
async fn drain_buffered<S>(
    conn: &mut Connection<S>,
    slot: &Slot,
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
        match message.opcode {
            OpCode::Binary => {}
            OpCode::Close => break,
            _ => continue,
        }
        match slot.deliver(overflow, message.payload).await {
            Delivery::Sent => drained.delivered += 1,
            Delivery::Dropped => drained.shed += 1,
            Delivery::Closed => break,
        }
    }
    drained
}

/// Writes one session's [`Requests`]. Never completes unless the writer
/// stopped, so racing it against `read` never cancels a read.
async fn write_requests(sender: FrameSender, requests: Requests) -> Loss {
    for text in requests.initial {
        if sender.text(text).await.is_err() {
            return Loss::WriterStopped;
        }
    }
    let Some(every) = requests.keepalive else {
        return std::future::pending().await;
    };
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        if sender.ping().await.is_err() {
            return Loss::WriterStopped;
        }
    }
}

/// Reads one session to its end, delivering market data into the slot's
/// queue.
///
/// Deciding to replace a session does not end it: when the session asks, or
/// at `max_age`, it reports [`Event::Relieve`] and keeps reading, so the
/// replacement handshakes while this socket still delivers. It stops when
/// retired, when the venue closes it, when it falls silent, when its
/// subscriptions go unanswered, or when the session gives up on it.
async fn run_session<T, S>(
    slot: Slot,
    mut conn: Connection<S>,
    id: SessionId,
    mut retire: oneshot::Receiver<()>,
    mut session: T,
    requests: Requests,
) -> SessionEnd
where
    T: Session,
    S: AsyncRead + Unpin,
{
    let sender = conn.sender();
    let opened = tokio::time::Instant::now();
    let unanswered = tokio::time::sleep_until(opened + SUBSCRIBE_TIMEOUT);
    tokio::pin!(unanswered);
    let writer = write_requests(sender.clone(), requests);
    tokio::pin!(writer);
    let quiet = tokio::time::sleep(RETIRE_QUIET);
    tokio::pin!(quiet);
    let mut overflow = Overflow::new(slot.source, slot.quality.clone());
    let mut settled = session.settled();
    let mut lease: Option<SourceLease> = None;
    let mut announced_live = false;
    let mut delivered = false;
    let mut relieved = false;
    let mut retiring = false;

    // Once retired: whether the socket stopped on a frame boundary, so the
    // backlog it holds can still be read.
    let drain = loop {
        // `read` is not cancel-safe, so it lives across wake-ups. Only an arm
        // that ends the session may drop it mid-frame, and nothing reads the
        // socket after that.
        let message = {
            let read = timeout(slot.idle_timeout, conn.read());
            tokio::pin!(read);
            loop {
                select! {
                    biased;
                    _ = &mut retire, if !retiring => {
                        retiring = true;
                        quiet
                            .as_mut()
                            .reset(tokio::time::Instant::now() + RETIRE_QUIET);
                    }
                    loss = &mut writer => return SessionEnd::Lost(loss),
                    result = &mut read => break Some(result),
                    // A quiet socket has no backlog to drain.
                    () = &mut quiet, if retiring => break None,
                    // After `read`, so an answer already waiting is still
                    // taken; this arm is for a socket that sends nothing.
                    () = &mut unanswered, if !settled => {
                        return SessionEnd::Lost(Loss::Unanswered(SUBSCRIBE_TIMEOUT));
                    }
                }
            }
        };
        let message = match message {
            Some(Ok(Ok(message))) => message,
            Some(Ok(Err(error))) => return SessionEnd::Lost(Loss::Io(error)),
            Some(Err(_)) => return SessionEnd::Lost(Loss::Idle(slot.idle_timeout)),
            None => break false,
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

        let verdict = match message.opcode {
            OpCode::Binary => {
                let verdict = session.on_data(&message.payload);
                match slot.deliver(&mut overflow, message.payload).await {
                    Delivery::Sent => {
                        if !std::mem::replace(&mut delivered, true) {
                            slot.delivered.store(true, Ordering::Relaxed);
                        }
                        // Lease first: going live may retire an older session
                        // at once. Only a frame that reached the queue after
                        // every answer proves this session carries the feed.
                        if settled {
                            lease.get_or_insert_with(|| slot.readiness.source_live(slot.source));
                            if !std::mem::replace(&mut announced_live, true) {
                                let _ = slot.events.try_send(Event::Live(id));
                            }
                        }
                    }
                    // The socket is alive but this process is not carrying
                    // the feed: revoke handover readiness until delivery
                    // succeeds again.
                    Delivery::Dropped => lease = None,
                    Delivery::Closed => return SessionEnd::Finished,
                }
                verdict
            }
            OpCode::Text => session.on_control(&message.payload),
            OpCode::Ping => {
                if let Err(error) = sender.pong(message.payload).await {
                    return SessionEnd::Lost(Loss::Io(error));
                }
                Verdict::Continue
            }
            OpCode::Close => {
                let (code, reason) = ws::close_reason(&message.payload);
                warn!(
                    endpoint = %slot.source,
                    lifetime = ?opened.elapsed(),
                    ?code,
                    %reason,
                    "websocket closed by the venue"
                );
                // `read` queued the close echo; let it reach the wire.
                conn.flush_close().await;
                return SessionEnd::Lost(Loss::ClosedByServer);
            }
            _ => Verdict::Continue,
        };

        match verdict {
            Verdict::Continue => {}
            Verdict::Relieve(reason) => {
                if !std::mem::replace(&mut relieved, true) {
                    slot.request_replacement(id, reason);
                }
            }
            Verdict::End(loss) => return SessionEnd::Lost(loss),
        }
        settled = settled || session.settled();
        // After the frame, so an answer arriving on the deadline still counts.
        if !settled && tokio::time::Instant::now() >= unanswered.deadline() {
            return SessionEnd::Lost(Loss::Unanswered(SUBSCRIBE_TIMEOUT));
        }
        if retiring {
            break true;
        }
    };

    let drained = if drain {
        drain_buffered(
            &mut conn,
            &slot,
            &mut overflow,
            Instant::now() + DRAIN_GRACE,
        )
        .await
    } else {
        Drained::default()
    };
    // Hand the slot back rather than dropping the socket: a handover holds
    // two of this IP's limited connections.
    let _ = timeout(RETIRE_GRACE, async {
        if sender.close(CLOSE_GOING_AWAY, "replaced").await.is_ok() {
            conn.flush_close().await;
        }
    })
    .await;
    info!(
        endpoint = %slot.source,
        session = id,
        lifetime = ?opened.elapsed(),
        recovered = drained.delivered,
        shed = drained.shed,
        "the replacement is carrying the feed; retiring this session"
    );
    SessionEnd::Retired
}

/// Reconnect pacing: exponential from 100 ms to 30 s with ±25 % jitter, so
/// connections that fail together do not retry in lockstep, and a venue that
/// stays down costs each connection about 20 attempts per 10 minutes.
#[derive(Debug, Default)]
struct Backoff {
    /// Consecutive sessions that ended before [`SETTLED`].
    failures: u32,
}

impl Backoff {
    /// Waits before the next attempt. `lifetime` is how long the session that
    /// just ended lasted; a failed handshake has none.
    async fn wait(&mut self, lifetime: Option<Duration>) {
        self.failures = if lifetime.is_some_and(|lifetime| lifetime > SETTLED) {
            0
        } else {
            self.failures.saturating_add(1)
        };
        tokio::time::sleep(jittered(self.failures)).await;
    }
}

fn jittered(failures: u32) -> Duration {
    let base_ms = (100_u64 << failures.min(9)).min(30_000);
    let spread = base_ms / 4;
    // Sub-second wall-clock nanos are random enough to decorrelate retries.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    Duration::from_millis(base_ms - spread + u64::from(nanos) % (2 * spread + 1))
}

/// Spaces the connection attempts of every slot in the process by
/// [`CONNECT_SPACING`]: each attempt reserves the next free turn.
struct Pacer {
    start: tokio::time::Instant,
    /// The next free turn, in nanoseconds after `start`.
    next: AtomicU64,
}

impl Pacer {
    fn new() -> Self {
        Self {
            start: tokio::time::Instant::now(),
            next: AtomicU64::new(0),
        }
    }

    /// Waits for this attempt's turn: now, or the next free one if that is
    /// later, which then moves on by [`CONNECT_SPACING`].
    async fn wait(&self) {
        let nanos = |duration: Duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        let now = nanos(self.start.elapsed());
        let spacing = nanos(CONNECT_SPACING);
        let mut next = self.next.load(Ordering::Relaxed);
        let turn = loop {
            let turn = next.max(now);
            match self.next.compare_exchange_weak(
                next,
                turn.saturating_add(spacing),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break turn,
                Err(taken) => next = taken,
            }
        };
        tokio::time::sleep_until(self.start + Duration::from_nanos(turn)).await;
    }
}

/// What every slot of the process shares.
struct Shared {
    tls: Arc<ClientConfig>,
    pacer: Pacer,
    /// Set once any slot's frame has reached the queue.
    delivered: Arc<AtomicBool>,
}

/// A refusal no retry can cure ends the collection, but only while no slot
/// has delivered: credentials or symbols wrong from the start. Once data has
/// flowed, a refused slot keeps retrying while the others deliver, rather
/// than take the process down mid-recording.
fn give_up(delivered: bool, refusal: impl std::fmt::Display) -> Option<anyhow::Error> {
    (!delivered).then(|| anyhow::anyhow!("{refusal}"))
}

/// Keeps one connection slot filled, replacing sessions before they are cut.
///
/// A session that asks to be relieved, or reaches the age limit, keeps
/// delivering while its replacement handshakes, and is retired only once the
/// replacement has data flowing. The overlap's duplicates are what
/// [`crate::dedup`] is for.
///
/// Returns once the consumer is gone, or with a refusal met before any slot
/// delivered: of the credentials a [`Protocol::credentialed`] upgrade
/// carries, or of every subscription.
async fn keep_connection<P: Protocol>(
    protocol: Arc<P>,
    shared: Arc<Shared>,
    connection: usize,
    ws_tx: mpsc::Sender<Frame>,
    quality: QualityReporter,
    readiness: Readiness,
) -> anyhow::Result<()> {
    // Relieved sessions still delivering. A `JoinSet` so that aborting this
    // supervisor at shutdown also drops their queue senders, which would
    // otherwise hold the feed open until the drain timed out.
    let mut lingering: JoinSet<()> = JoinSet::new();
    let mut handover = Handover::default();
    let mut backoff = Backoff::default();
    // `slot` holds `event_tx` for as long as this runs, so `recv` never
    // yields `None`.
    let (event_tx, mut event_rx) = mpsc::channel(EVENT_QUEUE_CAPACITY);
    let slot = Slot {
        source: Source {
            endpoint: P::SOURCE,
            connection,
        },
        ws_tx,
        events: event_tx,
        idle_timeout: protocol.idle_timeout(),
        max_age: protocol.max_age(connection),
        delivered: Arc::clone(&shared.delivered),
        quality,
        readiness,
    };
    let mut next_session: SessionId = 0;
    let mut first_attempt = true;
    // A refusal repeats on every attempt, and the quality log syncs every
    // event: reported once until a handshake succeeds again.
    let mut refusal_reported = false;

    loop {
        while lingering.try_join_next().is_some() {}

        shared.pacer.wait().await;
        let opened = Instant::now();
        let attempt = match protocol.handshake() {
            Ok(handshake) => {
                if std::mem::take(&mut first_attempt) {
                    info!(connection, url = %handshake.url, "connecting to {}", P::SOURCE);
                }
                ws::connect(handshake.url, handshake.headers, Arc::clone(&shared.tls)).await
            }
            Err(error) => Err(ConnectError::Failed(error)),
        };
        let conn = match attempt {
            Ok(conn) => {
                refusal_reported = false;
                conn
            }
            Err(error) => {
                let refused = matches!(error, ConnectError::Refused { .. });
                if refused
                    && protocol.credentialed()
                    && let Some(error) = give_up(
                        shared.delivered.load(Ordering::Relaxed),
                        format!("{}: {error}", P::SOURCE),
                    )
                {
                    return Err(error);
                }
                // Other slots may still be delivering, so this one keeps
                // trying rather than end the recording.
                error!(
                    endpoint = %slot.source,
                    %error,
                    attempt = backoff.failures + 1,
                    still_delivering = handover.waiting(),
                    "websocket handshake failed"
                );
                if refused && !std::mem::replace(&mut refusal_reported, true) {
                    slot.quality.report(QualityEvent::FeedDegraded {
                        at_ns: QualityEvent::now_ns(),
                        source: slot.source.to_string(),
                        detail: error.to_string(),
                    });
                }
                backoff.wait(None).await;
                continue;
            }
        };

        let id = next_session;
        next_session += 1;
        let (retire_tx, retire_rx) = oneshot::channel();
        let (session, requests) = protocol.open();
        let mut running = Box::pin(run_session(
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
                    Some(Event::Live(live)) => handover.on_live(live),
                    Some(Event::Relieve(from, reason)) if from == id => {
                        break Step::Relieved(reason);
                    }
                    // A session asks once, so this is a relieved one repeating.
                    Some(Event::Relieve(from, _)) => {
                        debug!(endpoint = %slot.source, session = from, "ignoring a stale relieve");
                    }
                    None => break Step::Ended(SessionEnd::Finished),
                },
                end = &mut running => break Step::Ended(end),
            }
        };

        // The lifetime tells the venue recycling a healthy connection from
        // this side failing.
        let lifetime = opened.elapsed();
        match step {
            Step::Relieved(reason) => {
                // Not retiring anything here: asking to be replaced says
                // nothing about whether the replacement will work.
                handover.on_relieved(id, retire_tx);
                warn!(
                    endpoint = %slot.source,
                    reason = reason.as_str(),
                    ?lifetime,
                    still_delivering = handover.waiting(),
                    "replacing this session; it keeps delivering until the new one is live"
                );
                let source = slot.source;
                lingering.spawn(async move {
                    let end = running.await;
                    info!(endpoint = %source, ?end, "the relieved session ended");
                });
                // A healthy session has reset the backoff, so this opens the
                // replacement almost at once. A short-lived one still pays it:
                // a venue rolling its pool can announce shutdowns seconds
                // after each handshake.
                backoff.wait(Some(lifetime)).await;
            }
            // Dropping `lingering` stops the relieved sessions too.
            Step::Ended(SessionEnd::Finished) => return Ok(()),
            Step::Ended(SessionEnd::Retired) => {
                // Unreachable: the active session's handle is not in
                // `Handover` until it is relieved. Reconnect, with backoff so
                // it cannot spin.
                warn!(
                    endpoint = %slot.source,
                    session = id,
                    "the active session was retired; this should not happen"
                );
                backoff.wait(None).await;
            }
            Step::Ended(SessionEnd::Lost(loss)) => {
                if let Loss::Refused(_) = &loss
                    && let Some(error) = give_up(
                        shared.delivered.load(Ordering::Relaxed),
                        format!("{}: {loss}", P::SOURCE),
                    )
                {
                    return Err(error);
                }
                error!(
                    endpoint = %slot.source,
                    %loss,
                    ?lifetime,
                    still_delivering = handover.waiting(),
                    "websocket session ended"
                );
                backoff.wait(Some(lifetime)).await;
            }
        }
    }
}

/// Starts one supervisor per connection slot, [`CONNECT_STAGGER`] apart, and
/// returns the queue they all deliver into.
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
    let shared = Arc::new(Shared {
        tls: ws::tls_config(),
        pacer: Pacer::new(),
        delivered: Arc::new(AtomicBool::new(false)),
    });
    for connection in 0..connections {
        let protocol = Arc::clone(&protocol);
        let shared = Arc::clone(&shared);
        let ws_tx = ws_tx.clone();
        let quality = quality.clone();
        let readiness = readiness.clone();
        tasks.spawn(async move {
            tokio::time::sleep(CONNECT_STAGGER * connection as u32).await;
            keep_connection(protocol, shared, connection, ws_tx, quality, readiness).await?;
            Err(anyhow::anyhow!(
                "{} connection supervisor {connection} exited",
                P::SOURCE
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

    const TEST_SOURCE: Source = Source {
        endpoint: "test",
        connection: 0,
    };

    const TEST_SESSION: SessionId = 7;

    /// The market frame the tests send.
    const MARKET: [u8; 4] = [0x12, 0x00, 0x10, 0x27];

    /// Asks for a replacement on the text `relieve` and gives up on `refuse`;
    /// each `ack` answers one of `pending` subscriptions. The market frame
    /// `gap` asks for a replacement.
    #[derive(Default)]
    struct Script {
        pending: usize,
    }

    impl Session for Script {
        fn on_control(&mut self, text: &[u8]) -> Verdict {
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

        fn on_data(&mut self, data: &[u8]) -> Verdict {
            match data {
                b"gap" => Verdict::Relieve(Reason::Gap),
                _ => Verdict::Continue,
            }
        }

        fn settled(&self) -> bool {
            self.pending == 0
        }
    }

    fn slot(ws_tx: mpsc::Sender<Frame>, events: mpsc::Sender<Event>, readiness: Readiness) -> Slot {
        Slot {
            source: TEST_SOURCE,
            ws_tx,
            events,
            idle_timeout: Duration::from_secs(60),
            max_age: None,
            delivered: Arc::new(AtomicBool::new(false)),
            quality: QualityReporter::disabled(),
            readiness,
        }
    }

    /// One session, its peer, and the channels the supervisor would watch.
    struct Harness {
        server: WebSocket<DuplexStream>,
        events: mpsc::Receiver<Event>,
        data: mpsc::Receiver<Frame>,
        /// The writer is marked ready, so this follows the session's lease.
        ready: watch::Receiver<ReadinessState>,
        delivered: Arc<AtomicBool>,
        retire: Option<oneshot::Sender<()>>,
        session: tokio::task::JoinHandle<SessionEnd>,
    }

    impl Harness {
        fn start(max_age: Option<Duration>) -> Self {
            Self::with(Script::default(), Requests::default(), max_age, 16)
        }

        fn with(
            script: Script,
            requests: Requests,
            max_age: Option<Duration>,
            capacity: usize,
        ) -> Self {
            let (conn, server) = ws::duplex_pair();
            let (event_tx, events) = mpsc::channel(2);
            let (ws_tx, data) = mpsc::channel(capacity);
            let (retire_tx, retire_rx) = oneshot::channel();
            let (readiness, ready) = Readiness::new(1);
            readiness.mark_writer_ready();
            let slot = Slot {
                max_age,
                ..slot(ws_tx, event_tx, readiness)
            };
            Self {
                server,
                events,
                data,
                ready,
                delivered: Arc::clone(&slot.delivered),
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

        async fn send_binary(&mut self, data: &[u8]) {
            self.server
                .write_frame(WsFrame::binary(Payload::Owned(data.to_vec())))
                .await
                .unwrap();
        }

        async fn send_market_data(&mut self) {
            self.send_binary(&MARKET).await;
        }

        /// Readiness once every frame sent so far has been handled, which a
        /// ping round-trip proves.
        async fn ready_after_ping(&mut self) -> bool {
            self.server
                .write_frame(WsFrame::new(
                    true,
                    OpCode::Ping,
                    None,
                    Payload::Owned(b"sync".to_vec()),
                ))
                .await
                .unwrap();
            let pong = self.server.read_frame().await.unwrap();
            assert_eq!(pong.opcode, OpCode::Pong);
            self.ready.borrow().ready()
        }
    }

    fn requests(initial: &[&str]) -> Requests {
        Requests {
            initial: initial.iter().map(|text| (*text).to_owned()).collect(),
            ..Requests::default()
        }
    }

    #[tokio::test]
    async fn binary_frames_are_queued_and_text_frames_are_not() {
        let mut harness = Harness::start(None);
        harness.send_text(r#"{"result":null,"id":1}"#).await;
        harness.send_market_data().await;

        let frame = harness.data.recv().await.unwrap();
        assert_eq!(&frame.data[..], MARKET);
        assert!(matches!(harness.events.recv().await, Some(Event::Live(7))));
        assert!(harness.ready_after_ping().await);
        assert!(harness.data.try_recv().is_err(), "text frames stay out");
    }

    /// Acting on the announcement must not end the session: it keeps
    /// delivering while the replacement is brought up.
    #[tokio::test]
    async fn a_shutdown_announcement_asks_for_a_replacement_and_keeps_delivering() {
        let mut harness = Harness::start(None);

        harness.send_market_data().await;
        assert!(matches!(harness.events.recv().await, Some(Event::Live(7))));
        assert!(harness.data.recv().await.is_some());

        harness.send_text("relieve").await;
        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(7, Reason::ServerShutdown))
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
            Some(Event::Relieve(7, Reason::ServerShutdown))
        ));
        harness.send_text("relieve").await;

        // Frames are processed in order: `Live` next means nothing was
        // requested in between.
        harness.send_market_data().await;
        assert!(matches!(harness.events.recv().await, Some(Event::Live(7))));
    }

    /// A market frame the session finds wanting still reaches the queue: the
    /// replacement it asks for takes over only once it carries the feed.
    #[tokio::test]
    async fn a_gap_in_the_data_asks_for_a_replacement_and_keeps_the_frame() {
        let mut harness = Harness::start(None);

        harness.send_binary(b"gap").await;

        assert!(matches!(harness.events.recv().await, Some(Event::Live(7))));
        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(7, Reason::Gap))
        ));
        assert_eq!(&harness.data.recv().await.unwrap().data[..], b"gap");
    }

    #[tokio::test]
    async fn a_session_past_its_age_cap_asks_for_a_replacement() {
        let mut harness = Harness::start(Some(Duration::ZERO));

        harness.send_market_data().await;

        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(7, Reason::Age))
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

    /// No latency and no overlap beyond what was already in flight.
    #[tokio::test(start_paused = true)]
    async fn retiring_a_quiet_session_closes_it_at_once() {
        let mut harness = Harness::start(None);

        let before = tokio::time::Instant::now();
        harness.retire.take().unwrap().send(()).unwrap();
        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Retired
        ));
        assert!(tokio::time::Instant::now() - before <= RETIRE_QUIET + RETIRE_GRACE);
        assert!(harness.data.try_recv().is_err());
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
        assert!(!harness.delivered.load(Ordering::Relaxed));
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
            SessionEnd::Lost(Loss::Idle(_))
        ));
    }

    /// What already arrived is delivered, protocol frames left out.
    #[tokio::test]
    async fn retiring_delivers_the_backlog_that_already_arrived() {
        let (mut conn, mut server) = ws::duplex_pair();
        let (ws_tx, data) = mpsc::channel(16);
        let (event_tx, _events) = mpsc::channel(2);
        let slot = slot(ws_tx, event_tx, Readiness::new(1).0);
        let mut overflow = Overflow::new(TEST_SOURCE, QualityReporter::disabled());

        for _ in 0..3 {
            server
                .write_frame(WsFrame::binary(Payload::Owned(vec![1, 2, 3, 4])))
                .await
                .unwrap();
        }
        server
            .write_frame(WsFrame::text(Payload::Owned(b"{}".to_vec())))
            .await
            .unwrap();

        let drained = drain_buffered(
            &mut conn,
            &slot,
            &mut overflow,
            Instant::now() + DRAIN_GRACE,
        )
        .await;

        assert_eq!(drained.delivered, 3, "the backlog must not be discarded");
        assert_eq!(drained.shed, 0);
        assert_eq!(data.len(), 3);
    }

    /// A retire that lands while a frame is half-read must not cut the read:
    /// the rest of the frame would then be parsed as frames. The frame
    /// completes, and the backlog behind it is drained intact.
    #[tokio::test(start_paused = true)]
    async fn retiring_mid_frame_finishes_the_frame_then_drains() {
        use tokio::io::AsyncWriteExt;

        let (client, mut venue) = tokio::io::duplex(64 * 1024);
        let mut client = WebSocket::after_handshake(client, fastwebsockets::Role::Client);
        client.set_auto_pong(false);
        let (event_tx, _events) = mpsc::channel(2);
        let (ws_tx, mut data) = mpsc::channel(16);
        let (retire, retire_rx) = oneshot::channel();
        let session = tokio::spawn(run_session(
            slot(ws_tx, event_tx, Readiness::new(1).0),
            Connection::from_websocket(client),
            TEST_SESSION,
            retire_rx,
            Script::default(),
            Requests::default(),
        ));
        // Unmasked frames, as a venue sends them.
        let (first, second) = ([1u8; 100], [2u8; 100]);
        venue.write_all(&[0x82, 100]).await.unwrap();
        venue.write_all(&first[..50]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        retire.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        venue.write_all(&first[50..]).await.unwrap();
        venue.write_all(&[0x81, 2]).await.unwrap();
        venue.write_all(b"{}").await.unwrap();
        venue.write_all(&[0x82, 100]).await.unwrap();
        venue.write_all(&second).await.unwrap();

        assert!(matches!(session.await.unwrap(), SessionEnd::Retired));
        let delivered: Vec<Bytes> =
            std::iter::from_fn(|| data.try_recv().ok().map(|frame| frame.data)).collect();
        assert_eq!(delivered, [&first[..], &second[..]]);
    }

    /// No latency and no overlap beyond what was already in flight.
    #[tokio::test(start_paused = true)]
    async fn draining_a_caught_up_socket_returns_at_once() {
        let (mut conn, _server) = ws::duplex_pair();
        let (ws_tx, _data) = mpsc::channel(16);
        let (event_tx, _events) = mpsc::channel(2);
        let slot = slot(ws_tx, event_tx, Readiness::new(1).0);
        let mut overflow = Overflow::new(TEST_SOURCE, QualityReporter::disabled());

        let before = tokio::time::Instant::now();
        let drained = drain_buffered(
            &mut conn,
            &slot,
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

    /// A shed frame never reached the queue, so it proves nothing: it must
    /// neither make the process ready nor retire an older session.
    #[tokio::test(start_paused = true)]
    async fn a_shed_frame_does_not_make_the_session_live() {
        let mut harness = Harness::with(Script::default(), Requests::default(), None, 1);
        harness.send_market_data().await;
        assert!(matches!(harness.events.recv().await, Some(Event::Live(7))));
        assert!(harness.ready_after_ping().await);

        // The queue is full: this one is shed, which withdraws readiness.
        harness.send_market_data().await;
        tokio::time::sleep(ws::QUEUE_FULL_GRACE * 2).await;
        assert!(!harness.ready_after_ping().await);

        harness.data.recv().await.unwrap();
        harness.send_market_data().await;
        assert!(harness.ready_after_ping().await);
    }

    /// A session shed from the start is never announced live, and has not
    /// delivered.
    #[tokio::test(start_paused = true)]
    async fn a_session_whose_first_frame_is_shed_is_not_announced() {
        let (conn, mut server) = ws::duplex_pair();
        let (event_tx, mut events) = mpsc::channel(2);
        let (ws_tx, mut data) = mpsc::channel(1);
        ws_tx
            .try_send(Frame {
                recv_time: Timestamp::now(),
                data: Bytes::new(),
            })
            .unwrap();
        let (readiness, ready) = Readiness::new(1);
        readiness.mark_writer_ready();
        let slot = slot(ws_tx, event_tx, readiness);
        let delivered = Arc::clone(&slot.delivered);
        let (_retire, retire_rx) = oneshot::channel();
        let _session = tokio::spawn(run_session(
            slot,
            conn,
            TEST_SESSION,
            retire_rx,
            Script::default(),
            Requests::default(),
        ));
        let market = || WsFrame::binary(Payload::Owned(MARKET.to_vec()));

        server.write_frame(market()).await.unwrap();
        tokio::time::sleep(ws::QUEUE_FULL_GRACE * 2).await;
        assert!(events.try_recv().is_err(), "shed, so not live");
        assert!(!ready.borrow().ready());
        assert!(!delivered.load(Ordering::Relaxed));

        data.recv().await.unwrap();
        server.write_frame(market()).await.unwrap();
        assert!(matches!(events.recv().await, Some(Event::Live(7))));
        assert!(ready.borrow().ready());
        assert!(delivered.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn requests_go_out_in_order_once_connected() {
        let mut harness = Harness::with(
            Script { pending: 2 },
            requests(&["first", "second"]),
            None,
            16,
        );

        harness.expect_text("first").await;
        harness.expect_text("second").await;
    }

    /// The keepalive races the read without cancelling it: data on either
    /// side of a ping arrives intact.
    #[tokio::test(start_paused = true)]
    async fn keepalive_pings_go_out_while_data_keeps_flowing() {
        let every = Duration::from_secs(20);
        let mut harness = Harness::with(
            Script::default(),
            Requests {
                keepalive: Some(every),
                ..Requests::default()
            },
            None,
            16,
        );

        harness.send_market_data().await;
        assert!(harness.data.recv().await.is_some());

        let before = tokio::time::Instant::now();
        let ping = harness.server.read_frame().await.unwrap();
        assert_eq!(ping.opcode, OpCode::Ping);
        assert!(tokio::time::Instant::now() - before <= every);

        harness.send_market_data().await;
        assert_eq!(&harness.data.recv().await.unwrap().data[..], MARKET);
        assert!(!harness.session.is_finished());
    }

    /// Data that arrives before the last reply is recorded, but proves nothing
    /// about the channels still unanswered: it must neither retire an older
    /// session nor make the process ready for a handover. It has delivered,
    /// though, so a later refusal no longer ends the process.
    #[tokio::test]
    async fn a_session_is_not_live_until_every_subscription_is_answered() {
        let mut harness = Harness::with(Script { pending: 1 }, requests(&["sub"]), None, 16);
        harness.expect_text("sub").await;

        harness.send_market_data().await;
        assert!(harness.data.recv().await.is_some());
        assert!(!harness.ready_after_ping().await);
        assert!(
            harness.events.try_recv().is_err(),
            "no Live before the reply"
        );
        assert!(harness.delivered.load(Ordering::Relaxed));

        harness.send_text("ack").await;
        harness.send_market_data().await;
        assert!(matches!(harness.events.recv().await, Some(Event::Live(7))));
        assert!(harness.data.recv().await.is_some());
        assert!(harness.ready.borrow().ready());
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
    fn only_a_process_that_never_delivered_gives_up() {
        let error = give_up(false, "okx-sbe: every subscription was refused").unwrap();
        assert_eq!(error.to_string(), "okx-sbe: every subscription was refused");
        assert!(give_up(true, "refused").is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_subscription_left_unanswered_ends_the_session() {
        let mut harness = Harness::with(Script { pending: 1 }, requests(&["sub"]), None, 16);
        harness.expect_text("sub").await;

        tokio::time::sleep(SUBSCRIBE_TIMEOUT).await;
        harness.send_market_data().await;

        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Lost(Loss::Unanswered(_))
        ));
    }

    /// A socket that sends nothing at all is given up at the deadline, not
    /// at the idle timeout.
    #[tokio::test(start_paused = true)]
    async fn a_silent_unanswered_session_ends_at_the_deadline() {
        let mut harness = Harness::with(Script { pending: 1 }, requests(&["sub"]), None, 16);
        harness.expect_text("sub").await;
        let start = tokio::time::Instant::now();

        assert!(matches!(
            harness.session.await.unwrap(),
            SessionEnd::Lost(Loss::Unanswered(_))
        ));
        assert!(tokio::time::Instant::now() - start <= SUBSCRIBE_TIMEOUT);
    }

    /// Once every reply is in, the deadline no longer applies.
    #[tokio::test(start_paused = true)]
    async fn an_answered_session_outlives_the_deadline() {
        let mut harness = Harness::with(Script { pending: 1 }, requests(&["sub"]), None, 16);
        harness.expect_text("sub").await;
        harness.send_text("ack").await;
        tokio::time::advance(SUBSCRIBE_TIMEOUT).await;

        harness.send_market_data().await;
        assert!(matches!(harness.events.recv().await, Some(Event::Live(7))));
        assert!(!harness.session.is_finished());
    }

    #[test]
    fn the_delay_doubles_up_to_thirty_seconds_within_a_quarter() {
        for (failures, base_ms) in [
            (0, 100),
            (1, 200),
            (3, 800),
            (6, 6_400),
            (8, 25_600),
            (9, 30_000),
            (40, 30_000),
        ] {
            let delay = jittered(failures).as_millis() as u64;
            assert!(
                (base_ms * 3 / 4..=base_ms * 5 / 4).contains(&delay),
                "{failures} failures: {delay} ms"
            );
        }
    }

    /// A healthy session ending is the venue recycling it, not a failure, so
    /// it must not leave the next reconnect waiting longer.
    #[tokio::test(start_paused = true)]
    async fn a_settled_session_resets_the_count() {
        let mut backoff = Backoff::default();
        backoff.wait(None).await;
        backoff.wait(Some(Duration::from_secs(1))).await;
        assert_eq!(backoff.failures, 2);

        backoff.wait(Some(SETTLED * 2)).await;
        assert_eq!(backoff.failures, 0);
    }

    /// Slots wait their turn: a venue-wide notice relieves every slot at once,
    /// and their reconnects must not burst past a per-IP attempt limit.
    #[tokio::test(start_paused = true)]
    async fn connection_attempts_are_spaced_across_slots() {
        let pacer = Arc::new(Pacer::new());
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
