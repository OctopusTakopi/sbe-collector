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
use tokio::{
    io::AsyncRead,
    select,
    sync::{mpsc, mpsc::Sender, oneshot},
    task::JoinSet,
    time::timeout,
};
use tracing::{error, info, warn};

use crate::ws::{Connection, Delivery, Overflow};

const WSS_HOST: &str = "stream-sbe.binance.com";
const WSS_PORT: u16 = 9443;
/// Binance server pings arrive every ~20 s, and market-data frames on active
/// symbols arrive in milliseconds, so 60 s of total silence means a dead socket.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// When a session starts arranging its own replacement.
///
/// The venue states that "a single connection to stream-sbe.binance.com is only
/// valid for 24 hours; expect to be disconnected at the 24 hour mark". Being
/// disconnected costs a reconnect's worth of data; handing over first costs
/// nothing, so this sits far enough inside the limit that even a session which
/// has gone quiet — [`IDLE_TIMEOUT`] bounds how late the check can run — is
/// replaced well before the venue cuts it.
const MAX_SESSION_AGE: Duration = Duration::from_secs(23 * 60 * 60);

/// How far apart redundant connections place their handovers.
///
/// `CONNECT_STAGGER` decorrelates the connections at startup; a fixed age cap
/// would re-synchronise them a day later and every leg would hand over within
/// seconds of the others — the same simultaneous handshakes against the same
/// server pool that staggering exists to avoid, and the same correlation
/// redundancy is paid to prevent.
const SESSION_AGE_STAGGER: Duration = Duration::from_secs(15 * 60);

/// When *this* connection's sessions start arranging their replacement.
fn max_session_age(connection: usize) -> Duration {
    MAX_SESSION_AGE.saturating_sub(SESSION_AGE_STAGGER * connection as u32)
}

/// A session that has lived this long was healthy, so ending it is a venue
/// decision rather than a local failure, and the next attempt need not wait.
const SETTLED: Duration = Duration::from_secs(30);

/// The event the venue pushes unprompted before it closes a connection:
/// `{"e":"serverShutdown","E":...}`.
const SERVER_SHUTDOWN: &str = "serverShutdown";

/// RFC 6455 "going away": this endpoint is leaving, not faulting.
const CLOSE_GOING_AWAY: u16 = 1001;

/// How long a retired session may spend closing politely before it is simply
/// dropped. Its replacement is already carrying the feed by then.
const RETIRE_GRACE: Duration = Duration::from_secs(2);

/// Why a session asked for its replacement to be opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reason {
    /// The venue announced that this server is about to shut down.
    ServerShutdown,
    /// The session is approaching the venue's 24-hour connection limit.
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

/// What a running session reports to its supervisor.
#[derive(Debug)]
enum Event {
    /// Market data from this session reached the queue, so whatever it was
    /// opened to replace has been superseded and can be let go.
    Live,
    /// Open the replacement now. The session keeps delivering until that
    /// replacement is live or the venue closes the socket, whichever is first.
    Relieve(Reason),
}

/// How a session ended.
#[derive(Debug)]
enum SessionEnd {
    /// The consumer is gone; the collector is shutting down.
    Finished,
    /// Retired by the supervisor once its replacement went live.
    Retired,
    /// The venue closed the socket, or it failed.
    Lost(Error),
}

/// What ended the supervisor's watch over the active session.
enum Step {
    /// The session wants to be replaced and is still running.
    Relieved(Reason),
    /// The session is over.
    Ended(SessionEnd),
}

/// The sessions that have been relieved but are still carrying the feed.
///
/// One rule, and the whole handover rests on it: **nothing is retired except by
/// a session that has proved it can carry the feed**. A replacement that has
/// not delivered a byte has proved nothing, so relieving *it* must never close
/// the session it was opened for — that would rest the recording on the
/// unproven socket and put back exactly the hole this feature removes. A venue
/// rolling its pool can announce a shutdown on a fresh connection before its
/// first frame arrives, which is precisely when that matters.
#[derive(Default)]
struct Handover {
    pending: Vec<oneshot::Sender<()>>,
}

impl Handover {
    /// A session is delivering: every older one is now redundant.
    fn on_live(&mut self) {
        for retire in self.pending.drain(..) {
            let _ = retire.send(());
        }
    }

    /// A session was relieved and goes on delivering. Hold its handle until
    /// something proves it can be let go.
    fn on_relieved(&mut self, retire: oneshot::Sender<()>) {
        // Sessions the venue has already closed cannot be retired, and holding
        // their handles would let this grow for as long as replacements keep
        // being announced away before they deliver.
        self.pending.retain(|pending| !pending.is_closed());
        self.pending.push(retire);
    }

    fn waiting(&self) -> usize {
        self.pending.len()
    }
}

/// True if `payload` is the venue's shutdown announcement.
///
/// Parsed rather than substring-matched: an error or rejection message quoting
/// the word must not trigger a handover. The endpoint documents the event as a
/// bare object, but the same connection carries combined-stream envelopes, so
/// an enveloped copy is accepted as well.
fn is_server_shutdown(payload: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return false;
    };
    let event = value.get("data").unwrap_or(&value);
    event.get("e").and_then(serde_json::Value::as_str) == Some(SERVER_SHUTDOWN)
}

/// Deliver what has already arrived on a socket that is about to be closed.
///
/// The replacement subscribes when it connects, so it only ever sees events
/// from that moment on. Anything already sitting in this socket's buffer is
/// older than that and exists nowhere else — dropping it would put a hole in
/// the recording at the very moment the handover exists to prevent one. A
/// session that fell behind under backpressure is exactly the one holding such
/// a backlog.
///
/// The zero deadline is what keeps this honest: it takes what has already
/// arrived and stops the instant the socket would block, so it adds no latency
/// and no overlap beyond the frames that were in flight anyway.
///
/// Cancelling `read` mid-frame is sound here and only here — the connection is
/// closed on the next line, so a half-consumed header has nothing left to
/// corrupt.
async fn drain_buffered<S>(
    conn: &mut Connection<S>,
    ws_tx: &Sender<(Timestamp, Bytes)>,
    overflow: &mut Overflow,
) -> usize
where
    S: AsyncRead + Unpin,
{
    let mut delivered = 0;
    while let Ok(Ok(message)) = timeout(Duration::ZERO, conn.read()).await {
        if message.opcode != OpCode::Binary {
            continue;
        }
        let recv_time = Timestamp::now();
        match crate::ws::deliver(ws_tx, overflow, (recv_time, message.payload), |_| true).await {
            Delivery::Sent | Delivery::Dropped => delivered += 1,
            Delivery::Closed | Delivery::Undeliverable => break,
        }
    }
    delivered
}

fn request_replacement(events: &mpsc::Sender<Event>, reason: Reason, connection: usize) {
    warn!(
        connection,
        reason = reason.as_str(),
        "asking for a replacement connection"
    );
    // The supervisor is the only reader and the channel holds both events a
    // session can send, so a failure here means it has already moved on.
    let _ = events.try_send(Event::Relieve(reason));
}

/// Read one websocket session to its end, delivering market data into `ws_tx`.
///
/// The decision to replace a session does not end it. On a shutdown
/// announcement, or at `max_age`, it reports [`Event::Relieve`] and *keeps
/// reading*: the replacement's handshake then happens while this socket is
/// still delivering, which is the whole point of the venue announcing a
/// shutdown ahead of it. The session stops when `retire` fires, when the venue
/// closes the socket, or when it falls silent.
async fn run_session<S>(
    mut conn: Connection<S>,
    connection: usize,
    ws_tx: Sender<(Timestamp, Bytes)>,
    events: mpsc::Sender<Event>,
    mut retire: oneshot::Receiver<()>,
    max_age: Duration,
) -> SessionEnd
where
    S: AsyncRead + Unpin,
{
    let sender = conn.sender();
    let mut overflow = Overflow::new("binance-sbe-spot");
    let opened = Instant::now();
    let mut live = false;
    let mut relieved = false;

    loop {
        // `Connection::read` is not cancel-safe, so it may only be raced
        // against arms that end the session. Both of these do.
        let message = select! {
            biased;
            _ = &mut retire => {
                // All of this is bounded: the replacement is already carrying
                // the feed, so a socket that will not drain or close must not
                // hold this task — and its share of the queue — open.
                let mut drained = 0;
                let _ = timeout(RETIRE_GRACE, async {
                    drained = drain_buffered(&mut conn, &ws_tx, &mut overflow).await;
                    // Hand the slot back rather than dropping the socket: this
                    // IP has a limited number of them, and a handover
                    // deliberately holds two at once.
                    if sender.close(CLOSE_GOING_AWAY, "replaced").await.is_ok() {
                        conn.flush_close().await;
                    }
                })
                .await;
                info!(
                    connection,
                    lifetime = ?opened.elapsed(),
                    drained,
                    "the replacement is carrying the feed; retiring this session"
                );
                return SessionEnd::Retired;
            }
            result = timeout(IDLE_TIMEOUT, conn.read()) => match result {
                Ok(result) => match result {
                    Ok(message) => message,
                    Err(error) => return SessionEnd::Lost(error),
                },
                Err(_) => {
                    warn!(
                        connection,
                        ?IDLE_TIMEOUT,
                        "no websocket frame received; reconnecting"
                    );
                    return SessionEnd::Lost(Error::from(io::Error::new(ErrorKind::TimedOut, "idle")));
                }
            },
        };

        // Checked between reads rather than raced as a `select!` arm: an arm
        // that did not end the session would cancel `read` mid-frame, and
        // `fastwebsockets` has already consumed the header by then. The idle
        // timeout bounds how late this can run to a minute, against a cap
        // measured in hours.
        if !relieved && opened.elapsed() >= max_age {
            relieved = true;
            request_replacement(&events, Reason::Age, connection);
        }

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
                    Delivery::Sent => {
                        // Only a frame the queue actually took proves this
                        // session can carry the feed in place of the one it
                        // replaced. A shed frame proves the opposite, and
                        // shedding is when retiring the predecessor costs the
                        // most: that is the session holding the longest
                        // unread backlog.
                        if !live {
                            live = true;
                            let _ = events.try_send(Event::Live);
                        }
                    }
                    Delivery::Dropped => {}
                    // Receiver dropped: the collector is shutting down.
                    Delivery::Closed => return SessionEnd::Finished,
                    Delivery::Undeliverable => {
                        return SessionEnd::Lost(Error::from(io::Error::new(
                            ErrorKind::TimedOut,
                            "frame could not be delivered",
                        )));
                    }
                }
            }
            OpCode::Text => {
                let text = String::from_utf8_lossy(&message.payload);
                if is_server_shutdown(&message.payload) {
                    warn!(connection, %text, "the venue announced a server shutdown");
                    // Repeating the announcement must not open a second
                    // replacement.
                    if !relieved {
                        relieved = true;
                        request_replacement(&events, Reason::ServerShutdown, connection);
                    }
                } else {
                    // The stream list is in the connect URL, so there is no
                    // subscription protocol and no routine text traffic. Any
                    // other text frame is therefore worth seeing — not least
                    // because it is the evidence that would show the shutdown
                    // announcement arriving in a form this does not recognise.
                    warn!(connection, "unexpected WS text frame: {}", text);
                }
            }
            OpCode::Ping => {
                if let Err(error) = sender.pong(message.payload.to_vec()).await {
                    return SessionEnd::Lost(error);
                }
            }
            OpCode::Close => {
                warn!(connection, lifetime = ?opened.elapsed(), "WS closed by server");
                // `read` has queued the close echo it is obliged to send; let
                // it reach the wire before the connection is dropped.
                conn.flush_close().await;
                return SessionEnd::Lost(Error::from(io::Error::new(
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

/// Pace the next connection attempt, and account for what ended the last one.
///
/// `lifetime` is how long the session that just ended lasted, where there was
/// one — a failed handshake has none. A session that outlived [`SETTLED`] was
/// healthy, so whatever ended it was the venue's doing and the count starts
/// over. Anything shorter is a persistent failure until proven otherwise, and
/// escalating there is what stops a rolling restart or a refused handshake from
/// spending this IP's connection-attempt budget in a burst.
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
/// A session that announces its end — or reaches the venue's age limit — is not
/// closed here. It is moved aside and keeps delivering while its replacement
/// handshakes, and is only retired once that replacement has real data flowing.
/// The recording therefore has no hole where a scheduled disconnect used to
/// put one; the duplicate frames the overlap produces are what [`crate::dedup`]
/// is for.
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
    let url = format!(
        "wss://{}:{}/stream?streams={}",
        WSS_HOST, WSS_PORT, streams_str
    );

    // Sessions that have been relieved but are still delivering while their
    // replacement is brought up. Held in a `JoinSet` because dropping one
    // aborts what it holds: shutdown aborts `keep_connection`, and a detached
    // `tokio::spawn` would keep its `ws_tx` clone — and with it the whole feed
    // — open until the drain timed out.
    let mut lingering: JoinSet<()> = JoinSet::new();
    let mut handover = Handover::default();
    let mut error_count: u32 = 0;
    let max_age = max_session_age(connection);

    loop {
        while lingering.try_join_next().is_some() {}

        let opened = Instant::now();
        // Reuse the pre-built TLS config (Arc clone is O(1)).
        let conn = match crate::ws::connect(&url, Some(&api_key), tls.clone()).await {
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

        let (event_tx, mut event_rx) = mpsc::channel(2);
        let (retire_tx, retire_rx) = oneshot::channel();
        let mut session = Box::pin(run_session(
            conn,
            connection,
            ws_tx.clone(),
            event_tx,
            retire_rx,
            max_age,
        ));

        // `event_tx` lives inside the session future, so `recv` can only yield
        // `None` once that future has returned — which the other arm breaks on
        // first. The guard is not for that ordering but for what happens if it
        // ever stops holding: a closed channel is ready forever, and under
        // `biased` that would starve the session arm into a hot loop.
        let mut reporting = true;
        let step = loop {
            select! {
                biased;
                event = event_rx.recv(), if reporting => match event {
                    // Proof that this session carries the feed, and the only
                    // thing that may retire the sessions it replaced.
                    Some(Event::Live) => handover.on_live(),
                    Some(Event::Relieve(reason)) => break Step::Relieved(reason),
                    None => reporting = false,
                },
                end = &mut session => break Step::Ended(end),
            }
        };

        match step {
            Step::Relieved(reason) => {
                let lifetime = opened.elapsed();
                // Deliberately *not* retiring anything here. This session has
                // asked to be replaced, which says nothing about whether the
                // replacement will work — only `Event::Live` does.
                handover.on_relieved(retire_tx);
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
                // A planned handover on a healthy session is not a failure, and
                // the announcement exists precisely so the replacement can be
                // opened at once. A short-lived one still pays the backoff — a
                // venue rolling its pool can announce a shutdown seconds after
                // the handshake, and answering each one immediately would spend
                // this IP's connection-attempt budget in a burst. That costs the
                // recording nothing, since the old session delivers throughout.
                back_off(&mut error_count, Some(lifetime)).await;
            }
            // Clean disconnect (ws_tx dropped) — exit. Dropping `lingering`
            // stops the relieved session too.
            Step::Ended(SessionEnd::Finished) => return,
            Step::Ended(SessionEnd::Retired) => {
                // Only a relieved session is ever retired, and this one was
                // active. Reconnecting is still right; backing off keeps an
                // impossible state from becoming a spin.
                back_off(&mut error_count, None).await;
            }
            Step::Ended(SessionEnd::Lost(err)) => {
                let lifetime = opened.elapsed();
                // The lifetime is what separates a venue recycling a healthy
                // connection from this side failing: an `Unexpected EOF` after
                // an hour is the former, one after a few seconds is the latter.
                error!(
                    connection,
                    ?err,
                    ?lifetime,
                    still_delivering = handover.waiting(),
                    "WS connection error"
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
            let session = tokio::spawn(run_session(conn, 0, ws_tx, event_tx, retire_rx, max_age));
            Self {
                server,
                events,
                data,
                retire: Some(retire_tx),
                session,
            }
        }

        async fn send_text(&mut self, text: &str) {
            self.server
                .write_frame(Frame::text(Payload::Owned(text.as_bytes().to_vec())))
                .await
                .unwrap();
        }

        /// A frame of market data. The contents do not matter here; the
        /// connector treats every binary frame as data to be forwarded.
        async fn send_market_data(&mut self) {
            self.server
                .write_frame(Frame::binary(Payload::Owned(vec![0x12, 0x00, 0x10, 0x27])))
                .await
                .unwrap();
        }
    }

    /// The announcement is what makes a gap-free handover possible. Acting on
    /// it must not end the session: it has to keep delivering while the
    /// replacement is brought up, which is the entire point of the venue
    /// warning ahead of the disconnect.
    #[tokio::test]
    async fn a_shutdown_announcement_asks_for_a_replacement_and_keeps_delivering() {
        let mut harness = Harness::start(NEVER);

        harness.send_market_data().await;
        assert!(matches!(harness.events.recv().await, Some(Event::Live)));
        assert!(harness.data.recv().await.is_some());

        harness.send_text(ANNOUNCEMENT).await;
        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(Reason::ServerShutdown))
        ));

        harness.send_market_data().await;
        assert!(
            harness.data.recv().await.is_some(),
            "the announced session must go on delivering"
        );
        assert!(!harness.session.is_finished());
    }

    /// A second announcement must not open a second replacement.
    #[tokio::test]
    async fn a_repeated_announcement_asks_only_once() {
        let mut harness = Harness::start(NEVER);

        harness.send_text(ANNOUNCEMENT).await;
        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(Reason::ServerShutdown))
        ));
        harness.send_text(ANNOUNCEMENT).await;

        // Ordering carries the assertion: the frame after the second
        // announcement is processed later, so if `Live` is the next event,
        // nothing was requested in between.
        harness.send_market_data().await;
        assert!(matches!(harness.events.recv().await, Some(Event::Live)));
    }

    /// Ordinary text frames must not trigger a handover — reconnecting on
    /// every subscription reply would be worse than not reconnecting at all.
    #[tokio::test]
    async fn an_ordinary_text_frame_asks_for_nothing() {
        let mut harness = Harness::start(NEVER);

        harness.send_text(r#"{"result":null,"id":1}"#).await;
        harness.send_market_data().await;

        // Frames are processed in order, so `Live` arriving first proves no
        // replacement was requested for the text frame.
        assert!(matches!(harness.events.recv().await, Some(Event::Live)));
    }

    /// The venue drops every connection at 24 hours, so a session must ask to
    /// be replaced before it gets there.
    #[tokio::test]
    async fn a_session_past_its_age_cap_asks_for_a_replacement() {
        let mut harness = Harness::start(Duration::ZERO);

        harness.send_market_data().await;

        assert!(matches!(
            harness.events.recv().await,
            Some(Event::Relieve(Reason::Age))
        ));
        assert!(
            !harness.session.is_finished(),
            "the aged session keeps delivering until it is retired"
        );
    }

    /// Retiring is how the supervisor closes a healthy session once its
    /// replacement has taken over. It must hand the connection back rather
    /// than drop it: a handover holds two of this IP's slots at once, and only
    /// a close returns the one that is no longer needed.
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

    /// A session whose consumer is gone reports it, so the supervisor stops
    /// reconnecting instead of reopening sockets nothing reads.
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

    /// A retired socket must hand over what it has already received. The
    /// replacement subscribed later, so those frames exist nowhere else —
    /// discarding them would put a hole exactly where the handover is meant to
    /// prevent one.
    #[tokio::test]
    async fn retiring_delivers_the_backlog_that_already_arrived() {
        let (mut conn, mut server) = crate::ws::duplex_pair();
        let (ws_tx, data) = mpsc::channel(16);
        let mut overflow = Overflow::new("test");

        for _ in 0..3 {
            server
                .write_frame(Frame::binary(Payload::Owned(vec![1, 2, 3, 4])))
                .await
                .unwrap();
        }

        let drained = drain_buffered(&mut conn, &ws_tx, &mut overflow).await;

        assert_eq!(drained, 3, "the backlog must not be discarded");
        assert_eq!(data.len(), 3);
    }

    /// And it must not *wait* for a backlog: the zero deadline stops the
    /// instant the socket would block, so a caught-up session adds no latency
    /// to the handover and no overlap beyond what was already in flight.
    #[tokio::test(start_paused = true)]
    async fn draining_a_caught_up_socket_returns_at_once() {
        let (mut conn, _server) = crate::ws::duplex_pair();
        let (ws_tx, _data) = mpsc::channel(16);
        let mut overflow = Overflow::new("test");

        let before = tokio::time::Instant::now();
        assert_eq!(drain_buffered(&mut conn, &ws_tx, &mut overflow).await, 0);
        assert_eq!(
            tokio::time::Instant::now(),
            before,
            "draining must not wait for frames that have not arrived"
        );
    }

    /// The rule the whole handover rests on. A replacement that is announced
    /// away before it has delivered anything has proved nothing, so it must not
    /// close the session still carrying the feed — doing so would rest the
    /// recording on the unproven socket and put the hole straight back.
    #[test]
    fn a_relieve_never_retires_the_session_still_carrying_the_feed() {
        let mut handover = Handover::default();
        let (live_session, mut live_rx) = oneshot::channel();
        let (unproven_session, _unproven_rx) = oneshot::channel();

        // The delivering session is relieved and keeps going.
        handover.on_relieved(live_session);
        // Its replacement is relieved too, before ever going live.
        handover.on_relieved(unproven_session);

        assert_eq!(handover.waiting(), 2);
        assert!(
            live_rx.try_recv().is_err(),
            "the delivering session must still be running"
        );
    }

    /// Once something is proven to carry the feed, everything older is
    /// genuinely redundant — all of it, not just the most recent.
    #[test]
    fn going_live_retires_every_older_session() {
        let mut handover = Handover::default();
        let (first, mut first_rx) = oneshot::channel();
        let (second, mut second_rx) = oneshot::channel();

        handover.on_relieved(first);
        handover.on_relieved(second);
        handover.on_live();

        assert!(first_rx.try_recv().is_ok());
        assert!(second_rx.try_recv().is_ok());
        assert_eq!(handover.waiting(), 0);
    }

    /// Sessions the venue already closed cannot be retired, and holding their
    /// handles would let this grow without bound through a long rollout.
    #[test]
    fn sessions_that_already_ended_are_forgotten() {
        let mut handover = Handover::default();
        let (ended, ended_rx) = oneshot::channel();
        handover.on_relieved(ended);
        drop(ended_rx); // the session ran to its end on its own

        let (current, _current_rx) = oneshot::channel();
        handover.on_relieved(current);

        assert_eq!(handover.waiting(), 1);
    }

    /// Redundant connections must not re-synchronise their handovers a day
    /// after `CONNECT_STAGGER` spread them out.
    #[test]
    fn redundant_connections_hand_over_at_different_times() {
        assert_eq!(max_session_age(0), MAX_SESSION_AGE);
        assert_eq!(max_session_age(1), MAX_SESSION_AGE - SESSION_AGE_STAGGER);
        // Even the last of the eight the CLI allows stays well inside the
        // venue's 24-hour limit.
        assert!(max_session_age(7) > Duration::from_secs(20 * 60 * 60));
    }

    #[test]
    fn the_announcement_is_recognised() {
        assert!(is_server_shutdown(ANNOUNCEMENT.as_bytes()));
        assert!(is_server_shutdown(
            br#"{"stream":"btcusdt@depth","data":{"e":"serverShutdown","E":1}}"#
        ));
    }

    /// The word appearing anywhere in a message is not an announcement; only
    /// the event type is. Matching on the text would reconnect on an error
    /// message that merely mentions it.
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
