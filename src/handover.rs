use std::{
    io,
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, anyhow, bail};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::{mpsc, oneshot, watch},
    time::timeout,
};

use crate::{
    quality::{HandoverState, QualityEvent, QualityReporter},
    readiness::ReadinessState,
};

const PROTOCOL_VERSION: u32 = 1;
const MAX_MESSAGE_BYTES: usize = 64 * 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const READY_TIMEOUT: Duration = Duration::from_secs(180);
const OVERLAP_GRACE: Duration = Duration::from_secs(2);
const BIND_RETRY: Duration = Duration::from_millis(10);
/// A challenger whose incumbent vanished before draining chooses its role
/// again after this pause, at most `MAX_RESELECTIONS` times, so a flapping
/// peer cannot keep it spinning.
const RESELECT_DELAY: Duration = Duration::from_millis(250);
const MAX_RESELECTIONS: u32 = 5;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    pub exchange: String,
    pub symbols: Vec<String>,
    pub connections: usize,
    pub output: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Message {
    Hello {
        protocol_version: u32,
        run_id: String,
        identity: Identity,
    },
    HelloAck {
        run_id: String,
    },
    Ready {
        run_id: String,
    },
    Drained {
        clean: bool,
        error: Option<String>,
    },
    Reject {
        reason: String,
    },
}

impl Message {
    fn drained(outcome: DrainOutcome) -> Self {
        let (clean, error) = match outcome {
            Ok(()) => (true, None),
            Err(error) => (false, Some(error)),
        };
        Self::Drained { clean, error }
    }
}

/// How the incumbent's drain went; `Err` says why it was degraded.
pub type DrainOutcome = Result<(), String>;

fn drain_outcome(clean: bool, error: Option<String>) -> DrainOutcome {
    if clean {
        Ok(())
    } else {
        Err(error.unwrap_or_default())
    }
}

pub struct TakeoverRequest {
    pub peer_run_id: String,
    complete: oneshot::Sender<DrainOutcome>,
}

impl TakeoverRequest {
    pub fn complete(self, outcome: DrainOutcome) {
        let _ = self.complete.send(outcome);
    }
}

enum Role {
    Incumbent(Listener),
    Challenger(UnixStream),
}

/// A bound handover socket and the inode its bind created. Releasing it
/// unlinks the path only while the path still names that inode: once this
/// process stops answering, another may clear the path as stale and bind its
/// own socket there.
struct Listener {
    socket: UnixListener,
    inode: (u64, u64),
}

impl Listener {
    fn bind(path: &Path) -> io::Result<Self> {
        let socket = UnixListener::bind(path)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        let metadata = std::fs::symlink_metadata(path)?;
        Ok(Self {
            socket,
            inode: (metadata.dev(), metadata.ino()),
        })
    }

    /// Unlink `path` if it still names this socket, then stop listening.
    fn release(self, path: &Path) {
        let unlinked = match std::fs::symlink_metadata(path) {
            Ok(metadata) if (metadata.dev(), metadata.ino()) == self.inode => {
                std::fs::remove_file(path)
            }
            Ok(_) => {
                tracing::info!(
                    path = %path.display(),
                    "handover socket path now names another socket; leaving it"
                );
                Ok(())
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        };
        if let Err(error) = unlinked {
            tracing::warn!(path = %path.display(), %error, "failed to unlink handover socket");
        }
    }
}

pub async fn run(
    path: PathBuf,
    identity: Identity,
    run_id: String,
    mut ready: watch::Receiver<ReadinessState>,
    takeover_tx: mpsc::Sender<TakeoverRequest>,
    quality: QualityReporter,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("cannot create UDS directory {}", parent.display()))?;
    }

    let mut reselections = 0;
    // The incumbent of the last challenge, if it vanished after this
    // process reported ready.
    let mut lost_after_ready = None;
    let listener = loop {
        let lost = match select_role(&path).await? {
            Role::Incumbent(listener) => {
                if let Some(incumbent_run_id) = lost_after_ready {
                    tracing::warn!(
                        "incumbent disappeared after challenger readiness; promoted challenger"
                    );
                    quality.report(QualityEvent::Handover {
                        at_ns: QualityEvent::now_ns(),
                        state: HandoverState::IncumbentLostAfterReadyPromoted,
                        peer_run_id: Some(incumbent_run_id),
                    });
                }
                break listener;
            }
            Role::Challenger(stream) => {
                let outcome = challenge(
                    stream,
                    &path,
                    &identity,
                    &run_id,
                    &mut ready,
                    &quality,
                    &mut shutdown,
                )
                .await?;
                match outcome {
                    Challenge::TookOver(listener) => break listener,
                    Challenge::Stopped => return Ok(()),
                    Challenge::Lost(lost) => lost,
                }
            }
        };
        reselections += 1;
        if reselections > MAX_RESELECTIONS {
            return Err(lost.error).context(format!(
                "handover incumbent disappeared {reselections} times before draining"
            ));
        }
        tracing::warn!(
            error = %lost.error,
            "handover incumbent disappeared before draining; choosing a role again"
        );
        lost_after_ready = lost.after_ready;
        tokio::select! {
            biased;
            () = stop_requested(&mut shutdown) => return Ok(()),
            () = tokio::time::sleep(RESELECT_DELAY) => {}
        }
    };
    server_loop(
        listener,
        &path,
        &identity,
        &run_id,
        takeover_tx,
        quality,
        &mut shutdown,
    )
    .await
}

/// How a challenge that did not fail ended.
enum Challenge {
    /// The incumbent drained and this process bound the socket.
    TookOver(Listener),
    /// This process was asked to stop before it reported ready. Retiring the
    /// incumbent now would leave nothing collecting.
    Stopped,
    /// The incumbent went away before reporting its drain.
    Lost(Lost),
}

struct Lost {
    error: io::Error,
    /// The incumbent's run id, if it was told this process is ready.
    after_ready: Option<String>,
}

impl Challenge {
    /// A peer that is gone lost the challenge; one that broke the protocol
    /// fails it.
    fn lost(error: WireError, after_ready: Option<String>) -> anyhow::Result<Self> {
        match error {
            WireError::Gone(error) => Ok(Self::Lost(Lost { error, after_ready })),
            WireError::Invalid(error) => Err(anyhow!(error)),
        }
    }
}

/// Ask the incumbent to drain once this process is stably ready.
async fn challenge(
    mut stream: UnixStream,
    path: &Path,
    identity: &Identity,
    run_id: &str,
    ready: &mut watch::Receiver<ReadinessState>,
    quality: &QualityReporter,
    shutdown: &mut watch::Receiver<bool>,
) -> anyhow::Result<Challenge> {
    let hello = Message::Hello {
        protocol_version: PROTOCOL_VERSION,
        run_id: run_id.to_owned(),
        identity: identity.clone(),
    };
    if let Err(error) = write_message(&mut stream, &hello).await {
        return Challenge::lost(error, None);
    }
    let acknowledgement = tokio::select! {
        biased;
        () = stop_requested(shutdown) => return Ok(Challenge::Stopped),
        acknowledgement = timeout(HANDSHAKE_TIMEOUT, read_message(&mut stream)) => acknowledgement,
    };
    let incumbent_run_id = match acknowledgement {
        Ok(Ok(Message::HelloAck { run_id })) => run_id,
        Ok(Ok(Message::Reject { reason })) => bail!("handover rejected: {reason}"),
        Ok(Ok(message)) => bail!("unexpected handover response: {message:?}"),
        Ok(Err(error)) => return Challenge::lost(error, None),
        Err(_) => bail!("timed out waiting for handover acknowledgement"),
    };

    // The incumbent sends nothing before `Drained`, so anything readable
    // while warming up is EOF or a protocol violation, and cancelling the
    // read loses nothing.
    tokio::select! {
        biased;
        () = stop_requested(shutdown) => return Ok(Challenge::Stopped),
        result = wait_until_stably_ready(ready, OVERLAP_GRACE) => result?,
        early = read_message(&mut stream) => match early {
            Err(error) => return Challenge::lost(error, None),
            Ok(Message::Reject { reason }) => bail!("handover rejected: {reason}"),
            Ok(message) => bail!("unexpected handover message before readiness: {message:?}"),
        },
    }
    quality.report(QualityEvent::Handover {
        at_ns: QualityEvent::now_ns(),
        state: HandoverState::ChallengerReady,
        peer_run_id: Some(incumbent_run_id.clone()),
    });
    let ready_message = Message::Ready {
        run_id: run_id.to_owned(),
    };
    if let Err(error) = write_message(&mut stream, &ready_message).await {
        return Challenge::lost(error, None);
    }

    match read_message(&mut stream).await {
        Ok(Message::Drained { clean, error }) => {
            let outcome = drain_outcome(clean, error);
            quality.report(QualityEvent::Handover {
                at_ns: QualityEvent::now_ns(),
                state: HandoverState::drained(outcome.is_ok()),
                peer_run_id: Some(incumbent_run_id),
            });
            if let Err(error) = outcome {
                tracing::error!(error, "incumbent reported a degraded drain");
            }
            Ok(Challenge::TookOver(bind_after_handover(path).await?))
        }
        Ok(Message::Reject { reason }) => bail!("handover rejected after readiness: {reason}"),
        Ok(message) => bail!("unexpected handover completion: {message:?}"),
        Err(error) => Challenge::lost(error, Some(incumbent_run_id)),
    }
}

async fn wait_until_stably_ready(
    ready: &mut watch::Receiver<ReadinessState>,
    stable_for: Duration,
) -> anyhow::Result<()> {
    timeout(READY_TIMEOUT, async {
        loop {
            while !ready.borrow_and_update().is_ready() {
                ready
                    .changed()
                    .await
                    .map_err(|_| anyhow!("readiness sender closed"))?;
            }

            let outage_epoch = ready.borrow().outage_epoch;
            let stable = tokio::time::sleep(stable_for);
            tokio::pin!(stable);
            loop {
                tokio::select! {
                    biased;
                    result = ready.changed() => {
                        result.map_err(|_| anyhow!("readiness sender closed"))?;
                        let state = ready.borrow_and_update();
                        if !state.is_ready() || state.outage_epoch != outage_epoch {
                            break;
                        }
                    }
                    _ = &mut stable => return Ok::<_, anyhow::Error>(()),
                }
            }
        }
    })
    .await
    .map_err(|_| anyhow!("new collector did not become ready within {READY_TIMEOUT:?}"))??;
    Ok(())
}

async fn server_loop(
    listener: Listener,
    path: &Path,
    identity: &Identity,
    run_id: &str,
    takeover_tx: mpsc::Sender<TakeoverRequest>,
    quality: QualityReporter,
    shutdown: &mut watch::Receiver<bool>,
) -> anyhow::Result<()> {
    tracing::info!(path = %path.display(), "UDS handover listener ready");
    loop {
        let mut stream = tokio::select! {
            biased;
            () = stop_requested(shutdown) => break,
            accepted = listener.socket.accept() => accepted?.0,
        };
        // Shutting down drops a challenger mid-handshake: it sees EOF and
        // chooses its role again.
        let peer_run_id = tokio::select! {
            biased;
            () = stop_requested(shutdown) => break,
            peer = negotiate(&mut stream, path, identity, run_id) => match peer {
                Some(peer_run_id) => peer_run_id,
                None => continue,
            },
        };

        quality.report(QualityEvent::Handover {
            at_ns: QualityEvent::now_ns(),
            state: HandoverState::TakeoverRequested,
            peer_run_id: Some(peer_run_id.clone()),
        });
        let (complete_tx, complete_rx) = oneshot::channel();
        takeover_tx
            .send(TakeoverRequest {
                peer_run_id,
                complete: complete_tx,
            })
            .await
            .map_err(|_| anyhow!("collector supervisor is unavailable"))?;
        let outcome = complete_rx
            .await
            .map_err(|_| anyhow!("collector stopped without completing handover"))?;

        // Unlink only after the old writer is fully drained. The listener stays
        // alive for this final response, while the challenger can bind the path
        // immediately after receiving it.
        remove_socket(path)?;
        if let Err(error) = write_message(&mut stream, &Message::drained(outcome)).await {
            tracing::warn!(%error, "handover challenger disconnected before drain report");
        }
        return Ok(());
    }
    tracing::info!(path = %path.display(), "shutting down; releasing the handover socket");
    listener.release(path);
    Ok(())
}

/// Resolves once the process is asked to stop; never if it no longer can be.
async fn stop_requested(shutdown: &mut watch::Receiver<bool>) {
    if shutdown.wait_for(|&stop| stop).await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// Authenticate a connected challenger, check that it records the same
/// streams, and wait for its readiness. Its run id once it is ready; `None`
/// if it was turned away or left.
async fn negotiate(
    stream: &mut UnixStream,
    path: &Path,
    identity: &Identity,
    run_id: &str,
) -> Option<String> {
    let peer_allowed = match same_uid(stream, path) {
        Ok(allowed) => allowed,
        Err(error) => {
            tracing::warn!(%error, "cannot authenticate UDS handover peer");
            return None;
        }
    };
    if !peer_allowed {
        let _ = write_message(
            stream,
            &Message::Reject {
                reason: "peer uid does not own the handover socket".to_owned(),
            },
        )
        .await;
        return None;
    }

    let hello = match timeout(HANDSHAKE_TIMEOUT, read_message(stream)).await {
        Ok(Ok(message)) => message,
        Ok(Err(error)) => {
            tracing::warn!(%error, "invalid UDS handover hello");
            return None;
        }
        Err(_) => {
            tracing::warn!("UDS handover peer timed out before hello");
            return None;
        }
    };
    let peer_run_id = match hello {
        Message::Hello {
            protocol_version,
            run_id: peer_run_id,
            identity: peer_identity,
        } if protocol_version == PROTOCOL_VERSION && peer_identity == *identity => peer_run_id,
        Message::Hello {
            protocol_version, ..
        } => {
            reject(
                stream,
                format!("protocol/configuration mismatch (peer protocol {protocol_version})"),
            )
            .await;
            return None;
        }
        message => {
            reject(stream, format!("expected hello, got {message:?}")).await;
            return None;
        }
    };

    if let Err(error) = write_message(
        stream,
        &Message::HelloAck {
            run_id: run_id.to_owned(),
        },
    )
    .await
    {
        tracing::warn!(%error, "handover challenger disconnected before acknowledgement");
        return None;
    }
    match timeout(READY_TIMEOUT, read_message(stream)).await {
        Ok(Ok(Message::Ready { run_id })) if run_id == peer_run_id => Some(peer_run_id),
        Ok(Ok(message)) => {
            tracing::warn!(?message, "handover peer did not send matching readiness");
            None
        }
        Ok(Err(error)) => {
            tracing::warn!(%error, "handover challenger disconnected before readiness");
            None
        }
        Err(_) => {
            tracing::warn!("handover challenger did not become ready in time");
            None
        }
    }
}

async fn reject(stream: &mut UnixStream, reason: String) {
    if let Err(error) = write_message(stream, &Message::Reject { reason }).await {
        tracing::warn!(%error, "handover peer disconnected before rejection");
    }
}

async fn select_role(path: &Path) -> anyhow::Result<Role> {
    for attempt in 0..3 {
        match UnixStream::connect(path).await {
            Ok(stream) => return Ok(Role::Challenger(stream)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return match Listener::bind(path) {
                    Ok(listener) => Ok(Role::Incumbent(listener)),
                    Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
                        Ok(Role::Challenger(UnixStream::connect(path).await?))
                    }
                    Err(error) => Err(error.into()),
                };
            }
            Err(error) if error.kind() == io::ErrorKind::ConnectionRefused && attempt < 2 => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => break,
            Err(error) => return Err(error.into()),
        }
    }

    // Three refused connections identify a stale pathname left by an
    // ungraceful exit; a merely racing incumbent was given time to listen.
    remove_socket(path)?;
    match Listener::bind(path) {
        Ok(listener) => Ok(Role::Incumbent(listener)),
        Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
            Ok(Role::Challenger(UnixStream::connect(path).await?))
        }
        Err(error) => Err(error.into()),
    }
}

async fn bind_after_handover(path: &Path) -> anyhow::Result<Listener> {
    timeout(HANDSHAKE_TIMEOUT, async {
        loop {
            match Listener::bind(path) {
                Ok(listener) => return Ok(listener),
                Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
                    tokio::time::sleep(BIND_RETRY).await;
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .map_err(|_| anyhow!("timed out taking ownership of {}", path.display()))?
    .map_err(Into::into)
}

fn remove_socket(path: &Path) -> anyhow::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            std::fs::remove_file(path)?;
            Ok(())
        }
        Ok(_) => bail!("refusing to remove non-socket path {}", path.display()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn same_uid(stream: &UnixStream, path: &Path) -> anyhow::Result<bool> {
    Ok(stream.peer_cred()?.uid() == std::fs::metadata(path)?.uid())
}

/// Why a message could not be exchanged.
#[derive(Debug, thiserror::Error)]
enum WireError {
    /// EOF or a socket error: the peer is gone.
    #[error("handover peer is gone: {0}")]
    Gone(#[from] io::Error),
    /// The peer is there but broke the protocol.
    #[error("{0}")]
    Invalid(String),
}

async fn write_message(stream: &mut UnixStream, message: &Message) -> Result<(), WireError> {
    let body = serde_json::to_vec(message)
        .map_err(|error| WireError::Invalid(format!("cannot encode handover message: {error}")))?;
    if body.len() > MAX_MESSAGE_BYTES {
        return Err(WireError::Invalid(format!(
            "handover message is too large: {} bytes",
            body.len()
        )));
    }
    stream.write_all(&(body.len() as u32).to_be_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_message(stream: &mut UnixStream) -> Result<Message, WireError> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length).await?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_MESSAGE_BYTES {
        return Err(WireError::Invalid(format!(
            "handover message exceeds {MAX_MESSAGE_BYTES} bytes"
        )));
    }
    let mut body = vec![0_u8; length];
    stream.read_exact(&mut body).await?;
    serde_json::from_slice(&body)
        .map_err(|error| WireError::Invalid(format!("invalid handover message: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::readiness::{Readiness, SourceLease};

    #[tokio::test]
    async fn readiness_must_remain_live_for_the_whole_overlap() {
        let (readiness, mut ready) = Readiness::new(1);
        readiness.mark_writer_ready();
        let first = readiness.source_live("source");
        let mut waiter = tokio::spawn(async move {
            wait_until_stably_ready(&mut ready, Duration::from_millis(200)).await
        });

        tokio::time::sleep(Duration::from_millis(80)).await;
        drop(first);
        // Reconnect without yielding, so only the outage epoch shows it.
        let _second = readiness.source_live("source");
        assert!(
            timeout(Duration::from_millis(150), &mut waiter)
                .await
                .is_err(),
            "a disconnected challenger must not complete the original grace"
        );

        timeout(Duration::from_millis(100), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    /// A socket path and the identity every peer in a test shares.
    struct Fixture {
        dir: PathBuf,
        path: PathBuf,
        identity: Identity,
    }

    /// One running handover supervisor.
    struct Peer {
        task: tokio::task::JoinHandle<anyhow::Result<()>>,
        takeover: mpsc::Receiver<TakeoverRequest>,
        readiness: Readiness,
        _source: Option<SourceLease>,
        shutdown: watch::Sender<bool>,
    }

    impl Fixture {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "sbe-handover-{tag}-test-{}-{}",
                std::process::id(),
                QualityEvent::now_ns()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            Self {
                path: dir.join("collector.sock"),
                identity: Identity {
                    exchange: "test".to_owned(),
                    symbols: vec!["btcusdt".to_owned()],
                    connections: 1,
                    output: dir.display().to_string(),
                },
                dir,
            }
        }

        /// A challenger is `ready` from the start: writer up, source live.
        fn start(&self, run_id: &str, ready: bool) -> Peer {
            let (readiness, ready_rx) = Readiness::new(1);
            let source = ready.then(|| {
                readiness.mark_writer_ready();
                readiness.source_live("source")
            });
            let (takeover_tx, takeover) = mpsc::channel(1);
            let (shutdown, shutdown_rx) = watch::channel(false);
            let task = tokio::spawn(run(
                self.path.clone(),
                self.identity.clone(),
                run_id.to_owned(),
                ready_rx,
                takeover_tx,
                QualityReporter::disabled(),
                shutdown_rx,
            ));
            Peer {
                task,
                takeover,
                readiness,
                _source: source,
                shutdown,
            }
        }

        async fn start_incumbent(&self) -> Peer {
            let incumbent = self.start("old", false);
            timeout(Duration::from_secs(2), async {
                while !self.path.exists() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            incumbent
        }

        async fn wait_until_listening(&self) {
            timeout(Duration::from_secs(2), async {
                while UnixStream::connect(&self.path).await.is_err() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
    }

    /// A handover supervisor on its own thread and runtime, as a separate
    /// process runs it. Never ready, so as a challenger it stays
    /// mid-handshake.
    struct Process {
        thread: std::thread::JoinHandle<anyhow::Result<()>>,
        shutdown: watch::Sender<bool>,
    }

    impl Process {
        fn spawn(fixture: &Fixture, run_id: &str) -> Self {
            let (shutdown, shutdown_rx) = watch::channel(false);
            let path = fixture.path.clone();
            let identity = fixture.identity.clone();
            let run_id = run_id.to_owned();
            let thread = std::thread::spawn(move || {
                let (_readiness, ready) = Readiness::new(1);
                let (takeover_tx, _takeover) = mpsc::channel(1);
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?
                    .block_on(run(
                        path,
                        identity,
                        run_id,
                        ready,
                        takeover_tx,
                        QualityReporter::disabled(),
                        shutdown_rx,
                    ))
            });
            Self { thread, shutdown }
        }

        /// Request shutdown, as a signal does, and wait for the supervisor.
        fn shut_down(self) -> anyhow::Result<()> {
            self.shutdown.send_replace(true);
            self.thread.join().unwrap()
        }
    }

    /// Blocking, for peers on their own threads.
    impl Fixture {
        fn wait_for_socket(&self) {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while !self.path.exists() {
                assert!(std::time::Instant::now() < deadline, "no socket appeared");
                std::thread::sleep(Duration::from_millis(5));
            }
        }

        /// The run id of whoever answers on the socket once someone does,
        /// asked as a challenger would.
        fn owner(&self) -> String {
            use std::io::{Read, Write};
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let mut stream = loop {
                match std::os::unix::net::UnixStream::connect(&self.path) {
                    Ok(stream) => break stream,
                    Err(error) => {
                        assert!(std::time::Instant::now() < deadline, "no owner: {error}");
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let hello = serde_json::to_vec(&Message::Hello {
                protocol_version: PROTOCOL_VERSION,
                run_id: "probe".to_owned(),
                identity: self.identity.clone(),
            })
            .unwrap();
            stream
                .write_all(&(hello.len() as u32).to_be_bytes())
                .unwrap();
            stream.write_all(&hello).unwrap();
            let mut length = [0; 4];
            stream.read_exact(&mut length).unwrap();
            let mut body = vec![0; u32::from_be_bytes(length) as usize];
            stream.read_exact(&mut body).unwrap();
            match serde_json::from_slice(&body).unwrap() {
                Message::HelloAck { run_id } => run_id,
                message => panic!("expected an acknowledgement, got {message:?}"),
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    async fn stop(peer: Peer) {
        peer.task.abort();
        let _ = peer.task.await;
    }

    #[tokio::test]
    async fn a_ready_challenger_causes_a_drained_takeover() {
        let fixture = Fixture::new("takeover");
        let mut old = fixture.start_incumbent().await;
        let new = fixture.start("new", true);

        let request = timeout(Duration::from_secs(5), old.takeover.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.peer_run_id, "new");
        request.complete(Ok(()));
        timeout(Duration::from_secs(2), old.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        fixture.wait_until_listening().await;
        stop(new).await;
    }

    #[tokio::test]
    async fn ready_challenger_promotes_when_incumbent_disappears() {
        let fixture = Fixture::new("promotion");
        let mut old = fixture.start_incumbent().await;
        let new = fixture.start("new", true);

        let request = timeout(Duration::from_secs(5), old.takeover.recv())
            .await
            .unwrap()
            .unwrap();
        drop(request);
        assert!(
            timeout(Duration::from_secs(2), old.task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );

        fixture.wait_until_listening().await;
        assert!(!new.task.is_finished());
        stop(new).await;
    }

    #[tokio::test]
    async fn challenger_told_to_stop_before_ready_leaves_the_incumbent_alone() {
        let fixture = Fixture::new("challenger-stop");
        let mut old = fixture.start_incumbent().await;
        let new = fixture.start("new", false);
        // Acknowledged and warming up.
        tokio::time::sleep(Duration::from_millis(200)).await;
        new.shutdown.send_replace(true);
        // Readiness that arrives now must not retire the incumbent.
        new.readiness.mark_writer_ready();
        let _source = new.readiness.source_live("source");

        timeout(Duration::from_secs(1), new.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(old.takeover.try_recv().is_err());
        assert!(!old.task.is_finished());
        fixture.wait_until_listening().await;
        stop(old).await;
    }

    #[tokio::test]
    async fn mismatched_configuration_cannot_retire_the_incumbent() {
        let fixture = Fixture::new("mismatch");
        let mut old = fixture.start_incumbent().await;

        let mut mismatch = fixture.identity.clone();
        mismatch.symbols.push("ethusdt".to_owned());
        let (_readiness, ready) = Readiness::new(1);
        let (takeover_tx, _takeover) = mpsc::channel(1);
        let (_shutdown, shutdown) = watch::channel(false);
        let error = run(
            fixture.path.clone(),
            mismatch,
            "new".to_owned(),
            ready,
            takeover_tx,
            QualityReporter::disabled(),
            shutdown,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("rejected"));
        assert!(old.takeover.try_recv().is_err());
        assert!(!old.task.is_finished());
        stop(old).await;
    }

    #[test]
    fn incumbent_shutdown_unlinks_its_socket() {
        let fixture = Fixture::new("release");
        let old = Process::spawn(&fixture, "old");
        fixture.wait_for_socket();
        old.shut_down().unwrap();
        assert!(!fixture.path.exists());
    }

    #[test]
    fn challenger_mid_handshake_takes_over_from_an_incumbent_that_shut_down() {
        let fixture = Fixture::new("reselect");
        let old = Process::spawn(&fixture, "old");
        fixture.wait_for_socket();
        let new = Process::spawn(&fixture, "new");
        // Long enough to be acknowledged; it never becomes ready.
        std::thread::sleep(Duration::from_millis(200));
        old.shut_down().unwrap();

        assert_eq!(fixture.owner(), "new");
        assert!(!new.thread.is_finished());
        new.shut_down().unwrap();
        assert!(!fixture.path.exists());
    }

    #[test]
    fn incumbent_shutdown_leaves_a_socket_bound_since() {
        let fixture = Fixture::new("rebound");
        let old = Process::spawn(&fixture, "old");
        fixture.wait_for_socket();
        // Another process cleared the path as stale and bound its own socket.
        std::fs::remove_file(&fixture.path).unwrap();
        let _other = std::os::unix::net::UnixListener::bind(&fixture.path).unwrap();
        let other = std::fs::metadata(&fixture.path).unwrap().ino();
        old.shut_down().unwrap();
        assert_eq!(std::fs::metadata(&fixture.path).unwrap().ino(), other);
    }

    #[test]
    fn drain_outcomes_round_trip_the_wire_fields() {
        let Message::Drained { clean, error } = Message::drained(Err("disk full".to_owned()))
        else {
            unreachable!();
        };
        assert!(!clean);
        assert_eq!(error.as_deref(), Some("disk full"));
        assert_eq!(drain_outcome(clean, error), Err("disk full".to_owned()));

        let Message::Drained { clean, error } = Message::drained(Ok(())) else {
            unreachable!();
        };
        assert!(clean);
        assert_eq!(error, None);
        assert_eq!(drain_outcome(clean, error), Ok(()));
    }
}
