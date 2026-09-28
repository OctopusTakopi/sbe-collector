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
    Incumbent(UnixListener),
    Challenger(UnixStream),
}

pub async fn run(
    path: PathBuf,
    identity: Identity,
    run_id: String,
    mut ready: watch::Receiver<ReadinessState>,
    takeover_tx: mpsc::Sender<TakeoverRequest>,
    quality: QualityReporter,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("cannot create UDS directory {}", parent.display()))?;
    }

    match select_role(&path).await? {
        Role::Incumbent(listener) => {
            server_loop(listener, &path, &identity, &run_id, takeover_tx, quality).await
        }
        Role::Challenger(mut stream) => {
            write_message(
                &mut stream,
                &Message::Hello {
                    protocol_version: PROTOCOL_VERSION,
                    run_id: run_id.clone(),
                    identity: identity.clone(),
                },
            )
            .await?;
            let incumbent_run_id = match timeout(HANDSHAKE_TIMEOUT, read_message(&mut stream)).await
            {
                Ok(Ok(Message::HelloAck { run_id })) => run_id,
                Ok(Ok(Message::Reject { reason })) => bail!("handover rejected: {reason}"),
                Ok(Ok(message)) => bail!("unexpected handover response: {message:?}"),
                Ok(Err(error)) => return Err(error),
                Err(_) => bail!("timed out waiting for handover acknowledgement"),
            };

            wait_until_stably_ready(&mut ready, OVERLAP_GRACE).await?;
            quality.report(QualityEvent::Handover {
                at_ns: QualityEvent::now_ns(),
                state: HandoverState::ChallengerReady,
                peer_run_id: Some(incumbent_run_id.clone()),
            });
            write_message(
                &mut stream,
                &Message::Ready {
                    run_id: run_id.clone(),
                },
            )
            .await?;

            let listener = match read_message(&mut stream).await {
                Ok(Message::Drained { clean, error }) => {
                    let outcome = drain_outcome(clean, error);
                    quality.report(QualityEvent::Handover {
                        at_ns: QualityEvent::now_ns(),
                        state: HandoverState::drained(outcome.is_ok()),
                        peer_run_id: Some(incumbent_run_id.clone()),
                    });
                    if let Err(error) = outcome {
                        tracing::error!(error, "incumbent reported a degraded drain");
                    }
                    bind_after_handover(&path).await?
                }
                Ok(Message::Reject { reason }) => {
                    bail!("handover rejected after readiness: {reason}")
                }
                Ok(message) => bail!("unexpected handover completion: {message:?}"),
                Err(error) => match select_role(&path).await? {
                    Role::Incumbent(listener) => {
                        tracing::warn!(%error, "incumbent disappeared after challenger readiness; promoted challenger");
                        quality.report(QualityEvent::Handover {
                            at_ns: QualityEvent::now_ns(),
                            state: HandoverState::IncumbentLostAfterReadyPromoted,
                            peer_run_id: Some(incumbent_run_id.clone()),
                        });
                        listener
                    }
                    Role::Challenger(_) => {
                        return Err(error).context(
                            "incumbent handover stream failed but the socket still has a live owner",
                        );
                    }
                },
            };
            server_loop(listener, &path, &identity, &run_id, takeover_tx, quality).await
        }
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
    listener: UnixListener,
    path: &Path,
    identity: &Identity,
    run_id: &str,
    takeover_tx: mpsc::Sender<TakeoverRequest>,
    quality: QualityReporter,
) -> anyhow::Result<()> {
    tracing::info!(path = %path.display(), "UDS handover listener ready");
    loop {
        let (mut stream, _) = listener.accept().await?;
        let peer_allowed = match same_uid(&stream, path) {
            Ok(allowed) => allowed,
            Err(error) => {
                tracing::warn!(%error, "cannot authenticate UDS handover peer");
                continue;
            }
        };
        if !peer_allowed {
            let _ = write_message(
                &mut stream,
                &Message::Reject {
                    reason: "peer uid does not own the handover socket".to_owned(),
                },
            )
            .await;
            continue;
        }

        let hello = match timeout(HANDSHAKE_TIMEOUT, read_message(&mut stream)).await {
            Ok(Ok(message)) => message,
            Ok(Err(error)) => {
                tracing::warn!(%error, "invalid UDS handover hello");
                continue;
            }
            Err(_) => {
                tracing::warn!("UDS handover peer timed out before hello");
                continue;
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
                if let Err(error) = write_message(
                    &mut stream,
                    &Message::Reject {
                        reason: format!(
                            "protocol/configuration mismatch (peer protocol {protocol_version})"
                        ),
                    },
                )
                .await
                {
                    tracing::warn!(%error, "handover peer disconnected before rejection");
                }
                continue;
            }
            message => {
                if let Err(error) = write_message(
                    &mut stream,
                    &Message::Reject {
                        reason: format!("expected hello, got {message:?}"),
                    },
                )
                .await
                {
                    tracing::warn!(%error, "handover peer disconnected before rejection");
                }
                continue;
            }
        };

        if let Err(error) = write_message(
            &mut stream,
            &Message::HelloAck {
                run_id: run_id.to_owned(),
            },
        )
        .await
        {
            tracing::warn!(%error, "handover challenger disconnected before acknowledgement");
            continue;
        }
        let ready = match timeout(READY_TIMEOUT, read_message(&mut stream)).await {
            Ok(Ok(Message::Ready { run_id })) if run_id == peer_run_id => true,
            Ok(Ok(message)) => {
                tracing::warn!(?message, "handover peer did not send matching readiness");
                false
            }
            Ok(Err(error)) => {
                tracing::warn!(%error, "handover challenger disconnected before readiness");
                false
            }
            Err(_) => {
                tracing::warn!("handover challenger did not become ready in time");
                false
            }
        };
        if !ready {
            continue;
        }

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
}

async fn select_role(path: &Path) -> anyhow::Result<Role> {
    for attempt in 0..3 {
        match UnixStream::connect(path).await {
            Ok(stream) => return Ok(Role::Challenger(stream)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return match bind_listener(path) {
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
    match bind_listener(path) {
        Ok(listener) => Ok(Role::Incumbent(listener)),
        Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
            Ok(Role::Challenger(UnixStream::connect(path).await?))
        }
        Err(error) => Err(error.into()),
    }
}

async fn bind_after_handover(path: &Path) -> anyhow::Result<UnixListener> {
    timeout(HANDSHAKE_TIMEOUT, async {
        loop {
            match bind_listener(path) {
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

fn bind_listener(path: &Path) -> io::Result<UnixListener> {
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
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

async fn write_message(stream: &mut UnixStream, message: &Message) -> anyhow::Result<()> {
    let body = serde_json::to_vec(message)?;
    if body.len() > MAX_MESSAGE_BYTES {
        bail!("handover message is too large: {} bytes", body.len());
    }
    stream.write_all(&(body.len() as u32).to_be_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_message(stream: &mut UnixStream) -> anyhow::Result<Message> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length).await?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_MESSAGE_BYTES {
        bail!("handover message exceeds {MAX_MESSAGE_BYTES} bytes");
    }
    let mut body = vec![0_u8; length];
    stream.read_exact(&mut body).await?;
    Ok(serde_json::from_slice(&body)?)
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
        _readiness: Readiness,
        _source: Option<SourceLease>,
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
            let task = tokio::spawn(run(
                self.path.clone(),
                self.identity.clone(),
                run_id.to_owned(),
                ready_rx,
                takeover_tx,
                QualityReporter::disabled(),
            ));
            Peer {
                task,
                takeover,
                _readiness: readiness,
                _source: source,
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
    async fn mismatched_configuration_cannot_retire_the_incumbent() {
        let fixture = Fixture::new("mismatch");
        let mut old = fixture.start_incumbent().await;

        let mut mismatch = fixture.identity.clone();
        mismatch.symbols.push("ethusdt".to_owned());
        let (_readiness, ready) = Readiness::new(1);
        let (takeover_tx, _takeover) = mpsc::channel(1);
        let error = run(
            fixture.path.clone(),
            mismatch,
            "new".to_owned(),
            ready,
            takeover_tx,
            QualityReporter::disabled(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("rejected"));
        assert!(old.takeover.try_recv().is_err());
        assert!(!old.task.is_finished());
        stop(old).await;
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
