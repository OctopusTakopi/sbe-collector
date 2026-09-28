use std::collections::HashMap;

use tokio::sync::watch;

/// Whether the writer and every required source are live, published to the
/// handover supervisor.
#[derive(Clone)]
pub struct Readiness(watch::Sender<ReadinessState>);

#[derive(Debug)]
pub struct ReadinessState {
    required_sources: usize,
    /// Live sessions per logical source.
    live_sources: HashMap<String, usize>,
    writer_ready: bool,
    /// Bumped whenever a source loses its last live session. A watch can
    /// coalesce a drop-and-reconnect into "still ready"; the epoch cannot.
    pub outage_epoch: u64,
}

impl ReadinessState {
    pub fn is_ready(&self) -> bool {
        self.writer_ready && self.live_sources.len() >= self.required_sources
    }
}

/// One live session of a source; dropping it revokes that liveness.
pub struct SourceLease {
    readiness: Readiness,
    source: String,
}

impl Readiness {
    pub fn new(required_sources: usize) -> (Self, watch::Receiver<ReadinessState>) {
        let (tx, rx) = watch::channel(ReadinessState {
            required_sources,
            live_sources: HashMap::with_capacity(required_sources),
            writer_ready: false,
            outage_epoch: 0,
        });
        (Self(tx), rx)
    }

    /// Overlapping sessions of one source are counted, so retiring either
    /// cannot hide the other.
    pub fn source_live(&self, source: impl Into<String>) -> SourceLease {
        let source = source.into();
        self.0
            .send_modify(|state| match state.live_sources.get_mut(&source) {
                Some(sessions) => *sessions += 1,
                None => {
                    state.live_sources.insert(source.clone(), 1);
                }
            });
        SourceLease {
            readiness: self.clone(),
            source,
        }
    }

    pub fn mark_writer_ready(&self) {
        self.0.send_modify(|state| state.writer_ready = true);
    }
}

impl Drop for SourceLease {
    fn drop(&mut self) {
        self.readiness.0.send_modify(|state| {
            let Some(sessions) = state.live_sources.get_mut(&self.source) else {
                return;
            };
            *sessions -= 1;
            if *sessions == 0 {
                state.live_sources.remove(&self.source);
                state.outage_epoch += 1;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_requires_the_writer_and_every_distinct_source() {
        let (readiness, ready) = Readiness::new(2);
        let connection_0 = readiness.source_live("connection-0");
        let duplicate_0 = readiness.source_live("connection-0");
        readiness.mark_writer_ready();
        assert!(!ready.borrow().is_ready());
        let connection_1 = readiness.source_live("connection-1");
        assert!(ready.borrow().is_ready());
        drop(duplicate_0);
        assert!(ready.borrow().is_ready());
        drop(connection_1);
        assert!(!ready.borrow().is_ready());
        drop(connection_0);
    }

    #[test]
    fn only_losing_a_sources_last_session_is_an_outage() {
        let (readiness, ready) = Readiness::new(1);
        let first = readiness.source_live("source");
        let overlap = readiness.source_live("source");
        drop(first);
        assert_eq!(ready.borrow().outage_epoch, 0);
        drop(overlap);
        assert_eq!(ready.borrow().outage_epoch, 1);
    }
}
