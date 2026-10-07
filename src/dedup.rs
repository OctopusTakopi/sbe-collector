//! Duplicate suppression for redundant websocket connections.
//!
//! Every connection subscribes to the same streams, so while one reconnects
//! the others keep delivering, and each message arrives once per healthy
//! connection. Exactly one copy is kept.
//!
//! # Key
//!
//! The frame minus the bytes its venue stamps per connection
//! ([`Exchange::stamp`], hashed by [`event_key`]). Binance stamps `eventTime`
//! when it serialises for a particular connection: redundant connections
//! receive one event with stamps 18-37 us apart (measured on live spot data),
//! so a whole-frame key recognises nothing. The rest still separates distinct
//! events: depth diffs carry their update id range, depth snapshots and
//! bestBidAsk an update id, trades a transact time and trade ids. On a
//! single-connection recording, where every frame is distinct, 11,446 frames
//! across two symbols gave zero collisions.
//!
//! The residual risk is a template that repeats unchanged but for its event
//! time, such as a partial-book snapshot pushed on a timer while the book
//! sits still: the repeat is dropped as a duplicate, without a log line.
//!
//! # Ordering
//!
//! Keeping the first copy preserves the merged stream's order. Each
//! connection delivers in order, so for events `m` before `n`,
//! `first(m) = min over connections of arrival(m)` precedes `first(n)`: every
//! term of the first minimum precedes the matching term of the second. The
//! kept timestamp is also the lowest latency any connection achieved, except
//! that the filter sees dequeue order: once the shared queue is full, senders
//! win permits in semaphore wake order.
//!
//! The exception is a connection that reconnects ahead of a lagging peer and
//! delivers an event whose predecessor has not arrived anywhere yet. The
//! depth continuity check reads that as a gap and refetches a snapshot, which
//! is the right repair.

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use sbe_collector::{exchange::Exchange, sbe::event_key};
use tracing::{info, warn};

/// How far back duplicates are remembered: the delivery skew between
/// connections, milliseconds in practice, with room for one that stalls on a
/// slow TCP path and catches up.
pub const DEDUP_WINDOW: Duration = Duration::from_secs(30);

/// Hard ceiling on keys per generation, so an unexpected message rate cannot
/// grow memory without bound. Reaching it rotates early, shortening the
/// window below [`DEDUP_WINDOW`].
///
/// `hashbrown` rounds to a power of two at a 7/8 load factor, so 500k keys
/// reserve 2^20 buckets of 17 bytes per generation: about 36 MiB for both,
/// retained once reached.
pub const DEDUP_MAX_ENTRIES: usize = 500_000;

const REPORT_INTERVAL: Duration = Duration::from_secs(300);

/// Messages between clock reads; rotation only has to be roughly on time.
///
/// On a sparse feed a generation therefore outlives its nominal duration,
/// which widens the window but never lets a duplicate through, and the rate
/// report falls silent with the feed instead of reporting zero.
const CLOCK_CHECK_INTERVAL: u32 = 1_024;

/// Drops the second and later copies of a frame.
///
/// Keys live in two generations that rotate on a timer, and a lookup checks
/// both, so a key is remembered for at least [`DEDUP_WINDOW`] with no
/// timestamp per key and no expiry walk. The upper bound is soft: nominally
/// twice the window, longer on a sparse feed, shorter when
/// [`DEDUP_MAX_ENTRIES`] forces a rotation.
pub struct Dedup {
    /// Checked before hashing, so a disabled filter costs one branch.
    enabled: bool,
    /// Whose per-connection stamps the key leaves out.
    exchange: Exchange,
    current: HashSet<u128>,
    previous: HashSet<u128>,
    window: Duration,
    max_entries: usize,
    rotate_at: Instant,
    since_clock_check: u32,
    unique: u64,
    duplicate: u64,
    last_report: Instant,
    last_ceiling_warning: Option<Instant>,
}

impl Dedup {
    /// A no-op filter, for a single connection.
    pub fn disabled(exchange: Exchange) -> Self {
        Self::build(false, exchange, DEDUP_WINDOW, DEDUP_MAX_ENTRIES)
    }

    pub fn new(exchange: Exchange, window: Duration, max_entries: usize) -> Self {
        Self::build(true, exchange, window, max_entries)
    }

    /// Enabled only when more than one connection can deliver the same frame.
    ///
    /// A session handover overlaps two sockets on one connection too, so a few
    /// frames a day are recorded twice. That is the cheaper error: filtering
    /// unconditionally would silently drop every timer-pushed snapshot that
    /// repeats while the book sits still. A repeat can be discarded when the
    /// recording is read; a frame never written cannot be recovered.
    pub fn for_connections(connections: usize, exchange: Exchange) -> Self {
        if connections > 1 {
            Self::new(exchange, DEDUP_WINDOW, DEDUP_MAX_ENTRIES)
        } else {
            Self::disabled(exchange)
        }
    }

    fn build(enabled: bool, exchange: Exchange, window: Duration, max_entries: usize) -> Self {
        let now = Instant::now();
        Self {
            enabled,
            exchange,
            current: HashSet::new(),
            previous: HashSet::new(),
            window,
            max_entries,
            rotate_at: now + window,
            since_clock_check: 0,
            unique: 0,
            duplicate: 0,
            last_report: now,
            last_ceiling_warning: None,
        }
    }

    /// The frame without its venue's per-connection stamp.
    fn key(&self, frame: &[u8]) -> u128 {
        event_key(frame, self.exchange.frame_stamp(frame))
    }

    /// True if this frame's event was already seen inside the window. A
    /// `false` return records it, so call this once per frame.
    pub fn is_duplicate(&mut self, frame: &[u8]) -> bool {
        if !self.enabled {
            return false;
        }

        self.since_clock_check += 1;
        if self.since_clock_check >= CLOCK_CHECK_INTERVAL || self.current.len() >= self.max_entries
        {
            self.since_clock_check = 0;
            self.maintain();
        }

        let key = self.key(frame);
        if self.previous.contains(&key) || !self.current.insert(key) {
            self.duplicate += 1;
            true
        } else {
            self.unique += 1;
            false
        }
    }

    /// Rotate generations when due, and periodically report the duplicate
    /// rate. With `n` connections delivering it settles near `(n - 1) / n`; a
    /// rate drifting toward zero means only one connection is feeding.
    fn maintain(&mut self) {
        let now = Instant::now();

        let full = self.current.len() >= self.max_entries;
        if now >= self.rotate_at || full {
            // The effective window is now however long the generation took to
            // fill, and skew beyond it leaks duplicates. Warn at the report
            // cadence, not on every rotation.
            if full
                && self
                    .last_ceiling_warning
                    .is_none_or(|last| now.duration_since(last) >= REPORT_INTERVAL)
            {
                let held = self
                    .window
                    .saturating_sub(self.rotate_at.saturating_duration_since(now));
                warn!(
                    entries = self.current.len(),
                    ?held,
                    configured = ?self.window,
                    "dedup entry ceiling reached; the duplicate window is shorter than configured"
                );
                self.last_ceiling_warning = Some(now);
            }
            // `clear` keeps the allocation and the swap hands it to `current`,
            // so steady-state rotation does not allocate.
            self.previous.clear();
            std::mem::swap(&mut self.previous, &mut self.current);
            self.rotate_at = now + self.window;
        }

        if now.duration_since(self.last_report) >= REPORT_INTERVAL {
            let total = self.unique + self.duplicate;
            if total > 0 {
                info!(
                    unique = self.unique,
                    duplicate = self.duplicate,
                    duplicate_pct = (self.duplicate as f64 * 100.0 / total as f64).round(),
                    "redundant connections: duplicate suppression"
                );
            }
            self.unique = 0;
            self.duplicate = 0;
            self.last_report = now;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BINANCE: Exchange = Exchange::BinanceSbeSpot;

    /// An SBE frame with the real field order: 8-byte message header,
    /// `eventTime`, the block's identifying fields, then the symbol. Keeping
    /// `event_time` separate keeps the discriminator out of the field real
    /// connections stamp differently.
    fn frame_at(template_id: u16, event_time: i64, update_id: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[0, 0]);
        bytes.extend_from_slice(&template_id.to_le_bytes());
        bytes.extend_from_slice(&[0, 0, 0, 0]);
        bytes.extend_from_slice(&event_time.to_le_bytes());
        bytes.extend_from_slice(&update_id.to_le_bytes());
        bytes.push(7);
        bytes.extend_from_slice(b"BTCUSDT");
        bytes
    }

    fn frame(template_id: u16, update_id: u64) -> Vec<u8> {
        frame_at(template_id, 1_785_046_950_114_506, update_id)
    }

    #[test]
    fn copies_differing_only_in_event_time_are_duplicates() {
        let mut dedup = Dedup::new(BINANCE, DEDUP_WINDOW, DEDUP_MAX_ENTRIES);
        let first = frame_at(10_003, 1_785_046_950_114_506, 42);
        let second = frame_at(10_003, 1_785_046_950_114_469, 42);

        assert_ne!(first, second, "the frames differ on the wire");
        assert!(!dedup.is_duplicate(&first));
        assert!(
            dedup.is_duplicate(&second),
            "the same event from another connection must be recognised"
        );
    }

    /// Excluding the event time must not blur distinct events together.
    #[test]
    fn distinct_events_sharing_an_event_time_are_both_kept() {
        let mut dedup = Dedup::new(BINANCE, DEDUP_WINDOW, DEDUP_MAX_ENTRIES);

        assert!(!dedup.is_duplicate(&frame_at(10_003, 1_000, 1)));
        assert!(!dedup.is_duplicate(&frame_at(10_003, 1_000, 2)));
        assert!(!dedup.is_duplicate(&frame_at(10_001, 1_000, 1)));
    }

    #[test]
    fn a_runt_frame_is_handled_whole() {
        let mut dedup = Dedup::new(BINANCE, DEDUP_WINDOW, DEDUP_MAX_ENTRIES);

        assert!(!dedup.is_duplicate(b"\x00\x01\x02"));
        assert!(dedup.is_duplicate(b"\x00\x01\x02"));
        assert!(!dedup.is_duplicate(b""));
    }

    #[test]
    fn a_repeated_frame_is_reported_once() {
        let mut dedup = Dedup::new(BINANCE, DEDUP_WINDOW, DEDUP_MAX_ENTRIES);
        let frame = frame(10_001, 42);

        assert!(!dedup.is_duplicate(&frame));
        assert!(dedup.is_duplicate(&frame));
        assert!(dedup.is_duplicate(&frame));
    }

    #[test]
    fn distinct_frames_are_all_kept() {
        let mut dedup = Dedup::new(BINANCE, DEDUP_WINDOW, DEDUP_MAX_ENTRIES);

        for update_id in 0..1_000 {
            assert!(
                !dedup.is_duplicate(&frame(10_001, update_id)),
                "{update_id}"
            );
        }
    }

    /// Frames that differ only in template id are different events.
    #[test]
    fn the_whole_frame_is_the_key() {
        let mut dedup = Dedup::new(BINANCE, DEDUP_WINDOW, DEDUP_MAX_ENTRIES);

        assert!(!dedup.is_duplicate(&frame(10_001, 1)));
        assert!(!dedup.is_duplicate(&frame(10_002, 1)));
    }

    #[test]
    fn a_disabled_filter_never_reports_a_duplicate() {
        let mut dedup = Dedup::disabled(BINANCE);
        let frame = frame(10_001, 1);

        assert!(!dedup.is_duplicate(&frame));
        assert!(!dedup.is_duplicate(&frame));
        assert!(dedup.current.is_empty());
    }

    #[test]
    fn redundancy_is_only_engaged_for_more_than_one_connection() {
        assert!(!Dedup::for_connections(1, BINANCE).enabled);
        assert!(Dedup::for_connections(2, BINANCE).enabled);
    }

    /// A key stays known across one rotation, so the window is never shorter
    /// than advertised.
    #[test]
    fn a_key_survives_one_rotation() {
        let mut dedup = Dedup::new(BINANCE, Duration::ZERO, DEDUP_MAX_ENTRIES);
        let frame = frame(10_001, 1);

        assert!(!dedup.is_duplicate(&frame));
        dedup.maintain(); // `frame` moves to `previous`
        assert!(dedup.is_duplicate(&frame));
    }

    /// Memory is bounded by the entry ceiling even if the window never elapses.
    #[test]
    fn the_entry_ceiling_forces_a_rotation() {
        let mut dedup = Dedup::new(BINANCE, Duration::from_secs(3_600), 16);

        for update_id in 0..64 {
            assert!(
                !dedup.is_duplicate(&frame(10_001, update_id)),
                "{update_id}"
            );
        }

        assert!(dedup.current.len() <= 16);
        assert!(dedup.previous.len() <= 16);
    }

    #[test]
    fn a_ceiling_forced_rotation_is_reported() {
        let mut dedup = Dedup::new(BINANCE, Duration::from_secs(3_600), 16);
        assert!(dedup.last_ceiling_warning.is_none());

        for update_id in 0..64 {
            dedup.is_duplicate(&frame(10_001, update_id));
        }

        assert!(
            dedup.last_ceiling_warning.is_some(),
            "hitting the ceiling must not be silent"
        );
    }

    /// A timed rotation is the normal path and must stay quiet.
    #[test]
    fn a_timed_rotation_is_not_reported() {
        let mut dedup = Dedup::new(BINANCE, Duration::ZERO, DEDUP_MAX_ENTRIES);

        dedup.is_duplicate(&frame(10_001, 1));
        dedup.maintain();

        assert!(dedup.last_ceiling_warning.is_none());
    }

    #[test]
    fn duplicate_and_unique_counts_are_tracked() {
        let mut dedup = Dedup::new(BINANCE, DEDUP_WINDOW, DEDUP_MAX_ENTRIES);

        dedup.is_duplicate(&frame(10_001, 1));
        dedup.is_duplicate(&frame(10_001, 2));
        dedup.is_duplicate(&frame(10_001, 1));

        assert_eq!(dedup.unique, 2);
        assert_eq!(dedup.duplicate, 1);
    }
}
