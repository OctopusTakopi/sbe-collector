//! Duplicate suppression for redundant websocket connections.
//!
//! Running the same subscription over several connections is what closes the
//! gap left by an exchange-initiated disconnect: while one socket is being
//! re-established, the others keep delivering. The cost is that every message
//! now arrives once per healthy connection, so exactly one copy has to be kept.
//!
//! # Why the frame *minus its event time* is the key
//!
//! Binance does not encode an SBE frame once and fan it out. It stamps
//! `eventTime` when it serialises for a *particular* connection, so redundant
//! connections receive the same event with timestamps tens of microseconds
//! apart — measured at 18-37 us on live spot data. Hashing the whole frame
//! therefore recognises nothing as a duplicate and records every copy, which is
//! exactly the failure this module exists to prevent.
//!
//! Excluding [`EVENT_TIME`] leaves a key that still separates distinct events:
//! depth diffs carry their book update id range, depth snapshots and bestBidAsk
//! a book update id, trades a transact time and per-trade ids. Verified against
//! a single-connection recording, where every frame is by construction a
//! distinct event: 11,446 frames across two symbols produced zero collisions on
//! this key.
//!
//! The residual risk is a template whose content can repeat with no change but
//! a new event time — a partial-book snapshot pushed on a timer while the book
//! sits still. Such a repeat carries no information, but it would be dropped
//! rather than recorded, and unlike a shed frame that loss counts as a
//! successful suppression rather than being logged.
//!
//! # Ordering
//!
//! Keeping the *first* copy preserves the order of the merged stream. Each
//! connection delivers events in order, so for events `m` before `n`,
//! `first(m) = min over connections of arrival(m)` is earlier than
//! `first(n) = min ... arrival(n)`: every term of the first minimum precedes
//! the matching term of the second. The recorded timestamp is therefore also
//! the lowest latency any connection achieved for that event.
//!
//! The one exception is a connection that comes back from a reconnect *ahead*
//! of where a lagging peer still is: it can deliver an event whose predecessor
//! has not arrived on any connection yet. The depth continuity check treats
//! that as a gap and refetches a snapshot, which is the correct repair.

use std::{
    collections::HashSet,
    ops::Range,
    time::{Duration, Instant},
};

use tracing::{info, warn};

/// The bytes of an SBE frame holding `eventTime`, excluded from the key.
///
/// `eventTime` is the first field of the fixed block on every template this
/// collector subscribes to — trades (10000), bestBidAsk (10001), depth snapshot
/// (10002) and depth diff (10003) — so it always sits directly behind the
/// 8-byte message header. It is stamped per connection, which is what makes it
/// useless for identifying an event and fatal to include.
pub const EVENT_TIME: Range<usize> = 8..16;

/// How far back duplicates are remembered.
///
/// Only has to cover the delivery skew between connections. They share one
/// queue and one consumer, so in practice that skew is milliseconds; this is
/// sized for a connection that stalls on a slow TCP path and then catches up.
pub const DEDUP_WINDOW: Duration = Duration::from_secs(30);

/// Hard ceiling on remembered keys per generation, so an unexpected message
/// rate cannot turn the window into unbounded memory.
///
/// Reaching it rotates early, which shortens the window below [`DEDUP_WINDOW`]
/// — `Dedup` logs when that happens, because a window shorter than the skew
/// between connections lets duplicates through.
///
/// Two generations are live at once and `hashbrown` rounds up to a power of two
/// at a 7/8 load factor, so 500k keys reserve 2^20 buckets of 17 bytes per
/// generation: about 36 MiB in total, retained once reached.
pub const DEDUP_MAX_ENTRIES: usize = 500_000;

const REPORT_INTERVAL: Duration = Duration::from_secs(300);

/// How many messages may pass between clock reads.
///
/// `Instant::now` per message is affordable but pointless: the window is tens
/// of seconds and the rotation only has to be approximately on time.
const CLOCK_CHECK_INTERVAL: u32 = 1_024;

/// Drops the second and later copies of a frame.
///
/// Keys are held in two generations that rotate on a timer. A lookup checks
/// both, so anything inserted is remembered for at least [`DEDUP_WINDOW`] and
/// at most twice that, without storing a timestamp per key or ever walking the
/// set to expire it — unless [`DEDUP_MAX_ENTRIES`] forces an early rotation,
/// which is logged.
pub struct Dedup {
    /// `false` for a single connection, where no frame can be a duplicate.
    /// Checked before hashing, so the whole module costs one branch.
    enabled: bool,
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
    pub fn disabled() -> Self {
        Self::build(false, DEDUP_WINDOW, DEDUP_MAX_ENTRIES)
    }

    pub fn new(window: Duration, max_entries: usize) -> Self {
        Self::build(true, window, max_entries)
    }

    /// Enabled only when more than one connection can deliver the same frame.
    pub fn for_connections(connections: usize) -> Self {
        if connections > 1 {
            Self::new(DEDUP_WINDOW, DEDUP_MAX_ENTRIES)
        } else {
            Self::disabled()
        }
    }

    fn build(enabled: bool, window: Duration, max_entries: usize) -> Self {
        let now = Instant::now();
        Self {
            enabled,
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

    /// The identity of an event: the frame with its [`EVENT_TIME`] stamp cut
    /// out, since that differs between connections for one and the same event.
    ///
    /// A frame too short to hold the stamp is not a template this collector
    /// knows; hashing it whole is the conservative choice, because keeping an
    /// unrecognised frame twice is recoverable and dropping a real one is not.
    fn key(frame: &[u8]) -> u128 {
        if frame.len() < EVENT_TIME.end {
            return xxhash_rust::xxh3::xxh3_128(frame);
        }
        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        hasher.update(&frame[..EVENT_TIME.start]);
        hasher.update(&frame[EVENT_TIME.end..]);
        hasher.digest128()
    }

    /// True if this frame's event has already been seen inside the window.
    ///
    /// A `false` return records the event, so calling this twice on the same
    /// frame reports the second call as a duplicate. Call it once per frame, on
    /// the path that decides whether to keep it.
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

        // 128 bits: at the ceiling above, a collision inside one window has
        // probability around 2^-85. A 64-bit key would be roughly 2^-21 per
        // window, which over a year of collection is a coin flip — and a
        // collision here silently discards a real frame.
        let key = Self::key(frame);
        if self.previous.contains(&key) || !self.current.insert(key) {
            self.duplicate += 1;
            true
        } else {
            self.unique += 1;
            false
        }
    }

    /// Rotate generations when due, and periodically report the duplicate rate.
    ///
    /// The rate is the health signal for redundancy: with `n` connections all
    /// delivering, it settles near `(n - 1) / n`. A rate drifting toward zero
    /// means only one connection is actually feeding, and the redundancy that
    /// was paid for is not there.
    fn maintain(&mut self) {
        let now = Instant::now();

        let full = self.current.len() >= self.max_entries;
        if now >= self.rotate_at || full {
            // The effective window is now however long it took to fill a
            // generation, not `self.window`. Any connection whose skew exceeds
            // that leaks duplicates past the filter, so this must not be
            // silent — but a ceiling that keeps being hit would log on every
            // rotation, so restate it at the reporting cadence instead.
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
            // `clear` keeps the allocation, and the swap hands it to `current`,
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

    /// An SBE frame with the real field order: 8-byte message header,
    /// `eventTime`, then the block's identifying fields, then the symbol the
    /// stream bridge appends as a `VarString8`.
    ///
    /// `event_time` is a separate argument on purpose. An earlier version of
    /// this helper put the discriminator where `eventTime` actually lives, so
    /// every test passed against frames that varied precisely the field real
    /// connections stamp differently — and the filter recognised nothing as a
    /// duplicate on live data.
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

    /// The bug this module shipped with: Binance stamps `eventTime` when it
    /// serialises for a particular connection, so two connections deliver the
    /// same event with timestamps microseconds apart. If that field reaches the
    /// key, nothing is ever a duplicate and every copy is recorded.
    #[test]
    fn copies_differing_only_in_event_time_are_duplicates() {
        let mut dedup = Dedup::new(DEDUP_WINDOW, DEDUP_MAX_ENTRIES);
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
        let mut dedup = Dedup::new(DEDUP_WINDOW, DEDUP_MAX_ENTRIES);

        assert!(!dedup.is_duplicate(&frame_at(10_003, 1_000, 1)));
        assert!(!dedup.is_duplicate(&frame_at(10_003, 1_000, 2)));
        assert!(!dedup.is_duplicate(&frame_at(10_001, 1_000, 1)));
    }

    /// A frame too short to hold an event time is hashed whole rather than
    /// panicking on the slice.
    #[test]
    fn a_runt_frame_is_handled_whole() {
        let mut dedup = Dedup::new(DEDUP_WINDOW, DEDUP_MAX_ENTRIES);

        assert!(!dedup.is_duplicate(b"\x00\x01\x02"));
        assert!(dedup.is_duplicate(b"\x00\x01\x02"));
        assert!(!dedup.is_duplicate(b""));
    }

    #[test]
    fn a_repeated_frame_is_reported_once() {
        let mut dedup = Dedup::new(DEDUP_WINDOW, DEDUP_MAX_ENTRIES);
        let frame = frame(10_001, 42);

        assert!(!dedup.is_duplicate(&frame));
        assert!(dedup.is_duplicate(&frame));
        assert!(dedup.is_duplicate(&frame));
    }

    #[test]
    fn distinct_frames_are_all_kept() {
        let mut dedup = Dedup::new(DEDUP_WINDOW, DEDUP_MAX_ENTRIES);

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
        let mut dedup = Dedup::new(DEDUP_WINDOW, DEDUP_MAX_ENTRIES);

        assert!(!dedup.is_duplicate(&frame(10_001, 1)));
        assert!(!dedup.is_duplicate(&frame(10_002, 1)));
    }

    /// A single connection must not pay for a filter that cannot fire.
    #[test]
    fn a_disabled_filter_never_reports_a_duplicate() {
        let mut dedup = Dedup::disabled();
        let frame = frame(10_001, 1);

        assert!(!dedup.is_duplicate(&frame));
        assert!(!dedup.is_duplicate(&frame));
        assert!(dedup.current.is_empty());
    }

    #[test]
    fn redundancy_is_only_engaged_for_more_than_one_connection() {
        assert!(!Dedup::for_connections(1).enabled);
        assert!(Dedup::for_connections(2).enabled);
    }

    /// The whole point of two generations: a key stays known across one
    /// rotation, so the window is never shorter than advertised.
    #[test]
    fn a_key_survives_one_rotation() {
        let mut dedup = Dedup::new(Duration::ZERO, DEDUP_MAX_ENTRIES);
        let frame = frame(10_001, 1);

        assert!(!dedup.is_duplicate(&frame));
        dedup.maintain(); // `frame` moves to `previous`
        assert!(dedup.is_duplicate(&frame));
    }

    /// Memory is bounded by the entry ceiling even if the window never elapses.
    #[test]
    fn the_entry_ceiling_forces_a_rotation() {
        let mut dedup = Dedup::new(Duration::from_secs(3_600), 16);

        for update_id in 0..64 {
            assert!(
                !dedup.is_duplicate(&frame(10_001, update_id)),
                "{update_id}"
            );
        }

        assert!(dedup.current.len() <= 16);
        assert!(dedup.previous.len() <= 16);
    }

    /// A ceiling-forced rotation shortens the window below what was asked for,
    /// so it has to leave a trace rather than silently letting duplicates
    /// through later.
    #[test]
    fn a_ceiling_forced_rotation_is_reported() {
        let mut dedup = Dedup::new(Duration::from_secs(3_600), 16);
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
        let mut dedup = Dedup::new(Duration::ZERO, DEDUP_MAX_ENTRIES);

        dedup.is_duplicate(&frame(10_001, 1));
        dedup.maintain();

        assert!(dedup.last_ceiling_warning.is_none());
    }

    /// Both counters have to move, or the health signal in the log is a lie.
    #[test]
    fn duplicate_and_unique_counts_are_tracked() {
        let mut dedup = Dedup::new(DEDUP_WINDOW, DEDUP_MAX_ENTRIES);

        dedup.is_duplicate(&frame(10_001, 1));
        dedup.is_duplicate(&frame(10_001, 2));
        dedup.is_duplicate(&frame(10_001, 1));

        assert_eq!(dedup.unique, 2);
        assert_eq!(dedup.duplicate, 1);
    }
}
