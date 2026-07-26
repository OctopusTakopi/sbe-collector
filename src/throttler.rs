//! Budgeting for Binance's REST rate limit.
//!
//! The limit is denominated in *request weight*, not requests: Binance charges
//! `/api/v3/depth` 5 at a limit of 100 and 250 at a limit of 5000, against one
//! shared per-IP allowance. Counting calls instead of weight therefore says
//! nothing about whether the budget is spent — 100 full-depth snapshots a
//! minute is 25,000 weight against an allowance of 6,000, and the reply to
//! overrunning it is a 418 with an IP ban of two minutes to three days, which
//! stops every symbol's collection, not just the snapshot that overran.

use std::{
    collections::VecDeque,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

use jiff::Timestamp;
use tokio::sync::Mutex;
use tracing::{error, warn};

const WINDOW_NANOS: i64 = 60_000_000_000;

/// Spot request weight allowed per IP per minute, as Binance reports it in
/// `exchangeInfo` under `REQUEST_WEIGHT`.
const SPOT_WEIGHT_PER_MINUTE: u32 = 6_000;

/// What this collector will spend of it.
///
/// Derived from the ceiling rather than written out, so the two cannot drift.
/// Half on purpose: the allowance is per IP, so anything else on the host
/// talking to Binance draws from the same pool, and the cost of being wrong is
/// a ban of up to three days rather than a rejected request.
pub const SNAPSHOT_WEIGHT_BUDGET: u32 = SPOT_WEIGHT_PER_MINUTE / 2;

#[derive(Clone)]
pub struct Throttler {
    // VecDeque allows O(1) amortised front-pop instead of O(n) retain.
    spent: Arc<Mutex<VecDeque<(i64, u32)>>>,
    /// When an observed ban lifts, in epoch nanoseconds; 0 when not banned.
    ///
    /// The weight window only knows what *this process* has spent, so it cannot
    /// see a ban earned before it started — by a previous run, or by anything
    /// else on the host. Binance does say, in the 418 body and the `Retry-After`
    /// header, so the ban is recorded here and every request refused until it
    /// lifts. Continuing to send during a ban is what turns Binance's two
    /// minutes into hours and then days.
    banned_until: Arc<AtomicI64>,
    weight_per_minute: u32,
}

impl Throttler {
    /// `weight_per_minute` is a request-weight allowance, not a call count.
    pub fn new(weight_per_minute: u32) -> Self {
        Self {
            spent: Default::default(),
            banned_until: Arc::new(AtomicI64::new(0)),
            weight_per_minute,
        }
    }

    /// Record a ban observed in a response, so nothing is sent until it lifts.
    ///
    /// Keeps the latest expiry seen: a second ban while one is in force is an
    /// escalation, never a reprieve.
    pub fn note_ban(&self, until: Timestamp) {
        let until_nanos = until.as_nanosecond() as i64;
        let previous = self.banned_until.fetch_max(until_nanos, Ordering::Relaxed);
        if previous < until_nanos {
            error!(
                until = %until,
                "Binance banned this IP; no further REST requests until it lifts"
            );
        }
    }

    /// The ban expiry currently in force, if any.
    fn ban_in_force(&self, now_nanos: i64) -> Option<i64> {
        let until = self.banned_until.load(Ordering::Relaxed);
        (until > now_nanos).then_some(until)
    }

    /// Execute `fut` only if `weight` still fits in the last 60 seconds'
    /// allowance. Returns `None` if it does not.
    ///
    /// Takes `&self` (not `&mut self`) — all mutation goes through the inner
    /// `Arc<Mutex>`, so callers can share a `&Throttler` without cloning.
    pub async fn execute<Fut, T>(&self, weight: u32, fut: Fut) -> Option<T>
    where
        Fut: Future<Output = T>,
    {
        let now = Timestamp::now().as_nanosecond() as i64;
        if let Some(until) = self.ban_in_force(now) {
            warn!(
                remaining_secs = (until - now) / 1_000_000_000,
                "skipping request: this IP is still banned"
            );
            return None;
        }
        {
            let mut spent = self.spent.lock().await;
            // Timestamps are monotonically increasing, so expired entries are
            // always at the front.
            while let Some(&(at, _)) = spent.front() {
                if at <= now - WINDOW_NANOS {
                    spent.pop_front();
                } else {
                    break;
                }
            }
            let in_window: u32 = spent.iter().map(|(_, weight)| weight).sum();
            if in_window + weight > self.weight_per_minute {
                return None;
            }
            spent.push_back((now, weight));
        }
        Some(fut.await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The budget is weight, not calls: one heavy request can exhaust what a
    /// call-counting limiter would have allowed a hundred of.
    #[tokio::test]
    async fn heavy_requests_exhaust_the_budget_early() {
        let throttler = Throttler::new(500);

        assert_eq!(throttler.execute(250, async { 1 }).await, Some(1));
        assert_eq!(throttler.execute(250, async { 2 }).await, Some(2));
        assert_eq!(
            throttler.execute(250, async { 3 }).await,
            None,
            "750 of a 500 allowance must be refused"
        );
        // Something cheap still fits in what is left.
        assert_eq!(throttler.execute(0, async { 4 }).await, Some(4));
    }

    /// A request larger than the whole allowance can never run, and must be
    /// refused rather than admitted and overrunning.
    #[tokio::test]
    async fn a_request_heavier_than_the_budget_is_refused() {
        let throttler = Throttler::new(100);

        assert_eq!(throttler.execute(250, async { 1 }).await, None);
    }

    /// The real numbers: how many full-depth snapshots a minute the configured
    /// budget actually admits, and that they fit Binance's per-IP allowance.
    ///
    /// The old call-counting limiter admitted 100, which at weight 250 was
    /// 25,000 against an allowance of 6,000.
    #[tokio::test]
    async fn the_configured_budget_stays_inside_binances_allowance() {
        let throttler = Throttler::new(SNAPSHOT_WEIGHT_BUDGET);
        let weight = crate::binancesbespot::snapshot::SNAPSHOT_WEIGHT;
        let mut admitted = 0;
        while throttler.execute(weight, async {}).await.is_some() {
            admitted += 1;
        }

        assert_eq!(admitted, SNAPSHOT_WEIGHT_BUDGET / weight);
        assert!(
            admitted * weight <= SPOT_WEIGHT_PER_MINUTE,
            "{admitted} x {weight} must fit in {SPOT_WEIGHT_PER_MINUTE}"
        );
    }
}
