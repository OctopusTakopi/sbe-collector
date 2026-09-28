//! Budgeting for Binance's REST rate limit.
//!
//! The limit is per-IP *request weight*, not requests: `/api/v3/depth` costs 5
//! at a limit of 100 and 250 at 5000, so 100 full-depth snapshots a minute is
//! 25,000 weight against an allowance of 6,000. Overrunning it earns a 418 and
//! an IP ban of two minutes to three days.

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

/// What this collector will spend of it: half, because anything else on the
/// host draws from the same per-IP pool, and being wrong costs a ban.
pub const SNAPSHOT_WEIGHT_BUDGET: u32 = SPOT_WEIGHT_PER_MINUTE / 2;

#[derive(Clone)]
pub struct Throttler {
    /// `(epoch ns, weight)` of the requests in the last minute, oldest first.
    spent: Arc<Mutex<VecDeque<(i64, u32)>>>,
    /// When an observed ban lifts, in epoch nanoseconds; 0 when not banned.
    ///
    /// The weight window only knows what this process spent, not a ban earned
    /// by a previous run or another program. Sending during a ban is what
    /// turns Binance's two minutes into hours and then days.
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
    /// The latest expiry wins: a second ban is an escalation, not a reprieve.
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

    /// Run `fut` only if no ban is in force and `weight` still fits in the
    /// last minute's allowance; `None` otherwise.
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
            // Entries are pushed in time order, so expired ones are at the front.
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

    /// How many full-depth snapshots a minute the configured budget admits,
    /// and that they fit Binance's per-IP allowance.
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
