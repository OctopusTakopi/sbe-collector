use std::{collections::VecDeque, future::Future, sync::Arc};

use jiff::Timestamp;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Throttler {
    // VecDeque allows O(1) amortised front-pop instead of O(n) retain.
    exec_ts: Arc<Mutex<VecDeque<i64>>>,
    rate_limit: usize,
}

impl Throttler {
    pub fn new(rate_limit: usize) -> Self {
        Self {
            exec_ts: Default::default(),
            rate_limit,
        }
    }

    /// Execute `fut` only if fewer than `rate_limit` calls have been made in the
    /// last 60 seconds. Returns `None` if rate-limited.
    ///
    /// L5/P2: takes `&self` (not `&mut self`) — all mutation goes through the
    /// inner `Arc<Mutex>`, so callers can share a `&Throttler` without cloning.
    pub async fn execute<Fut, T>(&self, fut: Fut) -> Option<T>
    where
        Fut: Future<Output = T>,
    {
        let cur_ts = Timestamp::now().as_nanosecond() as i64;
        {
            let mut exec_ts_ = self.exec_ts.lock().await;
            // Timestamps are monotonically increasing → expired entries are always
            // at the front.  O(1) amortised vs the previous O(n) Vec::retain.
            while let Some(&front) = exec_ts_.front() {
                if front <= cur_ts - 60_000_000_000 {
                    exec_ts_.pop_front();
                } else {
                    break;
                }
            }
            if exec_ts_.len() >= self.rate_limit {
                return None;
            }
            exec_ts_.push_back(cur_ts);
        }
        Some(fut.await)
    }
}
