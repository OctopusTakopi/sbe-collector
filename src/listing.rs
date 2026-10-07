//! The startup lookups of the command line's symbols in the venue's own
//! listings. A symbol the venue confirms it does not list stops the process
//! with the venue's answer before anything starts; a venue that cannot
//! answer, unreachable or rate-limited, is asked again for a while first,
//! unless it has [`Barred`] this IP.

use std::{future::Future, time::Duration};

use tokio::time::Instant;
use tracing::warn;

/// How long a venue whose listings cannot be had is asked again before the
/// process gives up: long enough to ride out a REST blip at a cold start.
pub(crate) const PATIENCE: Duration = Duration::from_secs(120);

/// The first pause between attempts, doubled up to [`MAX_RETRY_DELAY`].
const RETRY_DELAY: Duration = Duration::from_secs(2);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// The symbols a venue says it does not list, each with its reason.
#[derive(Debug, thiserror::Error)]
#[error("the venue does not list {}", .0.join("; "))]
pub(crate) struct Unlisted(pub(crate) Vec<String>);

/// The venue turned this IP away, banned or rate-limited: asking again is
/// what escalates a ban, so it is not asked again.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct Barred(pub(crate) String);

/// A client for the lookups. Some venues refuse a request without a
/// `User-Agent`.
pub(crate) fn client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION")
        ))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
}

/// Asks `ask` until it answers, pausing longer each time, for at most
/// `patience`. An `Err` from `ask` is no answer at all, and a [`Barred`] one
/// ends the asking at once; a refusal is an answer, and part of `T`.
pub(crate) async fn patiently<T, F, Fut>(patience: Duration, mut ask: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let deadline = Instant::now() + patience;
    let mut delay = RETRY_DELAY;
    loop {
        match ask().await {
            Ok(answer) => return Ok(answer),
            Err(error) if error.is::<Barred>() => return Err(error),
            Err(error) if Instant::now() + delay < deadline => {
                warn!(
                    error = format!("{error:#}"),
                    ?delay,
                    "cannot read the venue's listings; asking again"
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(MAX_RETRY_DELAY);
            }
            Err(error) => {
                return Err(error.context(format!(
                    "cannot check the symbols against the venue's listings within {patience:?}"
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn an_unreachable_venue_is_asked_again_until_it_answers() {
        let attempts = AtomicU32::new(0);
        let start = Instant::now();

        let answer = patiently(PATIENCE, || async {
            if attempts.fetch_add(1, Ordering::Relaxed) < 3 {
                anyhow::bail!("connection refused");
            }
            Ok("listed")
        })
        .await
        .unwrap();

        assert_eq!(answer, "listed");
        assert_eq!(attempts.load(Ordering::Relaxed), 4);
        assert_eq!(Instant::now() - start, Duration::from_secs(2 + 4 + 8));
    }

    #[tokio::test(start_paused = true)]
    async fn a_venue_that_never_answers_is_given_up_within_the_patience() {
        let start = Instant::now();

        let error = patiently::<(), _, _>(PATIENCE, || async { anyhow::bail!("503") })
            .await
            .unwrap_err();

        assert!(Instant::now() - start <= PATIENCE);
        assert_eq!(
            format!("{error:#}"),
            "cannot check the symbols against the venue's listings within 120s: 503"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_venue_that_bars_this_ip_is_not_asked_again() {
        let attempts = AtomicU32::new(0);
        let start = Instant::now();

        let error = patiently::<(), _, _>(PATIENCE, || async {
            attempts.fetch_add(1, Ordering::Relaxed);
            Err(Barred("418".to_owned()).into())
        })
        .await
        .unwrap_err();

        assert!(error.is::<Barred>());
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
        assert_eq!(Instant::now(), start);
    }

    #[test]
    fn unlisted_symbols_are_named_with_their_reasons() {
        let unlisted = Unlisted(vec![
            "FOOX-USDT (51001)".to_owned(),
            "BAR-USDT (51001)".to_owned(),
        ]);
        assert_eq!(
            unlisted.to_string(),
            "the venue does not list FOOX-USDT (51001); BAR-USDT (51001)"
        );
    }
}
