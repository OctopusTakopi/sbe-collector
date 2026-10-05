use std::time::Duration;

use hyper::header::{HeaderMap, HeaderValue};
use tracing::warn;

use crate::session::{Handshake, Protocol, Reason, Session, Verdict};

const WSS_HOST: &str = "stream-sbe.binance.com";
const WSS_PORT: u16 = 9443;

/// When a session starts arranging its own replacement.
///
/// The venue: "a single connection to stream-sbe.binance.com is only valid for
/// 24 hours; expect to be disconnected at the 24 hour mark". The idle timeout
/// bounds how late the check can run.
const MAX_SESSION_AGE: Duration = Duration::from_secs(23 * 60 * 60);

/// Spreads redundant connections' handovers. A fixed age cap would
/// re-synchronise a day later what the connect stagger spread out at startup.
const SESSION_AGE_STAGGER: Duration = Duration::from_secs(15 * 60);

fn max_session_age(connection: usize) -> Duration {
    MAX_SESSION_AGE.saturating_sub(SESSION_AGE_STAGGER * connection as u32)
}

/// Pushed unprompted before the venue closes a connection:
/// `{"e":"serverShutdown","E":...}`.
const SERVER_SHUTDOWN: &str = "serverShutdown";

/// True if `payload` is the venue's shutdown announcement.
///
/// Parsed rather than substring-matched, so an error quoting the word does not
/// trigger a handover. The event is documented as a bare object; an enveloped
/// combined-stream copy is accepted too.
fn is_server_shutdown(payload: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return false;
    };
    let event = value.get("data").unwrap_or(&value);
    event.get("e").and_then(serde_json::Value::as_str) == Some(SERVER_SHUTDOWN)
}

/// Where every connection slot connects.
pub(crate) struct Endpoint {
    url: String,
    /// `X-MBX-APIKEY`, validated once.
    api_key: HeaderValue,
}

impl Endpoint {
    /// The combined-stream URL for every `$symbol` stream of every symbol.
    pub(crate) fn new(
        streams: &[impl AsRef<str>],
        symbols: &[String],
        api_key: &str,
    ) -> anyhow::Result<Self> {
        let mut api_key = HeaderValue::from_str(api_key)
            .map_err(|_| anyhow::anyhow!("BINANCE_API_KEY is not a valid header value"))?;
        api_key.set_sensitive(true);
        let streams = symbols
            .iter()
            .flat_map(|symbol| {
                let symbol = symbol.to_lowercase();
                streams
                    .iter()
                    .map(move |stream| stream.as_ref().replace("$symbol", &symbol))
            })
            .collect::<Vec<_>>()
            .join("/");
        Ok(Self {
            url: format!("wss://{WSS_HOST}:{WSS_PORT}/stream?streams={streams}"),
            api_key,
        })
    }
}

impl Protocol for Endpoint {
    type Session = Announcements;

    const SOURCE: &'static str = "binance-sbe-spot";

    fn handshake(&self) -> anyhow::Result<Handshake<'_>> {
        let mut headers = HeaderMap::new();
        headers.insert("x-mbx-apikey", self.api_key.clone());
        Ok(Handshake {
            url: &self.url,
            headers,
        })
    }

    fn max_age(&self, connection: usize) -> Option<Duration> {
        Some(max_session_age(connection))
    }

    /// The stream list is in the URL, so there is nothing to subscribe.
    fn open(&self) -> (Announcements, Vec<String>) {
        (Announcements, Vec::new())
    }
}

/// The only text a Binance stream session expects: its shutdown notice.
pub(crate) struct Announcements;

impl Session for Announcements {
    fn on_text(&mut self, text: &[u8]) -> Verdict {
        let text_lossy = String::from_utf8_lossy(text);
        if is_server_shutdown(text) {
            warn!(text = %text_lossy, "the venue announced a server shutdown");
            Verdict::Relieve(Reason::ServerShutdown)
        } else {
            // No routine text traffic exists on this endpoint, so any other
            // text frame is worth seeing, not least a shutdown announcement in
            // a form this does not recognise.
            warn!("unexpected WS text frame: {text_lossy}");
            Verdict::Continue
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ANNOUNCEMENT: &str = r#"{"e":"serverShutdown","E":1770123456789}"#;

    #[test]
    fn redundant_connections_hand_over_at_different_times() {
        assert_eq!(max_session_age(0), MAX_SESSION_AGE);
        assert_eq!(max_session_age(1), MAX_SESSION_AGE - SESSION_AGE_STAGGER);
        // Even the last of the eight the CLI allows stays well inside 24 h.
        assert!(max_session_age(7) > Duration::from_secs(20 * 60 * 60));
    }

    #[test]
    fn the_endpoint_url_lists_every_stream_of_every_symbol() {
        let endpoint = Endpoint::new(
            &["$symbol@trade", "$symbol@depth"],
            &["BTCUSDT".to_owned(), "ethusdt".to_owned()],
            "key",
        )
        .unwrap();
        assert_eq!(
            endpoint.url,
            "wss://stream-sbe.binance.com:9443/stream?streams=\
             btcusdt@trade/btcusdt@depth/ethusdt@trade/ethusdt@depth"
        );
    }

    #[test]
    fn the_handshake_carries_the_api_key() {
        let endpoint = Endpoint::new(&["$symbol@trade"], &["btcusdt".to_owned()], "key").unwrap();
        let handshake = endpoint.handshake().unwrap();
        assert_eq!(handshake.headers["x-mbx-apikey"], "key");
        assert!(handshake.headers["x-mbx-apikey"].is_sensitive());
    }

    #[test]
    fn the_announcement_is_recognised() {
        assert!(is_server_shutdown(ANNOUNCEMENT.as_bytes()));
        assert!(is_server_shutdown(
            br#"{"stream":"btcusdt@depth","data":{"e":"serverShutdown","E":1}}"#
        ));
    }

    /// Only the event type counts, not the word anywhere in a message.
    #[test]
    fn only_the_event_type_counts() {
        assert!(!is_server_shutdown(
            br#"{"code":-1130,"msg":"serverShutdown is not a valid stream"}"#
        ));
        assert!(!is_server_shutdown(br#"{"e":"trade","E":1}"#));
        assert!(!is_server_shutdown(b"serverShutdown"));
        assert!(!is_server_shutdown(b"not json at all"));
    }

    #[test]
    fn the_announcement_asks_for_a_replacement() {
        assert!(matches!(
            Announcements.on_text(ANNOUNCEMENT.as_bytes()),
            Verdict::Relieve(Reason::ServerShutdown)
        ));
        assert!(matches!(
            Announcements.on_text(br#"{"result":null,"id":1}"#),
            Verdict::Continue
        ));
    }
}
