//! Gate's SBE websockets: no login for market data, one JSON subscription
//! per channel and pair, SBE frames back.

use std::{borrow::Cow, collections::HashMap, sync::Arc, time::Duration};

use hyper::header::HeaderMap;
use jiff::Timestamp;
use sbe_collector::sbe::gate::{self, Market};
use serde::Deserialize;
use tracing::warn;

use crate::{
    quality::QualityReporter,
    session::{Handshake, Protocol, Reason, Session, Verdict},
    subscriptions::{Answers, Topic, Topics},
};

/// Schema 1 pinned: the layouts `sbe::gate` decodes.
const SPOT_URL: &str = "wss://api.gateio.ws/ws/v4/ws/spot/sbe?sbe_schema_id=1";
const FUTURES_URL: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt/sbe?sbe_schema_id=1";

/// Gate sent no ping in six minutes of watching, so a quiet pair could fall
/// silent past the idle timeout; the pongs a keepalive earns prevent that.
const KEEPALIVE: Duration = Duration::from_secs(20);

/// `obu` sends a full snapshot only on subscribing. Replacing every session
/// hourly, make-before-break, puts a fresh one in each daily file within the
/// hour.
const MAX_SESSION_AGE: Duration = Duration::from_secs(60 * 60);

/// Spreads redundant connections' replacements, as the connect stagger
/// spread their starts.
const SESSION_AGE_STAGGER: Duration = Duration::from_secs(5 * 60);

/// Book depth subscribed: 50 levels every 20 ms (400 would be every 100 ms).
const BOOK_LEVELS: &str = "50";

/// Errors no reconnect can cure: an invalid request (1), an invalid argument
/// such as an unknown pair (2), a failed authentication (4).
fn is_refusal(code: i64) -> bool {
    matches!(code, 1 | 2 | 4)
}

/// What every Gate connection slot of one market shares.
pub(crate) struct Gate {
    market: Market,
    /// Each channel of each pair, by its subscription payload: the pair, or
    /// the `obu` stream's name.
    topics: Arc<Topics<String>>,
}

impl Gate {
    /// `pairs` as Gate spells them, `BTC_USDT`.
    pub(crate) fn new(market: Market, pairs: &[String], quality: QualityReporter) -> Self {
        let [book_ticker, trades, obu] = match market {
            Market::Spot => ["spot.book_ticker", "spot.trades", "spot.obu"],
            Market::Futures => ["futures.book_ticker", "futures.trades", "futures.obu"],
        };
        let topics = pairs.iter().flat_map(|pair| {
            let instrument: Arc<str> = Arc::from(pair.as_str());
            [
                (book_ticker, pair.clone()),
                (trades, pair.clone()),
                (obu, format!("ob.{pair}.{BOOK_LEVELS}")),
            ]
            .map(|(channel, target)| Topic {
                channel,
                instrument: Arc::clone(&instrument),
                target,
            })
        });
        Self {
            market,
            topics: Arc::new(Topics::new(Self::SOURCE, topics, quality)),
        }
    }
}

impl Protocol for Gate {
    type Session = Replies;

    const SOURCE: &'static str = "gate-sbe";

    fn handshake(&self) -> anyhow::Result<Handshake<'_>> {
        let url = match self.market {
            Market::Spot => SPOT_URL,
            Market::Futures => FUTURES_URL,
        };
        Ok(Handshake {
            url,
            headers: HeaderMap::new(),
        })
    }

    fn max_age(&self, connection: usize) -> Option<Duration> {
        Some(MAX_SESSION_AGE.saturating_sub(SESSION_AGE_STAGGER * connection as u32))
    }

    fn keepalive(&self) -> Option<Duration> {
        Some(KEEPALIVE)
    }

    /// One request per topic, so a refusal names its topic. Gate wants a
    /// `time` within 60 s of its own.
    fn open(&self) -> (Replies, Vec<String>) {
        let time = Timestamp::now().as_second();
        let requests = self
            .topics
            .numbered()
            .map(|(id, topic)| {
                serde_json::json!({
                    "time": time,
                    "id": id,
                    "channel": topic.channel,
                    "event": "subscribe",
                    "payload": [topic.target],
                })
                .to_string()
            })
            .collect();
        let replies = Replies {
            market: self.market,
            answers: Answers::new(Arc::clone(&self.topics)),
            books: HashMap::new(),
        };
        (replies, requests)
    }
}

/// A text frame from Gate; fields this does not use are ignored.
#[derive(Deserialize)]
struct Reply<'a> {
    id: Option<usize>,
    #[serde(borrow)]
    channel: Option<Cow<'a, str>>,
    #[serde(borrow)]
    event: Option<Cow<'a, str>>,
    #[serde(borrow)]
    error: Option<ReplyError<'a>>,
}

#[derive(Deserialize)]
struct ReplyError<'a> {
    code: i64,
    #[serde(borrow)]
    message: Cow<'a, str>,
}

/// One session's subscriptions, and the last id of each of its own book
/// streams.
pub(crate) struct Replies {
    market: Market,
    answers: Answers<String>,
    /// Stream name (`ob.BTC_USDT.50`) to the last id this socket applied.
    books: HashMap<Vec<u8>, i64>,
}

impl Session for Replies {
    fn on_text(&mut self, text: &[u8]) -> Verdict {
        let Ok(reply) = serde_json::from_slice::<Reply>(text) else {
            warn!(
                "unexpected WS text frame: {}",
                String::from_utf8_lossy(text)
            );
            return Verdict::Continue;
        };
        let topic = reply.id.and_then(|id| self.answers.topic(id));
        match (reply.event.as_deref(), topic, reply.error) {
            (Some("subscribe"), Some(topic), None) => {
                self.answers.ack(topic);
                Verdict::Continue
            }
            (Some("subscribe"), Some(topic), Some(refused)) if is_refusal(refused.code) => self
                .answers
                .refuse(topic, format!("{} {}", refused.code, refused.message)),
            (_, _, Some(failed)) => self
                .answers
                .reject(format!("{} {}", failed.code, failed.message)),
            _ if reply
                .channel
                .is_some_and(|channel| channel.ends_with(".pong")) =>
            {
                Verdict::Continue
            }
            _ => {
                warn!(
                    "unexpected WS text frame: {}",
                    String::from_utf8_lossy(text)
                );
                Verdict::Continue
            }
        }
    }

    /// Follow this socket's own `obu` chains: a delta that does not start at
    /// the last id plus one leaves this socket's book broken until a fresh
    /// subscription's snapshot, so the session asks to be replaced.
    fn on_binary(&mut self, frame: &[u8]) -> Verdict {
        let Some(link) = gate::obu_link(self.market, frame) else {
            return Verdict::Continue;
        };
        let previous = match self.books.get_mut(link.stream) {
            Some(last) => Some(std::mem::replace(last, link.last_id)),
            None => {
                self.books.insert(link.stream.to_vec(), link.last_id);
                None
            }
        };
        if link.full || previous.is_some_and(|last| link.first_id == last + 1) {
            return Verdict::Continue;
        }
        warn!(
            stream = %String::from_utf8_lossy(link.stream),
            after = ?previous,
            first_id = link.first_id,
            "a gap in this connection's book stream"
        );
        Verdict::Relieve(Reason::Gap)
    }

    fn settled(&self) -> bool {
        self.answers.settled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{quality::QualityEvent, session::Loss};

    fn gate(market: Market, quality: QualityReporter) -> Gate {
        Gate::new(
            market,
            &["BTC_USDT".to_owned(), "ETH_USDT".to_owned()],
            quality,
        )
    }

    fn ack(id: usize) -> String {
        format!(
            r#"{{"time":1791182070,"time_ms":1791182070040,"id":{id},"conn_id":"cf39484c4f270f22","channel":"spot.book_ticker","event":"subscribe","payload":["BTC_USDT"],"result":{{"status":"success"}}}}"#
        )
    }

    fn refusal(id: usize, code: i64) -> String {
        format!(
            r#"{{"time":1791182070,"id":{id},"channel":"spot.trades","event":"subscribe","payload":["NOPE_USDT"],"error":{{"code":{code},"message":"unknown currency pair: NOPE_USDT"}},"result":{{"status":"fail"}}}}"#
        )
    }

    /// An `obu` frame's bytes as far as the chain cares: header, block, two
    /// empty groups and the names.
    fn obu(full: bool, first_id: i64, last_id: i64, stream: &str) -> Vec<u8> {
        let mut frame = Vec::new();
        for field in [36u16, gate::TEMPLATE_OBU, 1, 1] {
            frame.extend_from_slice(&field.to_le_bytes());
        }
        frame.extend_from_slice(&[0; 9]);
        frame.extend_from_slice(&[0; 8]);
        frame.push(u8::from(full));
        frame.extend_from_slice(&first_id.to_le_bytes());
        frame.extend_from_slice(&last_id.to_le_bytes());
        frame.extend_from_slice(&[0, 0]);
        for _ in 0..2 {
            frame.extend_from_slice(&16u16.to_le_bytes());
            frame.extend_from_slice(&0u16.to_le_bytes());
        }
        for name in ["spot.obu", stream] {
            frame.push(name.len() as u8);
            frame.extend_from_slice(name.as_bytes());
        }
        frame
    }

    #[test]
    fn each_channel_and_pair_is_its_own_request() {
        let (_, requests) = gate(Market::Futures, QualityReporter::disabled()).open();
        assert_eq!(requests.len(), 6);
        let first: serde_json::Value = serde_json::from_str(&requests[0]).unwrap();
        assert_eq!(first["channel"], "futures.book_ticker");
        assert_eq!(first["event"], "subscribe");
        assert_eq!(first["payload"], serde_json::json!(["BTC_USDT"]));
        assert_eq!(first["id"], 0);
        let age = Timestamp::now().as_second() - first["time"].as_i64().unwrap();
        assert!((0..=1).contains(&age));
        let obu: serde_json::Value = serde_json::from_str(&requests[5]).unwrap();
        assert_eq!(obu["channel"], "futures.obu");
        assert_eq!(obu["payload"], serde_json::json!(["ob.ETH_USDT.50"]));
    }

    #[test]
    fn the_market_picks_the_endpoint_and_age_caps_are_staggered() {
        let spot = gate(Market::Spot, QualityReporter::disabled());
        assert_eq!(spot.handshake().unwrap().url, SPOT_URL);
        assert!(spot.handshake().unwrap().headers.is_empty());
        let futures = gate(Market::Futures, QualityReporter::disabled());
        assert_eq!(futures.handshake().unwrap().url, FUTURES_URL);
        assert_eq!(spot.max_age(0), Some(MAX_SESSION_AGE));
        assert_eq!(spot.max_age(7), Some(Duration::from_secs(25 * 60)));
        assert_eq!(spot.keepalive(), Some(KEEPALIVE));
    }

    #[test]
    fn every_ack_settles_and_a_pong_changes_nothing() {
        let (mut replies, requests) = gate(Market::Spot, QualityReporter::disabled()).open();
        let pong = br#"{"time":1791182070,"time_ms":1791182070045,"conn_id":"cf39484c4f270f22","channel":"spot.pong","event":"","result":null}"#;

        for id in 0..requests.len() {
            assert!(!replies.settled());
            assert!(matches!(
                replies.on_text(ack(id).as_bytes()),
                Verdict::Continue
            ));
        }
        assert!(replies.settled());
        assert!(matches!(replies.on_text(pong), Verdict::Continue));
    }

    #[test]
    fn a_refusal_is_reported_once_across_sessions() {
        let (quality, events) = QualityReporter::test_channel();
        let gate = gate(Market::Spot, quality);

        for _ in 0..2 {
            let (mut replies, _) = gate.open();
            assert!(matches!(
                replies.on_text(refusal(4, 2).as_bytes()),
                Verdict::Continue
            ));
        }

        let QualityEvent::FeedDegraded { source, detail, .. } = events.try_recv().unwrap() else {
            panic!("expected a degraded feed");
        };
        assert_eq!(source, "gate-sbe/ETH_USDT");
        assert_eq!(
            detail,
            "spot.trades refused: 2 unknown currency pair: NOPE_USDT"
        );
        assert!(events.try_recv().is_err());
    }

    #[test]
    fn a_session_with_every_subscription_refused_gives_up() {
        let (mut replies, requests) = gate(Market::Spot, QualityReporter::disabled()).open();
        let last = requests.len() - 1;
        for id in 0..last {
            replies.on_text(refusal(id, 2).as_bytes());
        }
        assert!(matches!(
            replies.on_text(refusal(last, 2).as_bytes()),
            Verdict::End(Loss::Refused(_))
        ));
    }

    #[test]
    fn a_server_error_reconnects_or_replaces() {
        let (mut replies, requests) = gate(Market::Spot, QualityReporter::disabled()).open();
        assert!(matches!(
            replies.on_text(refusal(0, 3).as_bytes()),
            Verdict::End(Loss::Rejected(_))
        ));

        let (mut replies, _) = gate(Market::Spot, QualityReporter::disabled()).open();
        for id in 0..requests.len() {
            replies.on_text(ack(id).as_bytes());
        }
        assert!(matches!(
            replies.on_text(refusal(0, 3).as_bytes()),
            Verdict::Relieve(Reason::VenueError)
        ));
    }

    #[test]
    fn a_contiguous_book_stream_asks_for_nothing() {
        let (mut replies, _) = gate(Market::Spot, QualityReporter::disabled()).open();
        for frame in [
            obu(true, 0, 100, "ob.BTC_USDT.50"),
            obu(false, 101, 105, "ob.BTC_USDT.50"),
            obu(true, 0, 7, "ob.ETH_USDT.50"),
            obu(false, 106, 106, "ob.BTC_USDT.50"),
            obu(false, 8, 9, "ob.ETH_USDT.50"),
        ] {
            assert!(matches!(replies.on_binary(&frame), Verdict::Continue));
        }
    }

    #[test]
    fn a_gap_in_this_sockets_book_asks_for_a_fresh_snapshot() {
        let (mut replies, _) = gate(Market::Spot, QualityReporter::disabled()).open();
        replies.on_binary(&obu(true, 0, 100, "ob.BTC_USDT.50"));

        assert!(matches!(
            replies.on_binary(&obu(false, 103, 105, "ob.BTC_USDT.50")),
            Verdict::Relieve(Reason::Gap)
        ));
        // A later snapshot re-anchors the chain.
        assert!(matches!(
            replies.on_binary(&obu(true, 0, 200, "ob.BTC_USDT.50")),
            Verdict::Continue
        ));
        assert!(matches!(
            replies.on_binary(&obu(false, 201, 202, "ob.BTC_USDT.50")),
            Verdict::Continue
        ));
    }

    #[test]
    fn a_delta_before_its_snapshot_is_a_gap() {
        let (mut replies, _) = gate(Market::Spot, QualityReporter::disabled()).open();
        assert!(matches!(
            replies.on_binary(&obu(false, 5, 9, "ob.BTC_USDT.50")),
            Verdict::Relieve(Reason::Gap)
        ));
    }
}
