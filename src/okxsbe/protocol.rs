//! OKX's `public-sbe` websocket: a login carried in the upgrade headers, one
//! JSON subscription per channel and instrument, SBE frames back.

use std::{
    borrow::Cow,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use base64::Engine as _;
use hmac::{Hmac, Mac};
use hyper::header::{HeaderMap, HeaderValue};
use jiff::Timestamp;
use serde::Deserialize;
use sha2::Sha256;
use tracing::{error, info, warn};

use super::instruments::Instrument;
use crate::{
    quality::{QualityEvent, QualityReporter},
    session::{Handshake, Loss, Protocol, Reason, Session, Verdict},
};

const URL: &str = "wss://ws.okx.com:8443/ws/v5/public-sbe";

/// What the login signature covers after its timestamp: always this
/// request, whatever the connection is for.
const SIGNED_REQUEST: &str = "GET/users/self/verify";

/// Pushed about 60 s before the venue closes a connection for an upgrade.
const NOTICE_SERVICE_UPGRADE: &str = "64008";

/// The account's trading fee tier is below the channel's (VIP4 for
/// `books-l2-tbt` and `trades`).
const TIER_TOO_LOW: &str = "64003";

/// No such channel or `instIdCode`: a relisting changes an instrument's code.
const NO_SUCH_TOPIC: &str = "60018";

/// The API key, its secret and passphrase. Deliberately not `Debug`.
pub(crate) struct Credentials {
    api_key: HeaderValue,
    /// Keyed once; each signature works on a clone.
    secret: Hmac<Sha256>,
    passphrase: HeaderValue,
}

impl Credentials {
    pub(crate) fn from_env() -> anyhow::Result<Self> {
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| anyhow::anyhow!("{name} must be set for the OKX SBE endpoint"))
        };
        Self::new(
            &var("OKX_API_KEY")?,
            &var("OKX_SECRET_KEY")?,
            &var("OKX_PASSPHRASE")?,
        )
    }

    fn new(api_key: &str, secret_key: &str, passphrase: &str) -> anyhow::Result<Self> {
        let header = |value: &str, name: &str| {
            let mut value = HeaderValue::from_str(value)
                .map_err(|_| anyhow::anyhow!("{name} is not a valid header value"))?;
            value.set_sensitive(true);
            anyhow::Ok(value)
        };
        Ok(Self {
            api_key: header(api_key, "OKX_API_KEY")?,
            secret: Hmac::new_from_slice(secret_key.as_bytes())
                .map_err(|_| anyhow::anyhow!("OKX_SECRET_KEY cannot key HMAC-SHA256"))?,
            passphrase: header(passphrase, "OKX_PASSPHRASE")?,
        })
    }

    /// `base64(HMAC-SHA256(secret, timestamp + "GET/users/self/verify"))`.
    fn sign(&self, timestamp: &str) -> String {
        let mut mac = self.secret.clone();
        mac.update(timestamp.as_bytes());
        mac.update(SIGNED_REQUEST.as_bytes());
        base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    }
}

/// One channel of one instrument.
struct Topic {
    channel: &'static str,
    code: i64,
    inst_id: Arc<str>,
}

/// What every OKX connection slot shares.
pub(crate) struct Okx {
    credentials: Credentials,
    topics: Arc<[Topic]>,
    /// Topics whose refusal the quality log already has. A refusal is the
    /// account's condition and recurs on every reconnect, so it is reported
    /// once per process, not once per session.
    reported: Arc<[AtomicBool]>,
    quality: QualityReporter,
}

impl Okx {
    pub(crate) fn new(
        credentials: Credentials,
        instruments: &[Instrument],
        channels: &[&'static str],
        quality: QualityReporter,
    ) -> Self {
        let topics: Arc<[Topic]> = instruments
            .iter()
            .flat_map(|instrument| {
                let inst_id: Arc<str> = Arc::from(instrument.inst_id.as_str());
                channels.iter().map(move |&channel| Topic {
                    channel,
                    code: instrument.code,
                    inst_id: Arc::clone(&inst_id),
                })
            })
            .collect();
        let reported = topics.iter().map(|_| AtomicBool::new(false)).collect();
        Self {
            credentials,
            topics,
            reported,
            quality,
        }
    }
}

impl Protocol for Okx {
    type Session = Replies;

    const SOURCE: &'static str = "okx-sbe";

    /// The login rides on the upgrade: OKX rejects a timestamp more than 30 s
    /// old, so the signature is made per attempt.
    fn handshake(&self) -> anyhow::Result<Handshake<'_>> {
        let timestamp = Timestamp::now().as_second().to_string();
        let mut sign = HeaderValue::from_str(&self.credentials.sign(&timestamp))?;
        sign.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert("ok-access-key", self.credentials.api_key.clone());
        headers.insert("ok-access-sign", sign);
        headers.insert("ok-access-timestamp", HeaderValue::from_str(&timestamp)?);
        headers.insert("ok-access-passphrase", self.credentials.passphrase.clone());
        Ok(Handshake { url: URL, headers })
    }

    fn max_age(&self, _connection: usize) -> Option<Duration> {
        None
    }

    /// One request per topic: an error names the request's `id` but not its
    /// channel, so only one topic per request can be told apart.
    fn open(&self) -> (Replies, Vec<String>) {
        let requests = self
            .topics
            .iter()
            .enumerate()
            .map(|(id, topic)| {
                serde_json::json!({
                    "id": id.to_string(),
                    "op": "subscribe",
                    "args": [{ "channel": topic.channel, "instIdCode": topic.code }],
                })
                .to_string()
            })
            .collect();
        let replies = Replies {
            topics: Arc::clone(&self.topics),
            answers: vec![Answer::Pending; self.topics.len()],
            pending: self.topics.len(),
            reported: Arc::clone(&self.reported),
            quality: self.quality.clone(),
        };
        (replies, requests)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Answer {
    Pending,
    Acked,
    Refused,
}

/// A text frame from OKX; fields this does not use are ignored.
#[derive(Deserialize)]
struct Reply<'a> {
    #[serde(borrow)]
    event: Option<Cow<'a, str>>,
    #[serde(borrow)]
    id: Option<Cow<'a, str>>,
    #[serde(borrow)]
    code: Option<Cow<'a, str>>,
    #[serde(borrow)]
    msg: Option<Cow<'a, str>>,
}

/// One session's subscriptions and what OKX answered.
pub(crate) struct Replies {
    topics: Arc<[Topic]>,
    answers: Vec<Answer>,
    pending: usize,
    reported: Arc<[AtomicBool]>,
    quality: QualityReporter,
}

impl Replies {
    fn answer(&mut self, topic: usize, answer: Answer) {
        if self.answers[topic] == Answer::Pending {
            self.pending -= 1;
        }
        self.answers[topic] = answer;
    }

    fn report_refusal(&self, topic: usize, refusal: &str) {
        let Topic {
            channel, inst_id, ..
        } = &self.topics[topic];
        if self.reported[topic].swap(true, Ordering::Relaxed) {
            return;
        }
        error!(%inst_id, channel, refusal, "OKX refused a subscription");
        self.quality.report(QualityEvent::FeedDegraded {
            at_ns: QualityEvent::now_ns(),
            source: format!("{}/{inst_id}", Okx::SOURCE),
            detail: format!("{channel} refused: {refusal}"),
        });
    }
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
        let topic = reply
            .id
            .as_deref()
            .and_then(|id| id.parse::<usize>().ok())
            .filter(|&topic| topic < self.answers.len());
        let code = reply.code.as_deref().unwrap_or("");
        let refusal = || format!("{code} {}", reply.msg.as_deref().unwrap_or(""));
        match (reply.event.as_deref(), topic) {
            (Some("subscribe"), Some(topic)) => {
                self.answer(topic, Answer::Acked);
                Verdict::Continue
            }
            (Some("error"), Some(topic)) if matches!(code, TIER_TOO_LOW | NO_SUCH_TOPIC) => {
                self.report_refusal(topic, &refusal());
                self.answer(topic, Answer::Refused);
                if self.answers.iter().all(|&answer| answer == Answer::Refused) {
                    Verdict::End(Loss::Refused(refusal()))
                } else {
                    Verdict::Continue
                }
            }
            // Before the last reply a fresh connection is the cheap retry;
            // after it, this socket carries the feed and a replacement
            // takes over make-before-break.
            (Some("error"), _) => {
                error!(refusal = %refusal(), "OKX rejected a request");
                if self.settled() {
                    Verdict::Relieve(Reason::VenueError)
                } else {
                    Verdict::End(Loss::Rejected(refusal()))
                }
            }
            (Some("notice"), _) if code == NOTICE_SERVICE_UPGRADE => {
                info!(notice = %refusal(), "OKX announced a service upgrade");
                Verdict::Relieve(Reason::ServerShutdown)
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

    fn settled(&self) -> bool {
        self.pending == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credentials() -> Credentials {
        Credentials::new("key", "22582BD0CFF14C41EDBF1AB98506286D", "phrase").unwrap()
    }

    fn okx(quality: QualityReporter) -> Okx {
        let instruments = [
            Instrument {
                inst_id: "BTC-USDT".to_owned(),
                code: 3,
            },
            Instrument {
                inst_id: "BTC-USDT-SWAP".to_owned(),
                code: 10_459,
            },
        ];
        Okx::new(credentials(), &instruments, &["bbo-tbt"], quality)
    }

    /// Checked against Python's `hmac` on the same inputs.
    #[test]
    fn the_signature_matches_an_independent_computation() {
        assert_eq!(
            credentials().sign("1538054050"),
            "+LdIr8lkkvhr5hoA3g9TMC0+uQJ849ftAcocA/ouu4M="
        );
    }

    #[test]
    fn the_handshake_signs_a_fresh_timestamp() {
        let okx = okx(QualityReporter::disabled());
        let handshake = okx.handshake().unwrap();
        let headers = &handshake.headers;

        assert_eq!(handshake.url, URL);
        assert_eq!(headers["ok-access-key"], "key");
        assert_eq!(headers["ok-access-passphrase"], "phrase");
        let timestamp = headers["ok-access-timestamp"].to_str().unwrap();
        let age = Timestamp::now().as_second() - timestamp.parse::<i64>().unwrap();
        assert!((0..=1).contains(&age), "{timestamp}");
        assert_eq!(
            headers["ok-access-sign"],
            credentials().sign(timestamp).as_str()
        );
        for secret in ["ok-access-key", "ok-access-sign", "ok-access-passphrase"] {
            assert!(headers[secret].is_sensitive(), "{secret}");
        }
    }

    #[test]
    fn each_topic_is_its_own_request() {
        let (_, requests) = okx(QualityReporter::disabled()).open();
        assert_eq!(
            requests,
            [
                r#"{"args":[{"channel":"bbo-tbt","instIdCode":3}],"id":"0","op":"subscribe"}"#,
                r#"{"args":[{"channel":"bbo-tbt","instIdCode":10459}],"id":"1","op":"subscribe"}"#,
            ]
        );
    }

    #[test]
    fn every_ack_settles_the_session() {
        let (mut replies, _) = okx(QualityReporter::disabled()).open();

        assert!(!replies.settled());
        let ack = |id| {
            format!(
                r#"{{"id":"{id}","event":"subscribe","arg":{{"channel":"bbo-tbt","instIdCode":3}},"connId":"accb8e21"}}"#
            )
        };
        assert!(matches!(
            replies.on_text(ack(0).as_bytes()),
            Verdict::Continue
        ));
        assert!(!replies.settled());
        assert!(matches!(
            replies.on_text(ack(1).as_bytes()),
            Verdict::Continue
        ));
        assert!(replies.settled());
    }

    /// A refusal settles its topic and is reported once per process, however
    /// many sessions meet it.
    #[test]
    fn a_refusal_is_reported_once_across_sessions() {
        let (quality, events) = QualityReporter::test_channel();
        let okx = okx(quality);
        let refused = br#"{"id":"1","event":"error","msg":"Your trading fee tier doesn't meet the requirement to access this channel.","code":"64003","connId":"a9e3ef40"}"#;
        let acked = br#"{"id":"0","event":"subscribe","arg":{"channel":"bbo-tbt","instIdCode":3},"connId":"a9e3ef40"}"#;

        for _ in 0..2 {
            let (mut replies, _) = okx.open();
            assert!(matches!(replies.on_text(acked), Verdict::Continue));
            assert!(matches!(replies.on_text(refused), Verdict::Continue));
            assert!(replies.settled(), "a refusal answers its subscription");
        }

        let QualityEvent::FeedDegraded { source, detail, .. } = events.try_recv().unwrap() else {
            panic!("expected a degraded feed");
        };
        assert_eq!(source, "okx-sbe/BTC-USDT-SWAP");
        assert!(detail.starts_with("bbo-tbt refused: 64003"), "{detail}");
        assert!(events.try_recv().is_err(), "reported once, not per session");
    }

    #[test]
    fn a_session_with_every_subscription_refused_gives_up() {
        let (mut replies, _) = okx(QualityReporter::disabled()).open();
        let refused = |id| {
            format!(
                r#"{{"id":"{id}","event":"error","msg":"Wrong URL or channel:bbo-tbt,instIdCode:3 doesn't exist.","code":"60018","connId":"d39d1fe0"}}"#
            )
        };

        assert!(matches!(
            replies.on_text(refused(0).as_bytes()),
            Verdict::Continue
        ));
        assert!(matches!(
            replies.on_text(refused(1).as_bytes()),
            Verdict::End(Loss::Refused(_))
        ));
    }

    /// Any other error is retried by a fresh connection before the session
    /// carries the feed, and by a make-before-break replacement after.
    #[test]
    fn other_errors_reconnect_or_replace() {
        let error = br#"{"event":"error","msg":"Requests too frequent.","code":"60014","connId":"d39d1fe0"}"#;
        let (mut replies, _) = okx(QualityReporter::disabled()).open();
        assert!(matches!(
            replies.on_text(error),
            Verdict::End(Loss::Rejected(_))
        ));

        let (mut replies, _) = okx(QualityReporter::disabled()).open();
        for id in 0..2 {
            let ack = format!(r#"{{"id":"{id}","event":"subscribe"}}"#);
            replies.on_text(ack.as_bytes());
        }
        assert!(matches!(
            replies.on_text(error),
            Verdict::Relieve(Reason::VenueError)
        ));
    }

    #[test]
    fn a_service_upgrade_notice_asks_for_a_replacement() {
        let (mut replies, _) = okx(QualityReporter::disabled()).open();
        let notice = br#"{"event":"notice","code":"64008","msg":"The connection will soon be closed for a service upgrade. Please reconnect.","connId":"a4d3ae55"}"#;
        assert!(matches!(
            replies.on_text(notice),
            Verdict::Relieve(Reason::ServerShutdown)
        ));
    }

    #[test]
    fn unknown_text_changes_nothing() {
        let (mut replies, _) = okx(QualityReporter::disabled()).open();
        for text in [
            &b"pong"[..],
            br#"{"event":"channel-conn-count","connCount":"2"}"#,
        ] {
            assert!(matches!(replies.on_text(text), Verdict::Continue));
        }
        assert!(!replies.settled());
    }
}
