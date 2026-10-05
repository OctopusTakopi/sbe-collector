//! Subscription bookkeeping for venues that subscribe over the socket, one
//! request per channel and instrument so that an error names its request:
//! which requests a session has had answered, and which refusals the process
//! has already reported.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tracing::error;

use crate::{
    quality::{QualityEvent, QualityReporter},
    session::{Loss, Reason, Verdict},
};

/// One channel of one instrument.
pub(crate) struct Topic<T> {
    pub(crate) channel: &'static str,
    /// As the venue spells it, for reports.
    pub(crate) instrument: Arc<str>,
    /// What the venue's request names it by: OKX's `instIdCode`, Gate's
    /// payload.
    pub(crate) target: T,
}

/// A venue's topics, shared by every session of the process, numbered by
/// their request id.
pub(crate) struct Topics<T> {
    /// Names the venue in quality events.
    source: &'static str,
    topics: Box<[Topic<T>]>,
    /// Topics whose refusal the quality log already has. A refusal is the
    /// account's or the listing's condition and recurs on every reconnect,
    /// so it is reported once per process, not once per session.
    reported: Box<[AtomicBool]>,
    quality: QualityReporter,
}

impl<T> Topics<T> {
    pub(crate) fn new(
        source: &'static str,
        topics: impl IntoIterator<Item = Topic<T>>,
        quality: QualityReporter,
    ) -> Self {
        let topics: Box<[Topic<T>]> = topics.into_iter().collect();
        let reported = topics.iter().map(|_| AtomicBool::new(false)).collect();
        Self {
            source,
            topics,
            reported,
            quality,
        }
    }

    /// Each topic with its request id.
    pub(crate) fn numbered(&self) -> impl Iterator<Item = (usize, &Topic<T>)> {
        self.topics.iter().enumerate()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Answer {
    Pending,
    Acked,
    Refused,
}

/// One session's answers, one per topic.
pub(crate) struct Answers<T> {
    topics: Arc<Topics<T>>,
    answers: Vec<Answer>,
    pending: usize,
}

impl<T> Answers<T> {
    pub(crate) fn new(topics: Arc<Topics<T>>) -> Self {
        let pending = topics.topics.len();
        Self {
            topics,
            answers: vec![Answer::Pending; pending],
            pending,
        }
    }

    /// The topic a reply's request id names, if this session sent it.
    pub(crate) fn topic(&self, id: usize) -> Option<usize> {
        (id < self.answers.len()).then_some(id)
    }

    fn answer(&mut self, topic: usize, answer: Answer) {
        if self.answers[topic] == Answer::Pending {
            self.pending -= 1;
        }
        self.answers[topic] = answer;
    }

    pub(crate) fn ack(&mut self, topic: usize) {
        self.answer(topic, Answer::Acked);
    }

    /// A refusal no reconnect can cure answers its topic. It is reported
    /// once per process, and a session whose every topic is refused gives
    /// up: nothing will ever flow on it.
    pub(crate) fn refuse(&mut self, topic: usize, refusal: String) -> Verdict {
        let Topic {
            channel,
            instrument,
            ..
        } = &self.topics.topics[topic];
        if !self.topics.reported[topic].swap(true, Ordering::Relaxed) {
            error!(%instrument, channel, refusal, "the venue refused a subscription");
            self.topics.quality.report(QualityEvent::FeedDegraded {
                at_ns: QualityEvent::now_ns(),
                source: format!("{}/{instrument}", self.topics.source),
                detail: format!("{channel} refused: {refusal}"),
            });
        }
        self.answer(topic, Answer::Refused);
        if self.answers.iter().all(|&answer| answer == Answer::Refused) {
            Verdict::End(Loss::Refused(refusal))
        } else {
            Verdict::Continue
        }
    }

    /// Any other error the venue reports. Before the last reply a fresh
    /// connection is the cheap retry; after it, this socket carries the feed
    /// and a replacement takes over make-before-break.
    pub(crate) fn reject(&self, detail: String) -> Verdict {
        error!(detail, "the venue rejected a request");
        if self.settled() {
            Verdict::Relieve(Reason::VenueError)
        } else {
            Verdict::End(Loss::Rejected(detail))
        }
    }

    /// Every topic answered.
    pub(crate) fn settled(&self) -> bool {
        self.pending == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topics(quality: QualityReporter) -> Arc<Topics<i64>> {
        let instrument: Arc<str> = Arc::from("BTC-USDT");
        Arc::new(Topics::new(
            "okx-sbe",
            [("bbo-tbt", 3), ("trades", 3)].map(|(channel, target)| Topic {
                channel,
                instrument: Arc::clone(&instrument),
                target,
            }),
            quality,
        ))
    }

    #[test]
    fn every_answer_settles_a_session() {
        let mut answers = Answers::new(topics(QualityReporter::disabled()));

        assert_eq!(answers.topic(1), Some(1));
        assert_eq!(answers.topic(2), None, "not a request this session sent");
        answers.ack(0);
        assert!(!answers.settled());
        answers.ack(0);
        assert!(!answers.settled(), "a repeated answer counts once");
        assert!(matches!(
            answers.refuse(1, "64003 fee tier".to_owned()),
            Verdict::Continue
        ));
        assert!(answers.settled());
    }

    #[test]
    fn a_refusal_is_reported_once_across_sessions() {
        let (quality, events) = QualityReporter::test_channel();
        let topics = topics(quality);

        for _ in 0..2 {
            let mut answers = Answers::new(Arc::clone(&topics));
            answers.refuse(1, "64003 fee tier".to_owned());
        }

        let QualityEvent::FeedDegraded { source, detail, .. } = events.try_recv().unwrap() else {
            panic!("expected a degraded feed");
        };
        assert_eq!(source, "okx-sbe/BTC-USDT");
        assert_eq!(detail, "trades refused: 64003 fee tier");
        assert!(events.try_recv().is_err());
    }

    #[test]
    fn a_session_with_every_topic_refused_gives_up() {
        let mut answers = Answers::new(topics(QualityReporter::disabled()));
        answers.refuse(0, "60018".to_owned());
        assert!(matches!(
            answers.refuse(1, "60018".to_owned()),
            Verdict::End(Loss::Refused(_))
        ));
    }

    #[test]
    fn other_errors_reconnect_or_replace() {
        let mut answers = Answers::new(topics(QualityReporter::disabled()));
        assert!(matches!(
            answers.reject("60014".to_owned()),
            Verdict::End(Loss::Rejected(_))
        ));

        answers.ack(0);
        answers.ack(1);
        assert!(matches!(
            answers.reject("60014".to_owned()),
            Verdict::Relieve(Reason::VenueError)
        ));
    }
}
