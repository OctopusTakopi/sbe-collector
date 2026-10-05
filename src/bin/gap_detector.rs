//! Gap detector for the sbe-collector's recording files.
//!
//! Scans directories for `<symbol>_<YYYYMMDD>.zst` files (plus handover
//! sidecars and legacy segments), groups them into per-symbol series, and
//! reports:
//!
//! * receive-time gaps above a threshold and receive-time regressions,
//!   including across the midnight rotation;
//! * venue sequence breaks. Redundant connections interleave, so each chain
//!   tracks its running maximum and only forward breaks count:
//!   - Binance depth diffs (10003): `firstBookUpdateId` normally follows the
//!     previous `lastBookUpdateId + 1` (the collector's own rule);
//!   - Binance trades (10000): trade ids increment by one per trade;
//!   - best bid/ask, Binance's `bookUpdateId` (10001), OKX's `bbo-tbt`
//!     `seqId` (1000) and Gate's `u` (1): must not go backwards;
//!   - Gate's `obu` book (3): each delta starts at the previous one's last
//!     id plus one, and a full snapshot re-anchors the chain;
//!   - Gate's trades (2): `idMarket` (spot) and trade ids (futures)
//!     increment by one per trade;
//! * range accounting for trade ids: ids are unique, so
//!   `(max - min + 1) - count` nets missing ids against late duplicates
//!   whatever the order. `--exact` builds id sets to split the two;
//! * event-time regressions per template;
//! * missing calendar dates within a series;
//! * undecodable, malformed, or foreign content (such as the JSON-line files
//!   of the sister collector, which are skipped).
//!
//! Sequence state carries across a series' files, so a break straddling the
//! rotation is attributed correctly.
//!
//! A series is decoded as the exchange its directory's `_exchange` marker
//! names, or Binance spot for a directory recorded before markers existed;
//! `--exchange` overrides.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fs::{self, File},
    io::Read,
    ops::AddAssign,
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use anyhow::{Context as _, Result};
use clap::Parser;
use jiff::{Span, Timestamp, civil, tz::TimeZone};
use sbe_collector::{
    exchange::Exchange,
    record::{HEADER_LEN, Header, Tag},
    sbe::{
        GroupSize16Encoding, MessageHeader,
        binance::{
            BestBidAskBlock, DepthDiffBlock, GroupSizeEncoding, TEMPLATE_BEST_BID_ASK,
            TEMPLATE_DEPTH_DIFF, TEMPLATE_DEPTH_SNAPSHOT, TEMPLATE_TRADES, TradesBlock,
        },
        event_key,
        gate::{
            self, FuturesBboBlock, FuturesTrade, FuturesTradeBlock, Market, ObuBlock, SpotBboBlock,
            SpotTradeBlock,
        },
        okx::{self, BboTbtBlock},
        read_group,
    },
};
use zerocopy::FromBytes;

/// A file whose tail is newer than this is assumed to still be written by a
/// live collector, so an unterminated zstd frame is labelled as such.
const LIVE_FILE_WINDOW: Duration = Duration::from_secs(15 * 60);

/// Break timestamps kept per stream.
const MAX_BREAK_TIMES: usize = 16;

/// Sanity ceiling for one payload; beyond it the stream is corrupt or
/// foreign.
const MAX_PAYLOAD_LEN: u32 = 64 * 1024 * 1024;

/// How far apart two rolling processes' copies of one event may be received
/// to be paired. Pairing is also confined to the runs' observed overlap.
const OVERLAP_DEDUP_WINDOW_NS: i64 = 5_000_000_000;

#[derive(Parser)]
#[command(version, about = "Detect gaps in the sbe-collector's recording files")]
struct Args {
    /// Directories to scan recursively for <symbol>_<YYYYMMDD>.zst files.
    #[arg(default_values = ["."])]
    paths: Vec<PathBuf>,

    /// Minimum silence between consecutive records, in seconds, to count as a gap.
    #[arg(long, default_value_t = 5.0)]
    min_gap: f64,

    /// Maximum number of individual gaps to print per series.
    #[arg(long, default_value_t = 10)]
    max_reported: usize,

    /// Only scan series whose path contains this substring.
    #[arg(long)]
    filter: Option<String>,

    /// Worker threads (0 = auto: one per CPU, capped at 16).
    #[arg(long, default_value_t = 0)]
    jobs: usize,

    /// Build exact id sets for trade streams: splits the net id deficit into
    /// exact missing and duplicate counts. Uses roughly 16 bytes per id.
    #[arg(long)]
    exact: bool,

    /// Exit with status 1 if any gap, sequence break, or decode error is found.
    #[arg(long)]
    fail_on_gaps: bool,

    /// Decode every series as this exchange instead of the one its directory
    /// names.
    #[arg(long)]
    exchange: Option<Exchange>,
}

/// A recording file, as its name describes it.
struct DatedFile {
    symbol: String,
    path: PathBuf,
    date: civil::Date,
    /// Legacy segments only; `i64::MIN` sorts daily files and sidecars first.
    segment_start_ns: i64,
    run_id: Option<String>,
}

struct Series {
    key: String,
    files: Vec<DatedFile>,
    exchange: Exchange,
}

/// Discover daily files, handover sidecars and legacy segments under the
/// roots, grouped into per-(directory, symbol) series in recording order,
/// each with the exchange to decode it as.
fn discover(roots: &[PathBuf], exchange: Option<Exchange>) -> Result<Vec<Series>> {
    let mut map: BTreeMap<(PathBuf, String), Vec<DatedFile>> = BTreeMap::new();
    let mut stack: Vec<PathBuf> = Vec::new();
    for root in roots {
        let meta =
            fs::metadata(root).with_context(|| format!("cannot access {}", root.display()))?;
        if meta.is_dir() || root.extension().is_some_and(|ext| ext == "zst") {
            stack.push(root.clone());
        }
    }
    let mut skipped = 0u64;
    while let Some(dir) = stack.pop() {
        let meta = fs::metadata(&dir)?;
        if meta.is_file() {
            if dir.extension().is_none_or(|ext| ext != "zst") {
                continue;
            }
            if let Some(file) = dated_file(&dir) {
                let parent = dir.parent().unwrap_or(Path::new(".")).to_path_buf();
                map.entry((parent, file.symbol.clone()))
                    .or_default()
                    .push(file);
            } else {
                skipped += 1;
            }
            continue;
        }
        if !meta.is_dir() {
            continue;
        }
        for entry in fs::read_dir(&dir)
            .with_context(|| format!("cannot read directory {}", dir.display()))?
        {
            stack.push(entry?.path());
        }
    }
    if skipped > 0 {
        eprintln!(
            "note: skipped {skipped} .zst file(s) not named <symbol>_<YYYYMMDD>.zst or a handover sidecar"
        );
    }

    let mut exchanges: HashMap<PathBuf, Exchange> = HashMap::new();
    let mut series: Vec<Series> = Vec::with_capacity(map.len());
    for ((dir, symbol), mut files) in map {
        files.sort_by(|a, b| {
            (a.date, a.segment_start_ns, &a.run_id, &a.path).cmp(&(
                b.date,
                b.segment_start_ns,
                &b.run_id,
                &b.path,
            ))
        });
        let exchange = match (exchange, exchanges.get(&dir)) {
            (Some(exchange), _) | (None, Some(&exchange)) => exchange,
            (None, None) => {
                let exchange = Exchange::of_recordings_in(&dir)
                    .with_context(|| format!("cannot tell the exchange of {}", dir.display()))?;
                exchanges.insert(dir.clone(), exchange);
                exchange
            }
        };
        series.push(Series {
            key: format!("{}/{symbol}", dir.display()),
            files,
            exchange,
        });
    }
    series.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(series)
}

fn dated_file(path: &Path) -> Option<DatedFile> {
    if path.extension().is_none_or(|ext| ext != "zst") {
        return None;
    }
    let stem = path.file_stem()?.to_str()?;
    let file = |symbol: &str, date, segment_start_ns, run_id| DatedFile {
        symbol: symbol.to_owned(),
        path: path.to_path_buf(),
        date,
        segment_start_ns,
        run_id,
    };

    // Legacy segment: <symbol>_<date>_<segment-start-ns>_<run-id>.zst.
    // `rsplitn` keeps underscores in symbols; the segment start is epoch
    // nanoseconds, never 8 digits.
    let mut parts = stem.rsplitn(4, '_');
    let run_id = parts.next()?;
    if let (Some(segment_start), Some(date), Some(symbol)) =
        (parts.next(), parts.next(), parts.next())
        && segment_start.len() != 8
        && let (Ok(segment_start_ns), Ok(date)) = (
            segment_start.parse::<i64>(),
            civil::Date::strptime("%Y%m%d", date),
        )
    {
        return Some(file(
            symbol,
            date,
            segment_start_ns,
            Some(run_id.to_owned()),
        ));
    }

    // Handover sidecar: <symbol>_<date>_<run-id>.zst.
    let mut parts = stem.rsplitn(3, '_');
    let run_id = parts.next()?;
    if let (Some(date), Some(symbol)) = (parts.next(), parts.next())
        && let Ok(date) = civil::Date::strptime("%Y%m%d", date)
    {
        return Some(file(symbol, date, i64::MIN, Some(run_id.to_owned())));
    }

    // Daily: <symbol>_<date>.zst.
    let (symbol, date) = stem.rsplit_once('_')?;
    let date = civil::Date::strptime("%Y%m%d", date).ok()?;
    Some(file(symbol, date, i64::MIN, None))
}

/// A run id without its `.late<n>` suffix.
fn logical_run_id(run_id: &str) -> &str {
    match run_id.rsplit_once(".late") {
        Some((base, suffix))
            if !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            base
        }
        _ => run_id,
    }
}

/// A venue id chain checked per series. Declared in report order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stream {
    BestBidAsk,
    DepthDiff,
    Trades,
}

impl Stream {
    const ALL: [Self; 3] = [Self::BestBidAsk, Self::DepthDiff, Self::Trades];

    fn name(self) -> &'static str {
        match self {
            Self::BestBidAsk => "best-bid-ask",
            Self::DepthDiff => "depth-diff",
            Self::Trades => "trades",
        }
    }

    /// Unique per-event ids, so range accounting applies.
    fn has_point_ids(self) -> bool {
        self == Self::Trades
    }
}

#[derive(Clone, Copy, Default)]
struct StreamStats {
    messages: u64,
    /// Forward breaks in the venue id chain: ids never seen between two
    /// consecutively recorded events.
    seq_fwd_events: u64,
    seq_fwd_ids: u64,
    /// Events that added nothing new: late duplicates or reorders.
    seq_back_events: u64,
    /// Range accounting for point-id streams.
    id_min: i64,
    id_max: i64,
    id_count: u64,
    /// Only with `--exact`.
    exact_missing: u64,
    exact_dups: u64,
    /// Exchange event times that went backwards.
    time_regressions: u64,
}

impl StreamStats {
    /// Net id deficit over the observed range: positive means at least that
    /// many ids are missing, negative at least that many duplicates.
    fn net_deficit(&self) -> i64 {
        if self.id_count == 0 {
            return 0;
        }
        (self.id_max - self.id_min + 1) - self.id_count as i64
    }

    fn is_notable(&self) -> bool {
        self.id_count > 0
            || self.seq_fwd_events > 0
            || self.seq_back_events > 0
            || self.time_regressions > 0
    }
}

#[derive(Default)]
struct StreamState {
    stats: StreamStats,
    /// Running maximum of the chain's id.
    last_id: Option<i64>,
    last_time: Option<i64>,
    /// Only with `--exact`, and only for point-id streams.
    ids: Option<HashSet<i64>>,
    /// Receive times of forward breaks, for locating incidents.
    break_times: Vec<i64>,
}

impl StreamState {
    /// An id that may jump forward but must not go back, such as a best
    /// bid/ask's book update id: the quote changes less often than the book.
    fn non_decreasing(&mut self, id: i64) {
        if self.last_id.is_some_and(|prev| id < prev) {
            self.stats.seq_back_events += 1;
        }
        self.advance(id);
    }

    fn note_break(&mut self, recv: i64) {
        if self.break_times.len() < MAX_BREAK_TIMES {
            self.break_times.push(recv);
        }
    }

    fn check_time(&mut self, event_time: i64) {
        if self.last_time.is_some_and(|prev| event_time < prev) {
            self.stats.time_regressions += 1;
        }
        self.last_time = Some(event_time);
    }

    fn advance(&mut self, id: i64) {
        self.last_id = Some(self.last_id.map_or(id, |prev| prev.max(id)));
    }

    /// Continuity of an id that increments by one (trade ids). Tracking the
    /// running maximum keeps one reorder from poisoning later checks.
    fn chain(&mut self, id: i64, recv: i64) {
        if let Some(prev) = self.last_id {
            if id > prev + 1 {
                self.stats.seq_fwd_events += 1;
                self.stats.seq_fwd_ids += (id - prev - 1) as u64;
                self.note_break(recv);
            } else if id <= prev {
                self.stats.seq_back_events += 1;
            }
        }
        self.advance(id);
    }

    fn observe_point_id(&mut self, id: i64) {
        let stats = &mut self.stats;
        if stats.id_count == 0 {
            stats.id_min = id;
            stats.id_max = id;
        } else {
            stats.id_min = stats.id_min.min(id);
            stats.id_max = stats.id_max.max(id);
        }
        stats.id_count += 1;
        if let Some(ids) = &mut self.ids {
            ids.insert(id);
        }
    }

    fn into_report(mut self, stream: Stream) -> StreamReport {
        if let Some(ids) = self.ids
            && self.stats.id_count > 0
        {
            let distinct = ids.len() as u64;
            let span = (self.stats.id_max - self.stats.id_min + 1) as u64;
            self.stats.exact_missing = span.saturating_sub(distinct);
            self.stats.exact_dups = self.stats.id_count.saturating_sub(distinct);
        }
        self.break_times.sort_unstable();
        StreamReport {
            stream,
            stats: self.stats,
            break_times: self.break_times,
        }
    }
}

#[derive(Clone, Copy)]
struct Gap {
    start: i64,
    end: i64,
}

impl Gap {
    fn silence_ns(self) -> i64 {
        self.end - self.start
    }
}

#[derive(Default)]
struct SeriesScan {
    exchange: Exchange,
    exact: bool,
    rows: u64,
    sbe_records: u64,
    rest_records: u64,
    bad_records: u64,
    recv_regressions: u64,
    first_recv: Option<i64>,
    last_recv: Option<i64>,
    prev_recv: Option<i64>,
    gaps: Vec<Gap>,
    /// Indexed by [`Stream`].
    streams: [Option<StreamState>; 3],
}

impl SeriesScan {
    fn stream(&mut self, stream: Stream) -> &mut StreamState {
        let exact = self.exact;
        self.streams[stream as usize].get_or_insert_with(|| StreamState {
            ids: (exact && stream.has_point_ids()).then(HashSet::new),
            ..StreamState::default()
        })
    }

    fn observe_recv(&mut self, recv: i64, min_gap_ns: i64) {
        self.first_recv.get_or_insert(recv);
        if let Some(prev) = self.prev_recv {
            if recv < prev {
                self.recv_regressions += 1;
            } else if recv - prev > min_gap_ns {
                self.gaps.push(Gap {
                    start: prev,
                    end: recv,
                });
            }
        }
        self.prev_recv = Some(recv);
        self.last_recv = Some(recv);
    }

    fn process_record(&mut self, recv: i64, tag: Tag, payload: &[u8], min_gap_ns: i64) {
        self.observe_recv(recv, min_gap_ns);
        self.rows += 1;
        match tag {
            Tag::Sbe => {
                self.sbe_records += 1;
                match self.exchange {
                    Exchange::BinanceSbeSpot => self.binance_payload(recv, payload),
                    Exchange::OkxSbe => self.okx_payload(payload),
                    Exchange::GateSbeSpot => self.gate_payload(Market::Spot, recv, payload),
                    Exchange::GateSbeFutures => self.gate_payload(Market::Futures, recv, payload),
                }
            }
            Tag::Rest => self.rest_records += 1,
        }
    }

    fn binance_payload(&mut self, recv: i64, payload: &[u8]) {
        let Ok((header, body)) = MessageHeader::read_from_prefix(payload) else {
            self.bad_records += 1;
            return;
        };
        match header.template_id.get() {
            TEMPLATE_DEPTH_DIFF => self.depth_diff(recv, body),
            TEMPLATE_TRADES => self.trades(recv, body, header.block_length.get() as usize),
            TEMPLATE_BEST_BID_ASK => self.best_bid_ask(body),
            // Counted, but no chain of its own to check.
            TEMPLATE_DEPTH_SNAPSHOT => {}
            _ => self.bad_records += 1,
        }
    }

    /// The collector's own rule: only a diff starting beyond `prev + 1`
    /// leaves a hole.
    fn depth_diff(&mut self, recv: i64, body: &[u8]) {
        let Ok((block, _)) = DepthDiffBlock::read_from_prefix(body) else {
            self.bad_records += 1;
            return;
        };
        let state = self.stream(Stream::DepthDiff);
        state.stats.messages += 1;
        state.check_time(block.event_time.get());
        let first = block.first_book_update_id.get();
        let last = block.last_book_update_id.get();
        if let Some(prev) = state.last_id {
            if last <= prev {
                state.stats.seq_back_events += 1;
            } else if first > prev + 1 {
                state.stats.seq_fwd_events += 1;
                state.stats.seq_fwd_ids += (first - prev - 1) as u64;
                state.note_break(recv);
            }
        }
        state.advance(last);
    }

    /// Trade ids increment by one per trade, within a frame's group and
    /// across frames.
    fn trades(&mut self, recv: i64, body: &[u8], block_length: usize) {
        let Ok((block, _)) = TradesBlock::read_from_prefix(body) else {
            self.bad_records += 1;
            return;
        };
        let state = self.stream(Stream::Trades);
        state.stats.messages += 1;
        state.check_time(block.event_time.get());
        let Some(Ok((dimension, mut entries))) = body
            .get(block_length..)
            .map(GroupSizeEncoding::read_from_prefix)
        else {
            self.bad_records += 1;
            return;
        };
        // The wire stride, not the struct size: a newer schema may append
        // fields to the entry.
        let stride = usize::from(dimension.block_length.get());
        if stride < size_of::<i64>() {
            self.bad_records += 1;
            return;
        }
        for _ in 0..dimension.num_in_group.get() {
            let Some(entry) = entries.get(..stride) else {
                break;
            };
            let id = i64::from_le_bytes(entry[..8].try_into().expect("stride >= 8"));
            state.chain(id, recv);
            state.observe_point_id(id);
            entries = &entries[stride..];
        }
    }

    /// `bookUpdateId` must never go backwards; forward jumps are normal, as
    /// the quote changes less often than the book.
    fn best_bid_ask(&mut self, body: &[u8]) {
        let Ok((block, _)) = BestBidAskBlock::read_from_prefix(body) else {
            self.bad_records += 1;
            return;
        };
        let state = self.stream(Stream::BestBidAsk);
        state.stats.messages += 1;
        state.check_time(block.event_time.get());
        state.non_decreasing(block.book_update_id.get());
    }

    fn okx_payload(&mut self, payload: &[u8]) {
        let Ok((header, body)) = MessageHeader::read_from_prefix(payload) else {
            self.bad_records += 1;
            return;
        };
        match header.template_id.get() {
            okx::TEMPLATE_BBO_TBT => self.okx_bbo(body),
            _ => self.bad_records += 1,
        }
    }

    fn gate_payload(&mut self, market: Market, recv: i64, payload: &[u8]) {
        let Ok((header, body)) = MessageHeader::read_from_prefix(payload) else {
            self.bad_records += 1;
            return;
        };
        let groups = size_of::<MessageHeader>() + usize::from(header.block_length.get());
        let decoded = match (market, header.template_id.get()) {
            (Market::Spot, gate::TEMPLATE_BBO) => SpotBboBlock::read_from_prefix(body)
                .map(|(block, _)| self.gate_bbo(block.u.get(), block.t.get()))
                .is_ok(),
            (Market::Futures, gate::TEMPLATE_BBO) => FuturesBboBlock::read_from_prefix(body)
                .map(|(block, _)| self.gate_bbo(block.u.get(), block.t.get()))
                .is_ok(),
            (Market::Spot, gate::TEMPLATE_PUBLIC_TRADE) => SpotTradeBlock::read_from_prefix(body)
                .map(|(block, _)| {
                    let state = self.stream(Stream::Trades);
                    state.stats.messages += 1;
                    state.check_time(block.create_time_us.get());
                    let id = block.id_market.get() as i64;
                    state.chain(id, recv);
                    state.observe_point_id(id);
                })
                .is_ok(),
            (Market::Futures, gate::TEMPLATE_PUBLIC_TRADE) => {
                FuturesTradeBlock::read_from_prefix(body).is_ok()
                    && self.gate_futures_trades(recv, payload, groups)
            }
            (_, gate::TEMPLATE_OBU) => ObuBlock::read_from_prefix(body)
                .map(|(block, _)| self.gate_obu(recv, &block))
                .is_ok(),
            _ => false,
        };
        if !decoded {
            self.bad_records += 1;
        }
    }

    /// `u` must not go backwards: Gate samples `book_ticker` per connection.
    fn gate_bbo(&mut self, u: i64, t: i64) {
        let state = self.stream(Stream::BestBidAsk);
        state.stats.messages += 1;
        state.check_time(t);
        state.non_decreasing(u);
    }

    /// Futures trades come in a group, each with an id one past the last.
    fn gate_futures_trades(&mut self, recv: i64, payload: &[u8], mut offset: usize) -> bool {
        let Some(group) = read_group::<GroupSize16Encoding>(payload, &mut offset) else {
            return false;
        };
        let state = self.stream(Stream::Trades);
        state.stats.messages += 1;
        for _ in 0..group.count {
            let Some(Ok((trade, _))) = payload.get(offset..).map(FuturesTrade::read_from_prefix)
            else {
                return false;
            };
            state.check_time(trade.t.get());
            let id = trade.id.get() as i64;
            state.chain(id, recv);
            state.observe_point_id(id);
            offset += group.stride;
        }
        true
    }

    /// A delta starts at the previous `u` plus one; a full snapshot carries
    /// no `U` to check and re-anchors. Redundant connections interleave, so
    /// the chain tracks its running maximum and only forward breaks count.
    fn gate_obu(&mut self, recv: i64, block: &ObuBlock) {
        let state = self.stream(Stream::DepthDiff);
        state.stats.messages += 1;
        state.check_time(block.t.get());
        let (first, last) = (block.first_id.get(), block.last_id.get());
        if block.full == 0
            && let Some(prev) = state.last_id
        {
            if last <= prev {
                state.stats.seq_back_events += 1;
            } else if first > prev + 1 {
                state.stats.seq_fwd_events += 1;
                state.stats.seq_fwd_ids += (first - prev - 1) as u64;
                state.note_break(recv);
            }
        }
        state.advance(last);
    }

    /// `seqId` must not go backwards. Each connection samples `bbo-tbt` its
    /// own way, so a redundant recording's merge reorders, and counts so.
    fn okx_bbo(&mut self, body: &[u8]) {
        let Ok((block, _)) = BboTbtBlock::read_from_prefix(body) else {
            self.bad_records += 1;
            return;
        };
        let state = self.stream(Stream::BestBidAsk);
        state.stats.messages += 1;
        state.check_time(block.ts_us.get());
        state.non_decreasing(block.seq_id.get());
    }
}

struct FileReport {
    path: PathBuf,
    rows: u64,
    foreign: bool,
    decode_error: Option<String>,
    live: bool,
}

struct StreamReport {
    stream: Stream,
    stats: StreamStats,
    break_times: Vec<i64>,
}

struct SeriesReport {
    key: String,
    files: Vec<FileReport>,
    missing_dates: Vec<civil::Date>,
    rows: u64,
    sbe_records: u64,
    rest_records: u64,
    bad_records: u64,
    recv_regressions: u64,
    first_recv: Option<i64>,
    last_recv: Option<i64>,
    gaps: Vec<Gap>,
    /// In [`Stream`] order.
    streams: Vec<StreamReport>,
}

impl SeriesReport {
    /// Lost or unrecordable data, as opposed to the reorder and duplicate
    /// artefacts of redundant collection.
    fn has_issues(&self) -> bool {
        !self.gaps.is_empty()
            || !self.missing_dates.is_empty()
            || self.bad_records > 0
            || self.files.iter().any(|f| f.decode_error.is_some())
            || self.streams.iter().any(|s| {
                s.stats.seq_fwd_events > 0 || s.stats.net_deficit() > 0 || s.stats.exact_missing > 0
            })
    }
}

/// Decode `file`'s records into `process`. A first record that does not even
/// hold a plausible header marks the whole file as foreign.
fn read_file<F>(file: &DatedFile, mut process: F) -> FileReport
where
    F: FnMut(i64, Tag, &[u8]),
{
    let mut report = FileReport {
        path: file.path.clone(),
        rows: 0,
        foreign: false,
        decode_error: None,
        live: false,
    };
    let handle = match File::open(&file.path) {
        Ok(handle) => handle,
        Err(error) => {
            report.decode_error = Some(format!("open failed: {error}"));
            return report;
        }
    };
    let modified = fs::metadata(&file.path).and_then(|m| m.modified()).ok();
    let Ok(decoder) = zstd::stream::read::Decoder::new(handle) else {
        report.decode_error = Some("not a zstd stream".into());
        return report;
    };
    let mut reader = std::io::BufReader::with_capacity(1 << 20, decoder);
    let mut header = [0u8; HEADER_LEN];
    let mut payload: Vec<u8> = Vec::with_capacity(1 << 16);
    loop {
        // A short read at the end means the file stops mid-record.
        let mut got = 0usize;
        let mut read_error = None;
        while got < HEADER_LEN {
            match reader.read(&mut header[got..]) {
                Ok(0) => break,
                Ok(n) => got += n,
                Err(error) => {
                    read_error = Some(error.to_string());
                    break;
                }
            }
        }
        if let Some(error) = read_error {
            report.decode_error = Some(error);
            break;
        }
        if got == 0 {
            break;
        }
        if got < HEADER_LEN {
            report.decode_error = Some(format!("truncated record header ({got} trailing bytes)"));
            break;
        }
        let Header { recv_ns, tag, len } = Header::parse(&header);
        let Some(tag) = Tag::from_byte(tag).filter(|_| len <= MAX_PAYLOAD_LEN) else {
            if report.rows == 0 {
                report.foreign = true;
            } else {
                report.decode_error =
                    Some(format!("implausible record (tag {tag:#04x}, len {len})"));
            }
            break;
        };
        payload.resize(len as usize, 0);
        if let Err(error) = reader.read_exact(&mut payload) {
            report.decode_error = Some(format!("truncated payload: {error}"));
            break;
        }
        process(recv_ns, tag, &payload);
        report.rows += 1;
    }
    if report.decode_error.is_some() {
        report.live = modified.is_some_and(|m| {
            SystemTime::now()
                .duration_since(m)
                .is_ok_and(|age| age < LIVE_FILE_WINDOW)
        });
    }
    report
}

struct RawRecord {
    recv: i64,
    tag: Tag,
    payload: Vec<u8>,
    /// Index of the run that wrote it.
    source: usize,
}

/// The receive times one run covered.
#[derive(Clone, Copy)]
struct RecvSpan {
    first: i64,
    last: i64,
}

impl RecvSpan {
    fn include(&mut self, recv: i64) {
        self.first = self.first.min(recv);
        self.last = self.last.max(recv);
    }

    /// Whether `recv` falls where both spans overlap.
    fn both_cover(self, other: Self, recv: i64) -> bool {
        (self.first.max(other.first)..=self.last.min(other.last)).contains(&recv)
    }
}

/// The live collector's event identity: a venue stamps some bytes of a
/// stream frame per connection, so the same event from two rolling processes
/// differs only there and must still collapse to one record.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct OverlapKey {
    tag: Tag,
    hash: u128,
}

impl OverlapKey {
    fn of(exchange: Exchange, tag: Tag, payload: &[u8]) -> Self {
        let stamp = match tag {
            Tag::Sbe => exchange.frame_stamp(payload),
            Tag::Rest => None,
        };
        Self {
            tag,
            hash: event_key(payload, stamp),
        }
    }
}

/// A record not yet paired with its copy from another run.
struct Unpaired {
    source: usize,
    recv: i64,
}

/// Time-merge the records of overlapping runs, dropping each record's copy
/// from another run at most once, and only inside both runs' observed span.
fn process_overlapping(scan: &mut SeriesScan, mut records: Vec<RawRecord>, min_gap_ns: i64) {
    let mut spans: HashMap<usize, RecvSpan> = HashMap::new();
    for record in &records {
        spans
            .entry(record.source)
            .and_modify(|span| span.include(record.recv))
            .or_insert(RecvSpan {
                first: record.recv,
                last: record.recv,
            });
    }
    records.sort_by_key(|record| (record.recv, record.source));
    let mut unpaired: HashMap<OverlapKey, VecDeque<Unpaired>> = HashMap::new();
    for record in records {
        let queue = unpaired
            .entry(OverlapKey::of(scan.exchange, record.tag, &record.payload))
            .or_default();
        while queue.front().is_some_and(|earlier| {
            earlier.recv < record.recv.saturating_sub(OVERLAP_DEDUP_WINDOW_NS)
        }) {
            queue.pop_front();
        }

        let copy = queue.iter().position(|earlier| {
            earlier.source != record.source
                && spans
                    .get(&earlier.source)
                    .zip(spans.get(&record.source))
                    .is_some_and(|(&earlier_span, &span)| {
                        earlier_span.both_cover(span, record.recv)
                            && record.recv.saturating_sub(earlier.recv) <= OVERLAP_DEDUP_WINDOW_NS
                    })
        });
        if let Some(index) = copy {
            queue.remove(index);
            continue;
        }

        scan.process_record(record.recv, record.tag, &record.payload, min_gap_ns);
        queue.push_back(Unpaired {
            source: record.source,
            recv: record.recv,
        });
    }
}

fn scan_series(series: &Series, min_gap_ns: i64, exact: bool) -> SeriesReport {
    let mut scan = SeriesScan {
        exchange: series.exchange,
        exact,
        ..SeriesScan::default()
    };
    let mut files = Vec::with_capacity(series.files.len());
    // Files of one (date, segment) are written concurrently by different
    // runs, so they are scanned together.
    for group in series
        .files
        .chunk_by(|a, b| (a.date, a.segment_start_ns) == (b.date, b.segment_start_ns))
    {
        let mut runs: HashMap<&str, usize> = HashMap::new();
        for file in group {
            let run = file.run_id.as_deref().map(logical_run_id).unwrap_or("");
            let next = runs.len();
            runs.entry(run).or_insert(next);
        }
        if runs.len() > 1 {
            let mut records = Vec::new();
            let mut decode_error = false;
            for file in group {
                let run = file.run_id.as_deref().map(logical_run_id).unwrap_or("");
                let source = runs[run];
                let report = read_file(file, |recv, tag, payload| {
                    records.push(RawRecord {
                        recv,
                        tag,
                        payload: payload.to_vec(),
                        source,
                    });
                });
                decode_error |= report.decode_error.is_some();
                files.push(report);
            }
            process_overlapping(&mut scan, records, min_gap_ns);
            if decode_error {
                // An unreadable tail hides the next receive time: keep the
                // sequence state, but measure no receive gap across it.
                scan.prev_recv = None;
            }
        } else {
            // A single run streams in the order it was recorded.
            for file in group {
                let report = read_file(file, |recv, tag, payload| {
                    scan.process_record(recv, tag, payload, min_gap_ns);
                });
                if report.decode_error.is_some() {
                    scan.prev_recv = None;
                }
                files.push(report);
            }
        }
    }

    let mut missing_dates = Vec::new();
    if let (Some(first), Some(last)) = (series.files.first(), series.files.last()) {
        let present: BTreeSet<civil::Date> = series.files.iter().map(|f| f.date).collect();
        let mut date = first.date;
        while date < last.date {
            date = date
                .checked_add(Span::new().days(1))
                .expect("date range is tiny");
            if !present.contains(&date) {
                missing_dates.push(date);
            }
        }
    }

    SeriesReport {
        key: series.key.clone(),
        files,
        missing_dates,
        rows: scan.rows,
        sbe_records: scan.sbe_records,
        rest_records: scan.rest_records,
        bad_records: scan.bad_records,
        recv_regressions: scan.recv_regressions,
        first_recv: scan.first_recv,
        last_recv: scan.last_recv,
        gaps: scan.gaps,
        streams: Stream::ALL
            .into_iter()
            .zip(scan.streams)
            .filter_map(|(stream, state)| Some(state?.into_report(stream)))
            .collect(),
    }
}

fn fmt_ts(ns: i64) -> String {
    let zoned = Timestamp::from_nanosecond(i128::from(ns))
        .expect("timestamp in range")
        .to_zoned(TimeZone::UTC);
    let ms = ns.rem_euclid(1_000_000_000) / 1_000_000;
    format!("{}.{:03}Z", zoned.strftime("%Y-%m-%d %H:%M:%S"), ms)
}

fn fmt_dur(ns: i64) -> String {
    let ms = ns / 1_000_000;
    if ms < 60_000 {
        return format!("{:.3}s", ns as f64 / 1e9);
    }
    let total_s = ms / 1000;
    let (h, m, s) = (total_s / 3600, total_s / 60 % 60, total_s % 60);
    if h > 0 {
        format!("{h}h{m:02}m{s:02}s")
    } else {
        format!("{m}m{s:02}s")
    }
}

/// `n` with thousands separators.
fn grouped(mut n: u64) -> String {
    let mut groups = Vec::new();
    loop {
        groups.push(n % 1000);
        n /= 1000;
        if n == 0 {
            break;
        }
    }
    let mut out = groups.last().copied().unwrap_or(0).to_string();
    for group in groups.iter().rev().skip(1) {
        out += &format!(",{group:03}");
    }
    out
}

fn stream_issues(s: &StreamStats, break_times: &[i64], exact: bool) -> Vec<String> {
    let mut parts = Vec::new();
    if s.id_count > 0 {
        let range = format!(
            "ids {}..{}",
            grouped(s.id_min as u64),
            grouped(s.id_max as u64)
        );
        let observed = grouped(s.id_count);
        let deficit = s.net_deficit();
        parts.push(if exact {
            format!(
                "{range}: exact {} missing, {} duplicates ({observed} observed)",
                grouped(s.exact_missing),
                grouped(s.exact_dups),
            )
        } else if deficit > 0 {
            format!(
                "{range}: net {} missing ({observed} observed)",
                grouped(deficit as u64)
            )
        } else if deficit < 0 {
            format!(
                "{range}: net {} extra from reorders/dups ({observed} observed)",
                grouped(deficit.unsigned_abs())
            )
        } else {
            format!("{range}: complete ({observed} observed)")
        });
    }
    if s.seq_fwd_events > 0 {
        parts.push(format!(
            "{} forward chain break(s), {} ids",
            grouped(s.seq_fwd_events),
            grouped(s.seq_fwd_ids)
        ));
        if !break_times.is_empty() {
            let times: Vec<String> = break_times.iter().map(|&ns| fmt_ts(ns)).collect();
            parts.push(format!("breaks at: {}", times.join(", ")));
        }
    }
    if s.seq_back_events > 0 {
        parts.push(format!(
            "{} duplicate/reordered event(s)",
            grouped(s.seq_back_events)
        ));
    }
    if s.time_regressions > 0 {
        parts.push(format!(
            "{} time regression(s)",
            grouped(s.time_regressions)
        ));
    }
    parts
}

fn print_report(report: &SeriesReport, min_gap_ns: i64, max_reported: usize, exact: bool) {
    println!("{}", report.key);
    let files: Vec<String> = report
        .files
        .iter()
        .map(|file| file.path.display().to_string())
        .collect();
    println!("  files: {}", files.join(", "));
    for file in &report.files {
        if file.foreign {
            println!(
                "    {}: foreign format (not sbe-collector records), skipped",
                file.path.display()
            );
        }
        if let Some(error) = &file.decode_error {
            if file.live {
                println!(
                    "    {}: unterminated zstd stream after {} records (file modified recently — still being written?): {error}",
                    file.path.display(),
                    grouped(file.rows)
                );
            } else {
                println!(
                    "    {}: decode error after {} records: {error}",
                    file.path.display(),
                    grouped(file.rows)
                );
            }
        }
    }
    if report.rows == 0 && report.files.iter().all(|f| !f.foreign) {
        println!("  (no records decoded)");
    }
    if report.rows > 0 {
        let span = match (report.first_recv, report.last_recv) {
            (Some(a), Some(b)) => format!("{} .. {}", fmt_ts(a), fmt_ts(b)),
            _ => String::from("?"),
        };
        println!(
            "  records: {} ({} stream, {} rest snapshots) | recv span: {} | bad records: {}",
            grouped(report.rows),
            grouped(report.sbe_records),
            grouped(report.rest_records),
            span,
            grouped(report.bad_records)
        );
    }
    if !report.missing_dates.is_empty() {
        let dates: Vec<String> = report
            .missing_dates
            .iter()
            .map(ToString::to_string)
            .collect();
        println!("  MISSING DATES: {}", dates.join(", "));
    }
    if report.recv_regressions > 0 {
        println!(
            "  receive time regressions: {} (arrival-vs-dequeue ordering)",
            grouped(report.recv_regressions)
        );
    }

    let threshold = fmt_dur(min_gap_ns);
    if report.gaps.is_empty() {
        println!("  recv gaps > {threshold}: none");
    } else {
        let total: i64 = report.gaps.iter().map(|gap| gap.silence_ns()).sum();
        println!(
            "  RECV GAPS > {threshold}: {} (total silence {})",
            report.gaps.len(),
            fmt_dur(total)
        );
        let mut largest: Vec<&Gap> = report.gaps.iter().collect();
        largest.sort_by_key(|gap| std::cmp::Reverse(gap.silence_ns()));
        for gap in largest.iter().take(max_reported) {
            println!(
                "    {} .. {} ({})",
                fmt_ts(gap.start),
                fmt_ts(gap.end),
                fmt_dur(gap.silence_ns())
            );
        }
        if report.gaps.len() > max_reported {
            println!("    ... {} more", report.gaps.len() - max_reported);
        }
    }

    let mut notable = report
        .streams
        .iter()
        .filter(|s| s.stats.is_notable())
        .peekable();
    if notable.peek().is_some() {
        println!("  streams:");
        for stream in notable {
            println!(
                "    {}: {} msgs",
                stream.stream.name(),
                grouped(stream.stats.messages)
            );
            for part in stream_issues(&stream.stats, &stream.break_times, exact) {
                println!("      {part}");
            }
        }
    }
    println!();
}

/// What the SUMMARY adds up over every series.
#[derive(Default)]
struct Totals {
    series: usize,
    rows: u64,
    recv_gaps: usize,
    gap_series: usize,
    silence_ns: i64,
    fwd_events: u64,
    fwd_ids: u64,
    net_missing_ids: i64,
    exact_missing_ids: u64,
    back_events: u64,
    time_regressions: u64,
    missing_dates: usize,
    decode_errors: usize,
    bad_records: u64,
    foreign_files: usize,
    with_issues: usize,
}

impl AddAssign<&SeriesReport> for Totals {
    fn add_assign(&mut self, report: &SeriesReport) {
        self.series += 1;
        self.rows += report.rows;
        self.recv_gaps += report.gaps.len();
        self.gap_series += usize::from(!report.gaps.is_empty());
        self.silence_ns += report.gaps.iter().map(|gap| gap.silence_ns()).sum::<i64>();
        for StreamReport { stats, .. } in &report.streams {
            self.fwd_events += stats.seq_fwd_events;
            self.fwd_ids += stats.seq_fwd_ids;
            self.net_missing_ids += stats.net_deficit().max(0);
            self.exact_missing_ids += stats.exact_missing;
            self.back_events += stats.seq_back_events;
            self.time_regressions += stats.time_regressions;
        }
        self.missing_dates += report.missing_dates.len();
        for file in &report.files {
            self.decode_errors += usize::from(file.decode_error.is_some());
            self.foreign_files += usize::from(file.foreign);
        }
        self.bad_records += report.bad_records;
        self.with_issues += usize::from(report.has_issues());
    }
}

fn print_summary(totals: &Totals, args: &Args) {
    println!("SUMMARY");
    println!("  series scanned        : {}", totals.series);
    println!("  records decoded       : {}", grouped(totals.rows));
    println!(
        "  recv gaps > {}s  : {} across {} series (total silence {})",
        args.min_gap,
        totals.recv_gaps,
        totals.gap_series,
        fmt_dur(totals.silence_ns)
    );
    println!(
        "  forward chain breaks  : {} event(s), {} ids",
        grouped(totals.fwd_events),
        grouped(totals.fwd_ids)
    );
    if args.exact {
        println!(
            "  exact missing ids     : {}",
            grouped(totals.exact_missing_ids)
        );
    } else {
        println!(
            "  net missing ids       : {} (range accounting)",
            grouped(totals.net_missing_ids as u64)
        );
    }
    println!("  dup/reordered events  : {}", grouped(totals.back_events));
    println!(
        "  time regressions      : {}",
        grouped(totals.time_regressions)
    );
    println!("  missing dates         : {}", totals.missing_dates);
    println!("  decode errors         : {}", totals.decode_errors);
    println!("  bad records           : {}", grouped(totals.bad_records));
    println!(
        "  foreign files         : {} (skipped)",
        totals.foreign_files
    );
    println!("RESULT: issues found in {} series", totals.with_issues);
}

fn main() -> Result<()> {
    let args = Args::parse();
    let min_gap_ns = (args.min_gap.max(0.0) * 1e9) as i64;

    let mut series = discover(&args.paths, args.exchange)?;
    if let Some(filter) = &args.filter {
        series.retain(|s| s.key.contains(filter));
    }
    if series.is_empty() {
        println!(
            "no <symbol>_<YYYYMMDD>.zst files found under: {}",
            args.paths
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        return Ok(());
    }

    let cpus = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4);
    // Capped so a shared machine is not flooded; --jobs overrides.
    let jobs = if args.jobs == 0 {
        cpus.min(16)
    } else {
        args.jobs
    };
    eprintln!(
        "scanning {} series across {} files with {jobs} worker(s), min gap {}s{}",
        series.len(),
        series.iter().map(|s| s.files.len()).sum::<usize>(),
        args.min_gap,
        if args.exact { ", exact id sets" } else { "" }
    );

    let next = AtomicUsize::new(0);
    let done = AtomicUsize::new(0);
    let results: Mutex<Vec<SeriesReport>> = Mutex::new(Vec::with_capacity(series.len()));
    let total = series.len();
    std::thread::scope(|scope| {
        for _ in 0..jobs.min(total) {
            scope.spawn(|| {
                loop {
                    let idx = next.fetch_add(1, Ordering::Relaxed);
                    if idx >= total {
                        break;
                    }
                    let started = std::time::Instant::now();
                    let report = scan_series(&series[idx], min_gap_ns, args.exact);
                    let finished = done.fetch_add(1, Ordering::Relaxed) + 1;
                    eprintln!(
                        "[{finished}/{total}] {} ({} records, {:.1}s)",
                        report.key,
                        grouped(report.rows),
                        started.elapsed().as_secs_f64()
                    );
                    results.lock().expect("results mutex").push(report);
                }
            });
        }
    });

    let mut reports = results.into_inner().expect("results mutex");
    reports.sort_by(|a, b| a.key.cmp(&b.key));

    println!();
    let mut totals = Totals::default();
    for report in &reports {
        print_report(report, min_gap_ns, args.max_reported, args.exact);
        totals += report;
    }
    print_summary(&totals, &args);
    if args.fail_on_gaps && totals.with_issues > 0 {
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sbe_collector::sbe::binance::EVENT_TIME;
    use std::io::Write;

    fn test_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "sbe-gap-detector-{tag}-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn record(recv: i64, tag: Tag, payload: &[u8]) -> Vec<u8> {
        let header = Header {
            recv_ns: recv,
            tag: tag as u8,
            len: payload.len() as u32,
        };
        [&header.encode()[..], payload].concat()
    }

    fn write_zst(path: &Path, records: &[Vec<u8>]) {
        let mut encoder =
            zstd::stream::write::Encoder::new(File::create(path).unwrap(), 1).unwrap();
        for record in records {
            encoder.write_all(record).unwrap();
        }
        encoder.finish().unwrap();
    }

    fn daily(path: PathBuf, date: civil::Date) -> Series {
        Series {
            exchange: Exchange::BinanceSbeSpot,
            key: "test/btcusdt".into(),
            files: vec![DatedFile {
                symbol: "btcusdt".into(),
                path,
                date,
                segment_start_ns: i64::MIN,
                run_id: None,
            }],
        }
    }

    fn sbe_header(block_length: u16, template_id: u16) -> Vec<u8> {
        let mut out = Vec::with_capacity(8);
        out.extend_from_slice(&block_length.to_le_bytes());
        out.extend_from_slice(&template_id.to_le_bytes());
        out.extend_from_slice(&1u16.to_le_bytes()); // schema id
        out.extend_from_slice(&1u16.to_le_bytes()); // version
        out
    }

    /// A `DepthDiffStreamEvent` payload: header plus fixed block, which is
    /// all the chain checks read.
    fn depth_diff(event_time: i64, first: i64, last: i64) -> Vec<u8> {
        let mut payload = sbe_header(26, TEMPLATE_DEPTH_DIFF);
        payload.extend_from_slice(&event_time.to_le_bytes());
        payload.extend_from_slice(&first.to_le_bytes());
        payload.extend_from_slice(&last.to_le_bytes());
        payload.push(0); // price exponent
        payload.push(0); // qty exponent
        payload
    }

    /// A `TradesStreamEvent` payload with the given trade ids.
    fn trades(event_time: i64, ids: &[i64]) -> Vec<u8> {
        let mut payload = sbe_header(18, TEMPLATE_TRADES);
        payload.extend_from_slice(&event_time.to_le_bytes()); // eventTime
        payload.extend_from_slice(&event_time.to_le_bytes()); // transactTime
        payload.push(0); // price exponent
        payload.push(0); // qty exponent
        // groupSizeEncoding: entry blockLength 25, numInGroup uint32
        payload.extend_from_slice(&25u16.to_le_bytes());
        payload.extend_from_slice(&(ids.len() as u32).to_le_bytes());
        for id in ids {
            payload.extend_from_slice(&id.to_le_bytes()); // id
            payload.extend_from_slice(&1i64.to_le_bytes()); // price
            payload.extend_from_slice(&1i64.to_le_bytes()); // qty
            payload.push(0); // isBuyerMaker
        }
        payload
    }

    fn scan_record(scan: &mut SeriesScan, recv: i64, tag: Tag, payload: &[u8]) {
        scan.process_record(recv, tag, payload, 5_000_000_000);
    }

    fn stats(scan: &SeriesScan, stream: Stream) -> &StreamStats {
        &scan.streams[stream as usize]
            .as_ref()
            .expect("stream present")
            .stats
    }

    #[test]
    fn recv_gap_and_regression() {
        let mut scan = SeriesScan::default();
        let frame = depth_diff(1, 1, 2);
        scan_record(&mut scan, 1_000_000_000, Tag::Sbe, &frame);
        scan_record(&mut scan, 8_000_000_000, Tag::Sbe, &frame);
        scan_record(&mut scan, 7_000_000_000, Tag::Sbe, &frame);
        assert_eq!(scan.gaps.len(), 1);
        assert_eq!(scan.gaps[0].start, 1_000_000_000);
        assert_eq!(scan.gaps[0].end, 8_000_000_000);
        assert_eq!(scan.recv_regressions, 1);
    }

    #[test]
    fn depth_diff_chain_hole_overlap_and_reorder() {
        let mut scan = SeriesScan::default();
        scan_record(&mut scan, 1, Tag::Sbe, &depth_diff(1, 10, 20));
        scan_record(&mut scan, 2, Tag::Sbe, &depth_diff(2, 21, 30)); // contiguous
        scan_record(&mut scan, 3, Tag::Sbe, &depth_diff(3, 41, 50)); // 31..40 lost
        scan_record(&mut scan, 4, Tag::Sbe, &depth_diff(4, 35, 50)); // covered straggler
        scan_record(&mut scan, 5, Tag::Sbe, &depth_diff(5, 51, 60)); // contiguous vs max
        let s = stats(&scan, Stream::DepthDiff);
        assert_eq!(s.seq_fwd_events, 1);
        assert_eq!(s.seq_fwd_ids, 10);
        assert_eq!(s.seq_back_events, 1);
        assert_eq!(s.time_regressions, 0);
    }

    #[test]
    fn trade_ids_chain_within_and_across_frames() {
        let mut scan = SeriesScan::default();
        scan_record(&mut scan, 1, Tag::Sbe, &trades(1, &[10, 11, 12]));
        scan_record(&mut scan, 2, Tag::Sbe, &trades(2, &[13, 15])); // 14 not seen yet
        scan_record(&mut scan, 3, Tag::Sbe, &trades(3, &[14])); // arrives late
        scan_record(&mut scan, 4, Tag::Sbe, &trades(4, &[16])); // contiguous vs max
        let s = stats(&scan, Stream::Trades);
        assert_eq!(s.seq_fwd_events, 1);
        assert_eq!(s.seq_fwd_ids, 1);
        assert_eq!(s.seq_back_events, 1);
        // Range accounting nets to zero: nothing actually missing.
        assert_eq!(s.net_deficit(), 0);
        assert_eq!(s.id_count, 7);
    }

    #[test]
    fn bba_update_id_regression_only() {
        let mut scan = SeriesScan::default();
        let bba = |id: i64| {
            let mut payload = sbe_header(50, TEMPLATE_BEST_BID_ASK);
            payload.extend_from_slice(&1i64.to_le_bytes()); // eventTime
            payload.extend_from_slice(&id.to_le_bytes()); // bookUpdateId
            payload.extend_from_slice(&[0u8; 34]); // exponents + bid/ask
            payload
        };
        scan_record(&mut scan, 1, Tag::Sbe, &bba(100));
        scan_record(&mut scan, 2, Tag::Sbe, &bba(500)); // forward jump: normal
        scan_record(&mut scan, 3, Tag::Sbe, &bba(400)); // backwards: dup/reorder
        let s = stats(&scan, Stream::BestBidAsk);
        assert_eq!(s.seq_fwd_events, 0);
        assert_eq!(s.seq_back_events, 1);
    }

    #[test]
    fn event_time_regressions_are_counted_per_stream() {
        let mut scan = SeriesScan::default();
        scan_record(&mut scan, 1, Tag::Sbe, &depth_diff(100, 1, 2));
        scan_record(&mut scan, 2, Tag::Sbe, &depth_diff(90, 3, 4));
        assert_eq!(stats(&scan, Stream::DepthDiff).time_regressions, 1);
    }

    #[test]
    fn net_deficit_detects_true_loss_and_duplicates() {
        let mut stats = StreamStats {
            id_min: 10,
            id_max: 15,
            id_count: 5,
            ..Default::default()
        };
        assert_eq!(stats.net_deficit(), 1); // span 6, saw 5
        stats.id_count = 8;
        assert_eq!(stats.net_deficit(), -2); // duplicates
    }

    #[test]
    fn a_foreign_first_record_marks_the_file_foreign() {
        let dir = test_dir("foreign");
        let path = dir.join("btcusdt_20260802.zst");
        write_zst(
            &path,
            &[b"1785542400000000000 {\"e\":\"trade\"}\n".to_vec()],
        );

        let report = read_file(&daily(path, civil::date(2026, 8, 2)).files[0], |_, _, _| {
            panic!("a foreign file yields no records")
        });
        assert!(report.foreign);
        assert!(report.decode_error.is_none());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn segmented_filename_preserves_symbols_with_underscores() {
        let file = dated_file(Path::new(
            "btc_usdt_20260820_1787200000000000000_1787200000000000001-42.zst",
        ))
        .unwrap();
        assert_eq!(file.symbol, "btc_usdt");
        assert_eq!(file.date, civil::Date::new(2026, 8, 20).unwrap());
        assert_eq!(file.segment_start_ns, 1_787_200_000_000_000_000);
        assert_eq!(file.run_id.as_deref(), Some("1787200000000000001-42"));
    }

    #[test]
    fn sidecar_filename_is_grouped_with_the_daily_file() {
        let file = dated_file(Path::new("btc_usdt_20260821_1787200000000000001-42.zst")).unwrap();
        assert_eq!(file.symbol, "btc_usdt");
        assert_eq!(file.date, civil::Date::new(2026, 8, 21).unwrap());
        assert_eq!(file.segment_start_ns, i64::MIN);
        assert_eq!(file.run_id.as_deref(), Some("1787200000000000001-42"));
    }

    #[test]
    fn daily_filename_has_no_run_id() {
        let file = dated_file(Path::new("btcusdt_20260821.zst")).unwrap();
        assert_eq!(file.symbol, "btcusdt");
        assert_eq!(file.date, civil::Date::new(2026, 8, 21).unwrap());
        assert!(file.run_id.is_none());
    }

    #[test]
    fn late_segment_uses_the_original_run_identity() {
        assert_eq!(
            logical_run_id("1787200000000000001-42.late3"),
            "1787200000000000001-42"
        );
        assert_eq!(logical_run_id("run.later"), "run.later");
    }

    /// A single run is scanned in the order it was written, not re-sorted.
    #[test]
    fn non_overlapping_records_preserve_recorded_order() {
        let dir = test_dir("recorded-order");
        let path = dir.join("btcusdt_20260802.zst");
        let payload = depth_diff(1, 1, 2);
        write_zst(
            &path,
            &[
                record(200, Tag::Sbe, &payload),
                record(100, Tag::Sbe, &payload),
            ],
        );

        let report = scan_series(&daily(path, civil::date(2026, 8, 2)), 1_000, false);
        assert_eq!(report.recv_regressions, 1);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn overlapping_runs_are_time_merged_and_cross_run_duplicates_are_removed() {
        let ten_old = trades(10, &[10]);
        let mut ten_new = ten_old.clone();
        ten_new[EVENT_TIME].copy_from_slice(&11_i64.to_le_bytes());
        let twelve_old = trades(12, &[12]);
        let mut twelve_new = twelve_old.clone();
        twelve_new[EVENT_TIME].copy_from_slice(&13_i64.to_le_bytes());
        let raw = |recv, tag, payload, source| RawRecord {
            recv,
            tag,
            payload,
            source,
        };
        let records = vec![
            raw(0, Tag::Rest, b"old-start".to_vec(), 0),
            raw(4, Tag::Sbe, twelve_old, 0),
            raw(1, Tag::Sbe, ten_old, 0),
            raw(2, Tag::Sbe, ten_new, 1),
            raw(3, Tag::Sbe, twelve_new, 1),
            raw(5, Tag::Rest, b"new-end".to_vec(), 1),
        ];
        let mut scan = SeriesScan::default();
        process_overlapping(&mut scan, records, 5_000_000_000);
        let stats = stats(&scan, Stream::Trades);
        assert_eq!(scan.rows, 4);
        assert_eq!(scan.recv_regressions, 0);
        assert!(scan.gaps.is_empty());
        assert_eq!(stats.id_count, 2);
        assert_eq!(stats.net_deficit(), 1);
    }

    #[test]
    fn overlap_dedup_is_one_to_one_and_stops_at_the_observed_overlap() {
        let raw = |recv, payload: &'static [u8], source| RawRecord {
            recv,
            tag: Tag::Rest,
            payload: payload.to_vec(),
            source,
        };
        let records = vec![
            raw(0, b"old-start", 0),
            raw(1, b"new-start", 1),
            raw(2, b"unchanged", 0),
            raw(3, b"unchanged", 1),
            raw(10, b"old-end", 0),
            // The same timer-pushed payload after the incumbent stopped is a
            // real event and must not be assigned permanently to the old run.
            raw(11, b"unchanged", 1),
            raw(20, b"new-end", 1),
        ];
        let mut scan = SeriesScan::default();
        process_overlapping(&mut scan, records, 100);
        assert_eq!(scan.rows, 6);
    }

    #[test]
    fn grouped_formats_thousands() {
        assert_eq!(grouped(0), "0");
        assert_eq!(grouped(999), "999");
        assert_eq!(grouped(1_000), "1,000");
        assert_eq!(grouped(1_000_000), "1,000,000");
        assert_eq!(grouped(12_345_678), "12,345,678");
    }

    /// End to end over a real zstd file: records in, report out.
    #[test]
    fn scans_a_written_file_end_to_end() {
        let dir = test_dir("end-to-end");
        let path = dir.join("btcusdt_20260802.zst");
        write_zst(
            &path,
            &[
                record(1_000_000_000, Tag::Sbe, &depth_diff(1, 1, 10)),
                record(2_000_000_000, Tag::Sbe, &depth_diff(2, 11, 20)),
                record(9_000_000_000, Tag::Sbe, &depth_diff(3, 21, 30)), // 7s silence
                record(9_500_000_000, Tag::Rest, b"{}"),
            ],
        );

        let report = scan_series(
            &daily(path.clone(), civil::date(2026, 8, 2)),
            5_000_000_000,
            false,
        );
        assert_eq!(report.rows, 4);
        assert_eq!(report.sbe_records, 3);
        assert_eq!(report.rest_records, 1);
        assert_eq!(report.gaps.len(), 1);
        assert_eq!(report.gaps[0].silence_ns(), 7_000_000_000);
        assert_eq!(report.files[0].path, path);

        fs::remove_dir_all(&dir).unwrap();
    }
}
