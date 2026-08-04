//! Gap detector for the sbe-collector's recording files.
//!
//! Files are zstd streams of length-prefixed records, one file per symbol
//! per UTC day (`<symbol>_<YYYYMMDD>.zst`):
//!
//! ```text
//! [i64 recv_ns LE][u8 tag][u32 payload_len LE][payload]
//! ```
//!
//! Tag `S` is a raw SBE stream frame, tag `R` a REST depth snapshot (JSON).
//! This tool scans a data directory, groups the files into per-symbol
//! series, and reports:
//!
//! * receive-time gaps above a threshold and receive-time regressions,
//!   including across the midnight file rotation;
//! * venue sequence breaks. Redundant connections can interleave, so the
//!   chains track the running maximum and only forward breaks count:
//!   - depth diffs (10003): the next event's `firstBookUpdateId` is
//!     normally `prev lastBookUpdateId + 1` (the collector's own rule);
//!   - trades (10000): trade ids increment by one per trade;
//!   - best bid/ask (10001): `bookUpdateId` must not go backwards;
//! * range accounting for trade ids: with unique ids,
//!   `(max - min + 1) - count` is the net of missing ids minus late
//!   duplicates — reorder-proof. `--exact` builds id sets to split that
//!   into exact missing and exact duplicate counts;
//! * event-time regressions per template;
//! * missing calendar dates within a series;
//! * undecodable, malformed, or foreign content (e.g. the JSON-line files
//!   of the sister collector are detected and skipped).
//!
//! Sequence state is kept per stream *across* files of a series, so breaks
//! straddling the rotation boundary are attributed correctly.

#[path = "../sbe_types.rs"]
mod sbe_types;

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs::{self, File},
    io::Read,
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
use sbe_types::{
    BestBidAskBlock, DepthDiffBlock, GroupSizeEncoding, MessageHeader, TEMPLATE_BEST_BID_ASK,
    TEMPLATE_DEPTH_DIFF, TEMPLATE_DEPTH_SNAPSHOT, TEMPLATE_TRADES, TradesBlock,
};
use zerocopy::FromBytes;

/// Files whose tail is newer than this are assumed to still be written by a
/// live collector, so an unterminated zstd frame is labelled rather than
/// treated as corruption.
const LIVE_FILE_WINDOW: Duration = Duration::from_secs(15 * 60);

/// Keep at most this many break timestamps per stream.
const MAX_BREAK_TIMES: usize = 16;

/// `[i64 recv_ns LE][u8 tag][u32 payload_len LE]`
const FRAME_HEADER_LEN: usize = 13;
const TAG_SBE: u8 = b'S';
const TAG_REST: u8 = b'R';

/// Sanity ceiling for one payload; real SBE frames are far smaller. A length
/// beyond this means the stream is corrupt (or the file is a foreign format).
const MAX_PAYLOAD_LEN: u32 = 64 * 1024 * 1024;

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
}

struct DatedFile {
    path: PathBuf,
    date: civil::Date,
}

struct Series {
    key: String,
    files: Vec<DatedFile>,
}

/// Discover `<symbol>_<YYYYMMDD>.zst` files under the roots and group them
/// into per-(directory, symbol) series sorted by date.
fn discover(roots: &[PathBuf]) -> Result<Vec<Series>> {
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
            if let Some(df) = dated_file(&dir) {
                let parent = dir.parent().unwrap_or(Path::new(".")).to_path_buf();
                let stem = df.path.file_stem().unwrap().to_string_lossy().into_owned();
                let symbol = stem
                    .rsplit_once('_')
                    .map(|(sym, _)| sym.to_string())
                    .unwrap_or(stem);
                map.entry((parent, symbol)).or_default().push(df);
            } else {
                skipped += 1;
            }
            continue;
        }
        for entry in fs::read_dir(&dir)
            .with_context(|| format!("cannot read directory {}", dir.display()))?
        {
            let entry = entry?;
            stack.push(entry.path());
        }
    }
    if skipped > 0 {
        eprintln!("note: skipped {skipped} .zst file(s) not named <symbol>_<YYYYMMDD>.zst");
    }

    let mut series: Vec<Series> = map
        .into_iter()
        .map(|((dir, symbol), mut files)| {
            files.sort_by_key(|f| f.date);
            Series {
                key: format!("{}/{symbol}", dir.display()),
                files,
            }
        })
        .collect();
    series.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(series)
}

fn dated_file(path: &Path) -> Option<DatedFile> {
    if path.extension().is_none_or(|ext| ext != "zst") {
        return None;
    }
    let stem = path.file_stem()?.to_str()?;
    let (_, date_str) = stem.rsplit_once('_')?;
    let date = civil::Date::strptime("%Y%m%d", date_str).ok()?;
    Some(DatedFile {
        path: path.to_path_buf(),
        date,
    })
}

// ---------------------------------------------------------------------------
// Scanning
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Default)]
struct StreamStats {
    messages: u64,
    /// Forward breaks in the venue id chain, i.e. ids the recording never
    /// saw between two consecutively recorded events.
    seq_fwd_events: u64,
    seq_fwd_ids: u64,
    /// Events that added nothing new: late duplicates or reordered events.
    seq_back_events: u64,
    /// Range accounting for point-id streams (trades): ids are unique, so
    /// `(id_max - id_min + 1) - id_count` nets missing ids against late
    /// duplicates regardless of recording order.
    id_min: i64,
    id_max: i64,
    id_count: u64,
    /// Only populated with `--exact`.
    exact_missing: u64,
    exact_dups: u64,
    /// Exchange event times that went backwards.
    time_regressions: u64,
}

impl StreamStats {
    /// Net id deficit over the observed range: positive means at least that
    /// many ids are missing, negative means at least that many duplicates.
    fn net_deficit(&self) -> i64 {
        if self.id_count == 0 {
            return 0;
        }
        (self.id_max - self.id_min + 1) - self.id_count as i64
    }
}

#[derive(Default)]
struct StreamState {
    stats: StreamStats,
    /// depth diff: running max `lastBookUpdateId`; bba: max `bookUpdateId`;
    /// trades: running max trade id
    last_id: Option<i64>,
    last_time: Option<i64>,
    /// Exact id set, only with `--exact` and only for point-id streams.
    ids: Option<HashSet<i64>>,
    /// Receive times of forward chain breaks, for locating incidents.
    break_times: Vec<i64>,
}

#[derive(Clone, Copy)]
struct Gap {
    start: i64,
    end: i64,
}

#[derive(Default)]
struct SeriesScan {
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
    streams: Vec<(&'static str, StreamState)>,
}

const STREAM_DEPTH_DIFF: &str = "depth-diff";
const STREAM_TRADES: &str = "trades";
const STREAM_BBA: &str = "best-bid-ask";

impl SeriesScan {
    fn stream(&mut self, key: &'static str, point_ids: bool) -> &mut StreamState {
        let pos = self.streams.iter().position(|(k, _)| *k == key);
        match pos {
            Some(i) => &mut self.streams[i].1,
            None => {
                let ids = if self.exact && point_ids {
                    Some(HashSet::new())
                } else {
                    None
                };
                self.streams.push((
                    key,
                    StreamState {
                        ids,
                        ..Default::default()
                    },
                ));
                &mut self.streams.last_mut().expect("just pushed").1
            }
        }
    }

    fn observe_recv(&mut self, recv: i64, min_gap_ns: i64) {
        if self.first_recv.is_none() {
            self.first_recv = Some(recv);
        }
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

    fn process_record(&mut self, recv: i64, tag: u8, payload: &[u8], min_gap_ns: i64) {
        self.observe_recv(recv, min_gap_ns);
        self.rows += 1;
        match tag {
            TAG_SBE => {
                self.sbe_records += 1;
                self.sbe_payload(recv, payload);
            }
            TAG_REST => self.rest_records += 1,
            _ => self.bad_records += 1,
        }
    }

    fn sbe_payload(&mut self, recv: i64, payload: &[u8]) {
        let Ok((header, _)) = MessageHeader::read_from_prefix(payload) else {
            self.bad_records += 1;
            return;
        };
        let template_id = header.template_id.get();
        let body = &payload[std::mem::size_of::<MessageHeader>()..];
        match template_id {
            TEMPLATE_DEPTH_DIFF => self.depth_diff(recv, body),
            TEMPLATE_TRADES => self.trades(recv, body, header.block_length.get() as usize),
            TEMPLATE_BEST_BID_ASK => self.best_bid_ask(body),
            // Depth snapshots (and anything a newer schema adds) are counted
            // but carry no chain of their own to check.
            TEMPLATE_DEPTH_SNAPSHOT => {}
            _ => self.bad_records += 1,
        }
    }

    /// The collector's own continuity rule: only a diff starting *beyond*
    /// `prev + 1` leaves a hole; the high-water mark only ever advances, so
    /// one reordered frame cannot cascade into further breaks.
    fn depth_diff(&mut self, recv: i64, body: &[u8]) {
        let Ok((blk, _)) = DepthDiffBlock::read_from_prefix(body) else {
            self.bad_records += 1;
            return;
        };
        let st = self.stream(STREAM_DEPTH_DIFF, false);
        st.stats.messages += 1;
        check_time(&mut st.stats, &mut st.last_time, blk.event_time.get());
        let first = blk.first_book_update_id.get();
        let last = blk.last_book_update_id.get();
        if let Some(prev) = st.last_id {
            if last > prev {
                if first > prev + 1 {
                    st.stats.seq_fwd_events += 1;
                    st.stats.seq_fwd_ids += (first - prev - 1) as u64;
                    note_break(st, recv);
                }
            } else {
                st.stats.seq_back_events += 1;
            }
        }
        st.last_id = Some(st.last_id.map_or(last, |prev| prev.max(last)));
    }

    /// Trade ids increment by one per trade, both within a frame's group and
    /// across frames.
    fn trades(&mut self, recv: i64, body: &[u8], block_length: usize) {
        let Ok((blk, _)) = TradesBlock::read_from_prefix(body) else {
            self.bad_records += 1;
            return;
        };
        let st = self.stream(STREAM_TRADES, true);
        st.stats.messages += 1;
        check_time(&mut st.stats, &mut st.last_time, blk.event_time.get());
        let mut off = block_length;
        if off + std::mem::size_of::<GroupSizeEncoding>() > body.len() {
            self.bad_records += 1;
            return;
        }
        let Ok((dim, _)) = GroupSizeEncoding::read_from_prefix(&body[off..]) else {
            self.bad_records += 1;
            return;
        };
        // The wire stride comes from the dimension header, not the
        // compile-time struct: a newer schema may append fields.
        let stride = dim.block_length.get() as usize;
        if stride < 8 {
            self.bad_records += 1;
            return;
        }
        off += std::mem::size_of::<GroupSizeEncoding>();
        for _ in 0..dim.num_in_group.get() {
            if off + stride > body.len() {
                break;
            }
            let id = i64::from_le_bytes(body[off..off + 8].try_into().expect("8 bytes"));
            chain(st, id, recv);
            observe_point_id(st, id);
            off += stride;
        }
    }

    /// `bookUpdateId` must never go backwards; forward jumps are normal,
    /// since the quote changes less often than the book updates.
    fn best_bid_ask(&mut self, body: &[u8]) {
        let Ok((blk, _)) = BestBidAskBlock::read_from_prefix(body) else {
            self.bad_records += 1;
            return;
        };
        let st = self.stream(STREAM_BBA, false);
        st.stats.messages += 1;
        check_time(&mut st.stats, &mut st.last_time, blk.event_time.get());
        let id = blk.book_update_id.get();
        if let Some(prev) = st.last_id
            && id < prev
        {
            st.stats.seq_back_events += 1;
        }
        st.last_id = Some(st.last_id.map_or(id, |prev| prev.max(id)));
    }
}

fn note_break(st: &mut StreamState, recv: i64) {
    if st.break_times.len() < MAX_BREAK_TIMES {
        st.break_times.push(recv);
    }
}

fn check_time(stats: &mut StreamStats, last: &mut Option<i64>, t: i64) {
    if let Some(prev) = *last
        && t < prev
    {
        stats.time_regressions += 1;
    }
    *last = Some(t);
}

/// Continuity of a strictly incrementing-by-one venue id (trade ids). The
/// chain tracks the running maximum, so a reordered or duplicated event
/// produces at most one backward step and does not poison later checks.
fn chain(st: &mut StreamState, id: i64, recv: i64) {
    if let Some(prev) = st.last_id {
        if id > prev + 1 {
            st.stats.seq_fwd_events += 1;
            st.stats.seq_fwd_ids += (id - prev - 1) as u64;
            note_break(st, recv);
        } else if id <= prev {
            st.stats.seq_back_events += 1;
        }
    }
    st.last_id = Some(st.last_id.map_or(id, |prev| prev.max(id)));
}

/// Track min/max/count of a point-id stream for range accounting.
fn observe_point_id(st: &mut StreamState, id: i64) {
    let stats = &mut st.stats;
    if stats.id_count == 0 {
        stats.id_min = id;
        stats.id_max = id;
    } else {
        stats.id_min = stats.id_min.min(id);
        stats.id_max = stats.id_max.max(id);
    }
    stats.id_count += 1;
    if let Some(set) = &mut st.ids {
        set.insert(id);
    }
}

// ---------------------------------------------------------------------------
// Reports
// ---------------------------------------------------------------------------

struct FileReport {
    date: civil::Date,
    rows: u64,
    foreign: bool,
    decode_error: Option<String>,
    live: bool,
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
    streams: Vec<(&'static str, StreamStats, Vec<i64>)>,
}

impl SeriesReport {
    /// Issues that indicate lost or unrecordable data (as opposed to
    /// reordering/duplication artefacts of redundant collection).
    fn has_issues(&self) -> bool {
        !self.gaps.is_empty()
            || !self.missing_dates.is_empty()
            || self.bad_records > 0
            || self.files.iter().any(|f| f.decode_error.is_some())
            || self
                .streams
                .iter()
                .any(|(_, s, _)| s.seq_fwd_events > 0 || s.net_deficit() > 0 || s.exact_missing > 0)
    }
}

/// A first record that does not even hold a plausible tag marks the whole
/// file as a foreign format rather than as one bad record.
fn is_foreign(tag: u8) -> bool {
    !matches!(tag, TAG_SBE | TAG_REST)
}

fn scan_file(scan: &mut SeriesScan, df: &DatedFile, min_gap_ns: i64) -> FileReport {
    let mut report = FileReport {
        date: df.date,
        rows: 0,
        foreign: false,
        decode_error: None,
        live: false,
    };
    let file = match File::open(&df.path) {
        Ok(file) => file,
        Err(error) => {
            report.decode_error = Some(format!("open failed: {error}"));
            return report;
        }
    };
    let modified = fs::metadata(&df.path).and_then(|m| m.modified()).ok();
    let Ok(decoder) = zstd::stream::read::Decoder::new(file) else {
        report.decode_error = Some("not a zstd stream".into());
        return report;
    };
    let mut reader = std::io::BufReader::with_capacity(1 << 20, decoder);
    let mut header = [0u8; FRAME_HEADER_LEN];
    let mut payload: Vec<u8> = Vec::with_capacity(1 << 16);
    loop {
        // Accumulate the fixed header; a short trailing read means the file
        // ends mid-record.
        let mut got = 0usize;
        let mut read_error = None;
        while got < FRAME_HEADER_LEN {
            match reader.read(&mut header[got..]) {
                Ok(0) => break,
                Ok(n) => got += n,
                Err(error) => {
                    read_error = Some(format!("{error}"));
                    break;
                }
            }
        }
        if let Some(error) = read_error {
            report.decode_error = Some(error);
            break;
        }
        if got == 0 {
            break; // clean EOF
        }
        if got < FRAME_HEADER_LEN {
            report.decode_error = Some(format!("truncated record header ({got} trailing bytes)"));
            break;
        }
        let recv = i64::from_le_bytes(header[0..8].try_into().expect("8 bytes"));
        let tag = header[8];
        let len = u32::from_le_bytes(header[9..13].try_into().expect("4 bytes"));
        if report.rows == 0 && (is_foreign(tag) || len > MAX_PAYLOAD_LEN) {
            report.foreign = true;
            break;
        }
        if is_foreign(tag) || len > MAX_PAYLOAD_LEN {
            report.decode_error = Some(format!("implausible record (tag {tag:#04x}, len {len})"));
            break;
        }
        payload.resize(len as usize, 0);
        if let Err(error) = reader.read_exact(&mut payload) {
            report.decode_error = Some(format!("truncated payload: {error}"));
            break;
        }
        scan.process_record(recv, tag, &payload, min_gap_ns);
        report.rows += 1;
    }
    if report.decode_error.is_some() {
        report.live = modified.is_some_and(|m| {
            SystemTime::now()
                .duration_since(m)
                .is_ok_and(|age| age < LIVE_FILE_WINDOW)
        });
        // The unreadable tail hides whenever the next record arrived, so a
        // receive-time gap measured across it would be fiction; the sequence
        // checks keep their state and will count the hole.
        scan.prev_recv = None;
    }
    report
}

fn scan_series(series: &Series, min_gap_ns: i64, exact: bool) -> SeriesReport {
    let mut scan = SeriesScan {
        exact,
        ..Default::default()
    };
    let mut files = Vec::with_capacity(series.files.len());
    for df in &series.files {
        files.push(scan_file(&mut scan, df, min_gap_ns));
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
        streams: scan
            .streams
            .into_iter()
            .map(|(name, mut state)| {
                let mut stats = state.stats;
                if let Some(set) = state.ids {
                    let distinct = set.len() as u64;
                    if stats.id_count > 0 {
                        let span = (stats.id_max - stats.id_min + 1) as u64;
                        stats.exact_missing = span.saturating_sub(distinct);
                        stats.exact_dups = stats.id_count.saturating_sub(distinct);
                    }
                }
                state.break_times.sort_unstable();
                (name, stats, state.break_times)
            })
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// Formatting
// ---------------------------------------------------------------------------

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
        let deficit = s.net_deficit();
        if exact {
            parts.push(format!(
                "ids {}..{}: exact {} missing, {} duplicates ({} observed)",
                grouped(s.id_min as u64),
                grouped(s.id_max as u64),
                grouped(s.exact_missing),
                grouped(s.exact_dups),
                grouped(s.id_count)
            ));
        } else if deficit > 0 {
            parts.push(format!(
                "ids {}..{}: net {} missing ({} observed)",
                grouped(s.id_min as u64),
                grouped(s.id_max as u64),
                grouped(deficit as u64),
                grouped(s.id_count)
            ));
        } else if deficit < 0 {
            parts.push(format!(
                "ids {}..{}: net {} extra from reorders/dups ({} observed)",
                grouped(s.id_min as u64),
                grouped(s.id_max as u64),
                grouped((-deficit) as u64),
                grouped(s.id_count)
            ));
        } else {
            parts.push(format!(
                "ids {}..{}: complete ({} observed)",
                grouped(s.id_min as u64),
                grouped(s.id_max as u64),
                grouped(s.id_count)
            ));
        }
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

fn stream_notable(s: &StreamStats) -> bool {
    s.id_count > 0 || s.seq_fwd_events > 0 || s.seq_back_events > 0 || s.time_regressions > 0
}

fn print_report(report: &SeriesReport, min_gap_ns: i64, max_reported: usize, exact: bool) {
    println!("{}", report.key);
    let dates: Vec<String> = report.files.iter().map(|f| f.date.to_string()).collect();
    println!("  files: {}", dates.join(", "));
    for file in &report.files {
        if file.foreign {
            println!(
                "    {}: foreign format (not sbe-collector records), skipped",
                file.date
            );
        }
        if let Some(error) = &file.decode_error {
            if file.live {
                println!(
                    "    {}: unterminated zstd stream after {} records (file modified recently — still being written?): {error}",
                    file.date,
                    grouped(file.rows)
                );
            } else {
                println!(
                    "    {}: decode error after {} records: {error}",
                    file.date,
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
        let total: i64 = report.gaps.iter().map(|g| g.end - g.start).sum();
        println!(
            "  RECV GAPS > {threshold}: {} (total silence {})",
            report.gaps.len(),
            fmt_dur(total)
        );
        let mut largest: Vec<&Gap> = report.gaps.iter().collect();
        largest.sort_by_key(|g| std::cmp::Reverse(g.end - g.start));
        for gap in largest.iter().take(max_reported) {
            println!(
                "    {} .. {} ({})",
                fmt_ts(gap.start),
                fmt_ts(gap.end),
                fmt_dur(gap.end - gap.start)
            );
        }
        if report.gaps.len() > max_reported {
            println!("    ... {} more", report.gaps.len() - max_reported);
        }
    }

    let mut streams: Vec<&(&'static str, StreamStats, Vec<i64>)> = report
        .streams
        .iter()
        .filter(|(_, s, _)| stream_notable(s))
        .collect();
    if !streams.is_empty() {
        streams.sort_by(|a, b| a.0.cmp(b.0));
        println!("  streams:");
        for (name, s, breaks) in streams {
            let parts = stream_issues(s, breaks, exact);
            println!("    {name}: {} msgs", grouped(s.messages));
            for part in parts {
                println!("      {part}");
            }
        }
    }
    println!();
}

// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    let args = Args::parse();
    let min_gap_ns = (args.min_gap.max(0.0) * 1e9) as i64;

    let mut series = discover(&args.paths)?;
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
    // Cap the default so a shared machine is not flooded; --jobs overrides.
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
    for report in &reports {
        print_report(report, min_gap_ns, args.max_reported, args.exact);
    }

    let foreign_files: usize = reports
        .iter()
        .flat_map(|r| &r.files)
        .filter(|f| f.foreign)
        .count();
    let decode_errors: usize = reports
        .iter()
        .flat_map(|r| &r.files)
        .filter(|f| f.decode_error.is_some())
        .count();
    let recv_gaps: usize = reports.iter().map(|r| r.gaps.len()).sum();
    let gap_series = reports.iter().filter(|r| !r.gaps.is_empty()).count();
    let silence_total: i64 = reports
        .iter()
        .flat_map(|r| &r.gaps)
        .map(|g| g.end - g.start)
        .sum();
    let fwd_events: u64 = reports
        .iter()
        .flat_map(|r| &r.streams)
        .map(|(_, s, _)| s.seq_fwd_events)
        .sum();
    let fwd_ids: u64 = reports
        .iter()
        .flat_map(|r| &r.streams)
        .map(|(_, s, _)| s.seq_fwd_ids)
        .sum();
    let net_positive: i64 = reports
        .iter()
        .flat_map(|r| &r.streams)
        .map(|(_, s, _)| s.net_deficit().max(0))
        .sum();
    let exact_missing: u64 = reports
        .iter()
        .flat_map(|r| &r.streams)
        .map(|(_, s, _)| s.exact_missing)
        .sum();
    let back_events: u64 = reports
        .iter()
        .flat_map(|r| &r.streams)
        .map(|(_, s, _)| s.seq_back_events)
        .sum();
    let time_regressions: u64 = reports
        .iter()
        .flat_map(|r| &r.streams)
        .map(|(_, s, _)| s.time_regressions)
        .sum();
    let missing_dates: usize = reports.iter().map(|r| r.missing_dates.len()).sum();
    let bad_records: u64 = reports.iter().map(|r| r.bad_records).sum();
    let rows: u64 = reports.iter().map(|r| r.rows).sum();
    let with_issues = reports.iter().filter(|r| r.has_issues()).count();

    println!("SUMMARY");
    println!("  series scanned        : {}", reports.len());
    println!("  records decoded       : {}", grouped(rows));
    println!(
        "  recv gaps > {}s  : {} across {} series (total silence {})",
        args.min_gap,
        recv_gaps,
        gap_series,
        fmt_dur(silence_total)
    );
    println!(
        "  forward chain breaks  : {} event(s), {} ids",
        grouped(fwd_events),
        grouped(fwd_ids)
    );
    if args.exact {
        println!("  exact missing ids     : {}", grouped(exact_missing));
    } else {
        println!(
            "  net missing ids       : {} (range accounting)",
            grouped(net_positive as u64)
        );
    }
    println!("  dup/reordered events  : {}", grouped(back_events));
    println!("  time regressions      : {}", grouped(time_regressions));
    println!("  missing dates         : {missing_dates}");
    println!("  decode errors         : {decode_errors}");
    println!("  bad records           : {}", grouped(bad_records));
    println!("  foreign files         : {foreign_files} (skipped)");

    println!("RESULT: issues found in {with_issues} series");
    if args.fail_on_gaps && with_issues > 0 {
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(recv: i64, tag: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
        out.extend_from_slice(&recv.to_le_bytes());
        out.push(tag);
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn sbe_header(block_length: u16, template_id: u16) -> Vec<u8> {
        let mut out = Vec::with_capacity(8);
        out.extend_from_slice(&block_length.to_le_bytes());
        out.extend_from_slice(&template_id.to_le_bytes());
        out.extend_from_slice(&1u16.to_le_bytes()); // schema id
        out.extend_from_slice(&1u16.to_le_bytes()); // version
        out
    }

    /// A `DepthDiffStreamEvent` payload: header plus fixed block. The chain
    /// checks read only the fixed block, so no groups are needed.
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

    fn scan_record(scan: &mut SeriesScan, recv: i64, tag: u8, payload: &[u8]) {
        scan.process_record(recv, tag, payload, 5_000_000_000);
    }

    fn stats<'a>(scan: &'a SeriesScan, key: &str) -> &'a StreamStats {
        &scan
            .streams
            .iter()
            .find(|(k, _)| *k == key)
            .expect("stream present")
            .1
            .stats
    }

    #[test]
    fn recv_gap_and_regression() {
        let mut scan = SeriesScan::default();
        let frame = depth_diff(1, 1, 2);
        scan_record(&mut scan, 1_000_000_000, TAG_SBE, &frame);
        scan_record(&mut scan, 8_000_000_000, TAG_SBE, &frame);
        scan_record(&mut scan, 7_000_000_000, TAG_SBE, &frame);
        assert_eq!(scan.gaps.len(), 1);
        assert_eq!(scan.gaps[0].start, 1_000_000_000);
        assert_eq!(scan.gaps[0].end, 8_000_000_000);
        assert_eq!(scan.recv_regressions, 1);
    }

    #[test]
    fn depth_diff_chain_hole_overlap_and_reorder() {
        let mut scan = SeriesScan::default();
        scan_record(&mut scan, 1, TAG_SBE, &depth_diff(1, 10, 20));
        scan_record(&mut scan, 2, TAG_SBE, &depth_diff(2, 21, 30)); // contiguous
        scan_record(&mut scan, 3, TAG_SBE, &depth_diff(3, 41, 50)); // 31..40 lost
        scan_record(&mut scan, 4, TAG_SBE, &depth_diff(4, 35, 50)); // covered straggler
        scan_record(&mut scan, 5, TAG_SBE, &depth_diff(5, 51, 60)); // contiguous vs max
        let s = stats(&scan, STREAM_DEPTH_DIFF);
        assert_eq!(s.seq_fwd_events, 1);
        assert_eq!(s.seq_fwd_ids, 10);
        assert_eq!(s.seq_back_events, 1);
        assert_eq!(s.time_regressions, 0);
    }

    #[test]
    fn trade_ids_chain_within_and_across_frames() {
        let mut scan = SeriesScan::default();
        scan_record(&mut scan, 1, TAG_SBE, &trades(1, &[10, 11, 12]));
        scan_record(&mut scan, 2, TAG_SBE, &trades(2, &[13, 15])); // 14 not seen yet
        scan_record(&mut scan, 3, TAG_SBE, &trades(3, &[14])); // arrives late
        scan_record(&mut scan, 4, TAG_SBE, &trades(4, &[16])); // contiguous vs max
        let s = stats(&scan, STREAM_TRADES);
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
        scan_record(&mut scan, 1, TAG_SBE, &bba(100));
        scan_record(&mut scan, 2, TAG_SBE, &bba(500)); // forward jump: normal
        scan_record(&mut scan, 3, TAG_SBE, &bba(400)); // backwards: dup/reorder
        let s = stats(&scan, STREAM_BBA);
        assert_eq!(s.seq_fwd_events, 0);
        assert_eq!(s.seq_back_events, 1);
    }

    #[test]
    fn event_time_regressions_are_counted_per_stream() {
        let mut scan = SeriesScan::default();
        scan_record(&mut scan, 1, TAG_SBE, &depth_diff(100, 1, 2));
        scan_record(&mut scan, 2, TAG_SBE, &depth_diff(90, 3, 4));
        assert_eq!(stats(&scan, STREAM_DEPTH_DIFF).time_regressions, 1);
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
    fn foreign_first_record_is_detected() {
        assert!(is_foreign(b'0'));
        assert!(is_foreign(b'{'));
        assert!(!is_foreign(TAG_SBE));
        assert!(!is_foreign(TAG_REST));
    }

    #[test]
    fn grouped_formats_thousands() {
        assert_eq!(grouped(0), "0");
        assert_eq!(grouped(999), "999");
        assert_eq!(grouped(1_000), "1,000");
        assert_eq!(grouped(1_000_000), "1,000,000");
        assert_eq!(grouped(12_345_678), "12,345,678");
    }

    /// End-to-end over a real zstd file: records in, report out.
    #[test]
    fn scans_a_written_file_end_to_end() {
        use std::io::Write;

        let dir = std::env::temp_dir().join(format!(
            "sbe-gap-detector-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("btcusdt_20260802.zst");
        {
            let file = File::create(&path).unwrap();
            let mut encoder = zstd::stream::write::Encoder::new(file, 1).unwrap();
            for frame in [
                record(1_000_000_000, TAG_SBE, &depth_diff(1, 1, 10)),
                record(2_000_000_000, TAG_SBE, &depth_diff(2, 11, 20)),
                record(9_000_000_000, TAG_SBE, &depth_diff(3, 21, 30)), // 7s silence
                record(9_500_000_000, TAG_REST, b"{}"),
            ] {
                encoder.write_all(&frame).unwrap();
            }
            encoder.finish().unwrap();
        }

        let series = Series {
            key: "test/btcusdt".into(),
            files: vec![DatedFile {
                path: path.clone(),
                date: civil::Date::new(2026, 8, 2).unwrap(),
            }],
        };
        let report = scan_series(&series, 5_000_000_000, false);
        assert_eq!(report.rows, 4);
        assert_eq!(report.sbe_records, 3);
        assert_eq!(report.rest_records, 1);
        assert_eq!(report.gaps.len(), 1);
        assert_eq!(report.gaps[0].end - report.gaps[0].start, 7_000_000_000);

        fs::remove_dir_all(&dir).unwrap();
    }
}
