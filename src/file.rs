use std::{
    borrow::Cow,
    collections::HashMap,
    fmt::{self, Write as _},
    fs::{File, TryLockError},
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    mem,
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::bail;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use jiff::{Timestamp, civil::Date, tz::TimeZone};
use sbe_collector::record::{HEADER_LEN, Header, Tag};
use tracing::{error, info, warn};
use zstd::stream::{read::Decoder as ZstdDecoder, write::Encoder as ZstdEncoder};

use crate::quality::{QualityEvent, QualityReporter};

pub type Symbol = Arc<str>;

type Encoder = ZstdEncoder<'static, File>;

const ZSTD_LEVEL: i32 = 1;
/// Uncompressed bytes batched per zstd call: the compressor's block size.
const ZSTD_BATCH_BYTES: usize = 128 * 1024;
/// Spacing of merge and reclaim attempts after a failure, doubled per
/// further consecutive failure up to the cap. An attempt fsyncs the daily
/// file, which stalls the writer for every symbol.
const MERGE_RETRY_BACKOFF: Duration = Duration::from_secs(5);
const MERGE_RETRY_BACKOFF_CAP: Duration = Duration::from_secs(10 * 60);
const ZSTD_MAGIC: u32 = 0xFD2F_B528;
const SKIPPABLE_MAGIC: u32 = 0x184D_2A50;
const SKIPPABLE_MAGIC_MASK: u32 = 0xFFFF_FFF0;
/// Clean-close trailer, appended after a sink's last frame: a zstd skippable
/// frame `[magic][size 16]["COLLECTR"][u64 LE offset]`, where `offset` is the
/// file length at which the trailer starts. A file ending in a trailer that
/// points at itself ends on a frame boundary, known from one read instead of
/// a walk over every block. Decoders skip it.
const TRAILER_MAGIC: u32 = 0x184D_2A5C;
const TRAILER_TAG: [u8; 8] = *b"COLLECTR";
const TRAILER_LEN: usize = 24;

/// An I/O step a test can make fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    Lock,
    Verify,
    SetAside,
    Append,
    Resume,
    Flush,
    Finish,
    Trailer,
    Sync,
}

/// Never fails outside tests.
#[cfg(not(test))]
#[inline]
fn fault(_: Step) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
use tests::fault;

/// `symbol` must be lowercase.
pub struct WriteRecord {
    pub recv_time: Timestamp,
    pub symbol: Symbol,
    pub tag: Tag,
    pub data: Bytes,
}

/// Bytes kept as-is in a filename: everything but the genuinely path-hostile.
fn is_safe_in_filename(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'@' | b'+' | b'=')
}

/// Encode a symbol into a filename component.
///
/// The encoding must be injective: each symbol owns one zstd encoder, and two
/// encoders appending to one file would interleave frames into garbage.
/// Percent escaping is injective because `%` itself is always escaped.
fn encode_symbol(symbol: &str) -> Cow<'_, str> {
    if symbol.bytes().all(is_safe_in_filename) {
        return Cow::Borrowed(symbol);
    }
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(symbol.len() + 8);
    for byte in symbol.bytes() {
        if is_safe_in_filename(byte) {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
    }
    Cow::Owned(encoded)
}

/// `<prefix>_<YYYYMMDD><suffix>.zst`, where `prefix` is `<dir>/<encoded symbol>`.
/// Formatted straight into the path: a sidecar probes its daily file's name
/// often through a handover.
fn day_file(prefix: &Path, date: Date, suffix: fmt::Arguments<'_>) -> PathBuf {
    let mut name = prefix.as_os_str().to_owned();
    write!(name, "_{}{suffix}.zst", date.strftime("%Y%m%d"))
        .expect("formatting into an OsString cannot fail");
    PathBuf::from(name)
}

/// `flock(LOCK_EX | LOCK_NB)` on Linux, so it excludes older binaries too.
fn try_exclusive_lock(file: &File) -> io::Result<bool> {
    match file.try_lock() {
        Ok(()) => Ok(true),
        Err(TryLockError::WouldBlock) => Ok(false),
        Err(TryLockError::Error(error)) => Err(error),
    }
}

fn lock_exclusively(file: &File, path: &Path) -> io::Result<()> {
    if try_exclusive_lock(file)? {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!("{} is locked by another writer", path.display()),
        ))
    }
}

/// Whether `path` still names the inode `file` refers to. A lock taken on a
/// file that was renamed away in the meantime guards nothing.
fn names_file(path: &Path, file: &File) -> io::Result<bool> {
    let held = file.metadata()?;
    match std::fs::metadata(path) {
        Ok(named) => Ok(named.dev() == held.dev() && named.ino() == held.ino()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn trailer(offset: u64) -> [u8; TRAILER_LEN] {
    let mut bytes = [0; TRAILER_LEN];
    bytes[..4].copy_from_slice(&TRAILER_MAGIC.to_le_bytes());
    bytes[4..8].copy_from_slice(&(TRAILER_LEN as u32 - 8).to_le_bytes());
    bytes[8..16].copy_from_slice(&TRAILER_TAG);
    bytes[16..].copy_from_slice(&offset.to_le_bytes());
    bytes
}

/// Append a trailer at `file`'s end, which its write position must be.
fn append_trailer(file: &mut File) -> io::Result<()> {
    let offset = file.metadata()?.len();
    file.write_all(&trailer(offset))
}

/// Whether `file` ends on a frame boundary: from its trailer if it ends with
/// one that points at itself, else by walking every frame.
fn ends_cleanly(file: &File) -> io::Result<bool> {
    fault(Step::Verify)?;
    let len = file.metadata()?.len();
    if len == 0 || ends_with_trailer(file, len)? {
        return Ok(true);
    }
    ends_on_frame_boundary(file, len)
}

fn ends_with_trailer(file: &File, len: u64) -> io::Result<bool> {
    let Some(start) = len.checked_sub(TRAILER_LEN as u64) else {
        return Ok(false);
    };
    let mut tail = [0; TRAILER_LEN];
    file.read_exact_at(&mut tail, start)?;
    Ok(tail == trailer(start))
}

/// Whether the first `len` bytes of `file` end exactly where a complete zstd
/// frame does.
///
/// Walks frame and block headers only: one small `pread` per block, no
/// decompression.
fn ends_on_frame_boundary(file: &File, len: u64) -> io::Result<bool> {
    let read_le = |pos: u64, width: usize| -> io::Result<Option<u32>> {
        if pos.saturating_add(width as u64) > len {
            return Ok(None);
        }
        let mut bytes = [0u8; 4];
        file.read_exact_at(&mut bytes[..width], pos)?;
        Ok(Some(u32::from_le_bytes(bytes)))
    };
    let mut pos = 0u64;
    while pos < len {
        let Some(magic) = read_le(pos, 4)? else {
            return Ok(false);
        };
        pos += 4;
        if magic & SKIPPABLE_MAGIC_MASK == SKIPPABLE_MAGIC {
            let Some(size) = read_le(pos, 4)? else {
                return Ok(false);
            };
            pos += 4 + u64::from(size);
            continue;
        }
        if magic != ZSTD_MAGIC {
            return Ok(false);
        }
        let Some(descriptor) = read_le(pos, 1)? else {
            return Ok(false);
        };
        let single_segment = descriptor & 0x20 != 0;
        let window = u64::from(!single_segment);
        let dictionary: u64 = [0, 1, 2, 4][(descriptor & 0x03) as usize];
        let content_size = match descriptor >> 6 {
            0 => u64::from(single_segment),
            1 => 2,
            2 => 4,
            _ => 8,
        };
        pos += 1 + window + dictionary + content_size;
        loop {
            let Some(block) = read_le(pos, 3)? else {
                return Ok(false);
            };
            pos += 3;
            pos += match (block >> 1) & 0x03 {
                1 => 1,                // RLE: one byte, repeated
                3 => return Ok(false), // reserved block type
                _ => u64::from(block >> 3),
            };
            if block & 1 != 0 {
                break;
            }
        }
        if descriptor & 0x04 != 0 {
            pos += 4; // content checksum
        }
    }
    Ok(pos == len)
}

fn open_locked(path: &Path) -> io::Result<Option<File>> {
    fault(Step::Lock)?;
    let file = File::options()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
    Ok((try_exclusive_lock(&file)? && names_file(path, &file)?).then_some(file))
}

/// An attempt to take a day's daily file.
enum DailyLock {
    /// Locked by this process, and ending on a frame boundary.
    Held(File),
    /// Another writer holds it.
    Busy,
    /// It could not be opened or locked, or it is damaged and could not be
    /// moved aside.
    Failed(io::Error),
}

/// Lock `date`'s daily file.
///
/// A file whose last writer died mid-frame, or that cannot be verified, is
/// moved aside first: a decoder cannot step past a torn tail, so anything
/// appended after it would be lost.
fn lock_daily(prefix: &Path, date: Date) -> DailyLock {
    let path = day_file(prefix, date, format_args!(""));
    let daily = match open_locked(&path) {
        Ok(Some(daily)) => daily,
        Ok(None) => return DailyLock::Busy,
        Err(error) => return DailyLock::Failed(error),
    };
    match ends_cleanly(&daily) {
        Ok(true) => return DailyLock::Held(daily),
        Ok(false) => warn!(path = %path.display(), "daily file ends mid-frame"),
        Err(error) => warn!(path = %path.display(), %error, "cannot verify daily file"),
    }
    let salvage = day_file(
        prefix,
        date,
        format_args!("_unterminated-{}", Timestamp::now().as_nanosecond()),
    );
    if let Err(error) = set_aside(&path, &salvage) {
        return DailyLock::Failed(error);
    }
    warn!(
        path = %path.display(),
        salvage = %salvage.display(),
        "moved the daily file aside instead of appending to it"
    );
    let fresh = open_locked(&path);
    // The damaged inode stays locked until the fresh one is.
    drop(daily);
    match fresh {
        Ok(Some(daily)) => DailyLock::Held(daily),
        Ok(None) => DailyLock::Busy,
        Err(error) => DailyLock::Failed(error),
    }
}

fn set_aside(path: &Path, salvage: &Path) -> io::Result<()> {
    fault(Step::SetAside)?;
    std::fs::rename(path, salvage).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("cannot move {} aside: {error}", path.display()),
        )
    })
}

fn rollback_append(daily: &mut File, start: u64) -> io::Result<()> {
    daily.set_len(start)?;
    daily.seek(SeekFrom::Start(start))?;
    daily.sync_all()
}

/// Append the sidecar and a trailer for the daily file's new length, so the
/// daily file stays verifiable from its tail.
fn copy_synced(daily: &mut File, sidecar: &mut File) -> io::Result<()> {
    fault(Step::Append)?;
    io::copy(sidecar, daily)?;
    append_trailer(daily)?;
    daily.sync_all()
}

fn append_sidecar(daily: &mut File, mut sidecar: File) -> io::Result<()> {
    let start = daily.metadata()?.len();
    let Err(error) = copy_synced(daily, &mut sidecar) else {
        return Ok(());
    };
    match rollback_append(daily, start) {
        Ok(()) => Err(error),
        Err(rollback) => Err(io::Error::new(
            error.kind(),
            format!("{error}; also failed to roll back partial append: {rollback}"),
        )),
    }
}

/// Outcome of appending a sidecar to its daily file.
enum Merge {
    /// Appended, made durable, and deleted.
    Done,
    /// The sidecar does not end on a frame boundary; it stays on disk.
    Rejected(io::Error),
    /// Reading, verifying or appending failed; the append was rolled back.
    Failed(io::Error),
}

/// Append `sidecar` to the locked daily file once it verifies. The only
/// place a sidecar is deleted: after its bytes are durable in the daily file.
fn merge_sidecar(daily: &mut File, sidecar: &Path) -> Merge {
    let verified = File::open(sidecar).and_then(|file| Ok((ends_cleanly(&file)?, file)));
    let source = match verified {
        Ok((true, file)) => file,
        Ok((false, _)) => {
            return Merge::Rejected(io::Error::new(
                io::ErrorKind::InvalidData,
                "sidecar does not end on a frame boundary",
            ));
        }
        Err(error) => return Merge::Failed(error),
    };
    if let Err(error) = append_sidecar(daily, source) {
        return Merge::Failed(error);
    }
    match std::fs::remove_file(sidecar) {
        Ok(()) => info!(
            sidecar = %sidecar.display(),
            "switch complete; merged and removed overlap sidecar"
        ),
        Err(error) => warn!(
            sidecar = %sidecar.display(),
            %error,
            "switch complete; merged overlap sidecar but failed to remove it"
        ),
    }
    Merge::Done
}

/// How sealing a sink failed.
enum SealError {
    /// The frame is unfinished: the batch could not be written, or `finish`
    /// failed. The batch is kept.
    Unfinished(io::Error),
    /// The frame is finished but its trailer or fsync failed: the file is
    /// whole as far as this process can tell.
    Unsynced(io::Error),
}

impl From<SealError> for io::Error {
    fn from(error: SealError) -> Self {
        match error {
            SealError::Unfinished(error) | SealError::Unsynced(error) => error,
        }
    }
}

/// Write the batched records, finish the zstd frame, append the trailer,
/// and fsync. The batch is cleared once the frame is finished.
fn seal(mut encoder: Encoder, batch: &mut BytesMut, path: &Path) -> Result<(), SealError> {
    let context = |what: &str, error: io::Error| {
        io::Error::new(
            error.kind(),
            format!("failed to {what} {}: {error}", path.display()),
        )
    };
    encoder
        .write_all(batch)
        .map_err(|error| SealError::Unfinished(context("write records to", error)))?;
    let mut file = fault(Step::Finish)
        .and_then(|()| encoder.finish())
        .map_err(|error| SealError::Unfinished(context("finish zstd stream", error)))?;
    batch.clear();
    let end = file
        .metadata()
        .map_err(|error| SealError::Unsynced(context("append trailer to", error)))?
        .len();
    if let Err(error) = fault(Step::Trailer).and_then(|()| file.write_all(&trailer(end))) {
        let error = context("append trailer to", error);
        // A torn trailer reads as a torn frame.
        return Err(match file.set_len(end) {
            Ok(()) => SealError::Unsynced(error),
            Err(_) => SealError::Unfinished(error),
        });
    }
    // No retry: Linux may drop the dirty pages of a failed fsync and mark
    // them clean, so a second attempt can succeed with the data gone.
    fault(Step::Sync)
        .and_then(|()| file.sync_all())
        .map_err(|error| SealError::Unsynced(context("sync", error)))
}

/// How much of `batch` the abandoned `sidecar` already holds: up to and
/// including the first record equal to the last whole record the sidecar
/// decodes to. `None` if that record is not in the batch.
fn persisted_prefix(sidecar: &Path, batch: &[u8]) -> io::Result<Option<usize>> {
    let Some(last) = last_record(sidecar)? else {
        return Ok(None);
    };
    let mut start = 0;
    while let Some(header) = batch.get(start..).and_then(<[u8]>::first_chunk) {
        let end = start + HEADER_LEN + Header::parse(header).len as usize;
        if batch.get(start..end) == Some(&last[..]) {
            return Ok(Some(end));
        }
        start = end;
    }
    Ok(None)
}

/// The last whole record `path` decodes to, up to where its last frame was
/// cut off. Any other decoding error is an error.
fn last_record(path: &Path) -> io::Result<Option<Vec<u8>>> {
    let mut decoder = BufReader::new(ZstdDecoder::new(File::open(path)?)?);
    let mut header = [0; HEADER_LEN];
    let (mut last, mut next) = (Vec::new(), Vec::new());
    let mut found = false;
    loop {
        match decoder.read_exact(&mut header) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        }
        let len = Header::parse(&header).len;
        next.clear();
        next.extend_from_slice(&header);
        match (&mut decoder).take(u64::from(len)).read_to_end(&mut next) {
            Ok(read) if read == len as usize => {}
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        }
        mem::swap(&mut last, &mut next);
        found = true;
    }
    Ok(found.then_some(last))
}

/// Keep writing a sealed sidecar after its merge failed: a new frame after
/// the sealed one, so a later merge still appends whole frames.
fn resume_sidecar(path: &Path) -> io::Result<Encoder> {
    fault(Step::Resume)?;
    let file = File::options().append(true).open(path)?;
    lock_exclusively(&file, path)?;
    Encoder::new(file, ZSTD_LEVEL)
}

fn no_sink(prefix: &Path) -> io::Error {
    io::Error::other(format!("{} has no open file", prefix.display()))
}

/// Where the open day's records go.
enum Sink {
    /// The daily file, whose lock this process holds.
    Daily(Encoder),
    /// A run-local sidecar, while the daily file is held by another writer
    /// or cannot be taken.
    Sidecar { encoder: Encoder, path: PathBuf },
    /// The sidecar after a failed write dropped its encoder (see
    /// [`RotatingFile::tear`]): it ends mid-frame and is never appended. The
    /// switch writes the batched records it lacks to the daily file.
    Torn(PathBuf),
    /// Sealed, or left without a file by a failed switch.
    Closed,
}

impl Sink {
    /// This run's sidecar, whole or torn: what a switch moves off.
    fn is_sidecar(&self) -> bool {
        matches!(self, Self::Sidecar { .. } | Self::Torn(_))
    }
}

/// The UTC day the open file covers.
#[derive(Clone, Copy)]
struct UtcDay {
    date: Date,
    end: Timestamp,
}

impl UtcDay {
    fn containing(timestamp: Timestamp) -> io::Result<Self> {
        let date = timestamp.to_zoned(TimeZone::UTC).date();
        let end = date
            .tomorrow()
            .and_then(|next| next.to_zoned(TimeZone::UTC))
            .map_err(io::Error::other)?
            .timestamp();
        Ok(Self { date, end })
    }
}

/// A sealed sidecar, appended once its daily file is free.
struct PendingSidecar {
    date: Date,
    path: PathBuf,
}

/// What one merge or reclaim attempt came to; attempts made together count
/// as their greatest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Progress {
    /// Nothing to do, or the daily file is held by another writer.
    Idle,
    Done,
    Failed,
}

/// Spacing of merge and reclaim attempts after consecutive failures.
#[derive(Debug, Default)]
struct Backoff {
    failures: u32,
    not_before: Option<Instant>,
}

impl Backoff {
    fn delay(failures: u32) -> Duration {
        MERGE_RETRY_BACKOFF
            .saturating_mul(1 << failures.min(16))
            .min(MERGE_RETRY_BACKOFF_CAP)
    }

    fn is_due(&self, now: Instant) -> bool {
        self.not_before.is_none_or(|not_before| now >= not_before)
    }

    fn record(&mut self, progress: Progress, now: Instant) {
        match progress {
            Progress::Idle => {}
            Progress::Done => *self = Self::default(),
            Progress::Failed => {
                self.not_before = Some(now + Self::delay(self.failures));
                self.failures = self.failures.saturating_add(1);
            }
        }
    }
}

/// Storage faults that collection survives.
struct Faults {
    /// The first one: collection continues, but the exit is not clean.
    first: Option<String>,
    quality: QualityReporter,
}

impl Faults {
    /// Log and report a fault.
    fn report(&mut self, target: &Path, error: &io::Error, what: &str) {
        error!(path = %target.display(), %error, "{what}");
        self.first
            .get_or_insert_with(|| format!("{}: {error}", target.display()));
        self.quality.report(QualityEvent::StorageDegraded {
            at_ns: QualityEvent::now_ns(),
            target: target.display().to_string(),
            error: error.to_string(),
        });
    }
}

/// Outcome of trying to move from the sidecar to the daily file.
enum Switch {
    /// Still where it was: not on a sidecar, the daily file is busy or could
    /// not be taken, or the sidecar was resumed after a failed merge.
    Stay(Progress),
    /// This process holds the daily file and the sidecar is sealed: merged,
    /// left on disk if it cannot be, or queued if it could neither be merged
    /// nor resumed. The sink is closed.
    Switched(File),
}

/// One symbol's recording: one zstd file per UTC day.
struct RotatingFile {
    /// `<dir>/<encoded symbol>`.
    prefix: PathBuf,
    run_id: Arc<str>,
    /// The open file's day. The daily-file lock is probed for this day,
    /// never for a record's timestamp: a late pre-midnight record must not
    /// switch today's sidecar into yesterday's file.
    day: UtcDay,
    sink: Sink,
    batch: BytesMut,
    pending: Vec<PendingSidecar>,
    retry: Backoff,
    faults: Faults,
}

impl RotatingFile {
    fn open(
        prefix: PathBuf,
        run_id: Arc<str>,
        timestamp: Timestamp,
        quality: QualityReporter,
    ) -> io::Result<Self> {
        let day = UtcDay::containing(timestamp)?;
        let mut file = Self {
            prefix,
            run_id,
            day,
            sink: Sink::Closed,
            batch: BytesMut::with_capacity(ZSTD_BATCH_BYTES),
            pending: Vec::new(),
            retry: Backoff::default(),
            faults: Faults {
                first: None,
                quality,
            },
        };
        file.sink = file.open_sink(day.date)?;
        Ok(file)
    }

    /// The daily file if it can be taken, else a new run-local sidecar
    /// `<symbol>_<YYYYMMDD>_<run-id>.zst`. Only failing to create that is an
    /// error.
    fn open_sink(&mut self, date: Date) -> io::Result<Sink> {
        match lock_daily(&self.prefix, date) {
            DailyLock::Held(daily) => return Ok(Sink::Daily(Encoder::new(daily, ZSTD_LEVEL)?)),
            DailyLock::Busy => {}
            DailyLock::Failed(error) => {
                let progress = self.lock_failed(date, &error);
                self.retry.record(progress, Instant::now());
            }
        }
        let path = day_file(&self.prefix, date, format_args!("_{}", self.run_id));
        let file = File::options().create_new(true).write(true).open(&path)?;
        lock_exclusively(&file, &path)?;
        warn!(
            path = %path.display(),
            "daily file unavailable; writing a run-local sidecar until it is free"
        );
        Ok(Sink::Sidecar {
            encoder: Encoder::new(file, ZSTD_LEVEL)?,
            path,
        })
    }

    fn lock_failed(&mut self, date: Date, error: &io::Error) -> Progress {
        let path = day_file(&self.prefix, date, format_args!(""));
        self.faults
            .report(&path, error, "cannot take the daily file");
        Progress::Failed
    }

    /// Append `sidecar` to the held daily file, reporting a failure. `Done`
    /// once the sidecar needs no further attempt.
    fn merge(&mut self, daily: &mut File, sidecar: &Path) -> Progress {
        match merge_sidecar(daily, sidecar) {
            Merge::Done => Progress::Done,
            Merge::Rejected(error) => {
                self.faults.report(
                    sidecar,
                    &error,
                    "overlap sidecar ends mid-frame; leaving it on disk unmerged",
                );
                Progress::Done
            }
            Merge::Failed(error) => {
                self.faults
                    .report(sidecar, &error, "failed to append overlap sidecar");
                Progress::Failed
            }
        }
    }

    fn merge_due(&self) -> bool {
        (self.sink.is_sidecar() || !self.pending.is_empty()) && self.retry.is_due(Instant::now())
    }

    /// Seal the open sink, reporting any failure. A sidecar whose frame is
    /// finished comes back even if its trailer or fsync failed: it is whole,
    /// and still to be appended once its daily file is free. Anything else
    /// that fails is an error.
    fn finalize(&mut self) -> io::Result<Option<PathBuf>> {
        match mem::replace(&mut self.sink, Sink::Closed) {
            Sink::Torn(_) | Sink::Closed => {
                if !self.batch.is_empty() {
                    let path = day_file(&self.prefix, self.day.date, format_args!(""));
                    let lost = io::Error::other(format!(
                        "{} batched bytes have no open file",
                        self.batch.len()
                    ));
                    self.batch.clear();
                    self.faults.report(
                        &path,
                        &lost,
                        "batched records lost: no file to seal them in",
                    );
                }
                Ok(None)
            }
            Sink::Daily(encoder) => {
                let path = day_file(&self.prefix, self.day.date, format_args!(""));
                seal(encoder, &mut self.batch, &path).map_err(|error| {
                    let error = io::Error::from(error);
                    self.faults
                        .report(&path, &error, "failed to seal the daily file");
                    error
                })?;
                Ok(None)
            }
            Sink::Sidecar { encoder, path } => match seal(encoder, &mut self.batch, &path) {
                Ok(()) => Ok(Some(path)),
                Err(SealError::Unsynced(error)) => {
                    self.faults.report(
                        &path,
                        &error,
                        "sealed overlap sidecar may not be durable; it will still be merged",
                    );
                    Ok(Some(path))
                }
                Err(SealError::Unfinished(error)) => {
                    self.faults.report(
                        &path,
                        &error,
                        "failed to seal overlap sidecar; leaving it on disk unfinished",
                    );
                    Err(error)
                }
            },
        }
    }

    /// If the daily file can be taken, seal this process's sidecar and
    /// append it there.
    fn try_switch(&mut self) -> Switch {
        if !self.sink.is_sidecar() {
            return Switch::Stay(Progress::Idle);
        }
        let mut daily = match lock_daily(&self.prefix, self.day.date) {
            DailyLock::Held(daily) => daily,
            DailyLock::Busy => return Switch::Stay(Progress::Idle),
            DailyLock::Failed(error) => {
                return Switch::Stay(self.lock_failed(self.day.date, &error));
            }
        };
        let (encoder, path) = match mem::replace(&mut self.sink, Sink::Closed) {
            Sink::Sidecar { encoder, path } => (encoder, path),
            // Never appended: it ends mid-frame. The fault was reported when
            // it tore; the batched records it lacks go to the daily file.
            Sink::Torn(path) => {
                self.drop_persisted(&path);
                return Switch::Switched(daily);
            }
            Sink::Daily(_) | Sink::Closed => unreachable!("checked above"),
        };
        match seal(encoder, &mut self.batch, &path) {
            Ok(()) => {}
            Err(SealError::Unsynced(error)) => self.faults.report(
                &path,
                &error,
                "sealed handover sidecar may not be durable; merging it all the same",
            ),
            Err(SealError::Unfinished(error)) => {
                // An unfinished frame is never appended (see `lock_daily`);
                // the batched records it lacks go to the daily file instead.
                self.faults.report(
                    &path,
                    &error,
                    "failed to seal handover sidecar; leaving it on disk unfinished",
                );
                self.drop_persisted(&path);
                return Switch::Switched(daily);
            }
        }
        match self.merge(&mut daily, &path) {
            Progress::Failed => self.resume_or_queue(path, daily),
            Progress::Idle | Progress::Done => Switch::Switched(daily),
        }
    }

    /// Drop the batched records an abandoned sidecar already holds, so
    /// rewriting the batch elsewhere does not repeat them. When that cannot
    /// be told, all of them are rewritten: duplicates over gaps.
    fn drop_persisted(&mut self, sidecar: &Path) {
        if self.batch.is_empty() {
            return;
        }
        match persisted_prefix(sidecar, &self.batch) {
            Ok(Some(len)) => self.batch.advance(len),
            Ok(None) => self.faults.report(
                sidecar,
                &io::Error::other("its last whole record is not in the batch"),
                "cannot tell which batched records the abandoned sidecar holds; rewriting the whole batch",
            ),
            Err(error) => self.faults.report(
                sidecar,
                &error,
                "cannot decode the abandoned sidecar; rewriting the whole batch",
            ),
        }
    }

    /// After a failed merge, keep writing the sealed sidecar and merge it
    /// later. If it cannot be reopened, retry the append through the lock
    /// already held, and failing that queue the sidecar and write to the
    /// daily file: dropping the lock to take it again would only verify the
    /// file again.
    fn resume_or_queue(&mut self, path: PathBuf, mut daily: File) -> Switch {
        match resume_sidecar(&path) {
            Ok(encoder) => {
                self.sink = Sink::Sidecar { encoder, path };
                return Switch::Stay(Progress::Failed);
            }
            Err(error) => self.faults.report(
                &path,
                &error,
                "failed to resume overlap sidecar after a failed merge",
            ),
        }
        if self.merge(&mut daily, &path) == Progress::Failed {
            self.pending.push(PendingSidecar {
                date: self.day.date,
                path,
            });
        }
        Switch::Switched(daily)
    }

    /// Switch to the daily file once it can be taken, and keep writing there.
    fn switch_to_daily(&mut self) -> io::Result<Progress> {
        match self.try_switch() {
            Switch::Stay(progress) => Ok(progress),
            Switch::Switched(daily) => {
                self.sink = Sink::Daily(Encoder::new(daily, ZSTD_LEVEL)?);
                Ok(Progress::Done)
            }
        }
    }

    /// Merge the sidecar if the daily file can be taken, without starting a
    /// frame that nothing will be written to (rotation, close).
    fn release_sidecar(&mut self) -> io::Result<Progress> {
        match self.try_switch() {
            Switch::Stay(progress) => Ok(progress),
            Switch::Switched(daily) => {
                if !self.batch.is_empty() {
                    self.sink = Sink::Daily(Encoder::new(daily, ZSTD_LEVEL)?);
                }
                Ok(Progress::Done)
            }
        }
    }

    /// Append queued sidecars whose daily file is free. One of the day whose
    /// daily file this process writes waits for its release (rotation,
    /// close).
    fn merge_pending_sidecars(&mut self) -> Progress {
        let mut progress = Progress::Idle;
        for sidecar in mem::take(&mut self.pending) {
            let attempt = if sidecar.date == self.day.date && matches!(self.sink, Sink::Daily(_)) {
                Progress::Idle
            } else {
                match lock_daily(&self.prefix, sidecar.date) {
                    DailyLock::Held(mut daily) => self.merge(&mut daily, &sidecar.path),
                    DailyLock::Busy => Progress::Idle,
                    DailyLock::Failed(error) => self.lock_failed(sidecar.date, &error),
                }
            };
            if attempt != Progress::Done {
                self.pending.push(sidecar);
            }
            progress = progress.max(attempt);
        }
        progress
    }

    fn rotate(&mut self, timestamp: Timestamp) -> io::Result<()> {
        let progress = self.release_sidecar()?;
        self.retry.record(progress, Instant::now());
        // A failure is reported by `finalize`; the new day starts regardless.
        if let Ok(Some(path)) = self.finalize() {
            warn!(
                path = %path.display(),
                "UTC date changed before switch; keeping the overlap sidecar until the daily file is free"
            );
            self.pending.push(PendingSidecar {
                date: self.day.date,
                path,
            });
        }
        let day = UtcDay::containing(timestamp)?;
        let sink = self.open_sink(day.date)?;
        self.batch.clear();
        self.day = day;
        self.sink = sink;
        info!(path = %self.prefix.display(), "date changed, file rotated");
        Ok(())
    }

    fn flush_batch(&mut self) -> io::Result<()> {
        let encoder = match &mut self.sink {
            Sink::Daily(encoder) | Sink::Sidecar { encoder, .. } => encoder,
            Sink::Torn(_) | Sink::Closed => return Err(no_sink(&self.prefix)),
        };
        // Flushed per batch, so a frame that fails unfinished can have
        // persisted only part of the batch being sealed.
        let flushed = encoder
            .write_all(&self.batch)
            .and_then(|()| fault(Step::Flush))
            .and_then(|()| encoder.flush());
        if let Err(error) = flushed {
            self.tear(&error);
            return Err(error);
        }
        self.batch.clear();
        Ok(())
    }

    /// Drop the encoder after a failed write or flush, unfinished.
    ///
    /// zstd takes input before it reports an earlier write's failure, so the
    /// encoder may already hold part of the batch: sealed with the batch, it
    /// would record that part twice behind a valid trailer. Unfinished, the
    /// file ends mid-frame: the daily file is set aside by its day's next
    /// writer, readable up to the tear, and a torn sidecar is never appended.
    /// The sidecar's batch is kept for the switch to the daily file.
    fn tear(&mut self, error: &io::Error) {
        match mem::replace(&mut self.sink, Sink::Closed) {
            Sink::Sidecar { path, .. } => {
                self.faults.report(
                    &path,
                    error,
                    "failed to write the overlap sidecar; leaving it on disk unfinished",
                );
                self.sink = Sink::Torn(path);
            }
            Sink::Daily(_) | Sink::Torn(_) | Sink::Closed => {
                let path = day_file(&self.prefix, self.day.date, format_args!(""));
                let lost = self.batch.len();
                self.batch.clear();
                self.faults.report(
                    &path,
                    error,
                    &format!(
                        "failed to write the daily file; leaving it unfinished, {lost} batched bytes lost"
                    ),
                );
            }
        }
    }

    /// Only failing to put the record into the open sink is an error.
    fn write(&mut self, timestamp: Timestamp, tag: Tag, data: Bytes) -> io::Result<()> {
        if timestamp >= self.day.end {
            self.rotate(timestamp)?;
        }
        if self.merge_due() {
            let progress = self.switch_to_daily()?.max(self.merge_pending_sidecars());
            self.retry.record(progress, Instant::now());
        }
        if !matches!(self.sink, Sink::Daily(_) | Sink::Sidecar { .. }) {
            return Err(no_sink(&self.prefix));
        }

        debug_assert!(
            data.len() <= u32::MAX as usize,
            "payload too large for u32 length prefix"
        );
        // Never `unwrap` here: under `panic = "abort"` a panic skips every
        // `Drop` and leaves all the other symbols' files unterminated.
        let header = Header {
            recv_ns: timestamp.as_nanosecond() as i64,
            tag: tag as u8,
            len: data.len() as u32,
        };
        self.batch.put_slice(&header.encode());
        self.batch.put(data);

        if self.batch.len() >= ZSTD_BATCH_BYTES {
            self.flush_batch()?;
        }
        Ok(())
    }

    /// Merge what the switch allows, then seal. A sidecar whose daily file is
    /// still held stays on disk.
    fn close(&mut self, symbol: &str) -> anyhow::Result<()> {
        let released = self.release_sidecar();
        if let Err(error) = &released {
            error!(symbol, %error, "failed to switch to the daily file at close");
        }
        let sealed = self.finalize();
        // After sealing: a sidecar queued for the day just sealed can merge
        // now.
        self.merge_pending_sidecars();
        if matches!(sealed, Ok(Some(_))) || !self.pending.is_empty() {
            info!(
                symbol,
                "closing before switch; leaving overlap sidecar on disk"
            );
        }
        match (&sealed, &self.faults.first) {
            (Err(error), _) => error!(symbol, %error, "failed to close file"),
            (Ok(_), Some(fault)) => {
                error!(symbol, fault, "file closed, but storage degraded earlier");
            }
            (Ok(_), None) => info!(symbol, "file closed cleanly"),
        }
        released?;
        sealed?;
        match &self.faults.first {
            Some(fault) => bail!("{symbol}: storage degraded earlier: {fault}"),
            None => Ok(()),
        }
    }
}

impl Drop for RotatingFile {
    /// Best effort; `finalize` reports failures, `close` also returns them.
    fn drop(&mut self) {
        let _ = self.finalize();
    }
}

pub struct Writer {
    dir: PathBuf,
    run_id: Arc<str>,
    /// Keyed by raw symbol: the filename encoding is injective, so this is
    /// still one encoder per file on disk.
    files: HashMap<Symbol, RotatingFile>,
    quality: QualityReporter,
}

impl Writer {
    pub fn new(dir: impl Into<PathBuf>, run_id: &str, quality: QualityReporter) -> Self {
        Self {
            dir: dir.into(),
            run_id: run_id.into(),
            files: HashMap::new(),
            quality,
        }
    }

    pub fn write(&mut self, record: WriteRecord) -> anyhow::Result<()> {
        let WriteRecord {
            recv_time,
            symbol,
            tag,
            data,
        } = record;
        if let Some(file) = self.files.get_mut(&*symbol) {
            return Ok(file.write(recv_time, tag, data)?);
        }
        let prefix = self.dir.join(&*encode_symbol(&symbol));
        let mut file = RotatingFile::open(
            prefix,
            Arc::clone(&self.run_id),
            recv_time,
            self.quality.clone(),
        )?;
        file.write(recv_time, tag, data)?;
        self.files.insert(symbol, file);
        Ok(())
    }

    /// Seal every file, merging owned sidecars whose daily file is free.
    /// Returns the first failure.
    pub fn close(&mut self) -> anyhow::Result<()> {
        let mut result = Ok(());
        for (symbol, mut file) in self.files.drain() {
            result = result.and(file.close(&symbol));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, sync::mpsc::Receiver};

    use super::*;

    thread_local! {
        /// Steps armed to fail on this thread, each at its next arrival.
        static ARMED: RefCell<Vec<Step>> = const { RefCell::new(Vec::new()) };
        /// Every step this thread arrived at.
        static REACHED: RefCell<Vec<Step>> = const { RefCell::new(Vec::new()) };
    }

    pub(super) fn fault(step: Step) -> io::Result<()> {
        REACHED.with_borrow_mut(|reached| reached.push(step));
        let armed = ARMED.with_borrow_mut(|armed| {
            let index = armed.iter().position(|&armed| armed == step)?;
            Some(armed.remove(index))
        });
        match armed {
            Some(step) => Err(io::Error::other(format!("injected {step:?} failure"))),
            None => Ok(()),
        }
    }

    fn arm(steps: &[Step]) {
        ARMED.with_borrow_mut(|armed| armed.extend_from_slice(steps));
    }

    fn reached(step: Step) -> usize {
        REACHED.with_borrow(|reached| reached.iter().filter(|&&s| s == step).count())
    }

    fn degraded(events: &Receiver<QualityEvent>) -> usize {
        events
            .try_iter()
            .filter(|event| matches!(event, QualityEvent::StorageDegraded { .. }))
            .count()
    }

    fn test_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "sbe-{tag}-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn writer(dir: &Path, run_id: &str) -> Writer {
        Writer::new(dir, run_id, QualityReporter::disabled())
    }

    fn record(symbol: &str, recv_time: Timestamp, data: &'static [u8]) -> WriteRecord {
        WriteRecord {
            recv_time,
            symbol: Symbol::from(symbol),
            tag: Tag::Sbe,
            data: Bytes::from_static(data),
        }
    }

    fn sbe(recv_time: Timestamp, data: &'static [u8]) -> WriteRecord {
        record("btcusdt", recv_time, data)
    }

    /// 2027-01-15T08:00:00Z.
    fn day_one() -> Timestamp {
        Timestamp::from_nanosecond(1_800_000_000_000_000_000).unwrap()
    }

    /// 24 hours after [`day_one`].
    fn day_two() -> Timestamp {
        Timestamp::from_nanosecond(1_800_086_400_000_000_000).unwrap()
    }

    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }

    fn decoded(path: &Path) -> Vec<u8> {
        zstd::decode_all(std::fs::read(path).unwrap().as_slice()).unwrap()
    }

    /// The decoded bytes of `records`, framed as the writer frames them.
    fn decoded_records(records: &[(Timestamp, &[u8])]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for (recv_time, payload) in records {
            let header = Header {
                recv_ns: recv_time.as_nanosecond() as i64,
                tag: Tag::Sbe as u8,
                len: payload.len() as u32,
            };
            bytes.extend_from_slice(&header.encode());
            bytes.extend_from_slice(payload);
        }
        bytes
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    fn zstd_frame_count(bytes: &[u8]) -> usize {
        const MAGIC: &[u8; 4] = &[0x28, 0xB5, 0x2F, 0xFD];
        bytes.windows(4).filter(|window| *window == MAGIC).count()
    }

    #[test]
    fn real_exchange_symbols_are_not_rewritten() {
        for symbol in ["btcusdt", "btcusd_perp", "btc-usdt", "@1"] {
            assert!(
                matches!(encode_symbol(symbol), Cow::Borrowed(_)),
                "{symbol} should pass through unchanged"
            );
        }
    }

    #[test]
    fn path_separators_in_symbols_are_escaped() {
        assert_eq!(encode_symbol("purr/usdc"), "purr%2Fusdc");
        assert_eq!(encode_symbol("../../etc/passwd"), "..%2F..%2Fetc%2Fpasswd");
    }

    #[test]
    fn encoding_is_injective() {
        let symbols = [
            "purr/usdc",
            "purr_usdc",
            "purr%2Fusdc",
            "purr%usdc",
            "PURR/USDC",
            "purr usdc",
            "",
        ];
        let mut encoded: Vec<String> = symbols
            .iter()
            .map(|symbol| encode_symbol(symbol).into_owned())
            .collect();
        let total = encoded.len();
        encoded.sort();
        encoded.dedup();
        assert_eq!(encoded.len(), total, "collision: {encoded:?}");
    }

    #[test]
    fn colliding_symbols_get_separate_files() {
        let dir = test_dir("collision");
        let mut writer = writer(&dir, "test-run");
        for symbol in ["foo/bar", "foo_bar"] {
            writer
                .write(record(symbol, Timestamp::now(), b"\x00"))
                .unwrap();
        }
        writer.close().unwrap();

        let written = names(&dir);
        assert_eq!(written.len(), 2, "{written:?}");
        for name in &written {
            decoded(&dir.join(name));
            let date = name
                .trim_end_matches(".zst")
                .rsplit_once('_')
                .map(|(_, date)| date)
                .unwrap();
            assert!(
                date.len() == 8 && date.bytes().all(|b| b.is_ascii_digit()),
                "expected daily symbol_YYYYMMDD.zst, got {name}"
            );
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn same_day_writes_share_one_file() {
        let dir = test_dir("daily");
        let mut writer = writer(&dir, "daily-run");
        let later_same_day = day_one()
            .checked_add(jiff::SignedDuration::from_hours(3))
            .unwrap();
        for recv_time in [day_one(), later_same_day] {
            writer.write(sbe(recv_time, b"frame")).unwrap();
        }
        writer.close().unwrap();

        assert_eq!(names(&dir), ["btcusdt_20270115.zst"]);
        decoded(&dir.join("btcusdt_20270115.zst"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn date_boundary_opens_a_new_daily_file() {
        let dir = test_dir("day-boundary");
        let mut writer = writer(&dir, "boundary-run");
        for recv_time in [day_one(), day_two()] {
            writer.write(sbe(recv_time, b"frame")).unwrap();
        }
        writer.close().unwrap();

        let names = names(&dir);
        assert_eq!(names, ["btcusdt_20270115.zst", "btcusdt_20270116.zst"]);
        for name in &names {
            decoded(&dir.join(name));
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn busy_daily_file_falls_back_to_run_sidecar() {
        let dir = test_dir("sidecar");
        let now = Timestamp::now();
        let mut incumbent = writer(&dir, "incumbent");
        incumbent.write(sbe(now, b"incumbent")).unwrap();

        let mut challenger = writer(&dir, "challenger");
        challenger.write(sbe(now, b"sidecar-overlap")).unwrap();
        assert!(
            challenger.files["btcusdt"].sink.is_sidecar(),
            "challenger must use a sidecar while the daily file is locked"
        );

        incumbent.close().unwrap();
        challenger.write(sbe(now, b"after-reclaim")).unwrap();
        assert!(
            matches!(challenger.files["btcusdt"].sink, Sink::Daily(_)),
            "after the incumbent exits, writes must reclaim the canonical daily file"
        );
        challenger.close().unwrap();

        let names = names(&dir);
        assert_eq!(
            names.len(),
            1,
            "sidecar must be merged and removed: {names:?}"
        );
        assert!(names[0].starts_with("btcusdt_") && names[0].ends_with(".zst"));
        let merged = decoded(&dir.join(&names[0]));
        for payload in [&b"incumbent"[..], b"sidecar-overlap", b"after-reclaim"] {
            assert!(
                contains(&merged, payload),
                "merged daily file missing {payload:?}"
            );
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn close_merges_sidecar_without_another_write() {
        let dir = test_dir("close-merge");
        let now = Timestamp::now();
        let mut incumbent = writer(&dir, "incumbent");
        incumbent.write(sbe(now, b"incumbent")).unwrap();
        let mut challenger = writer(&dir, "challenger");
        challenger.write(sbe(now, b"sidecar-overlap")).unwrap();
        incumbent.close().unwrap();
        assert!(
            names(&dir)
                .iter()
                .any(|name| name.contains("_challenger.zst")),
            "incumbent close must not steal a live overlap sidecar"
        );
        challenger.close().unwrap();

        let names = names(&dir);
        assert_eq!(
            names.len(),
            1,
            "close must merge and remove the sidecar: {names:?}"
        );
        let merged = std::fs::read(dir.join(&names[0])).unwrap();
        assert_eq!(
            zstd_frame_count(&merged),
            2,
            "close should not append an empty extra zstd frame"
        );
        assert!(
            ends_with_own_trailer(&merged),
            "the merge must leave the daily file verifiable from its tail"
        );
        let merged = zstd::decode_all(merged.as_slice()).unwrap();
        for payload in [&b"incumbent"[..], b"sidecar-overlap"] {
            assert!(
                contains(&merged, payload),
                "merged daily file missing {payload:?}"
            );
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn midnight_reclaim_merges_sidecar_after_incumbent_exits() {
        let dir = test_dir("midnight-merge");
        let mut incumbent = writer(&dir, "incumbent");
        incumbent.write(sbe(day_one(), b"incumbent")).unwrap();
        let mut challenger = writer(&dir, "challenger");
        challenger
            .write(sbe(day_one(), b"sidecar-overlap"))
            .unwrap();
        incumbent.close().unwrap();
        challenger.write(sbe(day_two(), b"next-day")).unwrap();
        challenger.close().unwrap();

        assert_eq!(
            names(&dir),
            ["btcusdt_20270115.zst", "btcusdt_20270116.zst"],
            "midnight rotation must merge yesterday's sidecar"
        );
        let day_one_bytes = std::fs::read(dir.join("btcusdt_20270115.zst")).unwrap();
        assert_eq!(
            zstd_frame_count(&day_one_bytes),
            2,
            "yesterday should be incumbent frame + sidecar frame, not an extra empty frame"
        );
        let day_one = zstd::decode_all(day_one_bytes.as_slice()).unwrap();
        for payload in [&b"incumbent"[..], b"sidecar-overlap"] {
            assert!(contains(&day_one, payload), "yesterday missing {payload:?}");
        }
        assert!(contains(
            &decoded(&dir.join("btcusdt_20270116.zst")),
            b"next-day"
        ));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn midnight_keeps_sidecar_while_incumbent_lock_is_held() {
        let dir = test_dir("midnight-lock");
        let mut incumbent = writer(&dir, "incumbent");
        incumbent.write(sbe(day_one(), b"incumbent")).unwrap();
        let mut challenger = writer(&dir, "challenger");
        challenger
            .write(sbe(day_one(), b"sidecar-overlap"))
            .unwrap();
        challenger.write(sbe(day_two(), b"next-day")).unwrap();
        assert!(
            !challenger.files["btcusdt"].pending.is_empty(),
            "overlap sidecar must remain until the incumbent releases yesterday's lock"
        );
        assert!(names(&dir).contains(&"btcusdt_20270115_challenger.zst".to_owned()));

        incumbent.close().unwrap();
        challenger.close().unwrap();

        assert_eq!(
            names(&dir),
            ["btcusdt_20270115.zst", "btcusdt_20270116.zst"],
            "close must merge the leftover midnight sidecar"
        );
        let day_one = decoded(&dir.join("btcusdt_20270115.zst"));
        for payload in [&b"incumbent"[..], b"sidecar-overlap"] {
            assert!(contains(&day_one, payload), "yesterday missing {payload:?}");
        }
        assert!(contains(
            &decoded(&dir.join("btcusdt_20270116.zst")),
            b"next-day"
        ));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn duplicate_run_id_cannot_share_a_sidecar() {
        let dir = test_dir("sidecar-exclusive");
        let now = Timestamp::now();
        let mut incumbent = writer(&dir, "incumbent");
        incumbent.write(sbe(now, b"incumbent")).unwrap();
        let mut first = writer(&dir, "same-run");
        first.write(sbe(now, b"first")).unwrap();
        let mut second = writer(&dir, "same-run");
        let error = second.write(sbe(now, b"second")).unwrap_err();
        assert!(
            error
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.kind() == io::ErrorKind::AlreadyExists),
            "second writer with the same run id must not share the sidecar: {error}"
        );
        first.close().ok();
        incumbent.close().ok();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn frame_walk_accepts_whole_frames_and_rejects_a_truncated_tail() {
        let dir = test_dir("frame-walk");
        let path = dir.join("walk.zst");
        let check = |bytes: &[u8]| {
            std::fs::write(&path, bytes).unwrap();
            ends_on_frame_boundary(&File::open(&path).unwrap(), bytes.len() as u64).unwrap()
        };
        let mut streamed = ZstdEncoder::new(Vec::new(), 1).unwrap();
        streamed.write_all(b"streamed, no content size").unwrap();
        let streamed = streamed.finish().unwrap();
        let whole = [
            zstd::encode_all(&b"single segment"[..], 1).unwrap(),
            zstd::encode_all(&vec![0u8; 300_000][..], 1).unwrap(),
            streamed,
        ]
        .concat();

        assert!(check(b""));
        assert!(check(&whole));
        assert!(!check(&whole[..whole.len() - 1]));
        let mut garbage = whole.clone();
        garbage.extend_from_slice(b"not zstd");
        assert!(!check(&garbage));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    fn ends_with_own_trailer(bytes: &[u8]) -> bool {
        bytes
            .len()
            .checked_sub(TRAILER_LEN)
            .is_some_and(|start| bytes[start..] == trailer(start as u64))
    }

    #[test]
    fn trailer_bytes_are_pinned() {
        let expected = [
            0x5C, 0x2A, 0x4D, 0x18, // skippable-frame magic
            16, 0, 0, 0, // content size
            b'C', b'O', b'L', b'L', b'E', b'C', b'T', b'R', //
            8, 7, 6, 5, 4, 3, 2, 1, // offset
        ];
        assert_eq!(trailer(0x0102_0304_0506_0708), expected);
    }

    #[test]
    fn a_trailer_that_points_at_itself_is_trusted_without_a_walk() {
        let dir = test_dir("trailer");
        let path = dir.join("trailer.zst");
        let check = |bytes: &[u8]| {
            std::fs::write(&path, bytes).unwrap();
            ends_cleanly(&File::open(&path).unwrap()).unwrap()
        };
        let frame = zstd::encode_all(&b"records"[..], 1).unwrap();
        // Not walkable: the walk would stop at the first byte.
        let garbage = b"not zstd at all".as_slice();

        assert!(check(&[garbage, &trailer(garbage.len() as u64)].concat()));
        // A trailer pointing elsewhere is just a skippable frame to the walk.
        assert!(!check(&[garbage, &trailer(0)].concat()));
        assert!(check(&[&frame[..], &trailer(0)].concat()));
        // No trailer: the walk decides.
        assert!(check(&frame));
        assert!(!check(&frame[..frame.len() - 1]));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn unterminated_daily_file_is_set_aside_not_appended_to() {
        let dir = test_dir("unterminated");
        let daily = dir.join("btcusdt_20270115.zst");
        let crashed = zstd::encode_all(&b"before-crash"[..], 1).unwrap();
        let crashed = &crashed[..crashed.len() - 2];
        std::fs::write(&daily, crashed).unwrap();

        let mut writer = writer(&dir, "restart");
        writer.write(sbe(day_one(), b"after-restart")).unwrap();
        writer.close().unwrap();

        assert!(contains(&decoded(&daily), b"after-restart"));
        let names = names(&dir);
        let salvage = names
            .iter()
            .find(|name| name.starts_with("btcusdt_20270115_unterminated-"))
            .unwrap_or_else(|| panic!("damaged file must be kept aside: {names:?}"));
        assert_eq!(std::fs::read(dir.join(salvage)).unwrap(), crashed);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn late_record_does_not_switch_into_yesterdays_file() {
        let dir = test_dir("late-record");
        let mut incumbent = writer(&dir, "incumbent");
        incumbent
            .write(sbe(day_one(), b"incumbent-day-one"))
            .unwrap();
        incumbent
            .write(sbe(day_two(), b"incumbent-day-two"))
            .unwrap();
        let mut challenger = writer(&dir, "challenger");
        challenger
            .write(sbe(day_two(), b"sidecar-day-two"))
            .unwrap();
        incumbent.close().unwrap();
        // Stamped before midnight, delivered after the incumbent let go.
        challenger.write(sbe(day_one(), b"late")).unwrap();
        challenger.close().unwrap();

        let day_one = decoded(&dir.join("btcusdt_20270115.zst"));
        let day_two = decoded(&dir.join("btcusdt_20270116.zst"));
        assert!(!contains(&day_one, b"sidecar-day-two"));
        assert!(!contains(&day_one, b"late"));
        for payload in [&b"incumbent-day-two"[..], b"sidecar-day-two", b"late"] {
            assert!(contains(&day_two, payload), "day two missing {payload:?}");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn records_are_framed_as_recv_ns_tag_len_payload() {
        let dir = test_dir("framing");
        let mut writer = writer(&dir, "framing-run");
        writer.write(sbe(day_one(), b"frame")).unwrap();
        writer
            .write(WriteRecord {
                tag: Tag::Rest,
                ..record("btcusdt", day_one(), b"{}")
            })
            .unwrap();
        writer.close().unwrap();

        let ns = 1_800_000_000_000_000_000i64.to_le_bytes();
        let expected = [
            &ns[..],
            b"S",
            &5u32.to_le_bytes(),
            b"frame",
            &ns,
            b"R",
            &2u32.to_le_bytes(),
            b"{}",
        ]
        .concat();
        let bytes = std::fs::read(dir.join("btcusdt_20270115.zst")).unwrap();
        assert!(ends_with_own_trailer(&bytes));
        assert_eq!(zstd::decode_all(bytes.as_slice()).unwrap(), expected);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A daily file that cannot be verified is set aside like a torn one.
    #[test]
    fn unverifiable_daily_file_is_set_aside() {
        let dir = test_dir("verify-error");
        let daily = dir.join("btcusdt_20270115.zst");
        let mut first = writer(&dir, "first");
        first.write(sbe(day_one(), b"before")).unwrap();
        first.close().unwrap();
        let before = std::fs::read(&daily).unwrap();

        arm(&[Step::Verify]);
        let mut second = writer(&dir, "second");
        second.write(sbe(day_one(), b"after")).unwrap();
        second.close().unwrap();

        assert_eq!(decoded(&daily), decoded_records(&[(day_one(), b"after")]));
        let names = names(&dir);
        let salvage = names
            .iter()
            .find(|name| name.starts_with("btcusdt_20270115_unterminated-"))
            .unwrap_or_else(|| panic!("unverified file must be kept aside: {names:?}"));
        assert_eq!(std::fs::read(dir.join(salvage)).unwrap(), before);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn torn_daily_file_that_cannot_be_set_aside_falls_back_to_a_sidecar() {
        let dir = test_dir("set-aside-error");
        let daily = dir.join("btcusdt_20270115.zst");
        let crashed = zstd::encode_all(&b"before-crash"[..], 1).unwrap();
        let crashed = &crashed[..crashed.len() - 2];
        std::fs::write(&daily, crashed).unwrap();

        let (quality, events) = QualityReporter::test_channel();
        let mut writer = Writer::new(&dir, "restart", quality);
        arm(&[Step::SetAside]);
        writer.write(sbe(day_one(), b"first")).unwrap();
        assert!(writer.files["btcusdt"].sink.is_sidecar());
        assert_eq!(degraded(&events), 1);
        writer.write(sbe(day_one(), b"second")).unwrap();
        assert_eq!(reached(Step::SetAside), 1, "the retry must back off");

        // Close tries once more, and the file can be set aside now.
        assert!(
            writer.close().is_err(),
            "a degraded run must not exit clean"
        );
        assert_eq!(
            decoded(&daily),
            decoded_records(&[(day_one(), b"first"), (day_one(), b"second")])
        );
        let names = names(&dir);
        assert_eq!(names.len(), 2, "sidecar must be merged: {names:?}");
        assert!(names[1].starts_with("btcusdt_20270115_unterminated-"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn daily_lock_error_falls_back_to_a_sidecar() {
        let dir = test_dir("lock-error");
        arm(&[Step::Lock]);
        let mut writer = writer(&dir, "run");
        writer.write(sbe(day_one(), b"frame")).unwrap();
        assert!(writer.files["btcusdt"].sink.is_sidecar());
        assert!(
            writer.close().is_err(),
            "a degraded run must not exit clean"
        );
        assert_eq!(names(&dir), ["btcusdt_20270115.zst"]);
        assert!(contains(
            &decoded(&dir.join("btcusdt_20270115.zst")),
            b"frame"
        ));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A challenger on a sidecar whose incumbent has just released the daily
    /// file.
    fn released_overlap(dir: &Path) -> Writer {
        let mut incumbent = writer(dir, "incumbent");
        incumbent.write(sbe(day_one(), b"incumbent")).unwrap();
        let mut challenger = writer(dir, "challenger");
        challenger.write(sbe(day_one(), b"overlap")).unwrap();
        incumbent.close().unwrap();
        challenger
    }

    fn assert_merged(dir: &Path, payloads: &[&[u8]]) {
        assert_eq!(names(dir), ["btcusdt_20270115.zst"]);
        let daily = decoded(&dir.join("btcusdt_20270115.zst"));
        for payload in payloads {
            assert!(contains(&daily, payload), "daily file missing {payload:?}");
        }
    }

    #[test]
    fn failed_merge_resumes_the_sidecar_and_backs_off() {
        let dir = test_dir("merge-error");
        let mut challenger = released_overlap(&dir);
        arm(&[Step::Append]);
        let locks = reached(Step::Lock);
        challenger.write(sbe(day_one(), b"resumed")).unwrap();
        challenger.write(sbe(day_one(), b"backing-off")).unwrap();
        assert_eq!(reached(Step::Lock) - locks, 1, "one attempt, then back off");
        assert!(challenger.files["btcusdt"].sink.is_sidecar());

        assert!(challenger.close().is_err());
        assert_merged(
            &dir,
            &[b"incumbent", b"overlap", b"resumed", b"backing-off"],
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn unresumable_sidecar_is_appended_through_the_held_lock() {
        let dir = test_dir("merge-retry");
        let mut challenger = released_overlap(&dir);
        arm(&[Step::Append, Step::Resume]);
        let locks = reached(Step::Lock);
        challenger.write(sbe(day_one(), b"daily")).unwrap();
        assert_eq!(
            reached(Step::Lock) - locks,
            1,
            "the held lock is not taken again"
        );
        let file = &challenger.files["btcusdt"];
        assert!(matches!(file.sink, Sink::Daily(_)) && file.pending.is_empty());

        assert!(challenger.close().is_err());
        assert_merged(&dir, &[b"incumbent", b"overlap", b"daily"]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn sidecar_that_cannot_be_merged_or_resumed_waits_for_the_held_daily_file() {
        let dir = test_dir("merge-queue");
        let mut challenger = released_overlap(&dir);
        arm(&[Step::Append, Step::Resume, Step::Append]);
        let locks = reached(Step::Lock);
        challenger.write(sbe(day_one(), b"daily")).unwrap();
        challenger.write(sbe(day_one(), b"still-daily")).unwrap();
        assert_eq!(
            reached(Step::Lock) - locks,
            1,
            "the held lock is not taken again"
        );
        let file = &challenger.files["btcusdt"];
        assert!(matches!(file.sink, Sink::Daily(_)));
        assert_eq!(file.pending.len(), 1);

        // Sealing the daily file at close lets the queued sidecar in.
        assert!(challenger.close().is_err());
        assert_merged(&dir, &[b"incumbent", b"overlap", b"daily", b"still-daily"]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn seal_tells_an_unfinished_frame_from_an_unsynced_one() {
        let dir = test_dir("seal");
        let seal_with = |name: &str, step: Step| {
            let path = dir.join(name);
            let encoder = Encoder::new(File::create(&path).unwrap(), ZSTD_LEVEL).unwrap();
            let mut batch = BytesMut::from(&b"record"[..]);
            arm(&[step]);
            let sealed = seal(encoder, &mut batch, &path);
            (sealed, batch, std::fs::read(&path).unwrap())
        };

        let (sealed, batch, _) = seal_with("finish.zst", Step::Finish);
        assert!(matches!(sealed, Err(SealError::Unfinished(_))));
        assert_eq!(&batch[..], b"record", "an unfinished frame keeps its batch");

        let (sealed, batch, bytes) = seal_with("trailer.zst", Step::Trailer);
        assert!(matches!(sealed, Err(SealError::Unsynced(_))));
        assert!(batch.is_empty());
        assert_eq!(zstd::decode_all(bytes.as_slice()).unwrap(), b"record");
        assert!(!ends_with_own_trailer(&bytes));

        let (sealed, batch, bytes) = seal_with("sync.zst", Step::Sync);
        assert!(matches!(sealed, Err(SealError::Unsynced(_))));
        assert!(batch.is_empty());
        assert_eq!(zstd::decode_all(bytes.as_slice()).unwrap(), b"record");
        assert!(ends_with_own_trailer(&bytes));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn rotation_queues_a_whole_sidecar_whose_fsync_failed() {
        let dir = test_dir("rotate-unsynced");
        let mut incumbent = writer(&dir, "incumbent");
        incumbent.write(sbe(day_one(), b"incumbent")).unwrap();
        let mut challenger = writer(&dir, "challenger");
        challenger.write(sbe(day_one(), b"overlap")).unwrap();
        arm(&[Step::Sync]);
        challenger.write(sbe(day_two(), b"next-day")).unwrap();
        assert_eq!(challenger.files["btcusdt"].pending.len(), 1);

        incumbent.close().unwrap();
        assert!(
            challenger.close().is_err(),
            "a degraded run must not exit clean"
        );
        assert_eq!(
            names(&dir),
            ["btcusdt_20270115.zst", "btcusdt_20270116.zst"]
        );
        let day_one = decoded(&dir.join("btcusdt_20270115.zst"));
        for payload in [&b"incumbent"[..], b"overlap"] {
            assert!(contains(&day_one, payload), "yesterday missing {payload:?}");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn rotation_leaves_an_unfinished_sidecar_on_disk() {
        let dir = test_dir("rotate-unfinished");
        let mut incumbent = writer(&dir, "incumbent");
        incumbent.write(sbe(day_one(), b"incumbent")).unwrap();
        let mut challenger = writer(&dir, "challenger");
        challenger.write(sbe(day_one(), b"overlap")).unwrap();
        arm(&[Step::Finish]);
        challenger.write(sbe(day_two(), b"next-day")).unwrap();
        assert!(challenger.files["btcusdt"].pending.is_empty());

        incumbent.close().unwrap();
        assert!(challenger.close().is_err());
        assert!(names(&dir).contains(&"btcusdt_20270115_challenger.zst".to_owned()));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn switch_merges_a_whole_sidecar_whose_fsync_failed() {
        let dir = test_dir("switch-unsynced");
        let mut challenger = released_overlap(&dir);
        arm(&[Step::Sync]);
        challenger.write(sbe(day_one(), b"after")).unwrap();
        assert!(matches!(challenger.files["btcusdt"].sink, Sink::Daily(_)));
        assert!(challenger.close().is_err());
        assert_merged(&dir, &[b"incumbent", b"overlap", b"after"]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn switch_rewrites_the_batch_of_an_unfinished_sidecar() {
        let dir = test_dir("switch-unfinished");
        let mut challenger = released_overlap(&dir);
        arm(&[Step::Finish]);
        challenger.write(sbe(day_one(), b"after")).unwrap();
        assert!(matches!(challenger.files["btcusdt"].sink, Sink::Daily(_)));
        assert!(challenger.close().is_err());

        assert_eq!(
            names(&dir),
            ["btcusdt_20270115.zst", "btcusdt_20270115_challenger.zst"]
        );
        assert_eq!(
            decoded(&dir.join("btcusdt_20270115.zst")),
            decoded_records(&[
                (day_one(), b"incumbent"),
                (day_one(), b"overlap"),
                (day_one(), b"after"),
            ])
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A sidecar left as a failed `finish` leaves it: `earlier` flushed whole,
    /// then `persisted` of the batch being sealed, then nothing.
    fn abandoned_sidecar(path: &Path, earlier: &[u8], persisted: &[u8]) {
        let mut encoder = Encoder::new(File::create(path).unwrap(), ZSTD_LEVEL).unwrap();
        for bytes in [earlier, persisted] {
            encoder.write_all(bytes).unwrap();
            encoder.flush().unwrap();
        }
    }

    #[test]
    fn only_batched_records_an_abandoned_sidecar_lacks_are_kept() {
        let dir = test_dir("persisted");
        let sidecar = dir.join("sidecar.zst");
        let (quality, events) = QualityReporter::test_channel();
        let mut file =
            RotatingFile::open(dir.join("btcusdt"), Arc::from("run"), day_one(), quality).unwrap();
        let earlier = decoded_records(&[(day_one(), b"earlier")]);
        let batch = decoded_records(&[(day_one(), b"a"), (day_one(), b"bb"), (day_one(), b"ccc")]);
        let first = decoded_records(&[(day_one(), b"a")]).len();
        let mut kept = |persisted: Option<&[u8]>| {
            match persisted {
                Some(persisted) => abandoned_sidecar(&sidecar, &earlier, persisted),
                None => std::fs::write(&sidecar, b"not zstd at all").unwrap(),
            }
            file.batch = BytesMut::from(&batch[..]);
            file.drop_persisted(&sidecar);
            mem::take(&mut file.batch).to_vec()
        };

        // Cut after the first record, or inside the second.
        assert_eq!(kept(Some(&batch[..first])), batch[first..]);
        assert_eq!(kept(Some(&batch[..first + 5])), batch[first..]);
        assert_eq!(kept(Some(&batch)), b"");
        assert_eq!(degraded(&events), 0);
        // Nothing of the batch persisted, or the sidecar cannot be decoded.
        assert_eq!(kept(Some(b"")), batch);
        assert_eq!(kept(None), batch);
        assert_eq!(degraded(&events), 2);
        drop(file);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Flushed batches stay in the abandoned sidecar and only the batch being
    /// sealed is rewritten: every record is on disk exactly once.
    #[test]
    fn unfinished_sidecar_and_daily_file_hold_each_record_once() {
        static BIG: [u8; 1024] = [7; 1024];
        let dir = test_dir("switch-flushed");
        let mut incumbent = writer(&dir, "incumbent");
        incumbent.write(sbe(day_one(), b"incumbent")).unwrap();
        let mut challenger = writer(&dir, "challenger");
        let flushed = ZSTD_BATCH_BYTES / (HEADER_LEN + BIG.len()) + 1;
        for _ in 0..flushed {
            challenger.write(sbe(day_one(), &BIG)).unwrap();
        }
        challenger.write(sbe(day_one(), b"batched")).unwrap();
        incumbent.close().unwrap();
        arm(&[Step::Finish]);
        challenger.write(sbe(day_one(), b"after")).unwrap();
        assert!(challenger.close().is_err());

        let mut sidecar = Vec::new();
        let truncated =
            ZstdDecoder::new(File::open(dir.join("btcusdt_20270115_challenger.zst")).unwrap())
                .unwrap()
                .read_to_end(&mut sidecar)
                .unwrap_err();
        assert_eq!(truncated.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(
            sidecar,
            decoded_records(&vec![(day_one(), &BIG[..]); flushed])
        );
        assert_eq!(
            decoded(&dir.join("btcusdt_20270115.zst")),
            decoded_records(&[
                (day_one(), b"incumbent"),
                (day_one(), b"batched"),
                (day_one(), b"after"),
            ])
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// `count` distinct 1 KiB records numbered from `first`, so a little over
    /// a hundred fill a batch.
    fn numbered(first: u64, count: u64) -> impl Iterator<Item = WriteRecord> {
        (first..first + count).map(|n| {
            let mut data = vec![7; 1024];
            data[..8].copy_from_slice(&n.to_le_bytes());
            WriteRecord {
                recv_time: day_one(),
                symbol: Symbol::from("btcusdt"),
                tag: Tag::Sbe,
                data: Bytes::from(data),
            }
        })
    }

    /// The numbers of the whole numbered records in decoded bytes.
    fn numbers(mut bytes: &[u8]) -> Vec<u64> {
        let mut numbers = Vec::new();
        while let Some(header) = bytes.first_chunk() {
            let end = HEADER_LEN + Header::parse(header).len as usize;
            let Some(record) = bytes.get(..end) else {
                break;
            };
            let payload = &record[HEADER_LEN..];
            if let Some(number) = payload.first_chunk()
                && payload.len() == 1024
            {
                numbers.push(u64::from_le_bytes(*number));
            }
            bytes = &bytes[end..];
        }
        numbers
    }

    /// What a file decodes to up to where it stops, whole or torn.
    fn readable(path: &Path) -> Vec<u8> {
        let mut decoder = ZstdDecoder::new(File::open(path).unwrap()).unwrap();
        let (mut bytes, mut chunk) = (Vec::new(), vec![0; 64 * 1024]);
        while let Ok(read @ 1..) = decoder.read(&mut chunk) {
            bytes.extend_from_slice(&chunk[..read]);
        }
        bytes
    }

    /// Writes numbered records until one fails; the number of the one that
    /// failed, which was batched before its flush did.
    fn write_until_failure(writer: &mut Writer, first: u64, count: u64) -> u64 {
        numbered(first, count)
            .position(|record| writer.write(record).is_err())
            .map(|offset| first + offset as u64)
            .expect("a flush must fail")
    }

    /// zstd takes input before it reports a failed write, so an encoder whose
    /// flush failed may hold the batch already: sealed with it at close, it
    /// would record the batch twice behind a valid trailer.
    #[test]
    fn a_failed_flush_tears_the_frame_instead_of_writing_the_batch_twice() {
        let dir = test_dir("failed-flush");
        let mut first = writer(&dir, "first");
        for record in numbered(0, 130) {
            first.write(record).unwrap();
        }
        arm(&[Step::Flush]);
        write_until_failure(&mut first, 130, 200);
        assert!(first.close().is_err(), "a torn frame is not a clean close");

        let daily = dir.join("btcusdt_20270115.zst");
        assert!(!ends_cleanly(&File::open(&daily).unwrap()).unwrap());
        let kept = numbers(&readable(&daily));
        assert!(!kept.is_empty(), "the first flushed batch stays readable");
        assert_eq!(kept, (0..kept.len() as u64).collect::<Vec<_>>());

        // The next writer of the day sets the torn file aside.
        let mut next = writer(&dir, "next");
        next.write(sbe(day_one(), b"after")).unwrap();
        next.close().unwrap();
        assert_eq!(decoded(&daily), decoded_records(&[(day_one(), b"after")]));
        assert!(
            names(&dir)
                .iter()
                .any(|name| name.starts_with("btcusdt_20270115_unterminated-"))
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The same for a sidecar: it stays on disk torn, and the switch writes
    /// the records it lacks to the daily file, once.
    #[test]
    fn a_torn_sidecar_is_left_and_its_batch_switched() {
        let dir = test_dir("torn-sidecar");
        let mut incumbent = writer(&dir, "incumbent");
        incumbent.write(sbe(day_one(), b"incumbent")).unwrap();
        let mut challenger = writer(&dir, "challenger");
        for record in numbered(0, 130) {
            challenger.write(record).unwrap();
        }
        arm(&[Step::Flush]);
        let last = write_until_failure(&mut challenger, 130, 200);
        incumbent.close().unwrap();
        assert!(challenger.close().is_err());

        let sidecar = dir.join("btcusdt_20270115_challenger.zst");
        assert!(
            !ends_cleanly(&File::open(&sidecar).unwrap()).unwrap(),
            "a torn sidecar is never appended"
        );
        let mut recorded = numbers(&readable(&sidecar));
        recorded.extend(numbers(&decoded(&dir.join("btcusdt_20270115.zst"))));
        recorded.sort_unstable();
        assert_eq!(recorded, (0..=last).collect::<Vec<_>>(), "each record once");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn only_a_verified_sidecar_is_merged() {
        let dir = test_dir("merge-verify");
        let daily_path = dir.join("daily.zst");
        let sidecar = dir.join("sidecar.zst");
        std::fs::write(&daily_path, zstd::encode_all(&b"daily"[..], 1).unwrap()).unwrap();
        let before = std::fs::read(&daily_path).unwrap();
        let mut daily = File::options()
            .read(true)
            .append(true)
            .open(&daily_path)
            .unwrap();
        let whole = zstd::encode_all(&b"sidecar"[..], 1).unwrap();

        std::fs::write(&sidecar, &whole[..whole.len() - 1]).unwrap();
        assert!(matches!(
            merge_sidecar(&mut daily, &sidecar),
            Merge::Rejected(_)
        ));
        std::fs::write(&sidecar, &whole).unwrap();
        arm(&[Step::Verify]);
        assert!(matches!(
            merge_sidecar(&mut daily, &sidecar),
            Merge::Failed(_)
        ));
        assert_eq!(std::fs::read(&daily_path).unwrap(), before);
        assert!(sidecar.exists());

        assert!(matches!(merge_sidecar(&mut daily, &sidecar), Merge::Done));
        assert!(!sidecar.exists());
        let merged = std::fs::read(&daily_path).unwrap();
        assert!(ends_with_own_trailer(&merged));
        assert_eq!(
            zstd::decode_all(merged.as_slice()).unwrap(),
            b"dailysidecar"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn retry_backoff_doubles_per_failure_up_to_ten_minutes() {
        let now = Instant::now();
        let mut backoff = Backoff::default();
        assert!(backoff.is_due(now));
        let delays: Vec<_> = (0..9)
            .map(|_| {
                backoff.record(Progress::Failed, now);
                backoff.not_before.unwrap() - now
            })
            .collect();
        let expected = [5, 10, 20, 40, 80, 160, 320, 600, 600].map(Duration::from_secs);
        assert_eq!(delays, expected);

        backoff.record(Progress::Idle, now);
        assert_eq!(backoff.failures, 9, "waiting is not a failure");
        assert!(!backoff.is_due(now));
        backoff.record(Progress::Done, now);
        assert!(backoff.is_due(now));
        backoff.record(Progress::Failed, now);
        assert_eq!(backoff.not_before.unwrap() - now, MERGE_RETRY_BACKOFF);
    }

    #[test]
    fn rollback_append_restores_prior_length() {
        let dir = test_dir("rollback");
        let path = dir.join("dst");
        let mut file = File::options()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .unwrap();
        file.write_all(b"keep").unwrap();
        file.sync_all().unwrap();
        let start = file.metadata().unwrap().len();
        file.write_all(b"junk").unwrap();
        rollback_append(&mut file, start).unwrap();
        drop(file);
        assert_eq!(std::fs::read(&path).unwrap(), b"keep");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
