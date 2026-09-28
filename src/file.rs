use std::{
    borrow::Cow,
    collections::HashMap,
    fs::{File, TryLockError},
    io::{self, Seek, SeekFrom, Write},
    mem,
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::bail;
use bytes::{BufMut, Bytes, BytesMut};
use jiff::{Timestamp, civil::Date, tz::TimeZone};
use sbe_collector::record::{Header, Tag};
use tracing::{error, info, warn};
use zstd::stream::write::Encoder as ZstdEncoder;

use crate::quality::{QualityEvent, QualityReporter};

pub type Symbol = Arc<str>;

type Encoder = ZstdEncoder<'static, File>;

const ZSTD_LEVEL: i32 = 1;
/// Uncompressed bytes batched per zstd call: the compressor's block size.
const ZSTD_BATCH_BYTES: usize = 128 * 1024;
/// Spacing of sidecar merge attempts after a failure. An attempt walks and
/// fsyncs the daily file, which stalls the writer for every symbol.
const MERGE_RETRY_BACKOFF: Duration = Duration::from_secs(5);
const ZSTD_MAGIC: u32 = 0xFD2F_B528;
const SKIPPABLE_MAGIC: u32 = 0x184D_2A50;
const SKIPPABLE_MAGIC_MASK: u32 = 0xFFFF_FFF0;

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
fn day_file(prefix: &Path, date: Date, suffix: &str) -> PathBuf {
    let mut name = prefix.as_os_str().to_owned();
    name.push(format!("_{}{suffix}.zst", date.strftime("%Y%m%d")));
    name.into()
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

/// Whether `file` ends exactly where a complete zstd frame does.
///
/// Walks frame and block headers only: one small `pread` per block, no
/// decompression.
fn ends_on_frame_boundary(file: &File) -> io::Result<bool> {
    let len = file.metadata()?.len();
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
    let file = File::options()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
    Ok((try_exclusive_lock(&file)? && names_file(path, &file)?).then_some(file))
}

/// Lock `date`'s daily file, or `None` while another writer holds it.
///
/// A file whose last writer died mid-frame is moved aside first: a decoder
/// cannot step past that tail, so anything appended after it would be lost.
fn lock_daily(prefix: &Path, date: Date) -> io::Result<Option<File>> {
    let path = day_file(prefix, date, "");
    let Some(daily) = open_locked(&path)? else {
        return Ok(None);
    };
    if ends_on_frame_boundary(&daily)? {
        return Ok(Some(daily));
    }
    let salvage = day_file(
        prefix,
        date,
        &format!("_unterminated-{}", Timestamp::now().as_nanosecond()),
    );
    std::fs::rename(&path, &salvage)?;
    warn!(
        path = %path.display(),
        salvage = %salvage.display(),
        "daily file ends mid-frame; moved it aside instead of appending to it"
    );
    let fresh = open_locked(&path);
    // The damaged inode stays locked until the fresh one is.
    drop(daily);
    fresh
}

fn rollback_append(daily: &mut File, start: u64) -> io::Result<()> {
    daily.set_len(start)?;
    daily.seek(SeekFrom::Start(start))?;
    daily.sync_all()
}

fn copy_synced(daily: &mut File, sidecar: &Path) -> io::Result<()> {
    io::copy(&mut File::open(sidecar)?, daily)?;
    daily.sync_all()
}

fn append_sidecar(daily: &mut File, sidecar: &Path) -> io::Result<()> {
    let start = daily.metadata()?.len();
    let Err(error) = copy_synced(daily, sidecar) else {
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

/// The only place a sidecar is deleted: after its bytes are durable in the
/// daily file.
fn append_and_remove_sidecar(daily: &mut File, sidecar: &Path) -> io::Result<()> {
    append_sidecar(daily, sidecar)?;
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
    Ok(())
}

/// Lock `date`'s daily file and append `sidecar` to it; `None` while another
/// writer holds the daily file.
fn merge_into_daily(prefix: &Path, date: Date, sidecar: &Path) -> io::Result<Option<File>> {
    let Some(mut daily) = lock_daily(prefix, date)? else {
        return Ok(None);
    };
    append_and_remove_sidecar(&mut daily, sidecar)?;
    Ok(Some(daily))
}

/// Write the batched records, finish the zstd frame, and fsync.
fn seal(mut encoder: Encoder, batch: &mut BytesMut, path: &Path) -> io::Result<()> {
    let context = |what: &str, error: io::Error| {
        io::Error::new(
            error.kind(),
            format!("failed to {what} {}: {error}", path.display()),
        )
    };
    encoder
        .write_all(batch)
        .map_err(|error| context("write records to", error))?;
    batch.clear();
    let file = encoder
        .finish()
        .map_err(|error| context("finish zstd stream", error))?;
    // No retry: Linux may drop the dirty pages of a failed fsync and mark
    // them clean, so a second attempt can succeed with the data gone.
    file.sync_all().map_err(|error| context("sync", error))
}

/// Keep writing a sealed sidecar after its merge failed: a new frame after
/// the sealed one, so a later merge still appends whole frames.
fn resume_sidecar(path: &Path) -> io::Result<Encoder> {
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
    /// A run-local sidecar, while another writer holds the daily file.
    Sidecar { encoder: Encoder, path: PathBuf },
    /// Sealed, or left without a file by a failed switch.
    Closed,
}

impl Sink {
    /// The daily file if it is free, else a new sidecar
    /// `<symbol>_<YYYYMMDD>_<run-id>.zst`.
    fn open(prefix: &Path, date: Date, run_id: &str) -> io::Result<Self> {
        if let Some(daily) = lock_daily(prefix, date)? {
            return Ok(Self::Daily(Encoder::new(daily, ZSTD_LEVEL)?));
        }
        let path = day_file(prefix, date, &format!("_{run_id}"));
        let file = File::options().create_new(true).write(true).open(&path)?;
        lock_exclusively(&file, &path)?;
        warn!(
            path = %path.display(),
            "daily file busy; writing a run-local sidecar until the incumbent releases it"
        );
        Ok(Self::Sidecar {
            encoder: Encoder::new(file, ZSTD_LEVEL)?,
            path,
        })
    }

    fn is_sidecar(&self) -> bool {
        matches!(self, Self::Sidecar { .. })
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

/// A sealed sidecar of an earlier day, appended once its daily file is free.
struct PendingSidecar {
    date: Date,
    path: PathBuf,
}

/// Outcome of trying to switch from the sidecar to the daily file.
enum Switch {
    /// No sidecar, the daily file is still held, or the sidecar was resumed
    /// after a failed merge. The sink is unchanged or still a sidecar.
    NotYet,
    /// The sidecar is merged, or abandoned unfinished, and this process holds
    /// the daily file. The sink is closed.
    Switched(File),
    /// The sidecar could neither be merged nor reopened. It is queued for a
    /// later merge and the sink is closed.
    Stranded(io::Error),
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
    merge_not_before: Instant,
    /// The first storage fault. Collection continues, but the exit is not
    /// clean.
    fault: Option<String>,
    quality: QualityReporter,
}

impl RotatingFile {
    fn open(
        prefix: PathBuf,
        run_id: Arc<str>,
        timestamp: Timestamp,
        quality: QualityReporter,
    ) -> io::Result<Self> {
        let day = UtcDay::containing(timestamp)?;
        let sink = Sink::open(&prefix, day.date, &run_id)?;
        Ok(Self {
            prefix,
            run_id,
            day,
            sink,
            batch: BytesMut::with_capacity(ZSTD_BATCH_BYTES),
            pending: Vec::new(),
            merge_not_before: Instant::now(),
            fault: None,
            quality,
        })
    }

    fn degrade(&mut self, target: String, error: &io::Error) {
        self.fault
            .get_or_insert_with(|| format!("{target}: {error}"));
        self.quality.report(QualityEvent::StorageDegraded {
            at_ns: QualityEvent::now_ns(),
            target,
            error: error.to_string(),
        });
    }

    fn merge_failed(&mut self, sidecar: &Path, error: &io::Error) {
        error!(
            path = %sidecar.display(),
            %error,
            "failed to append overlap sidecar after switch"
        );
        self.degrade(sidecar.display().to_string(), error);
        self.merge_not_before = Instant::now() + MERGE_RETRY_BACKOFF;
    }

    fn merge_due(&self) -> bool {
        (self.sink.is_sidecar() || !self.pending.is_empty())
            && Instant::now() >= self.merge_not_before
    }

    /// Seal the open sink. A sealed sidecar's path comes back: it still has
    /// to be appended once its daily file is free.
    fn finalize(&mut self) -> io::Result<Option<PathBuf>> {
        match mem::replace(&mut self.sink, Sink::Closed) {
            Sink::Closed => Ok(None),
            Sink::Daily(encoder) => seal(
                encoder,
                &mut self.batch,
                &day_file(&self.prefix, self.day.date, ""),
            )
            .map(|()| None),
            Sink::Sidecar { encoder, path } => {
                seal(encoder, &mut self.batch, &path).map(|()| Some(path))
            }
        }
    }

    /// If the incumbent has released the daily file, seal this process's
    /// sidecar and append it there.
    fn try_switch(&mut self) -> io::Result<Switch> {
        if !self.sink.is_sidecar() {
            return Ok(Switch::NotYet);
        }
        let Some(mut daily) = lock_daily(&self.prefix, self.day.date)? else {
            return Ok(Switch::NotYet);
        };
        let Sink::Sidecar { encoder, path } = mem::replace(&mut self.sink, Sink::Closed) else {
            unreachable!("checked above");
        };
        if let Err(error) = seal(encoder, &mut self.batch, &path) {
            // An unfinished frame is never appended (see `lock_daily`); the
            // records still batched go to the daily file instead.
            error!(
                path = %path.display(),
                %error,
                "failed to seal handover sidecar; leaving it on disk unfinished"
            );
            self.degrade(path.display().to_string(), &error);
            return Ok(Switch::Switched(daily));
        }
        match append_and_remove_sidecar(&mut daily, &path) {
            Ok(()) => Ok(Switch::Switched(daily)),
            Err(error) => {
                self.merge_failed(&path, &error);
                drop(daily);
                self.resume_or_strand(path)
            }
        }
    }

    fn resume_or_strand(&mut self, path: PathBuf) -> io::Result<Switch> {
        let resume_error = match resume_sidecar(&path) {
            Ok(encoder) => {
                self.sink = Sink::Sidecar { encoder, path };
                return Ok(Switch::NotYet);
            }
            Err(error) => error,
        };
        error!(
            path = %path.display(),
            error = %resume_error,
            "failed to resume overlap sidecar after a failed merge"
        );
        let stranded = PendingSidecar {
            date: self.day.date,
            path,
        };
        match merge_into_daily(&self.prefix, stranded.date, &stranded.path) {
            Ok(Some(daily)) => Ok(Switch::Switched(daily)),
            Ok(None) => {
                self.pending.push(stranded);
                Ok(Switch::Stranded(resume_error))
            }
            Err(error) => {
                self.degrade(stranded.path.display().to_string(), &error);
                self.pending.push(stranded);
                Ok(Switch::Stranded(error))
            }
        }
    }

    /// Switch to the daily file once it is free, and keep writing there.
    fn switch_to_daily(&mut self) -> io::Result<()> {
        match self.try_switch()? {
            Switch::NotYet => Ok(()),
            Switch::Switched(daily) => {
                self.sink = Sink::Daily(Encoder::new(daily, ZSTD_LEVEL)?);
                Ok(())
            }
            Switch::Stranded(error) => Err(error),
        }
    }

    /// Merge the sidecar if the daily file is free, without starting a frame
    /// that nothing will be written to (rotation, close).
    fn release_sidecar(&mut self) -> io::Result<()> {
        if let Switch::Switched(daily) = self.try_switch()?
            && !self.batch.is_empty()
        {
            self.sink = Sink::Daily(Encoder::new(daily, ZSTD_LEVEL)?);
        }
        Ok(())
    }

    fn merge_pending_sidecars(&mut self) {
        for sidecar in mem::take(&mut self.pending) {
            match merge_into_daily(&self.prefix, sidecar.date, &sidecar.path) {
                Ok(Some(_)) => {}
                Ok(None) => self.pending.push(sidecar),
                Err(error) => {
                    self.merge_failed(&sidecar.path, &error);
                    self.pending.push(sidecar);
                }
            }
        }
    }

    fn rotate(&mut self, timestamp: Timestamp) -> io::Result<()> {
        self.release_sidecar()?;
        match self.finalize() {
            Ok(None) => {}
            Ok(Some(path)) => {
                warn!(
                    path = %path.display(),
                    "UTC date changed before switch; keeping the overlap sidecar until the daily file is free"
                );
                self.pending.push(PendingSidecar {
                    date: self.day.date,
                    path,
                });
            }
            Err(error) => {
                error!(
                    path = %self.prefix.display(),
                    %error,
                    "failed to finalize file on rotation; continuing with the new file"
                );
                self.degrade(self.prefix.display().to_string(), &error);
            }
        }
        let day = UtcDay::containing(timestamp)?;
        let sink = Sink::open(&self.prefix, day.date, &self.run_id)?;
        self.batch.clear();
        self.day = day;
        self.sink = sink;
        info!(path = %self.prefix.display(), "date changed, file rotated");
        Ok(())
    }

    fn flush_batch(&mut self) -> io::Result<()> {
        let encoder = match &mut self.sink {
            Sink::Daily(encoder) | Sink::Sidecar { encoder, .. } => encoder,
            Sink::Closed => return Err(no_sink(&self.prefix)),
        };
        encoder.write_all(&self.batch)?;
        self.batch.clear();
        Ok(())
    }

    fn write(&mut self, timestamp: Timestamp, tag: Tag, data: Bytes) -> io::Result<()> {
        if timestamp >= self.day.end {
            self.rotate(timestamp)?;
        }
        if self.merge_due() {
            self.switch_to_daily()?;
            self.merge_pending_sidecars();
        }
        if matches!(self.sink, Sink::Closed) {
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
            error!(symbol, %error, "failed to merge overlap sidecar after switch");
        }
        self.merge_pending_sidecars();
        if self.sink.is_sidecar() || !self.pending.is_empty() {
            info!(
                symbol,
                "closing before switch; leaving overlap sidecar on disk"
            );
        }
        let sealed = self.finalize().map(|_| ());
        match (&sealed, &self.fault) {
            (Err(error), _) => error!(symbol, %error, "failed to close file"),
            (Ok(()), Some(fault)) => {
                error!(symbol, fault, "file closed, but storage degraded earlier");
            }
            (Ok(()), None) => info!(symbol, "file closed cleanly"),
        }
        released.and(sealed)?;
        match &self.fault {
            Some(fault) => bail!("{symbol}: storage degraded earlier: {fault}"),
            None => Ok(()),
        }
    }
}

impl Drop for RotatingFile {
    /// Best effort; `close` is the path that reports failures.
    fn drop(&mut self) {
        if let Err(error) = self.finalize() {
            warn!(path = %self.prefix.display(), %error, "failed to finalize file on drop");
        }
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
    use super::*;

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
            ends_on_frame_boundary(&File::open(&path).unwrap()).unwrap()
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
        assert_eq!(decoded(&dir.join("btcusdt_20270115.zst")), expected);
        std::fs::remove_dir_all(&dir).unwrap();
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
