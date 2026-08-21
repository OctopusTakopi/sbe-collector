use std::{
    borrow::Cow,
    collections::HashMap,
    fs::File,
    io::{self, Seek, SeekFrom, Write},
    os::fd::AsRawFd,
    sync::Arc,
    time::Duration,
};

use bytes::{BufMut, BytesMut};
use jiff::Timestamp;
use tracing::{error, info, warn};
use zstd::stream::write::Encoder as ZstdEncoder;

use crate::quality::{QualityEvent, QualityReporter};

/// exhaustively checked by the compiler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Tag {
    /// Raw SBE stream frame (binary).
    Sbe = b'S',
    /// REST depth snapshot (JSON).
    Rest = b'R',
}

pub type Symbol = Arc<str>;

const SYNC_ATTEMPTS: u32 = 3;
/// Uncompressed bytes accumulated before they are handed to zstd. Matches the
/// compressor's block size so we do not call into it once per websocket frame.
const ZSTD_BATCH_BYTES: usize = 128 * 1024;

/// `symbol` must always be lowercase (callers are responsible).
pub struct WriteRecord {
    pub recv_time: Timestamp,
    pub symbol: Symbol,
    pub tag: Tag,
    pub data: bytes::Bytes,
}

/// Characters left as-is in a filename.
///
/// Deliberately permissive: the point is to keep the exchange's own identifier
/// readable on disk, so only genuinely path-hostile bytes get escaped. All of
/// these are legal filename characters on Linux, macOS and Windows alike.
fn is_safe_in_filename(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'@' | b'+' | b'=')
}

/// Encode a symbol into a filename component.
///
/// Anything that is not a valid path component would open a file inside a
/// directory that was never created and take the whole collector down with it.
///
/// The encoding must be **injective**. Folding unsafe bytes to a single `_`
/// would map `foo/bar` and `foo_bar` to the same name, and since each symbol
/// gets its own `RotatingFile`, two independent zstd encoders would append
/// interleaved frames to one file and render it undecodable. Percent escaping
/// avoids that: `%` is itself unsafe, so it is always escaped and no two
/// distinct symbols can collide.
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

fn try_exclusive_lock(file: &File) -> io::Result<bool> {
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        return Ok(true);
    }
    let err = io::Error::last_os_error();
    if err.kind() == io::ErrorKind::WouldBlock {
        Ok(false)
    } else {
        Err(err)
    }
}

fn lock_exclusively(file: &File, path: &str) -> io::Result<()> {
    if try_exclusive_lock(file)? {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!("{path} is locked by another writer"),
        ))
    }
}

fn utc_date_str(timestamp: Timestamp) -> String {
    timestamp
        .to_zoned(jiff::tz::TimeZone::UTC)
        .date()
        .strftime("%Y%m%d")
        .to_string()
}

fn rollback_append(canonical: &mut File, start: u64) -> io::Result<()> {
    canonical.set_len(start)?;
    canonical.seek(SeekFrom::Start(start))?;
    canonical.sync_all()
}

pub struct RotatingFile {
    next_rotation: i64,
    path: String,
    run_id: String,
    /// Set while this process is writing a run-local overlap file because the
    /// canonical daily path is locked by another writer. Cleared after the
    /// switch: the sidecar has been appended into the daily file and removed.
    sidecar_path: Option<String>,
    /// Sidecars sealed at UTC midnight that still belong to this process and
    /// will be appended when that day's daily file lock is free.
    pending_sidecars: Vec<(String, String)>,
    file: Option<ZstdEncoder<'static, File>>,
    buf: BytesMut,
    /// Set when a rotation could not be finalized, so the already-rotated file
    /// may be missing its zstd footer. Collection continues, but the process
    /// must not report a clean exit.
    degraded: bool,
    quality: QualityReporter,
}

impl RotatingFile {
    fn try_lock_canonical_file(path: &str, date_str: &str) -> Result<Option<File>, io::Error> {
        let canonical = format!("{path}_{date_str}.zst");
        let primary = File::options().create(true).append(true).open(&canonical)?;
        if try_exclusive_lock(&primary)? {
            Ok(Some(primary))
        } else {
            Ok(None)
        }
    }

    fn try_lock_canonical(
        path: &str,
        date_str: &str,
    ) -> Result<Option<ZstdEncoder<'static, File>>, io::Error> {
        match Self::try_lock_canonical_file(path, date_str)? {
            Some(file) => Ok(Some(ZstdEncoder::new(file, 1)?)),
            None => Ok(None),
        }
    }

    fn append_sidecar(canonical: &mut File, sidecar: &str) -> io::Result<()> {
        let start = canonical.metadata()?.len();
        let copied = (|| {
            let mut src = File::open(sidecar)?;
            io::copy(&mut src, canonical)?;
            canonical.sync_all()
        })();
        if let Err(error) = copied {
            return match rollback_append(canonical, start) {
                Ok(()) => Err(error),
                Err(rollback) => Err(io::Error::new(
                    error.kind(),
                    format!("{error}; also failed to roll back partial append: {rollback}"),
                )),
            };
        }
        Ok(())
    }

    fn open_sidecar(
        path: &str,
        date_str: &str,
        run_id: &str,
    ) -> Result<(ZstdEncoder<'static, File>, String), io::Error> {
        let sidecar = format!("{path}_{date_str}_{run_id}.zst");
        let file = File::options()
            .create_new(true)
            .write(true)
            .open(&sidecar)?;
        lock_exclusively(&file, &sidecar)?;
        warn!(
            path = %sidecar,
            "daily file busy; writing a run-local sidecar until the incumbent releases it"
        );
        Ok((ZstdEncoder::new(file, 1)?, sidecar))
    }

    fn open_daily(
        path: &str,
        date_str: &str,
        run_id: &str,
    ) -> Result<(ZstdEncoder<'static, File>, Option<String>), io::Error> {
        if let Some(encoder) = Self::try_lock_canonical(path, date_str)? {
            return Ok((encoder, None));
        }
        let (encoder, sidecar) = Self::open_sidecar(path, date_str, run_id)?;
        Ok((encoder, Some(sidecar)))
    }

    fn create(
        timestamp: Timestamp,
        path: &str,
        run_id: &str,
    ) -> Result<(ZstdEncoder<'static, File>, i64, Option<String>), io::Error> {
        let zoned = timestamp.to_zoned(jiff::tz::TimeZone::UTC);
        let date_str = zoned.date().strftime("%Y%m%d").to_string();
        let (encoder, sidecar_path) = Self::open_daily(path, &date_str, run_id)?;

        let next_rotation = zoned
            .date()
            .tomorrow()
            .map_err(io::Error::other)?
            .at(0, 0, 0, 0)
            .to_zoned(jiff::tz::TimeZone::UTC)
            .map_err(io::Error::other)?
            .timestamp()
            .as_nanosecond();

        Ok((encoder, next_rotation as i64, sidecar_path))
    }

    pub fn new(
        timestamp: Timestamp,
        path: String,
        run_id: String,
        quality: QualityReporter,
    ) -> Result<Self, io::Error> {
        let (file, next_rotation, sidecar_path) = Self::create(timestamp, &path, &run_id)?;
        Ok(Self {
            next_rotation,
            file: Some(file),
            path,
            run_id,
            sidecar_path,
            pending_sidecars: Vec::new(),
            buf: BytesMut::with_capacity(ZSTD_BATCH_BYTES),
            degraded: false,
            quality,
        })
    }

    fn mark_degraded(&mut self, target: String, error: &io::Error) {
        self.degraded = true;
        self.quality.report(QualityEvent::StorageDegraded {
            at_ns: QualityEvent::now_ns(),
            target,
            error: error.to_string(),
        });
    }

    fn current_day_timestamp(&self) -> io::Result<Timestamp> {
        Timestamp::from_nanosecond(i128::from(self.next_rotation) - 1).map_err(io::Error::other)
    }

    fn resume_sidecar_encoder(sidecar: &str) -> io::Result<ZstdEncoder<'static, File>> {
        let file = File::options().append(true).open(sidecar)?;
        lock_exclusively(&file, sidecar)?;
        ZstdEncoder::new(file, 1)
    }

    fn append_and_remove_sidecar(canonical: &mut File, sidecar: &str) -> io::Result<()> {
        Self::append_sidecar(canonical, sidecar)?;
        match std::fs::remove_file(sidecar) {
            Ok(()) => info!(
                sidecar,
                "switch complete; merged and removed overlap sidecar"
            ),
            Err(error) => warn!(
                sidecar,
                %error,
                "switch complete; merged overlap sidecar but failed to remove it"
            ),
        }
        Ok(())
    }

    /// Merge a sidecar this process owns once its daily file lock is free.
    fn try_merge_owned_sidecar(path: &str, date_str: &str, sidecar: &str) -> io::Result<bool> {
        let Some(mut canonical) = Self::try_lock_canonical_file(path, date_str)? else {
            return Ok(false);
        };
        Self::append_and_remove_sidecar(&mut canonical, sidecar)?;
        Ok(true)
    }

    fn merge_pending_sidecars(&mut self) -> io::Result<()> {
        let pending = std::mem::take(&mut self.pending_sidecars);
        for (date_str, sidecar) in pending {
            match Self::try_merge_owned_sidecar(&self.path, &date_str, &sidecar) {
                Ok(true) => {}
                Ok(false) => self.pending_sidecars.push((date_str, sidecar)),
                Err(error) => {
                    error!(
                        path = %sidecar,
                        %error,
                        "failed to append overlap sidecar after switch"
                    );
                    self.mark_degraded(sidecar.clone(), &error);
                    self.pending_sidecars.push((date_str, sidecar));
                }
            }
        }
        Ok(())
    }

    fn resume_or_abandon_sidecar(
        &mut self,
        sidecar: String,
        date_str: &str,
        keep_writing: bool,
        sidecar_complete: bool,
    ) -> io::Result<()> {
        match Self::resume_sidecar_encoder(&sidecar) {
            Ok(encoder) => {
                self.file = Some(encoder);
                Ok(())
            }
            Err(resume_error) => {
                error!(
                    path = %sidecar,
                    %resume_error,
                    "failed to resume overlap sidecar after a failed merge"
                );
                if !sidecar_complete {
                    // Incomplete overlap still lives only in the sidecar.
                    // Keep sidecar_path so a later write retries; do not
                    // adopt the daily file as if the switch succeeded, and
                    // do not enqueue an unfinalized sidecar for append.
                    if keep_writing {
                        return Err(resume_error);
                    }
                    warn!(
                        path = %sidecar,
                        "leaving incomplete overlap sidecar on disk"
                    );
                    self.sidecar_path = None;
                    return Ok(());
                }
                let Some(mut canonical) = Self::try_lock_canonical_file(&self.path, date_str)?
                else {
                    if keep_writing {
                        return Err(resume_error);
                    }
                    self.pending_sidecars.push((date_str.to_string(), sidecar));
                    self.sidecar_path = None;
                    return Ok(());
                };
                if let Err(error) = Self::append_and_remove_sidecar(&mut canonical, &sidecar) {
                    self.mark_degraded(sidecar.clone(), &error);
                    if keep_writing {
                        return Err(error);
                    }
                    self.pending_sidecars.push((date_str.to_string(), sidecar));
                    self.sidecar_path = None;
                    return Ok(());
                }
                self.sidecar_path = None;
                if keep_writing {
                    self.file = Some(ZstdEncoder::new(canonical, 1)?);
                }
                Ok(())
            }
        }
    }

    /// After the incumbent releases the daily file lock, append this process's
    /// sidecar and delete it. That is the only time the sidecar is removed.
    ///
    /// `keep_writing`: wrap the canonical file so later records of this UTC day
    /// can follow. Pass false when rotating or closing — otherwise we would
    /// finish an empty extra frame.
    fn reclaim_canonical_if_free(
        &mut self,
        timestamp: Timestamp,
        keep_writing: bool,
    ) -> io::Result<()> {
        let Some(sidecar) = self.sidecar_path.clone() else {
            return Ok(());
        };
        let date_str = utc_date_str(timestamp);
        let Some(mut canonical) = Self::try_lock_canonical_file(&self.path, &date_str)? else {
            return Ok(());
        };
        if let Err(error) = self.finalize() {
            error!(
                path = %sidecar,
                %error,
                "failed to finalize handover sidecar before switching to the daily file"
            );
            self.mark_degraded(sidecar.clone(), &error);
            drop(canonical);
            return self.resume_or_abandon_sidecar(sidecar, &date_str, keep_writing, false);
        }
        if let Err(error) = Self::append_and_remove_sidecar(&mut canonical, &sidecar) {
            error!(
                path = %sidecar,
                %error,
                "failed to append overlap sidecar after switch"
            );
            self.mark_degraded(sidecar.clone(), &error);
            drop(canonical);
            return self.resume_or_abandon_sidecar(sidecar, &date_str, keep_writing, true);
        }
        self.sidecar_path = None;
        if keep_writing {
            self.file = Some(ZstdEncoder::new(canonical, 1)?);
        }
        Ok(())
    }

    fn flush_pending(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other(format!("{} has no open file", self.path)))?;
        file.write_all(&self.buf)?;
        self.buf.clear();
        Ok(())
    }

    /// Flush the zstd stream, write the zstd footer, and fsync to disk.
    pub fn finalize(&mut self) -> io::Result<()> {
        self.flush_pending()?;
        let Some(encoder) = self.file.take() else {
            return Ok(()); // already finalized
        };
        let raw_file = encoder.finish().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("failed to finish zstd stream {}: {error}", self.path),
            )
        })?;

        // Retry a transient fsync failure, then let the last attempt speak for
        // itself — no unreachable arm to fall out of sync with the bound.
        for attempt in 1..SYNC_ATTEMPTS {
            match raw_file.sync_all() {
                Ok(()) => {
                    let _ = unsafe { libc::flock(raw_file.as_raw_fd(), libc::LOCK_UN) };
                    return Ok(());
                }
                Err(error) => {
                    warn!(path = %self.path, attempt, %error, "sync_all failed; retrying");
                    std::thread::sleep(Duration::from_millis(25 * u64::from(attempt)));
                }
            }
        }
        raw_file.sync_all().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "failed to sync {} after {SYNC_ATTEMPTS} attempts: {error}",
                    self.path
                ),
            )
        })?;
        let _ = unsafe { libc::flock(raw_file.as_raw_fd(), libc::LOCK_UN) };
        Ok(())
    }

    /// Write one record with length-prefix framing:
    ///   [i64 nanos LE][u8 tag][u32 payload_len LE][payload bytes]
    pub fn write(
        &mut self,
        timestamp: Timestamp,
        tag: Tag,
        data: bytes::Bytes,
    ) -> Result<(), io::Error> {
        let ts_nanos = timestamp.as_nanosecond() as i64;

        if ts_nanos >= self.next_rotation {
            let outgoing_ts = self.current_day_timestamp()?;
            self.reclaim_canonical_if_free(outgoing_ts, false)?;
            if let Some(sidecar) = self.sidecar_path.take() {
                if let Err(error) = self.finalize() {
                    error!(
                        path = %self.path,
                        %error,
                        "failed to finalize file on rotation; continuing with the new file"
                    );
                    self.mark_degraded(self.path.clone(), &error);
                    warn!(
                        path = %sidecar,
                        "UTC date changed before switch; leaving incomplete overlap sidecar on disk"
                    );
                } else {
                    warn!(
                        path = %sidecar,
                        "UTC date changed before switch; keeping the overlap sidecar until the daily file is free"
                    );
                    self.pending_sidecars
                        .push((utc_date_str(outgoing_ts), sidecar));
                }
            } else if self.file.is_some() {
                if let Err(error) = self.finalize() {
                    error!(
                        path = %self.path,
                        %error,
                        "failed to finalize file on rotation; continuing with the new file"
                    );
                    self.mark_degraded(self.path.clone(), &error);
                }
            }
            let (new_file, next_rotation, sidecar_path) =
                Self::create(timestamp, &self.path, &self.run_id)?;
            self.buf.clear();
            self.file = Some(new_file);
            self.next_rotation = next_rotation;
            self.sidecar_path = sidecar_path;
            info!(%self.path, "date changed, file rotated");
        }

        self.reclaim_canonical_if_free(timestamp, true)?;
        self.merge_pending_sidecars()?;

        debug_assert!(
            data.len() <= u32::MAX as usize,
            "payload too large for u32 length prefix"
        );
        if self.file.is_none() {
            return Err(io::Error::other(format!("{} has no open file", self.path)));
        }

        // Never `unwrap`: the release profile is `panic = "abort"`, so a panic
        // here would skip every `Drop` and truncate all the other symbols' files.
        self.buf.put_i64_le(ts_nanos);
        self.buf.put_u8(tag as u8);
        self.buf.put_u32_le(data.len() as u32);
        self.buf.put(data);

        if self.buf.len() >= ZSTD_BATCH_BYTES {
            self.flush_pending()?;
        }
        Ok(())
    }
}

impl Drop for RotatingFile {
    fn drop(&mut self) {
        // Best-effort: write the zstd footer + fsync so the file is readable
        // even on panic. Errors are only warn-logged since we can't propagate them.
        if let Err(e) = self.finalize() {
            warn!(path = %self.path, error = %e, "failed to finalize file on drop");
        }
    }
}

pub struct Writer {
    path: String,
    run_id: String,
    files: HashMap<Symbol, RotatingFile>,
    quality: QualityReporter,
}

impl Writer {
    pub fn new(path: &str, run_id: &str, quality: QualityReporter) -> Self {
        Self {
            path: path.to_string(),
            run_id: run_id.to_owned(),
            files: Default::default(),
            quality,
        }
    }

    /// Write one `WriteRecord`. `record.symbol` must already be lowercase.
    pub fn write(&mut self, record: WriteRecord) -> Result<(), anyhow::Error> {
        let WriteRecord {
            recv_time,
            symbol,
            tag,
            data,
        } = record;
        // Keyed by the encoded name, not the raw symbol: one `RotatingFile` per
        // file on disk is what keeps two encoders from ever sharing an fd.
        let name = encode_symbol(&symbol);
        if let Some(rotating_file) = self.files.get_mut(name.as_ref()) {
            rotating_file.write(recv_time, tag, data)?;
        } else {
            let path = format!("{}/{}", self.path, name);
            let mut rotating_file =
                RotatingFile::new(recv_time, path, self.run_id.clone(), self.quality.clone())?;
            rotating_file.write(recv_time, tag, data)?;
            self.files
                .insert(Symbol::from(name.as_ref()), rotating_file);
        }
        Ok(())
    }

    /// Explicitly finalize all open files: flush zstd, write footer, fsync.
    ///
    /// If the switch has finished, any sidecar this process owns is appended
    /// and deleted first. If the incumbent still holds the daily file, the
    /// sidecar is sealed and left on disk.
    pub fn close(&mut self) -> Result<(), anyhow::Error> {
        let mut result = Ok(());
        for (symbol, rf) in &mut self.files {
            match rf.current_day_timestamp() {
                Ok(timestamp) => {
                    if let Err(error) = rf.reclaim_canonical_if_free(timestamp, false) {
                        error!(
                            symbol = %symbol,
                            %error,
                            "failed to merge overlap sidecar after switch"
                        );
                        if result.is_ok() {
                            result = Err(error.into());
                        }
                    }
                }
                Err(error) => {
                    error!(
                        symbol = %symbol,
                        %error,
                        "failed to determine current file date before close"
                    );
                    if result.is_ok() {
                        result = Err(error.into());
                    }
                }
            }
            if let Err(error) = rf.merge_pending_sidecars() {
                error!(
                    symbol = %symbol,
                    %error,
                    "failed to merge overlap sidecar after switch"
                );
                if result.is_ok() {
                    result = Err(error.into());
                }
            }
            if rf.sidecar_path.is_some() || !rf.pending_sidecars.is_empty() {
                info!(
                    symbol = %symbol,
                    "closing before switch; leaving overlap sidecar on disk"
                );
            }
            let degraded = rf.degraded;
            match rf.finalize() {
                Ok(()) if degraded => {
                    error!(
                        symbol = %symbol,
                        "file closed, but an earlier rotation could not be finalized"
                    );
                    if result.is_ok() {
                        result = Err(anyhow::anyhow!(
                            "{symbol}: an earlier rotation could not be finalized"
                        ));
                    }
                }
                Ok(()) => info!(symbol = %symbol, "file closed cleanly"),
                Err(error) => {
                    error!(symbol = %symbol, %error, "failed to close file");
                    if result.is_ok() {
                        result = Err(error.into());
                    }
                }
            }
        }
        self.files.clear();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// Two distinct symbols must never produce the same filename: they each get
    /// their own zstd encoder, and sharing a file would interleave frames.
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
        let dir = std::env::temp_dir().join(format!(
            "sbe-collision-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let mut writer = Writer::new(
            dir.to_str().unwrap(),
            "test-run",
            QualityReporter::disabled(),
        );
        for symbol in ["foo/bar", "foo_bar"] {
            writer
                .write(WriteRecord {
                    recv_time: Timestamp::now(),
                    symbol: Symbol::from(symbol),
                    tag: Tag::Sbe,
                    data: bytes::Bytes::from_static(b"\x00"),
                })
                .unwrap();
        }
        writer.close().unwrap();

        let mut written: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        written.sort();
        assert_eq!(written.len(), 2, "{written:?}");
        for name in &written {
            let bytes = std::fs::read(dir.join(name)).unwrap();
            assert!(zstd::decode_all(bytes.as_slice()).is_ok(), "{name}");
            let stem = name.trim_end_matches(".zst");
            let date = stem.rsplit_once('_').map(|(_, date)| date).unwrap();
            assert!(
                date.len() == 8 && date.bytes().all(|b| b.is_ascii_digit()),
                "expected daily symbol_YYYYMMDD.zst, got {name}"
            );
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn same_day_writes_share_one_file() {
        let dir = std::env::temp_dir().join(format!(
            "sbe-daily-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut writer = Writer::new(
            dir.to_str().unwrap(),
            "daily-run",
            QualityReporter::disabled(),
        );
        let first = Timestamp::from_nanosecond(1_800_000_000_000_000_000).unwrap();
        let later_same_day = first
            .checked_add(jiff::Span::new().try_hours(3).unwrap())
            .unwrap();
        for recv_time in [first, later_same_day] {
            writer
                .write(WriteRecord {
                    recv_time,
                    symbol: Symbol::from("btcusdt"),
                    tag: Tag::Sbe,
                    data: bytes::Bytes::from_static(b"frame"),
                })
                .unwrap();
        }
        writer.close().unwrap();

        let paths: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(paths.len(), 1, "{paths:?}");
        assert!(zstd::decode_all(std::fs::read(&paths[0]).unwrap().as_slice()).is_ok());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn date_boundary_opens_a_new_daily_file() {
        let dir = std::env::temp_dir().join(format!(
            "sbe-day-boundary-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut writer = Writer::new(
            dir.to_str().unwrap(),
            "boundary-run",
            QualityReporter::disabled(),
        );
        let before = Timestamp::from_nanosecond(1_800_000_000_000_000_000).unwrap(); // 2027-01-15
        // Next UTC day without calendar Span units on Timestamp.
        let after = Timestamp::from_nanosecond(1_800_086_400_000_000_000).unwrap();
        for recv_time in [before, after] {
            writer
                .write(WriteRecord {
                    recv_time,
                    symbol: Symbol::from("btcusdt"),
                    tag: Tag::Sbe,
                    data: bytes::Bytes::from_static(b"frame"),
                })
                .unwrap();
        }
        writer.close().unwrap();

        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert_eq!(names.len(), 2, "{names:?}");
        assert!(names[0].starts_with("btcusdt_"));
        assert!(names[1].starts_with("btcusdt_"));
        assert_ne!(names[0], names[1]);
        for name in &names {
            assert!(zstd::decode_all(std::fs::read(dir.join(name)).unwrap().as_slice()).is_ok());
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn busy_daily_file_falls_back_to_run_sidecar() {
        let dir = std::env::temp_dir().join(format!(
            "sbe-sidecar-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let now = Timestamp::now();
        let mut incumbent = Writer::new(
            dir.to_str().unwrap(),
            "incumbent",
            QualityReporter::disabled(),
        );
        incumbent
            .write(WriteRecord {
                recv_time: now,
                symbol: Symbol::from("btcusdt"),
                tag: Tag::Sbe,
                data: bytes::Bytes::from_static(b"incumbent"),
            })
            .unwrap();

        let mut challenger = Writer::new(
            dir.to_str().unwrap(),
            "challenger",
            QualityReporter::disabled(),
        );
        challenger
            .write(WriteRecord {
                recv_time: now,
                symbol: Symbol::from("btcusdt"),
                tag: Tag::Sbe,
                data: bytes::Bytes::from_static(b"sidecar-overlap"),
            })
            .unwrap();
        assert!(
            challenger.files["btcusdt"].sidecar_path.is_some(),
            "challenger must use a sidecar while the daily file is locked"
        );

        incumbent.close().unwrap();
        challenger
            .write(WriteRecord {
                recv_time: now,
                symbol: Symbol::from("btcusdt"),
                tag: Tag::Sbe,
                data: bytes::Bytes::from_static(b"after-reclaim"),
            })
            .unwrap();
        assert!(
            challenger.files["btcusdt"].sidecar_path.is_none(),
            "after the incumbent exits, writes must reclaim the canonical daily file"
        );
        challenger.close().unwrap();

        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert!(
            names.iter().all(|n| !n.contains("_challenger.zst")),
            "overlap sidecar must be removed after a successful handover: {names:?}"
        );
        assert_eq!(names.len(), 1, "{names:?}");
        assert!(
            names[0].starts_with("btcusdt_") && names[0].ends_with(".zst"),
            "canonical daily file must exist: {names:?}"
        );
        let decoded =
            zstd::decode_all(std::fs::read(dir.join(&names[0])).unwrap().as_slice()).unwrap();
        for payload in [
            b"incumbent".as_slice(),
            b"sidecar-overlap",
            b"after-reclaim",
        ] {
            assert!(
                decoded.windows(payload.len()).any(|w| w == payload),
                "merged daily file missing {payload:?}"
            );
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn close_merges_sidecar_without_another_write() {
        let dir = std::env::temp_dir().join(format!(
            "sbe-close-merge-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let now = Timestamp::now();
        let mut incumbent = Writer::new(
            dir.to_str().unwrap(),
            "incumbent",
            QualityReporter::disabled(),
        );
        incumbent
            .write(WriteRecord {
                recv_time: now,
                symbol: Symbol::from("btcusdt"),
                tag: Tag::Sbe,
                data: bytes::Bytes::from_static(b"incumbent"),
            })
            .unwrap();
        let mut challenger = Writer::new(
            dir.to_str().unwrap(),
            "challenger",
            QualityReporter::disabled(),
        );
        challenger
            .write(WriteRecord {
                recv_time: now,
                symbol: Symbol::from("btcusdt"),
                tag: Tag::Sbe,
                data: bytes::Bytes::from_static(b"sidecar-overlap"),
            })
            .unwrap();
        incumbent.close().unwrap();
        assert!(
            std::fs::read_dir(&dir).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains("_challenger.zst")
            }),
            "incumbent close must not steal a live overlap sidecar"
        );
        challenger.close().unwrap();

        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert!(
            names.iter().all(|n| !n.contains("_challenger.zst")),
            "close must merge and remove the overlap sidecar: {names:?}"
        );
        assert_eq!(names.len(), 1, "{names:?}");
        let merged_bytes = std::fs::read(dir.join(&names[0])).unwrap();
        assert_eq!(
            zstd_frame_count(&merged_bytes),
            2,
            "close should not append an empty extra zstd frame"
        );
        let decoded = zstd::decode_all(merged_bytes.as_slice()).unwrap();
        for payload in [b"incumbent".as_slice(), b"sidecar-overlap"] {
            assert!(
                decoded.windows(payload.len()).any(|w| w == payload),
                "merged daily file missing {payload:?}"
            );
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn midnight_reclaim_merges_sidecar_after_incumbent_exits() {
        let dir = std::env::temp_dir().join(format!(
            "sbe-midnight-merge-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let before = Timestamp::from_nanosecond(1_800_000_000_000_000_000).unwrap();
        let after = Timestamp::from_nanosecond(1_800_086_400_000_000_000).unwrap();
        let mut incumbent = Writer::new(
            dir.to_str().unwrap(),
            "incumbent",
            QualityReporter::disabled(),
        );
        incumbent
            .write(WriteRecord {
                recv_time: before,
                symbol: Symbol::from("btcusdt"),
                tag: Tag::Sbe,
                data: bytes::Bytes::from_static(b"incumbent"),
            })
            .unwrap();
        let mut challenger = Writer::new(
            dir.to_str().unwrap(),
            "challenger",
            QualityReporter::disabled(),
        );
        challenger
            .write(WriteRecord {
                recv_time: before,
                symbol: Symbol::from("btcusdt"),
                tag: Tag::Sbe,
                data: bytes::Bytes::from_static(b"sidecar-overlap"),
            })
            .unwrap();
        incumbent.close().unwrap();
        challenger
            .write(WriteRecord {
                recv_time: after,
                symbol: Symbol::from("btcusdt"),
                tag: Tag::Sbe,
                data: bytes::Bytes::from_static(b"next-day"),
            })
            .unwrap();
        challenger.close().unwrap();

        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert!(
            names.iter().all(|n| !n.contains("_challenger.zst")),
            "midnight rotation must merge yesterday's sidecar: {names:?}"
        );
        assert_eq!(names.len(), 2, "{names:?}");
        let day_one_bytes = std::fs::read(dir.join(&names[0])).unwrap();
        assert_eq!(
            zstd_frame_count(&day_one_bytes),
            2,
            "yesterday should be incumbent frame + sidecar frame, not an extra empty frame"
        );
        let day_one = zstd::decode_all(day_one_bytes.as_slice()).unwrap();
        let day_two =
            zstd::decode_all(std::fs::read(dir.join(&names[1])).unwrap().as_slice()).unwrap();
        for payload in [b"incumbent".as_slice(), b"sidecar-overlap"] {
            assert!(
                day_one.windows(payload.len()).any(|w| w == payload),
                "yesterday's daily file missing {payload:?}"
            );
        }
        assert!(
            day_two.windows(b"next-day".len()).any(|w| w == b"next-day"),
            "today's daily file missing next-day payload"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn midnight_keeps_sidecar_while_incumbent_lock_is_held() {
        let dir = std::env::temp_dir().join(format!(
            "sbe-midnight-lock-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let before = Timestamp::from_nanosecond(1_800_000_000_000_000_000).unwrap();
        let after = Timestamp::from_nanosecond(1_800_086_400_000_000_000).unwrap();
        let mut incumbent = Writer::new(
            dir.to_str().unwrap(),
            "incumbent",
            QualityReporter::disabled(),
        );
        incumbent
            .write(WriteRecord {
                recv_time: before,
                symbol: Symbol::from("btcusdt"),
                tag: Tag::Sbe,
                data: bytes::Bytes::from_static(b"incumbent"),
            })
            .unwrap();
        let mut challenger = Writer::new(
            dir.to_str().unwrap(),
            "challenger",
            QualityReporter::disabled(),
        );
        challenger
            .write(WriteRecord {
                recv_time: before,
                symbol: Symbol::from("btcusdt"),
                tag: Tag::Sbe,
                data: bytes::Bytes::from_static(b"sidecar-overlap"),
            })
            .unwrap();
        challenger
            .write(WriteRecord {
                recv_time: after,
                symbol: Symbol::from("btcusdt"),
                tag: Tag::Sbe,
                data: bytes::Bytes::from_static(b"next-day"),
            })
            .unwrap();
        assert!(
            !challenger.files["btcusdt"].pending_sidecars.is_empty()
                || std::fs::read_dir(&dir).unwrap().any(|entry| entry
                    .unwrap()
                    .file_name()
                    .into_string()
                    .unwrap()
                    .contains("_challenger.zst")),
            "overlap sidecar must remain until the incumbent releases yesterday's lock"
        );

        incumbent.close().unwrap();
        challenger.close().unwrap();

        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert!(
            names.iter().all(|n| !n.contains("_challenger.zst")),
            "close must merge the leftover midnight sidecar: {names:?}"
        );
        assert_eq!(names.len(), 2, "{names:?}");
        let day_one =
            zstd::decode_all(std::fs::read(dir.join(&names[0])).unwrap().as_slice()).unwrap();
        let day_two =
            zstd::decode_all(std::fs::read(dir.join(&names[1])).unwrap().as_slice()).unwrap();
        for payload in [b"incumbent".as_slice(), b"sidecar-overlap"] {
            assert!(
                day_one.windows(payload.len()).any(|w| w == payload),
                "yesterday's daily file missing {payload:?}"
            );
        }
        assert!(
            day_two.windows(b"next-day".len()).any(|w| w == b"next-day"),
            "today's daily file missing next-day payload"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn duplicate_run_id_cannot_share_a_sidecar() {
        let dir = std::env::temp_dir().join(format!(
            "sbe-sidecar-exclusive-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let now = Timestamp::now();
        let mut incumbent = Writer::new(
            dir.to_str().unwrap(),
            "incumbent",
            QualityReporter::disabled(),
        );
        incumbent
            .write(WriteRecord {
                recv_time: now,
                symbol: Symbol::from("btcusdt"),
                tag: Tag::Sbe,
                data: bytes::Bytes::from_static(b"incumbent"),
            })
            .unwrap();
        let mut first = Writer::new(
            dir.to_str().unwrap(),
            "same-run",
            QualityReporter::disabled(),
        );
        first
            .write(WriteRecord {
                recv_time: now,
                symbol: Symbol::from("btcusdt"),
                tag: Tag::Sbe,
                data: bytes::Bytes::from_static(b"first"),
            })
            .unwrap();
        let mut second = Writer::new(
            dir.to_str().unwrap(),
            "same-run",
            QualityReporter::disabled(),
        );
        let err = second
            .write(WriteRecord {
                recv_time: now,
                symbol: Symbol::from("btcusdt"),
                tag: Tag::Sbe,
                data: bytes::Bytes::from_static(b"second"),
            })
            .unwrap_err();
        assert!(
            err.to_string().contains("already exists")
                || err
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::AlreadyExists),
            "second writer with the same run id must not share the sidecar: {err}"
        );
        first.close().ok();
        incumbent.close().ok();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn rollback_append_restores_prior_length() {
        use std::io::Write;
        let dir = std::env::temp_dir().join(format!(
            "sbe-rollback-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
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
