use std::{collections::HashMap, fs::File, io, io::Write};

use bytes::{BufMut, BytesMut};
use jiff::Timestamp;
use tracing::{error, info, warn};
use zstd::stream::write::Encoder as ZstdEncoder;

/// exhaustively checked by the compiler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Tag {
    /// Raw SBE stream frame (binary).
    Sbe = b'S',
    /// REST depth snapshot (JSON).
    Rest = b'R',
}

/// `symbol` must always be lowercase (callers are responsible).
pub struct WriteRecord {
    pub recv_time: Timestamp,
    pub symbol: String,
    pub tag: Tag,
    pub data: bytes::Bytes,
}

pub struct RotatingFile {
    next_rotation: i64,
    path: String,
    file: Option<ZstdEncoder<'static, File>>,
    buf: BytesMut,
}

impl RotatingFile {
    fn create(
        timestamp: Timestamp,
        path: &str,
    ) -> Result<(ZstdEncoder<'static, File>, i64), io::Error> {
        let zoned = timestamp.to_zoned(jiff::tz::TimeZone::UTC);
        let date_str = zoned.date().strftime("%Y%m%d");
        let file = File::options()
            .create(true)
            .append(true)
            .open(format!("{path}_{date_str}.zst"))?;

        let next_rotation = zoned
            .date()
            .tomorrow()
            .map_err(io::Error::other)?
            .at(0, 0, 0, 0)
            .to_zoned(jiff::tz::TimeZone::UTC)
            .map_err(io::Error::other)?
            .timestamp()
            .as_nanosecond();

        // Level 1: fastest zstd setting — minimal CPU overhead for the write hot-path.
        let encoder = ZstdEncoder::new(file, 1)?;
        Ok((encoder, next_rotation as i64))
    }

    pub fn new(timestamp: Timestamp, path: String) -> Result<Self, io::Error> {
        let (file, next_rotation) = Self::create(timestamp, &path)?;
        Ok(Self {
            next_rotation,
            file: Some(file),
            path,
            buf: BytesMut::with_capacity(16 * 1024),
        })
    }

    /// Flush the zstd stream, write the zstd footer, and fsync to disk.
    pub fn finalize(&mut self) -> io::Result<()> {
        let Some(encoder) = self.file.take() else {
            return Ok(()); // already finalized
        };
        match encoder.finish() {
            Ok(raw_file) => {
                if let Err(e) = raw_file.sync_all() {
                    warn!(path = %self.path, error = %e, "sync_all failed on finalize");
                    return Err(e);
                }
            }
            Err(e) => {
                warn!(path = %self.path, error = %e, "zstd finish failed on finalize");
                return Err(e);
            }
        }
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

        // On day boundary: finalize the outgoing file, open the next one.
        if ts_nanos >= self.next_rotation {
            if let Err(e) = self.finalize() {
                error!(path = %self.path, error = %e, "failed to finalize file on rotation");
            }
            let (new_file, next_rotation) = Self::create(timestamp, &self.path)?;
            self.file = Some(new_file);
            self.next_rotation = next_rotation;
            info!(%self.path, "date changed, file rotated");
        }

        // guard against silent u32 truncation (impossible for real SBE/REST
        // payloads, but catches protocol changes early in debug builds).
        debug_assert!(
            data.len() <= u32::MAX as usize,
            "payload too large for u32 length prefix"
        );
        self.buf.clear();
        self.buf.put_i64_le(ts_nanos);
        self.buf.put_u8(tag as u8);
        self.buf.put_u32_le(data.len() as u32);
        self.buf.put(data);

        self.file.as_mut().unwrap().write_all(&self.buf)
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
    files: HashMap<String, RotatingFile>,
}

impl Writer {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
            files: Default::default(),
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
        if let Some(rotating_file) = self.files.get_mut(&symbol) {
            rotating_file.write(recv_time, tag, data)?;
        } else {
            // `symbol` is already lowercase — use it directly for the path.
            let path = format!("{}/{}", self.path, symbol);
            let mut rotating_file = RotatingFile::new(recv_time, path)?;
            rotating_file.write(recv_time, tag, data)?;
            self.files.insert(symbol, rotating_file);
        }
        Ok(())
    }

    /// Explicitly finalize all open files: flush zstd, write footer, fsync.
    ///
    /// Call before process exit for a clean shutdown. `Drop` also calls
    /// `finalize()` as a safety net, but errors there are only warn-logged.
    pub fn close(&mut self) {
        for (symbol, rf) in &mut self.files {
            match rf.finalize() {
                Ok(()) => info!(symbol = %symbol, "file closed cleanly"),
                Err(e) => error!(symbol = %symbol, error = %e, "failed to close file"),
            }
        }
        // Drop map — each RotatingFile::drop will no-op (file is None after finalize).
        self.files.clear();
    }
}
