use thiserror::Error;

/// Why a received frame could not be routed to the writer.
#[derive(Error, Debug)]
pub enum ConnectorError {
    #[error("{len}-byte frame is shorter than the SBE message header")]
    ShortFrame { len: usize },
    #[error("{len}-byte depth diff is shorter than its fixed block")]
    ShortDepthDiff { len: usize },
    #[error("writer channel is closed")]
    WriterClosed,
}
