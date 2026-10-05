use thiserror::Error;

/// Why a received frame could not be routed to the writer.
#[derive(Error, Debug)]
pub enum ConnectorError {
    #[error("{len}-byte frame is shorter than the SBE message header")]
    ShortFrame { len: usize },
    #[error("{len}-byte {name} is shorter than its fixed block")]
    ShortBlock { name: &'static str, len: usize },
    #[error("writer channel is closed")]
    WriterClosed,
}
