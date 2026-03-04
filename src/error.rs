use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConnectorError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("format error")]
    Format,
    #[error("SBE parsing error")]
    Sbe,
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),
    #[allow(dead_code)]
    #[error("WebSocket error: {0}")]
    Ws(String),
}
