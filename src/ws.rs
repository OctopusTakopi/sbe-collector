use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use fastwebsockets::{FragmentCollector, handshake};
use http_body_util::Empty;
use hyper::Request;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use rustls::ClientConfig;
use std::future::Future;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use url::Url;

pub type WsStream = FragmentCollector<TokioIo<Upgraded>>;

struct SpawnExecutor;

impl<F> hyper::rt::Executor<F> for SpawnExecutor
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, fut: F) {
        tokio::spawn(fut);
    }
}

/// Build a TLS client config backed by the Mozilla root certificate bundle.
/// Call this ONCE, wrap in `Arc`, and reuse across every reconnect to avoid
/// cloning ~140 root certificates on each connection attempt.
pub fn build_tls_config() -> ClientConfig {
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

/// Open a TLS WebSocket to `url`, optionally sending `X-MBX-APIKEY`.
///
/// Accepts a pre-built `Arc<ClientConfig>` (P1) so that certificate loading is
/// not repeated on every reconnect — just clone the Arc.
pub async fn connect(url: &str, api_key: Option<&str>, tls: Arc<ClientConfig>) -> Result<WsStream> {
    let url = Url::parse(url).context("Invalid URL")?;
    let host = url.host_str().ok_or_else(|| anyhow!("No host in url"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| anyhow!("No port in url"))?;
    let domain = rustls::pki_types::ServerName::try_from(host)
        .map_err(|e| anyhow!("Invalid domain: {}", e))?
        .to_owned();

    let connector = TlsConnector::from(tls);

    let addr = format!("{}:{}", host, port);
    let tcp_stream = TcpStream::connect(&addr)
        .await
        .context("Failed to connect via TCP")?;
    let tls_stream = connector
        .connect(domain, tcp_stream)
        .await
        .context("Failed to perform TLS handshake")?;

    let mut req_builder = Request::builder()
        .uri(url.as_str())
        .header("Host", host)
        .header("Upgrade", "websocket")
        .header("Connection", "Upgrade")
        .header("Sec-WebSocket-Key", handshake::generate_key())
        .header("Sec-WebSocket-Version", "13");

    if let Some(key) = api_key {
        req_builder = req_builder.header("X-MBX-APIKEY", key);
    }

    let req = req_builder
        .body(Empty::<Bytes>::new())
        .context("Failed to build request")?;

    let (mut ws, _) = handshake::client(&SpawnExecutor, req, tls_stream)
        .await
        .map_err(|e| anyhow!("WebSocket handshake failed: {:?}", e))?;

    ws.set_auto_pong(false);
    Ok(FragmentCollector::new(ws))
}
