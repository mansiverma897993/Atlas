//! Background JWKS refresher — the production wiring for [`crate::auth::JwksKeySource`].
//!
//! The gateway verifies RS256 access tokens against Identity's **public** signing keys. Those
//! keys live at Identity's JWKS endpoint (`cfg.jwt.jwks_url`, default
//! `http://identity:8081/.well-known/jwks.json`). This task fetches that document over HTTP and
//! loads it into the shared [`JwksKeySource`] cache, then refreshes it on an interval so key
//! rotation on the Identity side is picked up without a redeploy.
//!
//! Scope: plaintext **HTTP** only (the in-cluster east-west path — ADR-0011 keeps internal hops
//! on the mesh, which terminates TLS). If the JWKS must be fetched over HTTPS from outside the
//! mesh, supply the key directly via `APP__JWT__PUBLIC_KEY_PEM` instead (see `auth` module docs).

use std::sync::Arc;
use std::time::Duration;

use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use tokio_util::sync::CancellationToken;

use crate::auth::JwksKeySource;

/// Per-attempt HTTP timeout — a slow/hung Identity must not wedge the refresh loop.
const FETCH_TIMEOUT: Duration = Duration::from_secs(5);

type HttpClient = Client<HttpConnector, Empty<Bytes>>;

/// Fetch the JWKS document once and return its body as a string.
async fn fetch_once(client: &HttpClient, url: &str) -> anyhow::Result<String> {
    let uri: hyper::Uri = url.parse()?;
    if uri.scheme_str() != Some("http") {
        anyhow::bail!(
            "jwks_url must be http:// for the built-in fetcher (got {url:?}); use \
             APP__JWT__PUBLIC_KEY_PEM for HTTPS/out-of-mesh key delivery"
        );
    }
    let req = hyper::Request::builder()
        .method(hyper::Method::GET)
        .uri(uri)
        .header(hyper::header::ACCEPT, "application/json")
        .body(Empty::<Bytes>::new())?;

    let resp = tokio::time::timeout(FETCH_TIMEOUT, client.request(req))
        .await
        .map_err(|_| anyhow::anyhow!("JWKS fetch timed out after {FETCH_TIMEOUT:?}"))??;

    let status = resp.status();
    let body = tokio::time::timeout(FETCH_TIMEOUT, resp.into_body().collect())
        .await
        .map_err(|_| anyhow::anyhow!("JWKS body read timed out"))??
        .to_bytes();
    if !status.is_success() {
        anyhow::bail!("JWKS endpoint returned HTTP {status}");
    }
    Ok(String::from_utf8_lossy(&body).into_owned())
}

/// Fetch once and load the result into the shared cache, returning the resulting key count.
async fn refresh_once(
    client: &HttpClient,
    url: &str,
    source: &JwksKeySource,
) -> anyhow::Result<usize> {
    let json = fetch_once(client, url).await?;
    source.load_from_json(&json)?;
    Ok(source.len())
}

/// Do a best-effort initial load (so readiness reflects key availability at boot), then spawn a
/// background task that refreshes every `interval` until `cancel` fires.
///
/// Boot never blocks on Identity being up: a failed initial fetch is logged and the loop retries
/// on the normal cadence. Protected routes 401 only until the first successful load.
pub fn spawn(
    source: Arc<JwksKeySource>,
    url: String,
    interval: Duration,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();

    // Best-effort synchronous first load before we start serving.
    // (Kept inside the spawned task so `main` wiring stays non-blocking.)
    tokio::spawn(async move {
        match refresh_once(&client, &url, &source).await {
            Ok(n) => tracing::info!(keys = n, %url, "initial JWKS load succeeded"),
            Err(e) => tracing::warn!(error = %e, %url, "initial JWKS load failed; will retry"),
        }

        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await; // consume the immediate first tick (we just loaded)

        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    tracing::debug!("JWKS refresher shutting down");
                    return;
                }
                _ = ticker.tick() => {
                    match refresh_once(&client, &url, &source).await {
                        Ok(n) => tracing::debug!(keys = n, "JWKS refreshed"),
                        Err(e) => tracing::warn!(error = %e, %url, "JWKS refresh failed; keeping previous keys"),
                    }
                }
            }
        }
    })
}
