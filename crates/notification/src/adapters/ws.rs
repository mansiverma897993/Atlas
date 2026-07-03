//! The `GET /ws` WebSocket endpoint: authenticate on upgrade, then run the connection.
//!
//! ## Authentication (on upgrade)
//! The connection is authenticated *before* the HTTP→WS upgrade completes. A JWT is accepted
//! either as:
//! * the `?token=<jwt>` query parameter (primary, simplest for browsers), or
//! * the `Sec-WebSocket-Protocol` request header — browsers cannot set `Authorization` on a
//!   `WebSocket`, so the token is smuggled as a subprotocol. The first offered protocol is taken
//!   as the token and echoed back as the negotiated subprotocol so the handshake succeeds.
//!
//! The token is verified via the [`TokenVerifier`](crate::auth::TokenVerifier) seam; its `sub`
//! claim becomes the user id the connection registers under. A missing/invalid token yields
//! `401` and the socket is never opened.
//!
//! ## Connection lifecycle
//! On success we: register an [`mpsc`] sender in the hub, mark presence online, then run three
//! tasks — outbound (drain the channel to the socket), inbound (observe pings/close), and a
//! presence heartbeat. When any ends (client close, network error, shutdown) the others are
//! aborted and the connection is unregistered and marked offline.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::auth::TokenVerifier;
use crate::hub::ConnectionHub;
use crate::presence::Presence;

/// State injected into the WebSocket handler by the composition root.
#[derive(Clone)]
pub struct WsState {
    /// Live-connection registry.
    pub hub: ConnectionHub,
    /// Redis presence tracker.
    pub presence: Presence,
    /// Token verifier seam (RS256 in production).
    pub verifier: Arc<dyn TokenVerifier>,
    /// How often (seconds) to refresh a connection's presence TTL.
    pub heartbeat_seconds: u64,
}

/// Query string for the WS upgrade (`/ws?token=...`).
#[derive(Debug, Deserialize)]
pub struct WsQuery {
    /// Bearer JWT (alternative to the `Sec-WebSocket-Protocol` header).
    pub token: Option<String>,
}

/// `GET /ws` — authenticate then upgrade.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<WsQuery>,
    State(state): State<WsState>,
    headers: HeaderMap,
) -> Response {
    // The token may arrive via query param or the first offered subprotocol.
    let proto_token = first_subprotocol(&headers);
    let token = query.token.or_else(|| proto_token.clone());

    let Some(token) = token else {
        return (StatusCode::UNAUTHORIZED, "missing bearer token").into_response();
    };

    let claims = match state.verifier.verify(&token) {
        Ok(claims) => claims,
        Err(e) => {
            tracing::debug!(error = %e, "ws auth rejected");
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
    };
    let user = claims.sub;

    // If the token came in via a subprotocol, we must echo one back or the handshake fails.
    let ws = match proto_token {
        Some(proto) => ws.protocols([proto]),
        None => ws,
    };

    ws.on_upgrade(move |socket| handle_socket(socket, user, state))
}

/// Extract the first `Sec-WebSocket-Protocol` value (the token, by our convention).
fn first_subprotocol(headers: &HeaderMap) -> Option<String> {
    headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Drive a single authenticated connection until it closes.
async fn handle_socket(socket: WebSocket, user: String, state: WsState) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    let conn_id = state.hub.register(user.clone(), tx).await;
    if let Err(e) = state.presence.mark_online(&user).await {
        tracing::warn!(error = %e, %user, "failed to mark presence online");
    }
    tracing::info!(%user, conn_id, "ws connection opened");

    // Outbound: forward hub-enqueued messages to the socket.
    let mut send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sink.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    // Inbound: we don't accept client commands; just observe pings (axum auto-pongs) and close.
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = stream.next().await {
            if matches!(msg, Message::Close(_)) {
                break;
            }
        }
    });

    // Heartbeat: keep the presence TTL fresh while connected.
    let hb_presence = state.presence.clone();
    let hb_user = user.clone();
    let hb_secs = state.heartbeat_seconds.max(1);
    let heartbeat_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(hb_secs));
        interval.tick().await; // first tick fires immediately; skip it
        loop {
            interval.tick().await;
            if let Err(e) = hb_presence.heartbeat(&hb_user).await {
                tracing::warn!(error = %e, user = %hb_user, "presence heartbeat failed");
            }
        }
    });

    // Whichever of send/recv finishes first, tear the rest down.
    tokio::select! {
        _ = &mut send_task => recv_task.abort(),
        _ = &mut recv_task => send_task.abort(),
    }
    heartbeat_task.abort();

    state.hub.unregister(&user, conn_id).await;
    if let Err(e) = state.presence.mark_offline(&user).await {
        tracing::warn!(error = %e, %user, "failed to mark presence offline");
    }
    tracing::info!(%user, conn_id, "ws connection closed");
}
