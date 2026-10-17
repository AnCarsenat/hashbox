//! hashbox — two tools in one small binary:
//!
//! 1. Data-in-link sharing (text pastes and URL links). The content lives in the
//!    URL fragment (`#...`), which browsers never send to the server. Any hashbox
//!    instance (or any copy of the page) can open any link; the server stores nothing.
//! 2. Webhook fan-out relay. Anything sent to `/hook/<channel>` is rebroadcast,
//!    live, to every client listening on `/listen/<channel>` (Server-Sent Events).

use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{header, HeaderMap, Method, StatusCode, Uri},
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Response,
    },
    routing::{any, get},
    Json, Router,
};
use base64::Engine;
use futures_util::stream::{self, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};

const INDEX_HTML: &str = include_str!("../static/index.html");

/// Events kept per channel so a listener that connects late can see recent ones.
const HISTORY: usize = 50;
/// Per-channel broadcast buffer; slow listeners skip events beyond this.
const BROADCAST_CAP: usize = 256;
/// Upper bound on live channels, to keep memory bounded on a Pi.
const MAX_CHANNELS: usize = 1000;
/// Maximum accepted webhook body.
const MAX_BODY: usize = 1024 * 1024;

#[derive(Clone, Serialize)]
struct HookEvent {
    id: u64,
    /// Unix time in milliseconds.
    time: u64,
    method: String,
    /// Path after `/hook/<channel>` (may be empty), plus query string.
    path: String,
    headers: Vec<(String, String)>,
    /// Body as text when it is valid UTF-8.
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<String>,
    /// Body as base64 when it is binary.
    #[serde(skip_serializing_if = "Option::is_none")]
    body_base64: Option<String>,
}

struct Channel {
    tx: broadcast::Sender<Arc<HookEvent>>,
    history: VecDeque<Arc<HookEvent>>,
}

#[derive(Default)]
struct AppState {
    channels: Mutex<HashMap<String, Channel>>,
    next_id: AtomicU64,
}

type Shared = Arc<AppState>;

impl AppState {
    /// Get or create a channel. Returns None when the channel limit is reached.
    fn with_channel<T>(&self, name: &str, f: impl FnOnce(&mut Channel) -> T) -> Option<T> {
        let mut map = self.channels.lock().unwrap();
        if !map.contains_key(name) {
            if map.len() >= MAX_CHANNELS {
                // Free channels nobody listens to before giving up.
                map.retain(|_, c| c.tx.receiver_count() > 0);
                if map.len() >= MAX_CHANNELS {
                    return None;
                }
            }
            let (tx, _) = broadcast::channel(BROADCAST_CAP);
            map.insert(
                name.to_owned(),
                Channel {
                    tx,
                    history: VecDeque::with_capacity(HISTORY),
                },
            );
        }
        Some(f(map.get_mut(name).unwrap()))
    }
}

fn valid_channel(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Headers that reveal infrastructure details rather than describing the webhook.
fn skip_header(name: &str) -> bool {
    matches!(
        name,
        "x-forwarded-for" | "x-real-ip" | "forwarded" | "cf-connecting-ip" | "true-client-ip"
    )
}

async fn index() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            // The page builds everything from the fragment; keep it from leaking via Referer.
            (header::REFERRER_POLICY, "no-referrer"),
            (
                header::CONTENT_SECURITY_POLICY,
                "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; \
                 connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'",
            ),
        ],
        Html(INDEX_HTML),
    )
}

async fn health() -> &'static str {
    "ok"
}

#[derive(Serialize)]
struct HookReply {
    ok: bool,
    channel: String,
    id: u64,
    delivered_to: usize,
}

async fn hook(
    State(state): State<Shared>,
    Path(params): Path<HashMap<String, String>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let channel = params.get("channel").map(String::as_str).unwrap_or("");
    if !valid_channel(channel) {
        return (StatusCode::BAD_REQUEST, "invalid channel name\n").into_response();
    }

    let sub = params
        .get("rest")
        .map(|r| format!("/{r}"))
        .unwrap_or_default();
    let path = match uri.query() {
        Some(q) => format!("{sub}?{q}"),
        None => sub,
    };

    let headers = headers
        .iter()
        .filter(|(k, _)| !skip_header(k.as_str()))
        .map(|(k, v)| (k.to_string(), String::from_utf8_lossy(v.as_bytes()).into_owned()))
        .collect();

    let (body_text, body_b64) = if body.is_empty() {
        (None, None)
    } else {
        match std::str::from_utf8(&body) {
            Ok(s) => (Some(s.to_owned()), None),
            Err(_) => (
                None,
                Some(base64::engine::general_purpose::STANDARD.encode(&body)),
            ),
        }
    };

    let event = Arc::new(HookEvent {
        id: state.next_id.fetch_add(1, Ordering::Relaxed) + 1,
        time: now_ms(),
        method: method.to_string(),
        path,
        headers,
        body: body_text,
        body_base64: body_b64,
    });

    let delivered = state.with_channel(channel, |c| {
        if c.history.len() == HISTORY {
            c.history.pop_front();
        }
        c.history.push_back(event.clone());
        c.tx.send(event.clone()).unwrap_or(0)
    });

    match delivered {
        Some(n) => Json(HookReply {
            ok: true,
            channel: channel.to_owned(),
            id: event.id,
            delivered_to: n,
        })
        .into_response(),
        None => (StatusCode::SERVICE_UNAVAILABLE, "too many channels\n").into_response(),
    }
}

#[derive(Deserialize)]
struct ListenQuery {
    /// Number of recent events to send first (0..=50). Default 0.
    replay: Option<usize>,
}

async fn listen(
    State(state): State<Shared>,
    Path(channel): Path<String>,
    Query(q): Query<ListenQuery>,
) -> Response {
    if !valid_channel(&channel) {
        return (StatusCode::BAD_REQUEST, "invalid channel name\n").into_response();
    }
    let replay = q.replay.unwrap_or(0).min(HISTORY);

    // Subscribe and copy history under the same lock, so no event is missed or duplicated.
    let Some((rx, past)) = state.with_channel(&channel, |c| {
        let skip = c.history.len().saturating_sub(replay);
        let past: Vec<_> = c.history.iter().skip(skip).cloned().collect();
        (c.tx.subscribe(), past)
    }) else {
        return (StatusCode::SERVICE_UNAVAILABLE, "too many channels\n").into_response();
    };

    let to_sse = |e: Arc<HookEvent>| -> Result<Event, std::convert::Infallible> {
        Ok(Event::default()
            .event("hook")
            .id(e.id.to_string())
            .data(serde_json::to_string(&*e).unwrap_or_default()))
    };

    let ready = stream::once(async { Ok(Event::default().event("ready").data("{}")) });
    let past = stream::iter(past).map(to_sse);
    let live = BroadcastStream::new(rx).filter_map(move |r| async move {
        match r {
            Ok(e) => Some(to_sse(e)),
            // A slow listener fell behind; tell it how many it missed and carry on.
            Err(BroadcastStreamRecvError::Lagged(n)) => {
                Some(Ok(Event::default().event("lagged").data(n.to_string())))
            }
        }
    });

    let s: std::pin::Pin<Box<dyn Stream<Item = Result<Event, std::convert::Infallible>> + Send>> =
        Box::pin(ready.chain(past).chain(live));

    let mut res = Sse::new(s)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response();
    // Stop reverse proxies (nginx) from buffering the stream.
    res.headers_mut()
        .insert("x-accel-buffering", "no".parse().unwrap());
    res
}

#[tokio::main]
async fn main() {
    let bind: SocketAddr = std::env::var("HASHBOX_BIND")
        .unwrap_or_else(|_| "0.0.0.0:8080".into())
        .parse()
        .expect("HASHBOX_BIND must look like 0.0.0.0:8080");

    let state: Shared = Arc::new(AppState::default());

    let app = Router::new()
        .route("/", get(index))
        .route("/w/{channel}", get(index))
        .route("/healthz", get(health))
        .route("/hook/{channel}", any(hook))
        .route("/hook/{channel}/{*rest}", any(hook))
        .route("/listen/{channel}", get(listen))
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .unwrap_or_else(|e| panic!("cannot bind {bind}: {e}"));
    println!("hashbox listening on http://{bind}");

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .unwrap();
}
