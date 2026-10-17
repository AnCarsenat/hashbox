//! hashbox — small tools in one binary:
//!
//! 1. Data-in-link sharing (text pastes and URL links). The content lives in the
//!    URL fragment (`#...`), which browsers never send to the server. Any hashbox
//!    instance (or any copy of the page) can open any link; the server stores nothing.
//! 2. Webhook fan-out relay. Anything sent to `/hook/<channel>` is rebroadcast,
//!    live, to every client listening on `/listen/<channel>` (Server-Sent Events).
//! 3. `/api/isgd`: a stateless relay to the is.gd shortener (via the system `curl`),
//!    so the page never has to load third-party scripts. Nothing is stored here.
//!
//! Everything is kept in RAM and bounded: listener counts, webhook history bytes,
//! request bodies and per-channel publish rates all have limits (see `Limits`).

use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri},
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
const BROADCAST_CAP: usize = 64;
/// is.gd asks for low-volume use; stay well under it.
const ISGD_MAX_PER_MINUTE: usize = 20;

/// Resource limits, each overridable with an environment variable.
struct Limits {
    max_body: usize,
    max_channels: usize,
    max_listeners: usize,
    max_listeners_per_channel: usize,
    history_budget: usize,
    hooks_per_minute: usize,
}

fn env_num(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(v) => v
            .trim()
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a whole number, got {v:?}")),
        Err(_) => default,
    }
}

impl Limits {
    fn from_env() -> Self {
        Limits {
            max_body: env_num("HASHBOX_MAX_BODY_KB", 256) * 1024,
            max_channels: env_num("HASHBOX_MAX_CHANNELS", 1000),
            max_listeners: env_num("HASHBOX_MAX_LISTENERS", 200),
            max_listeners_per_channel: env_num("HASHBOX_MAX_LISTENERS_PER_CHANNEL", 20),
            history_budget: env_num("HASHBOX_HISTORY_MB", 8) * 1024 * 1024,
            hooks_per_minute: env_num("HASHBOX_HOOKS_PER_MINUTE", 120),
        }
    }
}

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

impl HookEvent {
    /// Approximate RAM held by this event, for the history budget.
    fn size(&self) -> usize {
        256 + self.method.len()
            + self.path.len()
            + self.headers.iter().map(|(k, v)| k.len() + v.len() + 48).sum::<usize>()
            + self.body.as_ref().map_or(0, String::len)
            + self.body_base64.as_ref().map_or(0, String::len)
    }
}

struct Channel {
    tx: broadcast::Sender<Arc<HookEvent>>,
    history: VecDeque<Arc<HookEvent>>,
    /// Bytes held by `history`.
    bytes: usize,
    /// Times of recent publishes, for the per-channel rate limit.
    recent: VecDeque<Instant>,
}

impl Channel {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAP);
        Channel { tx, history: VecDeque::new(), bytes: 0, recent: VecDeque::new() }
    }
}

/// Channels plus the bookkeeping for the global history budget, under one lock.
#[derive(Default)]
struct Registry {
    channels: HashMap<String, Channel>,
    /// Bytes held by all channel histories together.
    total_bytes: usize,
    /// Every stored event in arrival order (channel, id), for global oldest-first eviction.
    /// Entries can be stale (already evicted per channel); they are skipped when popped.
    order: VecDeque<(String, u64)>,
}

impl Registry {
    /// Get or create a channel. Returns None when the channel limit is reached.
    fn channel(&mut self, name: &str, max_channels: usize) -> Option<&mut Channel> {
        if !self.channels.contains_key(name) {
            if self.channels.len() >= max_channels {
                self.prune_idle();
                if self.channels.len() >= max_channels {
                    return None;
                }
            }
            self.channels.insert(name.to_owned(), Channel::new());
        }
        self.channels.get_mut(name)
    }

    /// Drop channels that nobody listens to, freeing their history.
    fn prune_idle(&mut self) {
        let mut freed = 0;
        self.channels.retain(|_, c| {
            let keep = c.tx.receiver_count() > 0;
            if !keep {
                freed += c.bytes;
            }
            keep
        });
        self.total_bytes -= freed;
        let channels = &self.channels;
        self.order.retain(|(name, _)| channels.contains_key(name));
    }

    /// Record an event in a channel's history and enforce both history limits.
    fn remember(&mut self, name: &str, event: Arc<HookEvent>, budget: usize) {
        let size = event.size();
        let Some(c) = self.channels.get_mut(name) else { return };
        c.history.push_back(event.clone());
        c.bytes += size;
        self.total_bytes += size;
        self.order.push_back((name.to_owned(), event.id));
        if c.history.len() > HISTORY {
            let old = c.history.pop_front().unwrap();
            let s = old.size();
            c.bytes -= s;
            self.total_bytes -= s;
        }
        // Over the global budget: evict the oldest events across all channels.
        while self.total_bytes > budget {
            let Some((ch, id)) = self.order.pop_front() else { break };
            if let Some(c) = self.channels.get_mut(&ch) {
                if c.history.front().is_some_and(|e| e.id == id) {
                    let s = c.history.pop_front().unwrap().size();
                    c.bytes -= s;
                    self.total_bytes -= s;
                }
            }
        }
        // Keep the order queue from growing with stale entries.
        if self.order.len() > 4 * HISTORY * self.channels.len().max(1) {
            let channels = &self.channels;
            self.order.retain(|(name, id)| {
                channels.get(name).is_some_and(|c| c.history.iter().any(|e| e.id == *id))
            });
        }
    }
}

struct AppState {
    limits: Limits,
    registry: Mutex<Registry>,
    next_id: AtomicU64,
    listeners: AtomicUsize,
    /// Times of recent is.gd calls, for a simple rate limit.
    isgd_calls: Mutex<VecDeque<Instant>>,
    /// Flips to true on shutdown so open listener streams end and restarts are quick.
    shutdown: tokio::sync::watch::Sender<bool>,
}

type Shared = Arc<AppState>;

/// Counts one open listener; releases its slot when the stream ends.
struct ListenerSlot(Shared);

impl Drop for ListenerSlot {
    fn drop(&mut self) {
        self.0.listeners.fetch_sub(1, Ordering::Relaxed);
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

/// Headers added by proxies or browsers that say nothing about the webhook itself
/// (client IPs, proxy hops, cookies). They are not rebroadcast.
fn skip_header(name: &str) -> bool {
    matches!(
        name,
        "x-forwarded-for"
            | "x-forwarded-host"
            | "x-forwarded-proto"
            | "x-real-ip"
            | "forwarded"
            | "via"
            | "cf-connecting-ip"
            | "cf-ipcountry"
            | "cf-ray"
            | "cdn-loop"
            | "true-client-ip"
            | "cookie"
    )
}

fn too_many(msg: &'static str) -> Response {
    (StatusCode::TOO_MANY_REQUESTS, [(header::RETRY_AFTER, "60")], msg).into_response()
}

async fn index() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            // The page builds everything from the fragment; keep it from leaking via Referer.
            (header::REFERRER_POLICY, "no-referrer"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (header::CACHE_CONTROL, "no-cache"),
            (
                header::CONTENT_SECURITY_POLICY,
                "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; \
                 connect-src 'self' https://api.github.com; img-src 'self' data:; \
                 base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
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

    let limits = &state.limits;
    let mut reg = state.registry.lock().unwrap();
    let Some(c) = reg.channel(channel, limits.max_channels) else {
        return (StatusCode::SERVICE_UNAVAILABLE, "too many channels\n").into_response();
    };

    let now = Instant::now();
    while c
        .recent
        .front()
        .is_some_and(|t| now.duration_since(*t) > Duration::from_secs(60))
    {
        c.recent.pop_front();
    }
    if c.recent.len() >= limits.hooks_per_minute {
        return too_many("too many webhooks on this channel, slow down\n");
    }
    c.recent.push_back(now);

    let delivered = c.tx.send(event.clone()).unwrap_or(0);
    reg.remember(channel, event.clone(), limits.history_budget);
    drop(reg);

    Json(HookReply {
        ok: true,
        channel: channel.to_owned(),
        id: event.id,
        delivered_to: delivered,
    })
    .into_response()
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

    // Reserve a global listener slot first; it is released when the stream is dropped.
    if state.listeners.fetch_add(1, Ordering::Relaxed) >= state.limits.max_listeners {
        state.listeners.fetch_sub(1, Ordering::Relaxed);
        return too_many("too many listeners on this server\n");
    }
    let slot = ListenerSlot(state.clone());

    // Subscribe and copy history under the same lock, so no event is missed or duplicated.
    let (rx, past) = {
        let mut reg = state.registry.lock().unwrap();
        let Some(c) = reg.channel(&channel, state.limits.max_channels) else {
            return (StatusCode::SERVICE_UNAVAILABLE, "too many channels\n").into_response();
        };
        if c.tx.receiver_count() >= state.limits.max_listeners_per_channel {
            return too_many("too many listeners on this channel\n");
        }
        let skip = c.history.len().saturating_sub(replay);
        let past: Vec<_> = c.history.iter().skip(skip).cloned().collect();
        (c.tx.subscribe(), past)
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

    // The slot lives inside the stream, so it is released when the client disconnects.
    // The stream also ends when the server shuts down, so restarts don't hang.
    let mut stop = state.shutdown.subscribe();
    let stopped = async move {
        let _ = stop.wait_for(|down| *down).await;
    };
    let s = ready
        .chain(past)
        .chain(live)
        .map(move |e| {
            let _slot = &slot;
            e
        })
        .take_until(Box::pin(stopped));
    let s: std::pin::Pin<Box<dyn Stream<Item = Result<Event, std::convert::Infallible>> + Send>> =
        Box::pin(s);

    let mut res = Sse::new(s)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response();
    // Stop reverse proxies (nginx) from buffering the stream.
    res.headers_mut()
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    res
}

#[derive(Deserialize)]
struct ShortenQuery {
    url: String,
}

fn json_error(status: StatusCode, msg: &str) -> Response {
    (status, Json(serde_json::json!({ "error": msg }))).into_response()
}

/// Shorten a URL with is.gd. hashbox keeps no record of it.
async fn isgd(State(state): State<Shared>, Query(q): Query<ShortenQuery>) -> Response {
    let url = q.url.trim();
    if !(url.starts_with("https://") || url.starts_with("http://")) || url.len() > 5000 {
        return json_error(StatusCode::BAD_REQUEST, "only http(s) URLs up to 5000 characters");
    }

    {
        let mut calls = state.isgd_calls.lock().unwrap();
        let now = Instant::now();
        while calls
            .front()
            .is_some_and(|t| now.duration_since(*t) > Duration::from_secs(60))
        {
            calls.pop_front();
        }
        if calls.len() >= ISGD_MAX_PER_MINUTE {
            return json_error(StatusCode::TOO_MANY_REQUESTS, "too many shortenings, wait a minute");
        }
        calls.push_back(now);
    }

    let endpoint = std::env::var("HASHBOX_ISGD_ENDPOINT")
        .unwrap_or_else(|_| "https://is.gd/create.php".into());
    // Arguments are passed directly (no shell). The URL was checked to start with
    // http(s)://, so curl reads `url=...` as name=content, never as a file path.
    let run = tokio::process::Command::new("curl")
        .args(["-sS", "--max-time", "10", "--max-filesize", "65536", "-G", "--data", "format=json", "--data-urlencode"])
        .arg(format!("url={url}"))
        .arg(&endpoint)
        .kill_on_drop(true)
        .output();

    let out = match tokio::time::timeout(Duration::from_secs(15), run).await {
        Ok(Ok(out)) => out,
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            return json_error(StatusCode::BAD_GATEWAY, "curl is not installed on the server")
        }
        Ok(Err(_)) | Err(_) => return json_error(StatusCode::BAD_GATEWAY, "is.gd did not answer"),
    };

    let reply: serde_json::Value = match serde_json::from_slice(&out.stdout) {
        Ok(v) => v,
        Err(_) => return json_error(StatusCode::BAD_GATEWAY, "is.gd is unreachable from the server"),
    };
    match reply.get("shorturl").and_then(|s| s.as_str()) {
        Some(short) if short.starts_with("https://") && short.len() < 200 => {
            Json(serde_json::json!({ "shorturl": short })).into_response()
        }
        _ => {
            let msg = reply
                .get("errormessage")
                .and_then(|m| m.as_str())
                .unwrap_or("is.gd refused this URL");
            let msg: String = msg.chars().take(200).collect();
            json_error(StatusCode::BAD_GATEWAY, &msg)
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = term => {} }
}

#[tokio::main]
async fn main() {
    let bind: SocketAddr = std::env::var("HASHBOX_BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".into())
        .parse()
        .expect("HASHBOX_BIND must look like 127.0.0.1:8080");

    let limits = Limits::from_env();
    let max_body = limits.max_body;
    println!(
        "hashbox: body ≤ {} KB, ≤ {} listeners ({} per channel), history ≤ {} MB, ≤ {} webhooks/min per channel",
        limits.max_body / 1024,
        limits.max_listeners,
        limits.max_listeners_per_channel,
        limits.history_budget / (1024 * 1024),
        limits.hooks_per_minute
    );

    let state: Shared = Arc::new(AppState {
        limits,
        registry: Mutex::new(Registry::default()),
        next_id: AtomicU64::new(0),
        listeners: AtomicUsize::new(0),
        isgd_calls: Mutex::new(VecDeque::new()),
        shutdown: tokio::sync::watch::channel(false).0,
    });
    let stopper = state.clone();

    let app = Router::new()
        .route("/", get(index))
        .route("/w/{channel}", get(index))
        .route("/healthz", get(health))
        .route("/hook/{channel}", any(hook))
        .route("/hook/{channel}/{*rest}", any(hook))
        .route("/listen/{channel}", get(listen))
        .route("/api/isgd", get(isgd))
        .layer(DefaultBodyLimit::max(max_body))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .unwrap_or_else(|e| panic!("cannot bind {bind}: {e}"));
    println!("hashbox listening on http://{bind}");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            stopper.shutdown.send_replace(true);
        })
        .await
        .unwrap();
}
