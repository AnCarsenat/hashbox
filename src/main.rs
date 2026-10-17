//! hashbox — small sharing tools in one binary, with nothing stored on disk:
//!
//! - Text pastes and URL links whose data lives in the link itself (the URL
//!   fragment, which browsers never send to the server). Links open behind a
//!   click-through page that shows the real destination; nothing auto-redirects.
//! - Webhook fan-out: `/hook/<channel>` is rebroadcast live to `/listen/<channel>`.
//! - Chat rooms joined with a code, and a clipboard (one-shot drops with a code,
//!   or live sync between devices). RAM only; rooms vanish after everyone leaves.
//! - `/api/isgd`: a rate-limited relay to the is.gd shortener (via the system `curl`).
//!
//! Every feature is bounded (see `Limits` and the per-IP limiters) and can be
//! switched off with HASHBOX_DISABLE, since the operator can't moderate content.

mod drops;
mod rooms;

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Bytes,
    extract::{ConnectInfo, DefaultBodyLimit, Path, Query, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri},
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Response,
    },
    routing::{any, get, post},
    Json, Router,
};
use base64::Engine;
use futures_util::stream::{self, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};

use drops::Drops;
use rooms::{IpLimiter, Item, Registry, HISTORY};

const INDEX_HTML: &str = include_str!("../static/index.html");

/// is.gd asks for low-volume use; stay well under it overall.
const ISGD_MAX_PER_MINUTE: usize = 20;
const CHAT_MAX_CHARS: usize = 1000;
const NICK_MAX_CHARS: usize = 24;
const SYNC_MAX_BYTES: usize = 64 * 1024;
const DROP_MAX_BYTES: usize = 64 * 1024;
/// Chat and sync rooms are deleted this long after their last listener leaves.
const ROOM_IDLE: Duration = Duration::from_secs(120);

// ---------------------------------------------------------------- configuration

/// Resource limits, each overridable with an environment variable.
struct Limits {
    max_body: usize,
    max_rooms: usize,
    max_listeners: usize,
    max_listeners_per_room: usize,
    max_listeners_per_ip: usize,
    history_budget: usize,
    hooks_per_minute: usize,
    clipboard_budget: usize,
    drop_minutes: u64,
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

fn env_flag(name: &str) -> bool {
    matches!(
        std::env::var(name).map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Ok("1" | "true" | "yes" | "on")
    )
}

impl Limits {
    fn from_env() -> Self {
        Limits {
            max_body: env_num("HASHBOX_MAX_BODY_KB", 256) * 1024,
            max_rooms: env_num("HASHBOX_MAX_CHANNELS", 1000),
            max_listeners: env_num("HASHBOX_MAX_LISTENERS", 200),
            max_listeners_per_room: env_num("HASHBOX_MAX_LISTENERS_PER_CHANNEL", 20),
            max_listeners_per_ip: env_num("HASHBOX_MAX_LISTENERS_PER_IP", 10),
            history_budget: env_num("HASHBOX_HISTORY_MB", 8) * 1024 * 1024,
            hooks_per_minute: env_num("HASHBOX_HOOKS_PER_MINUTE", 120),
            clipboard_budget: env_num("HASHBOX_CLIPBOARD_MB", 4) * 1024 * 1024,
            drop_minutes: env_num("HASHBOX_DROP_MINUTES", 10).clamp(1, 24 * 60) as u64,
        }
    }
}

/// Features the operator can switch off: HASHBOX_DISABLE=chat,clipboard,isgd,hooks
#[derive(Clone, Copy, Serialize)]
struct Features {
    chat: bool,
    clipboard: bool,
    isgd: bool,
    hooks: bool,
}

impl Features {
    fn from_env() -> Self {
        let off = std::env::var("HASHBOX_DISABLE").unwrap_or_default().to_ascii_lowercase();
        let off: Vec<&str> = off.split(',').map(str::trim).collect();
        for f in &off {
            if !f.is_empty() && !["chat", "clipboard", "isgd", "hooks"].contains(f) {
                panic!("HASHBOX_DISABLE: unknown feature {f:?} (use chat, clipboard, isgd, hooks)");
            }
        }
        Features {
            chat: !off.contains(&"chat"),
            clipboard: !off.contains(&"clipboard"),
            isgd: !off.contains(&"isgd"),
            hooks: !off.contains(&"hooks"),
        }
    }
}

// ---------------------------------------------------------------- state

struct Limiters {
    isgd: IpLimiter,
    chat: IpLimiter,
    sync: IpLimiter,
    drop_put: IpLimiter,
    drop_get: IpLimiter,
    join: IpLimiter,
}

struct AppState {
    limits: Limits,
    features: Features,
    contact: Option<String>,
    trust_proxy: bool,
    registry: Mutex<Registry>,
    drops: Mutex<Drops>,
    next_id: AtomicU64,
    listeners: AtomicUsize,
    listeners_by_ip: Mutex<HashMap<IpAddr, usize>>,
    per_ip: Limiters,
    /// Times of recent is.gd calls, for the global cap.
    isgd_calls: Mutex<std::collections::VecDeque<Instant>>,
    /// Flips to true on shutdown so open streams end and restarts are quick.
    shutdown: tokio::sync::watch::Sender<bool>,
}

type Shared = Arc<AppState>;

/// Holds one listener's place in the global and per-IP counts until its stream ends.
struct ListenerSlot {
    state: Shared,
    ip: IpAddr,
}

impl Drop for ListenerSlot {
    fn drop(&mut self) {
        self.state.listeners.fetch_sub(1, Ordering::Relaxed);
        let mut by_ip = self.state.listeners_by_ip.lock().unwrap();
        if let Some(n) = by_ip.get_mut(&self.ip) {
            *n -= 1;
            if *n == 0 {
                by_ip.remove(&self.ip);
            }
        }
    }
}

// ---------------------------------------------------------------- helpers

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The client's address, grouped so one IPv6 user can't dodge limits by rotating
/// through their /64. X-Forwarded-For is used only with HASHBOX_TRUST_PROXY=1.
fn client_ip(state: &AppState, peer: SocketAddr, headers: &HeaderMap) -> IpAddr {
    let mut ip = peer.ip();
    if state.trust_proxy {
        if let Some(fwd) = headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(',').next())
            .and_then(|v| v.trim().parse::<IpAddr>().ok())
        {
            ip = fwd;
        }
    }
    match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => {
                let s = v6.segments();
                IpAddr::V6(Ipv6Addr::new(s[0], s[1], s[2], s[3], 0, 0, 0, 0))
            }
        },
        v4 => v4,
    }
}

fn json_error(status: StatusCode, msg: &str) -> Response {
    let mut res = (status, Json(serde_json::json!({ "error": msg }))).into_response();
    if status == StatusCode::TOO_MANY_REQUESTS {
        res.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("60"));
    }
    res
}

fn disabled() -> Response {
    json_error(StatusCode::NOT_FOUND, "this feature is turned off on this server")
}

fn valid_channel(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Chat and sync codes: 4–32 lowercase letters, digits or dashes.
fn room_code(raw: &str) -> Option<String> {
    let code = raw.trim().to_ascii_lowercase();
    ((4..=32).contains(&code.len())
        && code.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'))
    .then_some(code)
}

/// Remove control characters (keeping newlines and tabs if `multiline`) and trim.
fn clean_text(s: &str, multiline: bool) -> String {
    s.chars()
        .filter(|c| !c.is_control() || (multiline && (*c == '\n' || *c == '\t')))
        .collect::<String>()
        .trim()
        .to_owned()
}

/// Headers added by proxies or browsers that say nothing about the webhook itself.
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

// ---------------------------------------------------------------- rooms: publish & stream

/// Broadcast an item to a room and keep it in history. Returns (id, delivered_to).
fn publish(
    state: &Shared,
    key: &str,
    kind: &'static str,
    keep: usize,
    make_json: impl FnOnce(u64) -> String,
) -> Result<(u64, usize), Response> {
    let id = state.next_id.fetch_add(1, Ordering::Relaxed) + 1;
    let item = Arc::new(Item { id, kind, json: make_json(id) });
    let limits = &state.limits;
    let mut reg = state.registry.lock().unwrap();
    let Some(room) = reg.room(key, limits.max_rooms) else {
        return Err(json_error(StatusCode::SERVICE_UNAVAILABLE, "server is busy, try again later"));
    };
    if !room.allow_publish(limits.hooks_per_minute) {
        return Err(json_error(StatusCode::TOO_MANY_REQUESTS, "too many messages here, slow down"));
    }
    let delivered = room.tx.send(item.clone()).unwrap_or(0);
    reg.remember(key, item, limits.history_budget, keep);
    Ok((id, delivered))
}

fn presence_item(online: usize) -> Arc<Item> {
    Arc::new(Item { id: 0, kind: "presence", json: format!("{{\"online\":{online}}}") })
}

/// Open a Server-Sent Events stream on a room, replaying up to `replay` stored items.
fn stream_room(state: &Shared, ip: IpAddr, key: String, replay: usize, presence: bool) -> Response {
    if !state.per_ip.join.allow(ip) {
        return json_error(StatusCode::TOO_MANY_REQUESTS, "too many connections, wait a minute");
    }
    // Reserve global and per-IP slots; released when the stream is dropped.
    if state.listeners.fetch_add(1, Ordering::Relaxed) >= state.limits.max_listeners {
        state.listeners.fetch_sub(1, Ordering::Relaxed);
        return json_error(StatusCode::TOO_MANY_REQUESTS, "server is full, try again later");
    }
    {
        let mut by_ip = state.listeners_by_ip.lock().unwrap();
        let n = by_ip.entry(ip).or_insert(0);
        if *n >= state.limits.max_listeners_per_ip {
            drop(by_ip);
            state.listeners.fetch_sub(1, Ordering::Relaxed);
            return json_error(StatusCode::TOO_MANY_REQUESTS, "too many open connections from your network");
        }
        *n += 1;
    }
    let slot = ListenerSlot { state: state.clone(), ip };

    // Subscribe and copy history under one lock, so nothing is missed or duplicated.
    let (rx, past) = {
        let mut reg = state.registry.lock().unwrap();
        let Some(room) = reg.room(&key, state.limits.max_rooms) else {
            return json_error(StatusCode::SERVICE_UNAVAILABLE, "server is busy, try again later");
        };
        if room.tx.receiver_count() >= state.limits.max_listeners_per_room {
            return json_error(StatusCode::TOO_MANY_REQUESTS, "this room is full");
        }
        let skip = room.history.len().saturating_sub(replay);
        let past: Vec<_> = room.history.iter().skip(skip).cloned().collect();
        let rx = room.tx.subscribe();
        if presence {
            let _ = room.tx.send(presence_item(room.tx.receiver_count()));
        }
        (rx, past)
    };

    let to_sse = |e: Arc<Item>| -> Result<Event, std::convert::Infallible> {
        let ev = Event::default().event(e.kind).data(e.json.as_str());
        Ok(if e.id > 0 { ev.id(e.id.to_string()) } else { ev })
    };
    let ready = stream::once(async { Ok(Event::default().event("ready").data("{}")) });
    let past = stream::iter(past).map(to_sse);
    let live = BroadcastStream::new(rx).filter_map(move |r| async move {
        match r {
            Ok(e) => Some(to_sse(e)),
            Err(BroadcastStreamRecvError::Lagged(n)) => {
                Some(Ok(Event::default().event("lagged").data(n.to_string())))
            }
        }
    });

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
    res.headers_mut()
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    res
}

#[derive(Deserialize)]
struct ReplayQuery {
    replay: Option<usize>,
}

// ---------------------------------------------------------------- pages & config

async fn index() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            // Links carry their data in the fragment; don't leak it via Referer.
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

async fn config(State(state): State<Shared>) -> Response {
    Json(serde_json::json!({
        "features": state.features,
        "contact": state.contact,
        "drop_minutes": state.limits.drop_minutes,
    }))
    .into_response()
}

// ---------------------------------------------------------------- webhooks

#[derive(Serialize)]
struct HookEvent<'a> {
    id: u64,
    time: u64,
    method: &'a str,
    path: &'a str,
    headers: &'a [(String, String)],
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body_base64: Option<String>,
}

async fn hook(
    State(state): State<Shared>,
    Path(params): Path<HashMap<String, String>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !state.features.hooks {
        return disabled();
    }
    let channel = params.get("channel").map(String::as_str).unwrap_or("");
    if !valid_channel(channel) {
        return (StatusCode::BAD_REQUEST, "invalid channel name\n").into_response();
    }
    let sub = params.get("rest").map(|r| format!("/{r}")).unwrap_or_default();
    let path = match uri.query() {
        Some(q) => format!("{sub}?{q}"),
        None => sub,
    };
    let headers: Vec<(String, String)> = headers
        .iter()
        .filter(|(k, _)| !skip_header(k.as_str()))
        .map(|(k, v)| (k.to_string(), String::from_utf8_lossy(v.as_bytes()).into_owned()))
        .collect();
    let (text, b64) = match std::str::from_utf8(&body) {
        _ if body.is_empty() => (None, None),
        Ok(s) => (Some(s), None),
        Err(_) => (None, Some(base64::engine::general_purpose::STANDARD.encode(&body))),
    };
    let time = now_ms();
    let method = method.to_string();
    let result = publish(&state, &format!("h/{channel}"), "hook", HISTORY, |id| {
        serde_json::to_string(&HookEvent {
            id,
            time,
            method: &method,
            path: &path,
            headers: &headers,
            body: text,
            body_base64: b64.clone(),
        })
        .unwrap_or_default()
    });
    match result {
        Ok((id, delivered)) => Json(serde_json::json!({
            "ok": true, "channel": channel, "id": id, "delivered_to": delivered
        }))
        .into_response(),
        Err(res) => res,
    }
}

async fn listen(
    State(state): State<Shared>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(channel): Path<String>,
    Query(q): Query<ReplayQuery>,
) -> Response {
    if !state.features.hooks {
        return disabled();
    }
    if !valid_channel(&channel) {
        return (StatusCode::BAD_REQUEST, "invalid channel name\n").into_response();
    }
    let ip = client_ip(&state, peer, &headers);
    stream_room(&state, ip, format!("h/{channel}"), q.replay.unwrap_or(0).min(HISTORY), false)
}

// ---------------------------------------------------------------- chat

#[derive(Deserialize)]
struct ChatPost {
    nick: String,
    text: String,
}

async fn chat_send(
    State(state): State<Shared>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(code): Path<String>,
    Json(msg): Json<ChatPost>,
) -> Response {
    if !state.features.chat {
        return disabled();
    }
    let Some(code) = room_code(&code) else {
        return json_error(StatusCode::BAD_REQUEST, "room codes use 4–32 letters, digits or dashes");
    };
    let nick = clean_text(&msg.nick, false);
    let text = clean_text(&msg.text, true);
    if nick.is_empty() || nick.chars().count() > NICK_MAX_CHARS {
        return json_error(StatusCode::BAD_REQUEST, "nicknames are 1–24 characters");
    }
    if text.is_empty() || text.chars().count() > CHAT_MAX_CHARS {
        return json_error(StatusCode::BAD_REQUEST, "messages are 1–1000 characters");
    }
    let ip = client_ip(&state, peer, &headers);
    if !state.per_ip.chat.allow(ip) {
        return json_error(StatusCode::TOO_MANY_REQUESTS, "you're sending too fast, wait a moment");
    }
    let time = now_ms();
    match publish(&state, &format!("c/{code}"), "msg", HISTORY, |id| {
        serde_json::json!({ "id": id, "time": time, "nick": nick, "text": text }).to_string()
    }) {
        Ok((id, _)) => Json(serde_json::json!({ "ok": true, "id": id })).into_response(),
        Err(res) => res,
    }
}

async fn chat_stream(
    State(state): State<Shared>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(code): Path<String>,
    Query(q): Query<ReplayQuery>,
) -> Response {
    if !state.features.chat {
        return disabled();
    }
    let Some(code) = room_code(&code) else {
        return json_error(StatusCode::BAD_REQUEST, "room codes use 4–32 letters, digits or dashes");
    };
    let ip = client_ip(&state, peer, &headers);
    stream_room(&state, ip, format!("c/{code}"), q.replay.unwrap_or(HISTORY).min(HISTORY), true)
}

// ---------------------------------------------------------------- clipboard: live sync

#[derive(Deserialize)]
struct SyncPost {
    text: String,
}

async fn sync_send(
    State(state): State<Shared>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(code): Path<String>,
    Json(msg): Json<SyncPost>,
) -> Response {
    if !state.features.clipboard {
        return disabled();
    }
    let Some(code) = room_code(&code) else {
        return json_error(StatusCode::BAD_REQUEST, "codes use 4–32 letters, digits or dashes");
    };
    if msg.text.is_empty() || msg.text.len() > SYNC_MAX_BYTES {
        return json_error(StatusCode::BAD_REQUEST, "clipboard text must be 1 byte to 64 KB");
    }
    let ip = client_ip(&state, peer, &headers);
    if !state.per_ip.sync.allow(ip) {
        return json_error(StatusCode::TOO_MANY_REQUESTS, "you're syncing too fast, wait a moment");
    }
    let time = now_ms();
    // Only the latest clipboard is kept for devices that join later.
    match publish(&state, &format!("s/{code}"), "clip", 1, |id| {
        serde_json::json!({ "id": id, "time": time, "text": msg.text }).to_string()
    }) {
        Ok((id, n)) => Json(serde_json::json!({ "ok": true, "id": id, "delivered_to": n })).into_response(),
        Err(res) => res,
    }
}

async fn sync_stream(
    State(state): State<Shared>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(code): Path<String>,
) -> Response {
    if !state.features.clipboard {
        return disabled();
    }
    let Some(code) = room_code(&code) else {
        return json_error(StatusCode::BAD_REQUEST, "codes use 4–32 letters, digits or dashes");
    };
    let ip = client_ip(&state, peer, &headers);
    stream_room(&state, ip, format!("s/{code}"), 1, true)
}

// ---------------------------------------------------------------- clipboard: one-shot drops

#[derive(Deserialize)]
struct DropPost {
    text: String,
    #[serde(default = "yes")]
    burn: bool,
}

fn yes() -> bool {
    true
}

async fn drop_put(
    State(state): State<Shared>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(d): Json<DropPost>,
) -> Response {
    if !state.features.clipboard {
        return disabled();
    }
    if d.text.is_empty() || d.text.len() > DROP_MAX_BYTES {
        return json_error(StatusCode::BAD_REQUEST, "text must be 1 byte to 64 KB");
    }
    let ip = client_ip(&state, peer, &headers);
    if !state.per_ip.drop_put.allow(ip) {
        return json_error(StatusCode::TOO_MANY_REQUESTS, "too many drops from your network, try later");
    }
    let mut drops = state.drops.lock().unwrap();
    let minutes = drops.ttl().as_secs() / 60;
    match drops.put(d.text, d.burn) {
        Some(code) => Json(serde_json::json!({ "code": code, "minutes": minutes, "burn": d.burn })).into_response(),
        None => json_error(StatusCode::SERVICE_UNAVAILABLE, "the clipboard is full right now, try again in a few minutes"),
    }
}

async fn drop_get(
    State(state): State<Shared>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(code): Path<String>,
) -> Response {
    if !state.features.clipboard {
        return disabled();
    }
    let ip = client_ip(&state, peer, &headers);
    // Limits guessing: at 60 tries a minute, finding a live 6-character code takes ages.
    if !state.per_ip.drop_get.allow(ip) {
        return json_error(StatusCode::TOO_MANY_REQUESTS, "too many tries, wait a minute");
    }
    let code = code.trim().to_ascii_lowercase();
    if !drops::valid_code(&code) {
        return json_error(StatusCode::NOT_FOUND, "no clipboard with that code (it may have expired or been read)");
    }
    match state.drops.lock().unwrap().take(&code) {
        Some((text, left, burned)) => {
            let mut res = Json(serde_json::json!({ "text": text, "seconds_left": left, "burned": burned }))
                .into_response();
            res.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            res
        }
        None => json_error(StatusCode::NOT_FOUND, "no clipboard with that code (it may have expired or been read)"),
    }
}

async fn drop_delete(State(state): State<Shared>, Path(code): Path<String>) -> Response {
    if !state.features.clipboard {
        return disabled();
    }
    let code = code.trim().to_ascii_lowercase();
    let gone = drops::valid_code(&code) && state.drops.lock().unwrap().remove(&code);
    Json(serde_json::json!({ "deleted": gone })).into_response()
}

// ---------------------------------------------------------------- is.gd

#[derive(Deserialize)]
struct ShortenQuery {
    url: String,
}

/// Shorten a URL with is.gd. hashbox keeps no record of it.
async fn isgd(
    State(state): State<Shared>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(q): Query<ShortenQuery>,
) -> Response {
    if !state.features.isgd {
        return disabled();
    }
    let url = q.url.trim();
    if !(url.starts_with("https://") || url.starts_with("http://")) || url.len() > 5000 {
        return json_error(StatusCode::BAD_REQUEST, "only http(s) URLs up to 5000 characters");
    }
    let ip = client_ip(&state, peer, &headers);
    if !state.per_ip.isgd.allow(ip) {
        return json_error(
            StatusCode::TOO_MANY_REQUESTS,
            "you've used is.gd 5 times this hour; use an in-link or Gist link instead",
        );
    }
    {
        let mut calls = state.isgd_calls.lock().unwrap();
        let now = Instant::now();
        while calls.front().is_some_and(|t| now.duration_since(*t) > Duration::from_secs(60)) {
            calls.pop_front();
        }
        if calls.len() >= ISGD_MAX_PER_MINUTE {
            return json_error(StatusCode::TOO_MANY_REQUESTS, "is.gd is busy on this server, wait a minute");
        }
        calls.push_back(now);
    }

    let endpoint = std::env::var("HASHBOX_ISGD_ENDPOINT")
        .unwrap_or_else(|_| "https://is.gd/create.php".into());
    // Arguments are passed directly (no shell). The URL starts with http(s)://, so
    // curl reads `url=...` as name=content, never as a file path.
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
        Err(_) => {
            // Log what curl actually said (journalctl -u hashbox); visitors get a generic message.
            let clip = |b: &[u8]| String::from_utf8_lossy(b).chars().take(300).collect::<String>();
            eprintln!(
                "is.gd relay failed: curl {} | stderr: {} | stdout: {}",
                out.status,
                clip(&out.stderr).trim(),
                clip(&out.stdout).trim()
            );
            // is.gd sometimes answers in plain text instead of JSON (e.g. "Error, database
            // insert failed" for some URLs). Pass a short, cleaned version of it along.
            let said: String = String::from_utf8_lossy(&out.stdout)
                .lines()
                .next()
                .unwrap_or("")
                .chars()
                .filter(|c| !c.is_control() && *c != '<' && *c != '>')
                .take(120)
                .collect();
            let said = said.trim();
            if out.status.success() && !said.is_empty() {
                return json_error(StatusCode::BAD_GATEWAY, &format!("is.gd refused this URL: {said}"));
            }
            return json_error(StatusCode::BAD_GATEWAY, "is.gd is unreachable from the server");
        }
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

// ---------------------------------------------------------------- housekeeping

/// Every 15 s: expire drops, delete chat/sync rooms nobody has used for a while,
/// and tell chat rooms how many people are connected.
async fn janitor(state: Shared) {
    let mut tick = tokio::time::interval(Duration::from_secs(15));
    loop {
        tick.tick().await;
        state.drops.lock().unwrap().purge_expired();
        let mut reg = state.registry.lock().unwrap();
        let now = Instant::now();
        reg.retain(|key, room| {
            key.starts_with("h/")
                || room.tx.receiver_count() > 0
                || now.duration_since(room.last_active) < ROOM_IDLE
        });
        for (key, room) in reg.rooms.iter() {
            let n = room.tx.receiver_count();
            if key.starts_with("c/") && n > 0 {
                let _ = room.tx.send(presence_item(n));
            }
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
    let features = Features::from_env();
    let contact = std::env::var("HASHBOX_CONTACT")
        .ok()
        .map(|c| c.trim().chars().take(200).collect::<String>())
        .filter(|c| !c.is_empty());
    let max_body = limits.max_body;
    let drops = Drops::new(limits.clipboard_budget, Duration::from_secs(limits.drop_minutes * 60));
    let on = |b: bool| if b { "on" } else { "off" };
    println!(
        "hashbox: chat {}, clipboard {}, is.gd {}, webhooks {}; history ≤ {} MB, clipboard ≤ {} MB, ≤ {} listeners",
        on(features.chat),
        on(features.clipboard),
        on(features.isgd),
        on(features.hooks),
        limits.history_budget / (1024 * 1024),
        limits.clipboard_budget / (1024 * 1024),
        limits.max_listeners,
    );

    let minute = Duration::from_secs(60);
    let hour = Duration::from_secs(3600);
    let state: Shared = Arc::new(AppState {
        limits,
        features,
        contact,
        trust_proxy: env_flag("HASHBOX_TRUST_PROXY"),
        registry: Mutex::new(Registry::default()),
        drops: Mutex::new(drops),
        next_id: AtomicU64::new(0),
        listeners: AtomicUsize::new(0),
        listeners_by_ip: Mutex::new(HashMap::new()),
        per_ip: Limiters {
            isgd: IpLimiter::new(5, hour),
            chat: IpLimiter::new(20, minute),
            sync: IpLimiter::new(30, minute),
            drop_put: IpLimiter::new(30, hour),
            drop_get: IpLimiter::new(60, minute),
            join: IpLimiter::new(60, minute),
        },
        isgd_calls: Mutex::new(std::collections::VecDeque::new()),
        shutdown: tokio::sync::watch::channel(false).0,
    });
    tokio::spawn(janitor(state.clone()));
    let stopper = state.clone();

    let app = Router::new()
        .route("/", get(index))
        .route("/w/{channel}", get(index))
        .route("/c/{code}", get(index))
        .route("/s/{code}", get(index))
        .route("/d/{code}", get(index))
        .route("/rules", get(index))
        .route("/healthz", get(health))
        .route("/api/config", get(config))
        .route("/hook/{channel}", any(hook))
        .route("/hook/{channel}/{*rest}", any(hook))
        .route("/listen/{channel}", get(listen))
        .route("/api/chat/{code}", post(chat_send))
        .route("/api/chat/{code}/stream", get(chat_stream))
        .route("/api/sync/{code}", post(sync_send))
        .route("/api/sync/{code}/stream", get(sync_stream))
        .route("/api/drop", post(drop_put))
        .route("/api/drop/{code}", get(drop_get).delete(drop_delete))
        .route("/api/isgd", get(isgd))
        .layer(DefaultBodyLimit::max(max_body))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .unwrap_or_else(|e| panic!("cannot bind {bind}: {e}"));
    println!("hashbox listening on http://{bind}");

    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            stopper.shutdown.send_replace(true);
        })
        .await
        .unwrap();
}
