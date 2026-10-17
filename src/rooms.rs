//! Rooms: the shared live-broadcast machinery behind webhooks, chat and live
//! clipboard sync. A room is a broadcast channel plus a short in-RAM history.
//! Every limit lives here, so all three features are bounded the same way.

use std::{
    collections::{HashMap, VecDeque},
    net::IpAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::sync::broadcast;

/// Items kept per room for late joiners.
pub const HISTORY: usize = 50;
/// Per-room broadcast buffer; slow listeners skip ahead beyond this.
const BROADCAST_CAP: usize = 64;

/// One message in a room, already serialized (sent as an SSE event of type `kind`).
pub struct Item {
    /// 0 for ephemeral items (presence), which are never stored.
    pub id: u64,
    pub kind: &'static str,
    pub json: String,
}

impl Item {
    fn size(&self) -> usize {
        128 + self.json.len()
    }
}

pub struct Room {
    pub tx: broadcast::Sender<Arc<Item>>,
    pub history: VecDeque<Arc<Item>>,
    bytes: usize,
    recent: VecDeque<Instant>,
    pub last_active: Instant,
}

impl Room {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAP);
        Room {
            tx,
            history: VecDeque::new(),
            bytes: 0,
            recent: VecDeque::new(),
            last_active: Instant::now(),
        }
    }

    /// Per-room publish rate limit (sliding one-minute window).
    pub fn allow_publish(&mut self, per_minute: usize) -> bool {
        let now = Instant::now();
        while self
            .recent
            .front()
            .is_some_and(|t| now.duration_since(*t) > Duration::from_secs(60))
        {
            self.recent.pop_front();
        }
        if self.recent.len() >= per_minute {
            return false;
        }
        self.recent.push_back(now);
        true
    }
}

/// All rooms plus the bookkeeping for the global history budget, under one lock.
/// Room keys are namespaced: `h/<channel>` webhooks, `c/<code>` chat, `s/<code>` sync.
#[derive(Default)]
pub struct Registry {
    pub rooms: HashMap<String, Room>,
    total_bytes: usize,
    /// Every stored item in arrival order, for global oldest-first eviction.
    /// Entries can be stale (already evicted); they are skipped when popped.
    order: VecDeque<(String, u64)>,
}

impl Registry {
    /// Get or create a room. None when the room limit is reached.
    pub fn room(&mut self, key: &str, max_rooms: usize) -> Option<&mut Room> {
        if !self.rooms.contains_key(key) {
            if self.rooms.len() >= max_rooms {
                self.retain(|_, r| r.tx.receiver_count() > 0);
                if self.rooms.len() >= max_rooms {
                    return None;
                }
            }
            self.rooms.insert(key.to_owned(), Room::new());
        }
        let r = self.rooms.get_mut(key)?;
        r.last_active = Instant::now();
        Some(r)
    }

    /// Remove rooms for which `keep` returns false, freeing their history.
    pub fn retain(&mut self, mut keep: impl FnMut(&str, &Room) -> bool) {
        let mut freed = 0;
        self.rooms.retain(|k, r| {
            let k2 = keep(k, r);
            if !k2 {
                freed += r.bytes;
            }
            k2
        });
        self.total_bytes -= freed;
        let rooms = &self.rooms;
        self.order.retain(|(k, _)| rooms.contains_key(k));
    }

    /// Store an item in a room's history and enforce both history limits.
    pub fn remember(&mut self, key: &str, item: Arc<Item>, budget: usize, per_room: usize) {
        let size = item.size();
        let Some(r) = self.rooms.get_mut(key) else { return };
        r.history.push_back(item.clone());
        r.bytes += size;
        self.total_bytes += size;
        self.order.push_back((key.to_owned(), item.id));
        while r.history.len() > per_room {
            let s = r.history.pop_front().unwrap().size();
            r.bytes -= s;
            self.total_bytes -= s;
        }
        while self.total_bytes > budget {
            let Some((k, id)) = self.order.pop_front() else { break };
            if let Some(r) = self.rooms.get_mut(&k) {
                if r.history.front().is_some_and(|e| e.id == id) {
                    let s = r.history.pop_front().unwrap().size();
                    r.bytes -= s;
                    self.total_bytes -= s;
                }
            }
        }
        if self.order.len() > 4 * HISTORY * self.rooms.len().max(1) {
            let rooms = &self.rooms;
            self.order
                .retain(|(k, id)| rooms.get(k).is_some_and(|r| r.history.iter().any(|e| e.id == *id)));
        }
    }
}

/// Sliding-window rate limiter keyed by client IP.
pub struct IpLimiter {
    max: usize,
    window: Duration,
    hits: Mutex<HashMap<IpAddr, VecDeque<Instant>>>,
}

impl IpLimiter {
    pub fn new(max: usize, window: Duration) -> Self {
        IpLimiter { max, window, hits: Mutex::new(HashMap::new()) }
    }

    /// Count one hit for `ip`; false if it is over the limit.
    pub fn allow(&self, ip: IpAddr) -> bool {
        let mut hits = self.hits.lock().unwrap();
        let now = Instant::now();
        if hits.len() > 20_000 {
            let w = self.window;
            hits.retain(|_, q| q.back().is_some_and(|t| now.duration_since(*t) < w));
        }
        let q = hits.entry(ip).or_default();
        while q.front().is_some_and(|t| now.duration_since(*t) > self.window) {
            q.pop_front();
        }
        if q.len() >= self.max {
            return false;
        }
        q.push_back(now);
        true
    }
}
