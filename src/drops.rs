//! Clipboard drops: paste text, get a short code, read it once on another device.
//! RAM only, deleted on first read (by default) or when it expires.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

/// Code alphabet: no 0/o, 1/l/i, so codes are easy to read and type on a phone.
const ALPHABET: &[u8] = b"23456789abcdefghjkmnpqrstuvwxyz";
pub const CODE_LEN: usize = 6;

struct Drop {
    text: String,
    expires: Instant,
    burn: bool,
}

pub struct Drops {
    map: HashMap<String, Drop>,
    bytes: usize,
    budget: usize,
    ttl: Duration,
}

pub fn random_code(len: usize) -> String {
    let mut out = String::with_capacity(len);
    while out.len() < len {
        let mut b = [0u8; 16];
        getrandom::getrandom(&mut b).expect("system randomness");
        for x in b {
            // Rejection sampling keeps every character equally likely.
            if (x as usize) < 256 - 256 % ALPHABET.len() && out.len() < len {
                out.push(ALPHABET[x as usize % ALPHABET.len()] as char);
            }
        }
    }
    out
}

pub fn valid_code(code: &str) -> bool {
    code.len() == CODE_LEN && code.bytes().all(|b| ALPHABET.contains(&b))
}

impl Drops {
    pub fn new(budget: usize, ttl: Duration) -> Self {
        Drops { map: HashMap::new(), bytes: 0, budget, ttl }
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    pub fn purge_expired(&mut self) {
        let now = Instant::now();
        let mut freed = 0;
        self.map.retain(|_, d| {
            let keep = d.expires > now;
            if !keep {
                freed += d.text.len();
            }
            keep
        });
        self.bytes -= freed;
    }

    /// Store text; returns its code, or None if the server's drop budget is full.
    pub fn put(&mut self, text: String, burn: bool) -> Option<String> {
        self.purge_expired();
        if self.bytes + text.len() > self.budget {
            return None;
        }
        let code = loop {
            let c = random_code(CODE_LEN);
            if !self.map.contains_key(&c) {
                break c;
            }
        };
        self.bytes += text.len();
        self.map.insert(
            code.clone(),
            Drop { text, expires: Instant::now() + self.ttl, burn },
        );
        Some(code)
    }

    /// Read a drop. Burn-after-reading drops are removed by the read.
    /// Returns (text, seconds left, was_burned).
    pub fn take(&mut self, code: &str) -> Option<(String, u64, bool)> {
        self.purge_expired();
        let d = self.map.get(code)?;
        let left = d.expires.saturating_duration_since(Instant::now()).as_secs();
        if d.burn {
            let d = self.map.remove(code)?;
            self.bytes -= d.text.len();
            Some((d.text, 0, true))
        } else {
            Some((d.text.clone(), left, false))
        }
    }

    pub fn remove(&mut self, code: &str) -> bool {
        match self.map.remove(code) {
            Some(d) => {
                self.bytes -= d.text.len();
                true
            }
            None => false,
        }
    }
}
