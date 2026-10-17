# hashbox

One small static binary (~1 MB, a few MB of RAM) for a VPS or a Raspberry Pi.
No accounts, no database, nothing written to disk:

- **Text & URL links with the data inside the link.** Content is compressed into
  the URL fragment (`#t:...` for text, `#h:...`, `#yt:...` … for URLs). Browsers never
  send the fragment to the server, so the server never sees it. A link made on
  server A opens on server B (just change the host). URL links never redirect
  automatically: visitors see the real destination and click to continue.
- **Clipboard.** *Send once*: paste, get a 6-character code, open it on another device
  (deleted on first view or after 10 minutes). *Live sync*: devices on the same code
  mirror each other's clipboard while the page is open.
- **Chat rooms** joined with a code. Plain text only, links aren't clickable, and a
  room is wiped about 2 minutes after its last member leaves.
- **Webhook fan-out relay.** Anything sent to `/hook/<channel>` is pushed live to
  every device listening on `/listen/<channel>`: browsers, `curl -N`, scripts.

## Running a public instance without moderating it

hashbox is designed so that one person can run it without watching content:

- **Nothing persists.** Links carry their own data; chat and clipboard live in RAM and expire on their own.
- **No open redirect.** Link pages show the destination's domain prominently and wait for a click,
  so they are useless for phishing redirects and don't get flagged as open redirectors.
- **Per-visitor limits** (IPv6 counted per /64): chat 20 messages/min, live clipboard 30 sends/min,
  clipboard drops 30/hour and 60 code lookups/min, is.gd 5/hour, 60 new connections/min,
  10 open connections at once. Plus the server-wide limits below.
- **Kill switches.** In `/etc/hashbox/hashbox.env`, set `HASHBOX_DISABLE=chat,clipboard,isgd,hooks`
  (any subset) and run `sudo systemctl restart hashbox`. The features disappear from the page.
- **Guidelines page** at `/rules`, linked from every page. Set `HASHBOX_CONTACT=you@example.com`
  to publish an address for abuse reports.
- **Legal notice** (*mentions légales*, `/rules#legal`) for a non-professional publisher in France:
  set `HASHBOX_HOST_INFO` to your hosting provider's name and address. Your own identity stays
  private as long as your provider has it and the site has no economic activity (LCEN art. 1-1, II).

Behind a reverse proxy, set `HASHBOX_TRUST_PROXY=1` so limits apply per visitor rather than to
the proxy. Without it, `X-Forwarded-For` is ignored (it could be forged).

## Install (VPS or Pi)

```bash
curl -fsSL https://github.com/AnCarsenat/hashbox/releases/download/latest/hashbox-deploy | sudo bash -s install
```

This creates a `hashbox` system user (no login, no home), installs the binary and a
sandboxed systemd service listening on `127.0.0.1:8080`, and enables a timer that
installs new builds automatically. Put Caddy in front for HTTPS:

```
hashbox.<server-ip>.sslip.io {
    reverse_proxy 127.0.0.1:8080
}
```

Use `--bind 0.0.0.0:8080` to listen on the network directly instead.

### How updates work

Every push to `main` runs `.github/workflows/release.yml`, which builds static x86_64
and aarch64 binaries and publishes them, with a `SHA256SUMS` file, to the rolling
[`latest` release](../../releases/tag/latest). Every 15 minutes `hashbox-update.timer`
runs `hashbox-deploy update`, which:

1. compares the published `VERSION` (the commit id) with the installed one, and stops if equal;
2. downloads the build for its CPU over HTTPS and verifies every file against `SHA256SUMS`;
3. swaps in the new binary and units, restarts, and checks `/healthz`;
4. **rolls back** to the previous binary automatically if the health check fails.

```bash
sudo hashbox-deploy status            # version, service, timer, health
sudo hashbox-deploy update            # check now (--force to reinstall)
journalctl -u hashbox-update          # update history
sudo hashbox-deploy uninstall         # remove (keeps /etc/hashbox)
```

Settings live in `/etc/hashbox/hashbox.env` and are never touched by updates.

## Limits and hardening

Everything is in RAM and bounded. Defaults, overridable in `/etc/hashbox/hashbox.env`:

| Setting | Default | What happens past it |
|---|---|---|
| `HASHBOX_MAX_BODY_KB` | 256 | Webhook rejected with 413 |
| `HASHBOX_HOOKS_PER_MINUTE` | 120 per channel | 429 with `Retry-After: 60` |
| `HASHBOX_MAX_LISTENERS` | 200 total | New listeners get 429 |
| `HASHBOX_MAX_LISTENERS_PER_CHANNEL` | 20 | New listeners get 429 |
| `HASHBOX_HISTORY_MB` | 8 | Oldest stored events (all channels) are evicted |
| `HASHBOX_MAX_CHANNELS` | 1000 | Idle rooms/channels are dropped, then 503 |
| `HASHBOX_MAX_LISTENERS_PER_IP` | 10 | That visitor's new connections get 429 |
| `HASHBOX_CLIPBOARD_MB` | 4 | New clipboard drops refused until old ones expire |
| `HASHBOX_DROP_MINUTES` | 10 | Clipboard drops are deleted |

Also: proxy and cookie headers are not rebroadcast, is.gd calls are capped at 20/min,
listener streams end on shutdown so restarts take milliseconds, and the systemd unit
runs with no capabilities, a read-only system, a syscall filter and `MemoryMax=64M`.

There is no authentication inside hashbox. Webhook channels are as private as their
names are hard to guess. To keep webhook listening to yourself on a public instance,
put a password on `/listen/*` and `/w/*` in the reverse proxy (Caddy example):

```
hashbox.<server-ip>.sslip.io {
    @private path /listen/* /w/*
    basic_auth @private {
        you <hash from `caddy hash-password`>
    }
    reverse_proxy 127.0.0.1:8080
}
```

## URL shortening

The URL tab offers three kinds of link and recommends one. You can switch between them:

| Option | Length | Where the URL lives |
|---|---|---|
| **In the link** (recommended when the link stays ≤ 80 chars) | varies | Nowhere: encoded in the link itself, opens on any hashbox |
| **is.gd** (recommended otherwise) | ~20 chars | Stored publicly and permanently by is.gd |
| **GitHub Gist** (needs a token with the `gist` scope) | ~47 chars | A secret gist in your GitHub account, opens on any hashbox |

In-link encodings pick the shortest of:
`#yt:<video>[~secs]` (YouTube), `#gh:<path>` (GitHub), `#wp:<lang>/<page>` (Wikipedia),
`#w:` / `#h:` / `#p:` (the URL minus `https://www.`, `https://`, `http://`), and `#u:` (deflate-compressed).
Tracking parameters (`utm_*`, `fbclid`, `gclid`, YouTube/Spotify `si`, …) are stripped unless you untick the box.

The server calls is.gd with the system `curl` and stores nothing.
The Gist token is kept in the browser's localStorage and sent only to api.github.com.
Opening a gist link uses GitHub's unauthenticated API (60 lookups per hour per IP).

## Endpoints

| Path | What |
|---|---|
| `/` | Web UI: create text/URL links, open the webhook monitor |
| `/#t:<data>`, `/#yt:…`, `/#h:…`, `/#s:…` … | View a text paste / follow a URL link (3 s countdown, http/https only) |
| `/w/<channel>` | Live webhook monitor in the browser |
| `ANY /hook/<channel>[/sub/path]` | Receive a webhook, rebroadcast it. Replies `{"delivered_to": N}` |
| `GET /listen/<channel>[?replay=N]` | Server-Sent Events stream; `replay` re-sends up to 50 recent events |
| `GET /api/isgd?url=<url>` | Shorten via is.gd (5/hour per visitor, 20/min overall). Returns `{"shorturl": ...}` or `{"error": ...}` |
| `/c/<code>` · `POST /api/chat/<code>` · `GET /api/chat/<code>/stream` | Chat room page, send `{"nick","text"}`, live stream (`msg`, `presence` events) |
| `/s/<code>` · `POST /api/sync/<code>` · `GET /api/sync/<code>/stream` | Live clipboard page, send `{"text"}`, live stream (`clip`, `presence`) |
| `POST /api/drop` · `/d/<code>` · `GET`/`DELETE /api/drop/<code>` | Clipboard drop: `{"text","burn"}` → `{"code"}`; read once; delete |
| `/rules` · `GET /api/config` | Guidelines page; enabled features and contact |
| `/healthz` | Returns `ok` |

Channel names are `[A-Za-z0-9_-]{1,64}`; use long random names (the UI generates them).
Events vanish on restart.

## Listening from devices

```bash
curl -N https://<host>/listen/<channel>          # raw stream (add -u user:pass behind basic_auth)
./examples/listen.sh https://<host> <channel>    # runs a command per event
```

In JavaScript: `new EventSource("/listen/<channel>").addEventListener("hook", e => JSON.parse(e.data))`.

## Building by hand

```bash
rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl     # VPS
cargo build --release --target aarch64-unknown-linux-musl    # Raspberry Pi
```
