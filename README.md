# hashbox

One small binary (~800 KB, ~5 MB RAM) for the Raspberry Pi:

- **Text & URL links with the data inside the link.** Content is compressed into
  the URL fragment (`#t:...` for text, `#u:...` for URLs). Browsers never send the
  fragment to the server, so the server stores and sees nothing. A link made on
  server A opens on server B (just change the host) with no communication between them.
- **Webhook fan-out relay.** Anything sent to `/hook/<channel>` is pushed live to
  every device listening on `/listen/<channel>`: browsers, `curl -N`, scripts.

## Endpoints

| Path | What |
|---|---|
| `/` | Web UI: create text/URL links, open the webhook monitor |
| `/#t:<data>` / `/#u:<data>` | View a text paste / follow a URL link (3 s countdown, http/https only) |
| `/w/<channel>` | Live webhook monitor in the browser |
| `ANY /hook/<channel>[/sub/path]` | Receive a webhook, rebroadcast it. Replies `{"delivered_to": N}` |
| `GET /listen/<channel>[?replay=N]` | Server-Sent Events stream; `replay` re-sends up to 50 recent events |
| `/healthz` | Returns `ok` |

Channel names are `[A-Za-z0-9_-]{1,64}`. There is no auth: use long random
channel names (the UI generates them) as the secret. Events are kept in RAM only
(last 50 per channel) and vanish on restart. Body limit is 1 MB.

## Build for the Pi (from your Arch laptop)

```bash
rustup target add aarch64-unknown-linux-musl
cargo build --release --target aarch64-unknown-linux-musl
# -> target/aarch64-unknown-linux-musl/release/hashbox  (static, runs on any aarch64 Linux)
```

(Or build directly on the Pi with `cargo build --release`. It works but takes a while on a Pi 3.)

## Install on the Pi

```bash
scp target/aarch64-unknown-linux-musl/release/hashbox hashbox.service alarm@192.168.1.150:
ssh alarm@192.168.1.150
sudo install -m755 hashbox /usr/local/bin/
sudo install -m644 hashbox.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now hashbox
```

Then open http://192.168.1.150:8080.

## Listening from devices

```bash
curl -N http://192.168.1.150:8080/listen/<channel>          # raw stream
./examples/listen.sh http://192.168.1.150:8080 <channel>     # runs a command per event
```

In JavaScript: `new EventSource("/listen/<channel>").addEventListener("hook", e => JSON.parse(e.data))`.

## Exposing it to the internet

External services (GitHub, Stripe…) need to reach `/hook/...`. Put Caddy in front for HTTPS
and expose it with a port forward, Tailscale Funnel, or a Cloudflare Tunnel. If you use nginx,
disable buffering on `/listen/` (hashbox already sends `X-Accel-Buffering: no`).
