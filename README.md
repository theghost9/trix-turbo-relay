---
title: TRiX Turbo Relay
emoji: ⚡
colorFrom: blue
colorTo: purple
sdk: docker
app_port: 7860
pinned: false
license: unlicense
---

# TRiX Turbo Relay

Persistent WebSocket relay for the TRiX Turbo Browser (Android) — instant
push routing between Muse and the phone. No polling.

- **Health:** `GET /health` → `{"ok": true, "service": "trix-turbo-relay", ...}`
- **WebSocket:** `wss://<this-space>.hf.space/ws`
- **Pairing:** the phone shows a 6-char code; Muse joins with it.
- **Protocol:** see `PROTOCOL.md` in the TRiX Turbo repo.

Runs on `PORT` (HF injects 7860). One port serves `GET /health` and
WebSocket at `/ws`.
