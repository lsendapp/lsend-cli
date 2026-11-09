# lsend CLI — Agent Integration Guide

Machine-readable, non-interactive CLI for the LocalSend protocol.

**Progressive docs (offline):** `lsend agent [alias|scan|send|receive|errors|eval]`

**Cursor skill:** `skills/localsend-cli/SKILL.md` (install to `.cursor/skills/` or use `npx skills add` if published)

## Principles

- No prompts — all inputs via flags
- **`--json`** — structured stdout only (no human text); also enabled when stdout is piped or `LSEND_NO_TUI=1`
- **`--quiet`** — minimal stdout in human mode (`--json` implies quiet)
- Logs on **stderr** via `-v` / `RUST_LOG`
- Stable **exit codes** + **`code`** field in error JSON

## Global flags

| Flag | Description |
|------|-------------|
| `--json` | JSON stdout (`scan`/`send` object, `receive` NDJSON); auto-enabled when piped |
| `--quiet` / `-q` | Suppress progress text (human mode) |
| `LSEND_NO_TUI=1` | Same as `--json` for non-interactive stdout |
| `-v, --verbose` | Diagnostics on stderr |
| `--http` | Plain HTTP (default: HTTPS) |
| `--port PORT` | Default `53317`. **Keep 53317 for receive** — alternate ports break multicast discovery (the LocalSend app and default `scan` will not see this device). |
| `--alias NAME` | Override persisted alias for one command only |

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error (`code: "error"`, `transfer_declined`, partial send failures) |
| 2 | Not found / validation (`target_not_found`, `no_files`, `invalid_alias`) |
| 3 | Port in use (`port_in_use`) |

## scan

```bash
lsend scan --json --timeout 5000
```

```json
{
  "command": "scan",
  "ok": true,
  "timeout_ms": 5000,
  "devices": [{ "alias": "...", "ip": "192.168.1.10", "port": 53317, "https": true, ... }]
}
```

Empty `devices` with `ok: true` is not an error.

`--timeout` bounds the **whole** scan (announcement bursts + multicast listen +
optional HTTP fallback). Budgets under 4000 ms skip the subnet fallback; larger
budgets reserve its tail (up to 2000 ms) for it.

## alias

Manage the persisted device alias (the LocalSend app UI: **Device name**). Stored in `~/.config/lsend/alias.txt`.

```bash
lsend alias show --json
lsend alias regenerate --json
lsend alias regenerate --json --locale zh-CN
lsend alias set "My Laptop" --json
```

`lsend alias` without a subcommand is the same as `alias show`.

Regenerate JSON:

```json
{
  "command": "alias",
  "action": "regenerate",
  "ok": true,
  "previous": "Adorable Orange",
  "alias": "Clever Banana",
  "path": "/Users/me/.config/lsend/alias.txt",
  "locale": "en"
}
```

Priority for network commands: `--alias NAME` > `alias.txt`.

Restart `receive` after changing the persisted alias so peers see the new name.

## send

**Always prefer IP** from scan. Use **`--no-scan`** to forbid slow alias lookup.

```bash
lsend scan --json --timeout 5000
lsend send 192.168.1.10 ./file.pdf --json --no-scan
echo "status ok" | lsend send 192.168.1.10 --text --json --no-scan
lsend send 192.168.1.10 --message "status ok" --json --no-scan
```

```json
{
  "command": "send",
  "ok": true,
  "target": { "ip": "192.168.1.10", ... },
  "resolved_via": "ip",
  "kind": "file",
  "files": [{ "name": "file.pdf", "path": "/abs/file.pdf", "size": 1024, "status": "finished" }]
}
```

Message send (`--text`, `--message`, `--clipboard`):

```json
{
  "command": "send",
  "ok": true,
  "target": { "ip": "192.168.1.10", ... },
  "resolved_via": "ip",
  "kind": "message",
  "text": "status ok",
  "size": 9,
  "status": "finished"
}
```

`kind`: `"file"` for path-based transfers, `"message"` for text modes.

`resolved_via`: `"ip"` (preferred) or `"scan"` (alias triggered discovery).

`files[].status`:

| Status | Meaning |
|--------|---------|
| `finished` | Uploaded and acknowledged by the receiver |
| `skipped` | Receiver did not accept this file (no upload token issued) |
| `failed` | Upload attempted and failed; sibling `error` field carries the reason |

Multi-file sends **continue after a per-file failure**: every accepted file is
attempted, the result document carries per-file statuses with `ok: false` when
any file failed, and the exit code is `1`. A file send answered by `204` (the
receiver accepted no files) fails with `code: "transfer_declined"`.

Integrity: each file's sha256 is announced in prepare-upload; a verifying
receiver answers `422` on mismatch, which surfaces as a `failed` file.

## receive

```bash
lsend receive --json --once --dir /tmp/inbox
lsend receive --json --once --pin 123456 --dir /tmp/inbox
lsend receive --json --once --accept-unpaired --dir /tmp/inbox
```

Receive PIN priority: `--pin` (persisted to `receive_pin`) > config file > `LSEND_RECEIVE_PIN` env.

NDJSON events: `ready` → `transfer_started` → `file_saved` → `transfer_complete` → `shutdown`

While a pair list exists, an unpaired sender is rejected before a session is
created: `{"event":"transfer_rejected","sender_alias":"...","reason":"not_paired"}`
and the sender receives HTTP 403. `--accept-unpaired` disables the gate for
one run. See [pair](#pair).

Received files are verified against the sender's announced sha256; on mismatch
the sender gets `422` and the partial file is discarded. Peer-supplied names
are sanitized per path segment (reserved names, illegal characters) while
preserving the sender's directory structure.

## Errors (`--json`)

```json
{
  "command": "send",
  "ok": false,
  "code": "target_not_found",
  "error": "No device found with alias \"...\". Run `lsend scan --json` first or pass an IP address.",
  "hint": "Run `lsend scan --json` first and use the device IP, or pass an IP address directly."
}
```

## pair

Manage the paired-device trust list stored at `~/.config/lsend/paired.json`
(shared with the desktop app; reloaded per transfer, so changes apply live).

```bash
lsend pair list --json
lsend pair add 192.168.1.10 --json        # discovers the peer, stores its fingerprint
lsend pair remove "Cute Apple" --json     # by alias or fingerprint
```

Semantics:

- Empty or missing `paired.json` → receive auto-accepts any LAN peer (default).
- Non-empty list → receive rejects unpaired senders with 403 (`transfer_rejected` event).
- `receive --accept-unpaired` → one-off override.
- Removing the last entry returns receive to auto-accept.

`pair add` runs a discovery scan and requires the peer to be present; the
stored fingerprint is the peer's announced value (not cryptographic proof of
identity — it stops ordinary unpaired peers, not active spoofers).

## Agent eval checklist

See `lsend agent eval` for a step-by-step smoke test.

## Notes

- Do not run `receive` while another process holds port 53317 (e.g. the LocalSend app or another `lsend receive`)
- **Do not use alternate `--port` for receive** unless you accept that other devices cannot discover this CLI via scan. LocalSend multicast discovery binds UDP and TCP to the same port; the LocalSend app and default `scan` listen on 53317 only.
- On `port_in_use`, prefer closing whatever holds port 53317 (e.g. the LocalSend app, another `lsend receive`) or reusing an existing receiver — do not auto-kill processes or silently switch ports
- Identity (TLS) stored in `~/.config/lsend/`
- Device alias persisted in `alias.txt`; manage with `lsend alias`; `--alias` overrides for one run
- Receive PIN via `receive --pin` (persisted in `receive_pin`) or `LSEND_RECEIVE_PIN`
