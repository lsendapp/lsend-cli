---
name: localsend-cli
description: >-
  Send and receive files on the LAN with the headless lsend CLI. Use when
  an agent needs to discover LocalSend peers, transfer files to a device IP,
  or run a one-shot receive server with JSON output.
---

# lsend CLI (agent workflow)

Non-interactive file transfer compatible with the LocalSend app.

## Before running

1. **Install the prebuilt binary** (don't compile from source — agents don't need a Rust toolchain). Auto-detects arch and OS:
   ```bash
   curl -L https://github.com/lsendapp/cli/releases/latest/download/lsend-$(uname -m | sed 's/x86_64/x86_64/;s/aarch64/aarch64/')-$(uname -s | tr A-Z a-z | sed 's/darwin/apple-darwin/;s/linux/unknown-linux-musl/').tar.gz \
     | tar xz \
     && sudo install lsend /usr/local/bin/
   lsend --version   # verify
   ```
   Windows: download `lsend-x86_64-pc-windows-msvc.zip` (or `-aarch64-`), extract, add the folder to `PATH`. macOS binaries are signed + notarized, so no Gatekeeper warning.
2. Read focused docs offline: `lsend agent` or `lsend agent send`
3. Use **`--json`**, piped stdout, or **`LSEND_NO_TUI=1`** for machine-parseable output
4. Close whatever holds port 53317 (e.g. the LocalSend app, another `lsend receive`) before `receive`
5. **Keep port 53317 for receive** — alternate `--port` breaks multicast discovery; the LocalSend app and default `scan` will not see this device

## Device alias

```bash
lsend alias show --json
lsend alias regenerate --json
lsend alias set "My Laptop" --json
```

Persisted in `~/.config/lsend/alias.txt`. The global `--alias` flag overrides for one command only.

## Discover devices

```bash
lsend scan --json --timeout 5000
```

Parse `.devices[].ip` from the JSON object. Prefer IP over alias for send.

## Send files (preferred path)

```bash
lsend send <IP_FROM_SCAN> /path/to/file --json --no-scan
echo "hello" | lsend send <IP_FROM_SCAN> --text --json --no-scan
lsend send <IP_FROM_SCAN> --message "hello" --json --no-scan
lsend send <IP_FROM_SCAN> --clipboard --json --no-scan
```

- **`--no-scan`** avoids a slow implicit rescan when the target is an alias
- Add **`--pin`** if the receiver requires a PIN
- **`--text`** reads UTF-8 from stdin; **`--message`** sends inline text; **`--clipboard`** sends plain clipboard text

Success JSON:
- File send: `"kind": "file"` and `"files"[].status == "finished"`
- Message send (`--text` / `--message` / `--clipboard`): `"kind": "message"`, `"text"`, and `"status": "finished"`

## Receive files (automation)

```bash
lsend receive --json --once --dir /tmp/lsend-inbox
lsend receive --json --once --pin 123456 --dir /tmp/lsend-inbox
```

Stdout is NDJSON (`ready` → `file_saved` → `transfer_complete` → `shutdown`).
Use **`--once`** so the process exits after one transfer.
Receive PIN: **`--pin`** (persisted to `receive_pin`) > config file > `LSEND_RECEIVE_PIN` env.

## Errors

Check exit code and JSON envelope:

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error (incl. partial send failure, `transfer_declined`) |
| 2 | Target not found / no files / invalid alias |
| 3 | Port 53317 in use |

Failure JSON: `{"command":"...","ok":false,"code":"port_in_use","error":"...","hint":"..."}`

## More detail

Repository files: `AGENTS.md`, `lsend agent alias`, `lsend agent errors`, `lsend agent eval`
