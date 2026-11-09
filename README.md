# lsend

Headless command-line tool for sending and receiving files over the local network, compatible with the [LocalSend](https://localsend.org) protocol.

## Requirements

- Rust 1.75+

> The LocalSend Rust `core` crate is vendored under `vendor/localsend-core/` — no need to clone the upstream repository.

## Build

```bash
git clone <this-repo-url>
cd lsend
cargo build --release
# binary at: target/release/lsend
```

To install into `~/.cargo/bin/`:

```bash
cargo install --path .
```

## Usage

```bash
# Discover nearby devices
lsend scan

# Manage persisted device alias (device name)
lsend alias show
lsend alias regenerate
lsend alias set "My Laptop"

# Manage paired devices (receive trust list)
lsend pair list
lsend pair add 192.168.1.42
lsend pair remove 192.168.1.42

# Receive files (auto-accept, saves to ~/Downloads by default)
lsend receive

# Send files to a device (IP address or alias from scan)
lsend send 192.168.1.42 ./photo.png ./notes.txt
echo "hello" | lsend send 192.168.1.42 --text
lsend send 192.168.1.42 --message "hello"
lsend send 192.168.1.42 --clipboard
```

### Global options

| Flag | Description |
|------|-------------|
| `--http` | Use plain HTTP instead of HTTPS |
| `--port PORT` | Listen/connect port (default: 53317). Keep 53317 for receive — alternate ports break multicast discovery. |
| `--alias NAME` | Device display name for one command (overrides alias.txt) |
| `-v, --verbose` | Print diagnostic logs on stderr |
| `--json` | Machine-readable JSON on stdout (see [AGENTS.md](AGENTS.md)) |
| `-q, --quiet` | Suppress progress text (human mode; implied by `--json`) |

### Per-subcommand options

| Subcommand | Flag | Description |
|------------|------|-------------|
| `scan` | `--timeout MS` | Total scan budget in milliseconds (default: 1500) |
| `send` | `--pin PIN` | PIN when sending to a PIN-protected receiver |
| `send` | `--no-scan` | Do not scan for alias; use IP or fail fast |
| `send` | `--text` | Read UTF-8 text from stdin (pipe) |
| `send` | `--message TEXT` | Send inline text |
| `send` | `--clipboard` | Send plain text from the system clipboard |
| `receive` | `--dir PATH` | Directory where received files are saved (default: Downloads) |
| `receive` | `--once` | Exit after the first completed transfer |
| `receive` | `--accept-unpaired` | Accept unpaired senders while a pair list exists |

Multi-file sends continue after a per-file failure; the send result reports
`finished`, `skipped`, or `failed` per file and exits non-zero when any file
failed. Sent files carry a sha256 checksum; receivers that verify it answer
422 on mismatch. Received files are verified the same way, and peer-supplied
file names are sanitized per path segment while preserving subdirectories.

## AI agents and automation

- **`lsend agent`** — offline progressive docs (`alias`, `scan`, `send`, `receive`, `errors`, `eval`)
- **[AGENTS.md](AGENTS.md)** — JSON schemas and exit codes
- **`skills/localsend-cli/SKILL.md`** — Cursor agent skill (copy to `.cursor/skills/`)

```bash
lsend agent
lsend scan --json --timeout 5000
lsend send 192.168.1.42 ./file.pdf --json --no-scan
lsend receive --json --once --dir /tmp/inbox
```

## Configuration

Identity (TLS certificate and fingerprint) and the device alias are stored under:

- macOS/Linux: `~/.config/lsend/`
- Windows: `%APPDATA%\lsend\`

Files:

- `cert.pem`, `key.pem`, `fingerprint.txt` — TLS identity
- `alias.txt` — persisted device name (official LocalSend word lists + system locale; override per run with `--alias`; manage with `lsend alias`)
- `receive_pin` — persisted receive PIN when set via `lsend receive --pin`
- `paired.json` — paired-device trust list (manage with `lsend pair`); while non-empty, `receive` rejects unpaired senders with 403

Receivers reclaim abandoned sessions after five minutes without an active upload when
another transfer is requested. Streaming uploads time out after five minutes without
another data chunk. Active uploads can run longer, and failed files can be retried
within the idle window. Files already saved are kept when a session expires.

HTTP-only identities are automatically upgraded to a TLS identity on the next HTTPS
command. Invalid persisted TLS material is regenerated.

Stdout is automatically JSON when piped or when `LSEND_NO_TUI=1` is set (same as `--json`).

## License

`lsend-cli` is licensed under the [Apache License 2.0](LICENSE).

This project vendors the `core` crate from the [LocalSend](https://github.com/localsend/localsend) project,
which is also licensed under the [Apache License 2.0](vendor/localsend-core/LICENSE) — Copyright 2022-2024 Tien Do Nam.
