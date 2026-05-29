# pi-server

Experimental OpenCode-compatible HTTP API server backed by `pi --mode rpc` session processes.

`pi-server` is intended to let OpenCode clients attach to `pi` through the same headless server API exposed by `opencode serve`.

## Motivation

The goal is a native Rust server with low overhead:

- Fast startup in container / sandbox environments thanks to [pi_agent_rust](https://github.com/Dicklesworthstone/pi_agent_rust).
- Low resource usage compared with running a full JavaScript server stack.
- One `pi --mode rpc` process per active session, keeping backend state isolated.
- Compatibility with existing OpenCode clients, including `opencode attach`, `opencode run --attach`, and the desktop app.

This project is **experimental**. Expect compatibility gaps while the OpenCode API surface evolves.

## Usage

### Dependencies

`pi-server` expects the `pi` and `opencode` commands to be available.

Install `pi_agent_rust`, which provides the `pi` binary:

```sh
curl -fsSL "https://raw.githubusercontent.com/Dicklesworthstone/pi_agent_rust/main/install.sh?$(date +%s)" | bash
```

Install OpenCode:

```sh
curl -fsSL https://opencode.ai/install | bash
# or, with npm:
npm i -g opencode-ai@latest
```

By default, `pi-server` looks for `pi` at `~/.local/bin/pi`. Set `PI_BIN_PATH`
if your install puts it somewhere else.

Install `pi-server` from GitHub releases:

```sh
curl -fsSL "https://raw.githubusercontent.com/mikesoylu/pi-server/main/setup.sh?$(date +%s)" | bash
```

The installer detects Linux architecture and libc/runtime, then installs the
matching release archive for Debian/glibc or Alpine/musl into `~/.local/bin`.

Pinned and custom installs:

```sh
# install a specific release tag
curl -fsSL "https://raw.githubusercontent.com/mikesoylu/pi-server/main/setup.sh?$(date +%s)" | \
  bash -s -- --version v0.1.0

# install somewhere else
curl -fsSL "https://raw.githubusercontent.com/mikesoylu/pi-server/main/setup.sh?$(date +%s)" | \
  bash -s -- --dest /usr/local/bin
```

#### Dockerfile Example

```Dockerfile
FROM debian:bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl tar \
  && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://raw.githubusercontent.com/mikesoylu/pi-server/main/setup.sh?$(date +%s)" \
  | bash -s -- --dest /usr/local/bin

EXPOSE 4096
ENTRYPOINT ["pi-server"]
CMD ["--hostname", "0.0.0.0", "--port", "4096"]
```

Build and run:

```sh
cargo run -- --hostname 127.0.0.1 --port 4096
```

Attach with OpenCode:

```sh
opencode attach http://127.0.0.1:4096
opencode run --attach http://127.0.0.1:4096 'tell me a joke'
```

Configuration:

- `PI_BIN_PATH`: path to the `pi` binary. Defaults to `~/.local/bin/pi`.
- `PI_SERVER_WORKDIR`: default working directory for new sessions. Defaults to the current directory.
- `PI_SERVER_DB`: SQLite database path. Defaults to `~/.local/share/pi-server/pi-server.db`.
- `--hostname`: bind host. Defaults to `127.0.0.1`.
- `--port`: bind port. Defaults to `4096`.

Message history is stored by `pi_agent_rust` in a per-OpenCode-session `pi`
session directory under `sessions/` next to the SQLite database. With defaults,
that storage lives under `~/.local/share/pi-server/sessions/`.

## Release CI

Each push to `main` creates a GitHub prerelease with four binary archives:

| Archive | Rust Target | Runtime |
| --- | --- | --- |
| `linux-debian-amd64` | `x86_64-unknown-linux-gnu` | Debian/glibc amd64 |
| `linux-debian-arm64` | `aarch64-unknown-linux-gnu` | Debian/glibc arm64 |
| `linux-alpine-amd64` | `x86_64-unknown-linux-musl` | Alpine/musl amd64 |
| `linux-alpine-arm64` | `aarch64-unknown-linux-musl` | Alpine/musl arm64 |

Versioning uses SemVer from `Cargo.toml`:

- Main branch snapshots: `v<Cargo.toml version>-main.<run_number>.sha<short_sha>`.
- Stable releases: update `Cargo.toml` to the desired SemVer and push a matching `vX.Y.Z` tag.
- While experimental and pre-1.0, bump minor versions for compatibility/API changes and patch versions for bug fixes.

## API Endpoint Status

Status legend:

- **Implemented**: backed by server state, `pi --mode rpc`, filesystem reads, or SSE behavior.
- **Partial**: returns OpenCode-compatible shapes, but behavior is incomplete.
- **Stub**: compatibility placeholder that returns an empty/no-op response.

Each registered method/path is listed separately so API parity can be tracked
without hiding partial implementations behind grouped rows.

### Core, Events, and Config

| Endpoint | Status | Notes |
| --- | --- | --- |
| `GET /doc` | Implemented | OpenAPI route document generated from the local route matrix. |
| `GET /global/health` | Implemented | Health response for attach/bootstrap. |
| `GET /global/event` | Implemented | Global SSE stream with directory-scoped events. |
| `GET /event` | Implemented | Instance-shaped SSE stream for TUI/CLI compatibility. |
| `POST /global/dispose` | Stub | No-op success response. |
| `POST /global/upgrade` | Stub | Returns current `pi-server` version. |
| `GET /global/config` | Partial | Minimal config shape. |
| `PATCH /global/config` | Partial | Echoes config update payload or an empty object. |
| `GET /config` | Partial | Minimal config shape. |
| `PATCH /config` | Partial | Echoes config update payload or an empty object. |
| `GET /config/providers` | Implemented | Minimal `pi` provider/model config. |
| `POST /instance/dispose` | Stub | No-op success response. |
| `POST /log` | Stub | No-op success response. |
| `GET /path` | Implemented | Returns directory/worktree paths, including directory query support. |

### Providers, Auth, Agents, and Tools

| Endpoint | Status | Notes |
| --- | --- | --- |
| `GET /provider` | Implemented | Minimal `pi` provider list. |
| `GET /provider/auth` | Stub | Empty auth state. |
| `POST /provider/{providerID}/oauth/authorize` | Stub | Empty response. |
| `POST /provider/{providerID}/oauth/callback` | Stub | No-op success response. |
| `GET /api/provider` | Implemented | v2 provider list shape. |
| `GET /api/provider/{providerID}` | Implemented | v2 provider detail shape. |
| `GET /api/model` | Implemented | v2 model list shape. |
| `PUT /auth/{providerID}` | Stub | No-op success response. |
| `DELETE /auth/{providerID}` | Stub | No-op success response. |
| `GET /agent` | Implemented | Minimal `build` agent. |
| `GET /command` | Stub | Empty command list. |
| `GET /skill` | Stub | Empty skill list. |
| `GET /experimental/tool` | Stub | Empty tool list. |
| `GET /experimental/tool/ids` | Stub | Empty tool ID list. |

### Sessions and Messages

| Endpoint | Status | Notes |
| --- | --- | --- |
| `GET /session` | Implemented | SQLite-backed list with `directory`, `workspace`, `scope=project`, `path`, `roots`, `start`, `search`, and `limit` filters. |
| `POST /session` | Implemented | Creates a session and spawns `pi --mode rpc`; honors `directory`/`workspace` query and `x-opencode-directory`. |
| `GET /session/status` | Implemented | In-memory busy/idle status map. |
| `GET /session/{sessionID}` | Implemented | Returns session metadata. |
| `PATCH /session/{sessionID}` | Partial | Supports title, archived timestamp, and permission updates. |
| `DELETE /session/{sessionID}` | Implemented | Removes session metadata/messages and shuts down the live RPC process if present. |
| `GET /session/{sessionID}/children` | Implemented | Lists sessions explicitly created with `parentID`. |
| `GET /session/{sessionID}/message` | Implemented | Lists stored messages/parts, including OpenCode-compatible `limit`/`before` cursor headers. |
| `POST /session/{sessionID}/message` | Implemented | Sends prompt to `pi` and records assistant response. |
| `GET /session/{sessionID}/message/{messageID}` | Implemented | Fetches a stored message by ID. |
| `DELETE /session/{sessionID}/message/{messageID}` | Implemented | Deletes a stored message. |
| `PATCH /session/{sessionID}/message/{messageID}/part/{partID}` | Implemented | Replaces a full OpenCode part with identity validation. |
| `DELETE /session/{sessionID}/message/{messageID}/part/{partID}` | Implemented | Deletes a stored part. |
| `POST /session/{sessionID}/prompt_async` | Implemented | Starts background prompt and publishes live events. |
| `POST /session/{sessionID}/command` | Partial | Validates the OpenCode command payload shape and converts it into a prompt. |
| `POST /session/{sessionID}/shell` | Partial | Validates the OpenCode shell payload shape and converts the shell command into a prompt. |
| `POST /session/{sessionID}/fork` | Implemented | Creates an independent persisted fork and copies message history up to optional `messageID`. |
| `POST /session/{sessionID}/abort` | Implemented | Calls RPC abort when the session has a live RPC process. |
| `POST /session/{sessionID}/share` | Partial | Sets local share metadata. |
| `DELETE /session/{sessionID}/share` | Partial | Clears local share metadata. |
| `POST /session/{sessionID}/revert` | Partial | Stores revert metadata on the session. |
| `POST /session/{sessionID}/unrevert` | Partial | Clears stored revert metadata. |
| `POST /session/{sessionID}/init` | Partial | Validates the OpenCode payload, records a real `/init` turn through `pi`, then returns success. |
| `POST /session/{sessionID}/summarize` | Partial | Validates the OpenCode payload, clears revert state, and persists summary metadata. |
| `GET /session/{sessionID}/todo` | Partial | Returns SQLite-backed todo state for the session; updates are currently internal/event-translation driven. |
| `GET /session/{sessionID}/diff` | Partial | Returns current Git diff shape for the session directory when available. |
| `POST /session/{sessionID}/permissions/{permissionID}` | Partial | Replies to a pending OpenCode permission request, publishes `permission.replied`, and persists `always` allow rules. |
| `GET /api/session` | Partial | v2 list wrapper around stored sessions with OpenCode-compatible `limit`, `order`, filter, and cursor handling. |
| `GET /api/session/{sessionID}/message` | Partial | v2 wrapper around stored messages with `limit`, `order`, and cursor handling. |
| `GET /api/session/{sessionID}/context` | Partial | Currently mirrors message list. |
| `POST /api/session/{sessionID}/prompt` | Implemented | Matches OpenCode v2 behavior: validates the session/payload, then returns `ServiceUnavailableError` because v2 prompt is not available yet. |
| `POST /api/session/{sessionID}/compact` | Implemented | Matches OpenCode v2 behavior: validates the session, then returns `ServiceUnavailableError` because v2 compact is not available yet. |
| `POST /api/session/{sessionID}/wait` | Implemented | Matches OpenCode v2 behavior: validates the session, then returns `ServiceUnavailableError` because v2 wait is not available yet. |

### Files, Search, VCS, and Projects

| Endpoint | Status | Notes |
| --- | --- | --- |
| `GET /file` | Implemented | Lists filesystem entries under server directory. |
| `GET /file/content` | Implemented | Reads text files under server directory. |
| `GET /file/status` | Stub | Empty file status list. |
| `GET /find` | Partial | Regex text search under server directory. |
| `GET /find/file` | Partial | Filename search under server directory. |
| `GET /find/symbol` | Stub | Empty symbol list. |
| `GET /vcs` | Partial | Minimal root/branch response. |
| `GET /vcs/status` | Stub | Empty status list. |
| `GET /vcs/diff` | Stub | Empty diff list. |
| `GET /vcs/diff/raw` | Stub | Empty text response. |
| `POST /vcs/apply` | Stub | No-op success response. |
| `GET /project` | Implemented | Lists SQLite-backed project metadata. |
| `GET /project/current` | Implemented | Returns or creates project metadata for the requested directory and publishes `project.updated`. |
| `POST /project/git/init` | Partial | Runs `git init --quiet`, marks the directory project as Git-initialized in metadata, and publishes `project.updated`. |
| `PATCH /project/{projectID}` | Partial | Persists project metadata updates while preserving `id` and `worktree`, then publishes `project.updated`. |

### MCP, Permissions, Questions, PTY, Sync, and Experimental

| Endpoint | Status | Notes |
| --- | --- | --- |
| `GET /mcp` | Stub | Empty MCP state. |
| `POST /mcp` | Stub | Empty MCP state. |
| `POST /mcp/{name}/auth` | Stub | Placeholder auth start response. |
| `DELETE /mcp/{name}/auth` | Stub | Placeholder auth remove response. |
| `POST /mcp/{name}/auth/callback` | Stub | Empty response. |
| `POST /mcp/{name}/auth/authenticate` | Stub | Empty response. |
| `POST /mcp/{name}/connect` | Stub | No-op success response. |
| `POST /mcp/{name}/disconnect` | Stub | No-op success response. |
| `GET /permission` | Partial | Lists in-memory pending permission requests. |
| `POST /permission/{requestID}/reply` | Partial | Replies to a pending permission request and publishes OpenCode-shaped reply events. |
| `GET /question` | Stub | Empty question list. |
| `POST /question/{requestID}/reply` | Stub | No-op success response. |
| `POST /question/{requestID}/reject` | Stub | No-op success response. |
| `GET /lsp` | Stub | Empty LSP list. |
| `GET /formatter` | Stub | Empty formatter list. |
| `GET /pty/shells` | Partial | Static shell list. |
| `GET /pty` | Stub | Empty PTY list. |
| `POST /pty` | Stub | Fake PTY metadata. |
| `GET /pty/{ptyID}` | Stub | Fake PTY metadata. |
| `PUT /pty/{ptyID}` | Stub | Fake PTY metadata. |
| `DELETE /pty/{ptyID}` | Stub | No-op success response. |
| `GET /pty/{ptyID}/connect` | Stub | No-op success response. |
| `POST /pty/{ptyID}/connect-token` | Stub | Fake token response. |
| `POST /sync/start` | Stub | No-op success response. |
| `POST /sync/replay` | Stub | Fake replay response. |
| `POST /sync/steal` | Stub | Echoes payload. |
| `POST /sync/history` | Stub | Empty history list. |
| `GET /experimental/console` | Stub | Empty console provider state. |
| `GET /experimental/console/orgs` | Stub | Empty org list. |
| `POST /experimental/console/switch` | Stub | No-op success response. |
| `GET /experimental/session` | Partial | Experimental wrapper over stored sessions. |
| `GET /experimental/resource` | Stub | Empty resource object. |
| `GET /experimental/worktree` | Stub | Empty worktree list. |
| `POST /experimental/worktree` | Stub | Current-directory placeholder metadata. |
| `DELETE /experimental/worktree` | Stub | No-op success response. |
| `POST /experimental/worktree/reset` | Stub | No-op success response. |
| `GET /experimental/workspace` | Stub | Empty workspace list. |
| `POST /experimental/workspace` | Stub | Fake local workspace metadata. |
| `GET /experimental/workspace/adapter` | Stub | Empty adapter list. |
| `GET /experimental/workspace/status` | Stub | Empty status list. |
| `POST /experimental/workspace/sync-list` | Stub | No content response. |
| `POST /experimental/workspace/warp` | Stub | No content response. |
| `DELETE /experimental/workspace/{id}` | Stub | No-op success response. |

### TUI Control

| Endpoint | Status | Notes |
| --- | --- | --- |
| `POST /tui/append-prompt` | Stub | No-op success response. |
| `POST /tui/clear-prompt` | Stub | No-op success response. |
| `GET /tui/control/next` | Stub | Null response. |
| `POST /tui/control/response` | Stub | No-op success response. |
| `POST /tui/execute-command` | Stub | No-op success response. |
| `POST /tui/open-help` | Stub | No-op success response. |
| `POST /tui/open-models` | Stub | No-op success response. |
| `POST /tui/open-sessions` | Stub | No-op success response. |
| `POST /tui/open-themes` | Stub | No-op success response. |
| `POST /tui/publish` | Stub | No-op success response. |
| `POST /tui/select-session` | Stub | No-op success response. |
| `POST /tui/show-toast` | Stub | No-op success response. |
| `POST /tui/submit-prompt` | Stub | No-op success response. |

## Development

Compatibility work is test-driven in `tests/api_compat.rs`. When fixing a client issue, add a regression that uses the same route, query params, headers, and event shape as the real OpenCode client.

Run before handing off changes:

```sh
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
git diff --check
```
