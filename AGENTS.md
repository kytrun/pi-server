# AGENTS.md

## Project Overview

`pi-server` is a Rust HTTP API server that aims to be wire-compatible with `opencode serve`, while using `pi --mode rpc` processes as the backend for each session.

Core layout:

- `src/server.rs`: Axum routes, session state, SSE event publishing, opencode compatibility surface.
- `src/pi_rpc.rs`: JSON-RPC process wrapper for `pi --mode rpc`.
- `src/storage.rs`: SQLite-backed project/session/todo metadata storage. Message history is owned by `pi_agent_rust` session storage and accessed through `pi --mode rpc`.
- `src/models.rs`: OpenCode-shaped API/event models.
- `src/ids.rs`: sortable OpenCode-style IDs for sessions, messages, and parts.
- `src/config.rs`: CLI/env config. `PI_BIN_PATH` defaults to `~/.local/bin/pi`; `PI_SERVER_DB` defaults to `~/.pi-server.db`.
- `tests/api_compat.rs`: main compatibility and smoke-test harness.

Reference repos:

- `pi` source: `/Users/mikesoylu/Projects/pi_agent_rust`
- `opencode` source: `/Users/mikesoylu/Projects/opencode`

## Development Rules

- Prefer idiomatic Rust and existing crates over low-level custom plumbing.
- Keep compatibility behavior in tests before changing route/event logic.
- Do not hand-wave OpenCode behavior: inspect the local opencode source when matching shapes, event names, or query/header behavior.
- Be careful with directory scoping. The desktop app routes events by `directory`; session, message, part, status, and raw pi events must publish under the session directory, not always the server cwd.
- Preserve event ordering. The TUI and desktop rely on sortable IDs and live event order for correct rendering.

## TDD Patterns Used Here

For every compatibility bug:

1. Reproduce it in `tests/api_compat.rs` using the same route/header/query pattern the real client uses.
2. Assert the exact OpenCode shape or event routing rule that failed.
3. Implement the smallest server change.
4. Run the focused test first, then the full verification suite.

Useful regression patterns already in the harness:

- Desktop session creation: use encoded `x-opencode-directory` on `POST /session`.
- Sidebar list behavior: assert `/session?directory=...&roots=true&limit=...`.
- Live desktop rendering: assert `message.updated`, `message.part.updated`, `message.part.delta`, and `session.status` events are published to the session directory.
- Stream fidelity: assert translated thinking/tool/text pi RPC events become native OpenCode message parts.
- Ordering: assert generated assistant IDs/part IDs sort after client-supplied user message IDs.
- Persistence: recreate `AppState` against the same `PI_SERVER_DB` and assert projects, sessions, messages, and lazy RPC restart behavior.
- OpenCode compatibility details: assert cursor-shaped message paging, fork history cloning, workspace filters, and payload validation for session/project routes.

## Verification

Run before handing off:

```sh
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
git diff --check
```

For quick iteration, run the narrow test first:

```sh
cargo test <test_name>
```

For manual smoke testing:

```sh
cargo run -- --hostname 127.0.0.1 --port 4096
opencode run --attach http://127.0.0.1:4096 'tell me a joke'
opencode attach http://127.0.0.1:4096
```
