# Sandbox Control Plane

Rust workspace. Agent sandboxes over the Kubernetes Agent Sandbox CRD, with a local
process backend for development.

Design decisions and their reasoning live in `rfcs/0001-sandbox.md`. Read it before
changing anything in `sandbox-core`.

## Commands

- Build: `cargo build --workspace`
- Test: `cargo nextest run`
- Lint: `cargo clippy --all-targets --all-features -- -D warnings`
- Format: `cargo fmt --all`
- Run clippy and the nearest tests before reporting a task complete.

## Layout

- `sandbox-core/` — trait, `SandboxHandle`, spec, status, errors. No backend deps.
- `sandbox-local/` — dev/test backend over a child process
- `sandbox-agent-k8s/` — Agent Sandbox CRD backend
- `sandbox-testkit/` — conformance suite every backend runs
- `sandbox-manager/` — internal orchestration, library only
- `session-runtime/` — sessions, stdio pumps, byte-to-event framing

## Invariants

These are load-bearing. Breaking one is a correctness bug, not a style preference.

- `sandbox-core` depends on no backend. No kube client, no HTTP, no CRD types.
- Sandbox I/O is bytes end to end. No `String`, no line framing, no UTF-8 assumption
  below `session-runtime`.
- `SandboxBackend` must stay object-safe. No associated types, no generic methods,
  return `BoxStream` not `impl Stream`.
- Backends are ID-keyed and hold no ownership state. Ownership discipline
  (once-only io, consuming destroy) lives only in `SandboxHandle`.
- State volumes are never shared across `SandboxId`s.
- `stop()` and `destroy()` are idempotent. Terminal states return an error, never panic.
- Timestamps come from the injected clock, never `SystemTime::now()`.
- Nothing in `sandbox-manager` or below knows what a session is.

## Conventions

- Errors: `thiserror` in libraries, `anyhow` only in binaries.
- No `unwrap()` or `expect()` outside tests.
- Every fallible async operation takes a `CancellationToken`.
- New types in `sandbox-core` get the `Sandbox` prefix — see the naming section in
  the RFC.
- Backend changes need a matching `sandbox-testkit` case.

## Do not

- Do not add comments explaining what code does. Comment non-obvious *why* only.
- Do not write README, CHANGELOG, or doc files unless asked.
- Do not add dependencies without asking.
- Do not create abstractions for a single call site.
- Do not add error branches for cases that cannot occur.
- Do not build warm-pooling logic. The `SandboxWarmPool` CRD owns that.
- Do not let CRD types escape `sandbox-agent-k8s`.
- Do not summarize your changes at the end. One line stating what changed is enough.
- Do not restate the task before starting.