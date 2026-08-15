# RFC: Sandbox Abstraction Trait

**Status:** Draft · **Author:** Salman · **Updated:** 2026-08-15

---

## What this is

One Rust trait that hides the difference between sandbox backends (Firecracker microVMs, containers, a fake local backend for tests).

Three things it does:

1. **Lifecycle** — create, pause, resume, stop, destroy.
2. **I/O** — hands you owned byte streams you can move wherever you want.
3. **Status** — tells you what state a sandbox is in, and lets you watch for changes.

That's the whole scope. It starts and stops sandboxes and moves bytes. It does not know what the bytes mean.

---

## Why bother

Right now sandbox code is welded to Firecracker. Socket paths, snapshot handles, tap device names — all of it leaks into call sites. Three problems fall out of that:

**You need a hypervisor to run a test.** Anything that touches a sandbox needs a real VM to boot. CI is slow and expensive.

**You can't compare backends.** Deciding between Firecracker, gVisor, and Kata means rewriting call sites, not swapping a constructor. So the decision gets made on vibes instead of numbers.

**There's no word for "paused."** Snapshot-and-resume is the whole latency strategy, but the code has no way to say a sandbox is paused rather than stopped. So pooling logic reinvents state tracking in every backend.

---

## Goals

- One trait, several backends.
- Pause and resume are real lifecycle operations, not side effects of something else.
- I/O handles are **owned** — you can move them into other tasks.
- I/O is **bytes**. No strings, no JSON, no line framing.
- State is reconstructible from the host after a crash.

## Non-goals

- **Exposing create/stop/pause/resume through public API endpoints.** This is internal orchestration only. Whether any of it is ever public is a separate decision with a separate threat model.
- **Porting durable execution, workflow engine, chat delivery, API key issuance, tool discovery, or assignment management.** None of that belongs behind this trait.
- The exec protocol, tool-call schema, or harness wire format. Those sit on top of the byte streams.
- Scheduling and pool sizing policy. This provides the primitives; policy is built from them.
- Deterministic replay of execution. See the replay section for why.

---

## The state machine

```
                    ┌──────────┐
      create() ────▶│ Creating │
                    └────┬─────┘
                         │
                    ┌────▼─────┐
              ┌────▶│ Running  │◀────┐
              │     └────┬─────┘     │
       resume()│    pause()│     stop()│
              │     ┌────▼─────┐     │
              └─────┤  Paused  ├─────┘
                    └────┬─────┘
                    stop()│
                    ┌────▼─────┐
                    │ Stopped  │
                    └────┬─────┘
                 destroy()│
                    ┌────▼─────┐
                    │ Destroyed│  terminal
                    └──────────┘

     Failed(reason) reachable from any non-terminal state. Also terminal.
```

Rules that matter:

- **A paused sandbox can be stopped directly.** You should never have to wake a VM just to kill it — pool eviction does this constantly.
- **`stop()` and `destroy()` are idempotent.** Calling them twice succeeds. This isn't a nicety: cleanup runs under failure conditions, often more than once, and an abstraction that errors on double-cleanup leaks resources exactly when leaks hurt most.
- **`pause()` on a paused sandbox is a no-op**, same for `resume()` on a running one.
- **Terminal states never panic.** Calling anything on a destroyed sandbox returns `Error::Terminal`.

### Why pause and resume are first-class

They could have been backend detail — "the pool manager knows how to snapshot." Three reasons they're not:

**They're the latency strategy.** Resuming a paused VM is roughly 10x faster than creating one. If the trait can't say "paused," every consumer rebuilds pooling logic per backend.

**Paused ≠ stopped.** A paused sandbox keeps its memory, file descriptors, and process state. A stopped one doesn't. Collapsing them loses information that pooling and reaping both need.

**Backends that can't pause should say so out loud.** The local test backend can't meaningfully pause. It declares that through `capabilities()`, so callers find out at construction time instead of at first use.

---

## The trait

```rust
#[async_trait]
pub trait Sandbox: Send + Sync {
    fn id(&self) -> SandboxId;
    fn backend(&self) -> BackendId;
    fn capabilities(&self) -> Capabilities;

    async fn status(&self) -> Status;
    async fn usage(&self) -> Result<Usage, Error>;

    /// Replays from `since_seq` so a late subscriber can't miss a transition.
    fn watch(&self, since_seq: u64) -> impl Stream<Item = StatusEvent> + Send;

    /// Take the byte streams. Succeeds once; `None` after that.
    fn take_io(&mut self) -> Option<SandboxIo>;

    async fn pause(&mut self, cancel: CancellationToken) -> Result<(), Error>;
    async fn resume(&mut self, cancel: CancellationToken) -> Result<(), Error>;
    async fn stop(&mut self, cancel: CancellationToken) -> Result<(), Error>;

    /// Always succeeds from the caller's side. Anything it couldn't clean up
    /// is recorded as a leak for the reaper.
    async fn destroy(self: Box<Self>) -> Destroyed;
}

#[async_trait]
pub trait SandboxBackend: Send + Sync {
    type Sandbox: Sandbox;

    async fn create(&self, spec: SandboxSpec, cancel: CancellationToken)
        -> Result<Self::Sandbox, Error>;

    fn capabilities(&self) -> Capabilities;

    /// Can this backend take more work right now? For admission control.
    async fn health(&self) -> BackendHealth;

    /// Everything this backend can see on the host, whether or not the
    /// control plane knows about it. See reconciliation.
    async fn discover(&self) -> Result<Vec<Discovered>, Error>;
}
```

A few shape decisions worth explaining:

**`create` is on the backend, not the sandbox.** A sandbox that doesn't exist can't create itself. The backend is also the natural home for pool state.

**`take_io` returns `Option` and works once.** Two owners of the same stdout is a bug. The type system enforces it instead of a doc comment.

**`destroy` consumes `Box<Self>` and doesn't return `Result`.** Consuming self makes use-after-destroy a compile error. `Box<Self>` (rather than plain `self`) keeps the trait object-safe, which you need because backend selection happens at runtime. And it can't fail *from the caller's perspective* — if teardown half-works, there's no handle left to retry with, so partial failures get recorded as leaks and the reaper deals with them.

**Every fallible operation takes a `CancellationToken`.** Creating a Firecracker VM takes 125ms+. If the caller walks away, you need to abort and clean up rather than finish booting a VM nobody wants.

**`status()` takes `&self`.** Metrics scraping shouldn't contend with lifecycle operations.

---

## I/O

```rust
pub struct SandboxIo {
    pub stdin:  Box<dyn AsyncWrite + Send + Unpin>,
    pub stdout: Box<dyn AsyncRead  + Send + Unpin>,
    pub stderr: Box<dyn AsyncRead  + Send + Unpin>,
}
```

**Owned, not borrowed.** This is the most important ergonomic call in the RFC. The alternative — `fn stdout(&mut self) -> &mut impl AsyncRead` — borrows the sandbox for as long as you're reading. That makes it impossible to drain output and call `pause()` at the same time, because both need the sandbox. Since draining output while managing lifecycle is the *normal* pattern, borrowed I/O forces `Arc<Mutex<Sandbox>>` on everyone and serializes operations that have no reason to be serial.

**Bytes, not strings.** Untrusted code emits invalid UTF-8 routinely, sometimes on purpose. A `String` interface either panics, lossily replaces, or errors — all three are wrong here. Agents also produce legitimately binary output: images, archives, compiled artifacts. Framing is a protocol concern for the harness layer; baking line-orientation into the transport forecloses on protocols that aren't line-oriented.

**Backpressure is yours, deliberately.** If you stop reading, the backend applies natural backpressure. The trait does not buffer on your behalf, because an unbounded buffer in the control plane fed by untrusted guest output is a memory-exhaustion vector. Output caps belong where the policy limits live.

**Handles survive pause.** Pausing doesn't close them; reads just go pending until resume. Pool logic pauses VMs while consumers still hold handles, and invalidating them would force reconnection on every resume.

---

## The spec

```rust
pub struct SandboxSpec {
    pub id: SandboxId,
    pub image: ImageRef,
    pub resources: Resources,
    pub env: EnvSpec,
    pub storage: StorageSpec,
    pub deadline: Duration,
    pub labels: BTreeMap<String, String>,
}
```

### Resources

```rust
pub struct Resources {
    pub memory_mb: u32,
    pub vcpus: u8,
    pub disk_mb: u32,
    pub max_pids: u32,
}
```

**All required. No defaults.** A spec that doesn't state its memory ceiling will eventually get a bad default and OOM the host. Make the caller say the number.

**These are limits, not requests.** Hard-enforced, no burst allowance. Anything implying burstable semantics will get overcommitted by someone eventually, and overcommitting memory across a security boundary under adversarial load is how you OOM the host — an agent can trivially allocate right up to its ceiling.

`max_pids` earns its place: fork bombs are the cheapest DoS an agent can write by accident.

### Image

```rust
pub enum ImageRef {
    Digest(ImageDigest),   // content-addressed, preferred
    Tag(String),           // resolved at create time
}
```

**Content-addressed, not a path.** If `ImageRef` contains `/var/lib/fc/images/foo.ext4`, the abstraction has leaked and the container backend can't implement it. Each backend resolves the digest to its own format: rootfs + kernel for Firecracker, OCI image for containers, a working directory for the local backend.

**Digest over tag**, for the same reason you pin dependencies. "Which image did this agent run in" has to be answerable after the fact, and mutable tags make that impossible.

### Env

```rust
pub struct EnvSpec {
    pub vars: BTreeMap<String, String>,
    pub secrets: BTreeMap<String, SecretRef>,
}

pub struct SecretRef(String);  // an identifier, never a value
```

Splitting secrets from plain vars buys three things: `Debug` on the spec doesn't print secrets; audit logs can record *which* secrets were mounted without recording values; and the backend can pick a better injection mechanism than env vars — file mount, delivery over vsock at init — without changing the spec.

**Nothing is inherited from the host.** The guest starts with exactly what the spec says. Inheritance is how host credentials end up inside sandboxes.

Most secrets shouldn't be here at all — prefer having the egress proxy attach credentials so the agent never holds a key. This field is for the cases where a tool genuinely won't work otherwise.

### Storage

```rust
pub struct StorageSpec {
    pub workspace_mb: u32,        // wiped on destroy, always present
    pub seed: Vec<SeedFile>,      // read-only, written before boot
}
```

Note what's **not** here: persistent volumes. The moment a volume outlives a sandbox, "ephemeral and independently disposable" stops being true and you've built exactly the cross-sandbox contamination channel the isolation design exists to prevent. If something needs to survive, export it out through the control plane rather than mounting a shared volume in.

Seed files are written by the *host* before the guest boots. The guest never gets a channel to reach back into the host filesystem.

---

## Backend selection

```rust
pub struct BackendRegistry {
    backends: HashMap<BackendId, Arc<dyn SandboxBackend>>,
    default: BackendId,
}
```

**Selection is a control-plane policy decision, not a spec field.** Deliberately no `backend: BackendId` in `SandboxSpec` — that lets callers pin themselves to a backend and defeats the point. The spec declares *what it needs* (capabilities, resources); the registry decides *what satisfies that*, using tenant tier and config.

`Status` does report which backend actually ran it. You want that in the audit record and in every postmortem.

---

## Reconciliation

**The host is authoritative, not the control plane's memory.** The in-memory map is a cache, and it will be lost — process restarts, deploys, crashes.

```rust
pub struct Discovered {
    pub id: SandboxId,
    pub state: State,
    pub artifacts: HostArtifacts,   // pids, tap devices, overlay paths
}
```

This only works if identity is **externalized into host artifacts**. Concretely: encode `SandboxId` into the cgroup name, the tap device name, and the overlay directory path. Then `discover()` is a process-and-filesystem scan.

If you allocate tap devices as `tap0`, `tap1`, reconciliation is impossible. Name them `tap-{short_id}` and it's a scan.

On startup, diff:

| Host has | Control plane has | Action |
|---|---|---|
| Running VM | Matching record | Nothing — it's fine |
| Running VM | Nothing | Orphan → reap |
| Nothing | Active record | Stale → mark `Failed(BackendCrashed)` |
| Tap/overlay, no VM | Anything | Leaked resource → clean up |

**Policy: never adopt, always reap.** On restart, kill everything and let callers retry. Adoption sounds nicer but has a long tail of edge cases — was that VM mid-pause, is its I/O reattachable (probably not), who owns its deadline. Sandboxes are supposed to be ephemeral; killing them on restart is consistent with that. Revisit only if long-running sandboxes turn out to matter.

A background reaper runs the same scan every 30s to catch leaks that accumulate during normal operation.

---

## Replay

Three different things get called "replay." They need different answers.

**Replaying inputs** — re-run the same spec and command sequence in a fresh sandbox. *Supported.* This is why the spec is digest-pinned, env is explicit, and nothing is inherited from the host. A spec is a complete, reproducible description. The command sequence itself is logged one layer up.

**Replaying state transitions** — reconstruct what happened to a sandbox for debugging. *Supported*, via the event log:

```rust
pub struct StatusEvent {
    pub id: SandboxId,
    pub seq: u64,
    pub at: Timestamp,
    pub state: State,
    pub cause: TransitionCause,
}
```

The `seq` field is what makes it replayable — a late subscriber can detect gaps and catch up rather than silently missing the transition to `Failed`. That's why `watch()` takes `since_seq`.

**Replaying execution deterministically** — identical output from an identical re-run. *Not supported, and not worth attempting.* Agents call LLMs, hit networks, and read clocks. You'd need to record and replay every syscall boundary to get determinism, and the result would still diverge the moment a model version changed.

---

## Status and errors

```rust
pub struct Status {
    pub id: SandboxId,
    pub backend: BackendId,
    pub state: State,
    pub created_at: Timestamp,
    pub state_since: Timestamp,
    pub outcome: Option<Outcome>,
}

pub enum Outcome {
    Exited(i32),
    TimedOut,
    OutOfMemory,
    Killed,
    BackendFailure,
}
```

`state_since` exists so reaping can express "paused longer than N" without every consumer keeping its own timers keyed by sandbox ID. That bookkeeping belongs with the state.

`Outcome` is an enum rather than a bare exit code because "exit code of a sandbox killed by timeout" isn't a meaningful integer, and callers need to distinguish OOM from a clean exit from a backend crash.

**Timestamps come from an injected clock**, not `SystemTime::now()`. Timeout logic that reads the wall clock directly can't be tested without sleeping.

```rust
pub enum Error {
    Terminal(State),
    InvalidTransition { from: State, op: &'static str },
    Unsupported(&'static str),
    Timeout(Duration),
    Cancelled,
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
}
```

`Backend` boxes the source error rather than flattening to a string, so callers who care about a specific backend can downcast and everyone else can log the chain.

### Concurrent lifecycle calls

`&mut self` prevents this within one owner, but behind an `Arc<Mutex<_>>` two tasks can race. Stated rule: **stop wins.** If `stop()` and `pause()` are in flight together, the sandbox ends up stopped and the pause becomes a no-op. Destruction always beats anything else.

---

