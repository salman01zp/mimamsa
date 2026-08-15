# RFC 0001: Sandbox Abstraction

**Status:** Draft · **Author:** Salman · **Updated:** 2026-08-15

---

## Summary

One object-safe trait, `SandboxBackend`, that backends implement. One concrete struct, `SandboxHandle`, that consumers use.

Backends are ID-keyed and stateless about ownership — that's how Kubernetes and every container API actually work. The handle sits on top and provides the ownership ergonomics (`&mut self`, consuming `destroy`, once-only I/O) so that discipline is written once instead of re-derived per backend.

Scope: start sandboxes, stop them, move bytes. Nothing about what the bytes mean — framing happens in `session-runtime`.

---

## Why

**Testing needs a cluster.** Anything touching a sandbox needs the CRD, a controller, and a scheduled pod. Slow loop for logic that isn't about Kubernetes.

**Backend detail leaks.** Pod names, PVC names, replica patches, CRD conditions — none of it belongs in orchestration code.

**"Paused" means different things per backend**, and without shared vocabulary each consumer reinvents state tracking and gets the edges wrong.

The honest payoff is **testability, not portability**. You will likely never swap backends in production. The value is that `sandbox-manager` and `session-runtime` are developable without a cluster, and Kubernetes stays in one crate.

---

## Non-goals

- **No public API endpoints for create/stop/pause/resume.** `sandbox-manager` is a library. A future API layer can call it once the higher-level data model settles.
- **No porting of** durable execution, workflow engine, chat delivery, API key issuance, tool discovery, or assignment management.
- Exec protocol, tool-call schema, session events — those live in `session-runtime`.
- Deterministic execution replay. See Replay.

---

## Crates

```
sandbox-core/         trait, handle, spec, status, errors. no backend deps.
sandbox-local/        dev/test backend over a local child process
sandbox-agent-k8s/    Agent Sandbox CRD backend (agents.x-k8s.io)
sandbox-testkit/      conformance suite, run by every backend
sandbox-manager/      internal orchestration. no HTTP.
session-runtime/      sessions, pumps, framing, replayable event streams
```

`sandbox-core` stays dependency-light: trait, handle, types, errors. No kube client, no HTTP. If it ever depends on something backend-flavored, the abstraction has leaked.

`sandbox-agent-k8s` translates `SandboxSpec` into `agents.x-k8s.io` Sandbox resources, configures state volume claim templates, does pause/resume via replica patches, deletes CRD-owned state on stop, resolves the backing Pod for I/O transport and status, and lists Sandbox objects plus Pods for reconciliation.

`session-runtime` is the boundary that matters: bytes stay bytes through `sandbox-core` and the backends, and framing happens exactly once in the only crate that knows what a session is.

> Pick one workspace prefix and apply it everywhere — the crate list and `session-runtime`'s description currently disagree. Resolve before step 1.

---

## The trait

```rust
#[async_trait]
pub trait SandboxBackend: Send + Sync {
    fn id(&self) -> SandboxBackendId;
    fn capabilities(&self) -> SandboxBackendCapabilities;

    async fn create(&self, spec: SandboxSpec, cancel: CancellationToken)
        -> Result<SandboxId, SandboxError>;

    async fn pause(&self, id: &SandboxId, cancel: CancellationToken)
        -> Result<(), SandboxError>;
    async fn resume(&self, id: &SandboxId, cancel: CancellationToken)
        -> Result<(), SandboxError>;
    async fn stop(&self, id: &SandboxId, cancel: CancellationToken)
        -> Result<(), SandboxError>;
    async fn destroy(&self, id: &SandboxId) -> SandboxDestroyed;

    async fn open_io(&self, id: &SandboxId) -> Result<SandboxIo, SandboxError>;
    async fn status(&self, id: &SandboxId) -> Result<SandboxStatus, SandboxError>;
    async fn usage(&self, id: &SandboxId) -> Result<SandboxUsage, SandboxError>;

    fn watch(&self, id: &SandboxId, since_seq: u64)
        -> BoxStream<'static, SandboxStatusEvent>;

    /// Everything observable, whether or not the manager knows about it.
    async fn discover(&self) -> Result<Vec<DiscoveredSandbox>, SandboxError>;

    /// Admission control.
    async fn health(&self) -> SandboxBackendHealth;
}
```

Implementations: `LocalSandboxBackend`, `AgentSandboxBackend`.

**Object-safe, deliberately.** No associated types, no generic methods, `watch` returns `BoxStream` rather than `impl Stream`. This is what makes `Arc<dyn SandboxBackend>` and a heterogeneous registry possible. An earlier draft used `type Sandbox: Sandbox` and would not have compiled with a registry at all.

**ID-keyed, deliberately.** This mirrors the underlying systems. Kubernetes has no sandbox object on the wire — a name and a client. Reconciliation falls out naturally because everything is already keyed the same way.

---

## The handle

```rust
pub struct SandboxHandle {
    backend: Arc<dyn SandboxBackend>,
    id: SandboxId,
    generation: u64,
    io_taken: bool,
}

impl SandboxHandle {
    pub async fn create(
        backend: Arc<dyn SandboxBackend>,
        spec: SandboxSpec,
        cancel: CancellationToken,
    ) -> Result<Self, SandboxError>;

    pub fn id(&self) -> &SandboxId;
    pub fn capabilities(&self) -> SandboxBackendCapabilities;

    pub async fn status(&self) -> Result<SandboxStatus, SandboxError>;
    pub async fn usage(&self) -> Result<SandboxUsage, SandboxError>;
    pub fn watch(&self, since_seq: u64) -> BoxStream<'static, SandboxStatusEvent>;

    /// Once per generation. `None` if already taken.
    pub async fn take_io(&mut self) -> Result<Option<SandboxIo>, SandboxError>;
    pub fn io_generation(&self) -> u64;

    pub async fn pause(&mut self, cancel: CancellationToken) -> Result<(), SandboxError>;
    pub async fn resume(&mut self, cancel: CancellationToken) -> Result<(), SandboxError>;
    pub async fn stop(&mut self, cancel: CancellationToken) -> Result<(), SandboxError>;

    /// Consumes. Use-after-destroy is a compile error.
    pub async fn destroy(self) -> SandboxDestroyed;
}
```

**One concrete struct, not a second trait.** Backend authors implement one thing. Consumers get `&mut self` discipline, consuming `destroy`, and once-only `take_io` without anyone re-implementing them. Shared behaviour can't diverge between backends if it isn't implemented per backend.

The handle owns: generation tracking, `io_taken`, and the resume-bumps-generation rule. The backend owns: everything that touches the actual system.

---

## Pause fidelity

The load-bearing constraint. Pause is not the same operation everywhere.

Firecracker-style snapshot preserves memory and running processes. A replica patch on the K8s backend scales the pod to zero — memory gone, processes gone, state volume survives. `sandbox-local` may not pause meaningfully at all.

The state stays uniform; the fidelity is declared:

```rust
pub enum PauseFidelity {
    /// pause() returns Unsupported.
    None,
    /// Processes and memory gone. State volume survives.
    /// Resume gives a fresh execution environment on the same state.
    StatePreserving,
    /// Memory and process state survive. Resume continues where it left off.
    MemoryPreserving,
}
```

| Backend | Fidelity | Mechanism |
|---|---|---|
| `sandbox-local` | `None` | see open question 4 |
| `sandbox-agent-k8s` | `StatePreserving` | replica patch to zero and back |

**Consequence for `session-runtime`:** with `StatePreserving`, anything the agent held in memory is gone across a pause, but the sandbox looks alive on the other side. State that must survive belongs on the state volume or in the durable session store.

Do not rely on this RFC to prevent that mistake. The workload mode API should offer **no place to put in-memory state that survives an execution**. If the types don't offer the mistake, nobody makes it.

---

## State machine

```
create() → Creating → Running ⇄ Paused
                         ↓         ↓
                      Stopping ← ──┘
                         ↓
                      Stopped → Destroyed (terminal)

Failed(reason) reachable from any non-terminal state. Also terminal.
```

```rust
pub enum SandboxState {
    Creating, Running, Paused, Stopping, Stopped, Destroyed,
    Failed(SandboxFailure),
}

pub enum SandboxFailure {
    CreateTimeout, ReadinessTimeout, BackendCrashed,
    OutOfMemory, Evicted, Killed, Backend(String),
}
```

- **A paused sandbox can be stopped directly.** Never wake one just to kill it.
- **`stop()` and `destroy()` are idempotent.** Cleanup runs under failure, often twice. Erroring on double-cleanup leaks resources exactly when leaks hurt most.
- **`pause()` on paused and `resume()` on running are no-ops.**
- **Terminal states never panic** — `SandboxError::Terminal`.
- **Stop wins.** If `stop()` and `pause()` race, the sandbox ends up stopped.

`Evicted` is separate from `BackendCrashed` because it's routine on Kubernetes and usually worth retrying.

---

## I/O

```rust
pub struct SandboxIo {
    pub generation: u64,
    pub stdin:  Box<dyn AsyncWrite + Send + Unpin>,
    pub stdout: Box<dyn AsyncRead  + Send + Unpin>,
    pub stderr: Box<dyn AsyncRead  + Send + Unpin>,
}
```

**Owned, not borrowed.** Borrowed accessors (`fn stdout(&mut self) -> &mut impl AsyncRead`) hold the sandbox for the whole read, making it impossible to drain output and pause concurrently — which is the normal pattern. That forces `Arc<Mutex<_>>` on everyone and serializes operations with no reason to be serial.

**Bytes, not strings.** Untrusted code emits invalid UTF-8 routinely, sometimes deliberately, and legitimately binary output too. A `String` interface panics, lossily replaces, or errors — all wrong here. Framing is `session-runtime`'s job.

**Backpressure is the caller's.** No buffering on your behalf: an unbounded buffer fed by untrusted output is a memory-exhaustion vector.

**Handles do not survive pause.** On the K8s backend pause deletes the pod, killing the transport; resume produces a new pod and a new connection. So I/O is **generation-scoped** — `take_io()` is valid once per generation, `io_generation()` bumps on resume, stale handles give `SandboxError::StaleIoGeneration`. `session-runtime` owns the pumps and is responsible for noticing and re-pumping. Better explicit in the type than a silently dead stream in production.

---

## Spec

```rust
pub struct SandboxSpec {
    pub image: ImageRef,
    pub resources: SandboxResources,
    pub env: SandboxEnv,
    pub storage: SandboxStorage,
    pub deadline: Duration,
    pub labels: BTreeMap<String, String>,
}
```

Runtime-only: everything needed to run a sandbox, nothing about sessions or executions. `session-runtime`'s workload modes translate session intent into this; `sandbox-manager` never sees session concepts.

No `id` field — `create()` returns the `SandboxId`. No `backend` field either: backend selection is a `sandbox-manager` policy decision, not something callers pin.

```rust
pub struct SandboxResources {
    pub memory_mb: u32,
    pub cpu_millis: u32,
    pub disk_mb: u32,
    pub max_pids: u32,
}
```

All required, **no `Default` impl** — a spec that doesn't state its memory ceiling will get a bad one. **Limits, not requests**: on K8s, requests and limits set equal (Guaranteed QoS), because Burstable under adversarial load means an agent allocates to its ceiling and gets neighbours evicted. `max_pids` earns its place — fork bombs are the cheapest accidental DoS.

```rust
pub enum ImageRef {
    Digest(SandboxImageDigest),   // preferred
    Tag(String),
}
```

Content-addressed, never a path. Digest over tag for the same reason you pin dependencies: "which image ran this agent" must be answerable afterward.

```rust
pub struct SandboxEnv {
    pub vars: BTreeMap<String, String>,
    pub secrets: BTreeMap<String, SecretRef>,
}
```

Splitting secrets out means `Debug` on the spec doesn't print them, audit logs record *which* secrets without values, and the backend picks its own injection (on K8s, a Secret reference in the pod template). `SecretRef` gets a manual `Debug` printing `<redacted>`. **Nothing is inherited from the host** — inheritance is how host credentials end up inside sandboxes.

```rust
pub struct SandboxStorage {
    pub workspace_mb: u32,
    pub seed: Vec<SeedFile>,
    pub state_volume: Option<StateVolumeSpec>,
}
```

State volumes exist because K8s pause is `StatePreserving` — without one, pause destroys everything and is indistinguishable from stop.

**Lifetime rule:** survives pause/resume · **deleted on stop** · **never shared across `SandboxId`s.** That last line preserves the isolation property and is exactly what someone later makes shared for a good-sounding reason.

---

## Backend selection

```rust
pub struct SandboxBackendCapabilities {
    pub pause_fidelity: PauseFidelity,
    pub state_volumes: bool,
    pub warm_pool: bool,
    pub gpu: bool,
    pub max_memory_mb: u32,
}

pub enum SandboxBackendHealth {
    Ready { available_slots: Option<u32> },
    Saturated,
    Degraded(String),
    Unavailable(String),
}

pub struct SandboxBackendRegistry {
    backends: HashMap<SandboxBackendId, Arc<dyn SandboxBackend>>,
    default: SandboxBackendId,
}
```

The spec declares what it needs; the registry decides what satisfies it. `available_slots` is `Option` because the K8s backend can't answer — cluster capacity is a scheduler question.

> **Decided: do not build a warm pool.** `SandboxWarmPool` already exists as an extension CRD. Reimplementing means two pool implementations with divergent semantics, and the CRD one wins eventually. `sandbox-manager` expresses warm-eligibility in the spec and lets the controller serve it.

---

## Reconciliation

Observed backend state is authoritative; the manager's map is a cache that will be lost.

On K8s this is **three-way**: manager ↔ Sandbox CRD ↔ backing Pod. The CRD layer is eventually consistent — a Sandbox object routinely exists before its Pod schedules.

```rust
pub struct DiscoveredSandbox {
    pub id: SandboxId,
    pub backend_id: SandboxBackendId,
    pub state: SandboxState,
    pub artifacts: SandboxObservedArtifacts,   // pid | crd_uid, pod_name, pvc_name
}
```

`SandboxId` must be recoverable from observed state — encode it in the Sandbox object's name and a Pod label, or `discover()` returns objects it can't map back.

| Observed | Manager has | Action |
|---|---|---|
| CRD + Pod running | Matching record | Nothing |
| CRD + Pod running | Nothing | Orphan → reap |
| **CRD, no Pod yet** | Matching record | **Wait** — still scheduling |
| CRD, no Pod, past grace | Matching record | `Failed(ReadinessTimeout)` → reap |
| CRD, replicas 0 | Record says paused | Nothing — correct |
| No CRD | Active record | Stale → `Failed(BackendCrashed)` |
| PVC, no CRD | Anything | Leaked → delete |

**The "wait" row matters most.** Reaping on first inconsistency kills sandboxes that are merely still scheduling, and does it hardest under load. Every discrepancy needs a grace period before it becomes an action.

**Policy: never adopt, always reap** — except across pause, where the CRD is legitimately the durable record. On restart, in-flight sandboxes die and callers retry. Adoption has a long tail: was it mid-pause, is its I/O reattachable (no — generations), who owns its deadline.

A background reaper runs the same scan periodically and consumes `SandboxDestroyed::leaked`.

---

## Replay

**Inputs** — re-run the same spec fresh. *Supported.* Why the spec is digest-pinned, env explicit, nothing inherited.

**State transitions** — reconstruct what happened. *Supported*, via the event log:

```rust
pub struct SandboxStatusEvent {
    pub id: SandboxId,
    pub seq: u64,
    pub at: Timestamp,
    pub state: SandboxState,
    pub cause: SandboxTransitionCause,   // Requested | DeadlineExceeded
                                         // | BackendReported | Reconciled
}
```

`seq` makes it replayable — a late subscriber detects gaps instead of silently missing the transition to `Failed`. `SandboxStatus` carries `seq` too, so a caller holding a snapshot subscribes from exactly that point: no gap, no duplicate. `session-runtime`'s replayable session streams build on this ordering discipline.

`SandboxTransitionCause` separates "operator stopped it" from "hit its deadline" from "found gone during reconciliation." Without it the log says the sandbox stopped but not why.

**Deterministic execution** — *not supported, not worth attempting.* Agents call models, hit networks, read clocks.

---

## Status and errors

```rust
pub struct SandboxStatus {
    pub id: SandboxId,
    pub backend_id: SandboxBackendId,
    pub state: SandboxState,
    pub seq: u64,
    pub io_generation: u64,
    pub created_at: Timestamp,
    pub state_since: Timestamp,
    pub outcome: Option<SandboxOutcome>,   // Exited(i32) | TimedOut | OutOfMemory
                                           // | Evicted | Killed | BackendFailure
}
```

`state_since` lets reaping express "paused longer than N" without consumers keeping their own timers. `io_generation` lets a pump detect a stale handle without an extra call. `SandboxOutcome` is an enum because "exit code of a sandbox killed by timeout" isn't a meaningful integer.

**Timestamps come from an injected clock.** Timeout logic reading the wall clock can't be tested without sleeping.

```rust
pub enum SandboxError {
    NotFound(SandboxId),
    Terminal(SandboxState),
    InvalidTransition { from: SandboxState, op: &'static str },
    Unsupported(&'static str),
    StaleIoGeneration { stale: u64, current: u64 },
    Timeout(Duration),
    Cancelled,
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
}
```

`NotFound` is new and unavoidable with an ID-keyed trait — it's the cost of object safety. The handle converts it where it can, so consumers rarely see it. `Backend` boxes the source rather than flattening to a string, so callers who care can downcast.

---

## Alternatives considered

**Two traits (`Sandbox` + `SandboxBackend` with an associated type).** The previous draft. Not object-safe, so the registry couldn't compile. Also forced every backend to reimplement ownership discipline. `SandboxHandle` gives the same ergonomics with one trait.

**Single trait with no handle.** Loses consuming `destroy`, `&mut self`, and once-only `take_io` — all become runtime errors. The handle is cheap and recovers them.

**Uniform `Paused` with no fidelity distinction.** Makes a snapshot backend and a scale-to-zero backend claim the same guarantee; consumers written against the stronger one break silently on the weaker.

**Handles that survive pause.** Impossible on K8s, where pause deletes the pod.

**Typestate (`Sandbox<Running>`).** Not object-safe, and a sandbox can transition to `Failed` asynchronously, so the type-level state would routinely lie.

**Shared state volumes.** Reintroduces exactly the cross-sandbox contamination the design exists to prevent.

**Exec/tool-call semantics on the trait.** Couples the isolation primitive to the harness protocol and forces `sandbox-local` to implement things it has no business implementing.

---

## Rollout

Spike `sandbox-agent-k8s` against a throwaway interface **first**. Both prior revisions of this RFC were driven by discovering CRD facts — create-to-ready latency, what status surfaces during pause, whether Pod resolution for I/O works as expected. `sandbox-local` will validate any interface you write and teach you nothing. A week of spike prevents a third rewrite.

Then:

1. `sandbox-core` + `sandbox-local` + `sandbox-testkit`
2. `sandbox-agent-k8s`, verified against the conformance suite
3. `sandbox-manager` over the registry
4. `session-runtime` migrated onto it
5. Thin HTTP adapters, once the data model settles

---

## Open questions

1. **CRD API version.** Spec references `v1alpha1`; the group has graduated to `v1beta1` and releases land every week or two. Confirm the target cluster and pin exactly — a graduated group breaks a backend crate quietly. Keep the translation layer mechanical so a breaking change is a contained diff.
2. **Should the handle enforce `deadline` itself** rather than trusting `sandbox-manager` to arm a timer? A forgotten timeout is a leaked sandbox and a running bill.
3. **Is `usage()` lifecycle or metrics?** Reaping heuristics need it, which argues for keeping it, but it's the one method that isn't about lifecycle.
4. **Does `sandbox-local` pause at all?** `SIGSTOP` gives `MemoryPreserving` — *stronger* than production, so tests could pass against semantics you don't have. Argues for `None`, or a deliberately crippled `StatePreserving` simulation.
5. **Should `sandbox-manager` and `session-runtime` merge for now?** Their descriptions overlap — both ensure a backing sandbox exists, both reconcile. Six crates before any exist is a lot of boundary to defend. Merging and splitting later costs less than defending a seam in the wrong place.