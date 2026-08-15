use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use futures::io::{AsyncRead, AsyncWrite};
use thiserror::Error;

// ---------------------------------------------------------------------------
// IDs
// ---------------------------------------------------------------------------

/// ID-keyed, deliberately: mirrors the underlying systems. Kubernetes has no sandbox
/// object on the wire, only a name and a client — reconciliation falls out naturally
/// because everything is already keyed the same way.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SandboxId(String);

impl SandboxId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SandboxId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SandboxBackendId(String);

impl SandboxBackendId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SandboxBackendId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Comes from an injected clock, never `SystemTime::now()` — timeout and reaping logic
/// that reads the wall clock directly can't be tested without sleeping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(pub u64);

// ---------------------------------------------------------------------------
// Pause fidelity and state machine
// ---------------------------------------------------------------------------

/// The state stays uniform across backends; the fidelity of `pause()` is declared
/// separately so a snapshot backend and a scale-to-zero backend can't be mistaken for
/// offering the same guarantee.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PauseFidelity {
    /// pause() returns Unsupported.
    None,
    /// Processes and memory gone. State volume survives.
    /// Resume gives a fresh execution environment on the same state.
    StatePreserving,
    /// Memory and process state survive. Resume continues where it left off.
    MemoryPreserving,
}

/// - A paused sandbox can be stopped directly; never wake one just to kill it.
/// - `pause()` on paused and `resume()` on running are no-ops.
/// - Stop wins: if `stop()` and `pause()` race, the sandbox ends up stopped.
/// - `Failed` is reachable from any non-terminal state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SandboxState {
    Creating,
    Running,
    Paused,
    Stopping,
    Stopped,
    Destroyed,
    Failed(SandboxFailure),
}

/// `Evicted` is kept separate from `BackendCrashed` because it's routine on Kubernetes
/// and usually worth retrying.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SandboxFailure {
    CreateTimeout,
    ReadinessTimeout,
    BackendCrashed,
    OutOfMemory,
    Evicted,
    Killed,
    Backend(String),
}

// ---------------------------------------------------------------------------
// I/O
// ---------------------------------------------------------------------------

/// Owned, not borrowed: a borrowed accessor (`fn stdout(&mut self) -> &mut impl
/// AsyncRead`) would hold the sandbox for the whole read, making it impossible to drain
/// output and pause concurrently — the normal pattern. Bytes, not strings: untrusted
/// code emits invalid UTF-8 routinely, and a `String` interface panics, lossily
/// replaces, or errors — all wrong here. Framing is `session-runtime`'s job.
///
/// Backpressure is the caller's: nothing here buffers on the caller's behalf, since an
/// unbounded buffer fed by untrusted output is a memory-exhaustion vector.
///
/// Handles do not survive pause, so I/O is generation-scoped: `take_io()` is valid once
/// per generation, `io_generation()` bumps on resume, stale handles give
/// `SandboxError::StaleIoGeneration`.
pub struct SandboxIo {
    pub generation: u64,
    pub stdin: Box<dyn AsyncWrite + Send + Unpin>,
    pub stdout: Box<dyn AsyncRead + Send + Unpin>,
    pub stderr: Box<dyn AsyncRead + Send + Unpin>,
}

impl fmt::Debug for SandboxIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SandboxIo")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Spec
// ---------------------------------------------------------------------------

/// Runtime-only: everything needed to run a sandbox, nothing about sessions or
/// executions. No `id` field (`create()` returns the `SandboxId`) and no `backend`
/// field (backend selection is a `sandbox-manager` policy decision, not something
/// callers pin).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxSpec {
    pub image: ImageRef,
    pub resources: SandboxResources,
    pub env: SandboxEnv,
    pub storage: SandboxStorage,
    pub deadline: Duration,
    pub labels: BTreeMap<String, String>,
}

/// All required, no `Default` impl: a spec that doesn't state its memory ceiling would
/// get a bad one. Treated as limits, not requests — on K8s, requests and limits are set
/// equal (Guaranteed QoS), because Burstable under adversarial load means an agent
/// allocates to its ceiling and gets neighbours evicted. `max_pids` earns its place:
/// fork bombs are the cheapest accidental DoS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxResources {
    pub memory_mb: u32,
    pub cpu_millis: u32,
    pub disk_mb: u32,
    pub max_pids: u32,
}

/// Content-addressed, never a path. Digest over tag for the same reason you pin
/// dependencies: "which image ran this agent" must be answerable afterward.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImageRef {
    Digest(SandboxImageDigest),
    Tag(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SandboxImageDigest(String);

impl SandboxImageDigest {
    pub fn new(digest: impl Into<String>) -> Self {
        Self(digest.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Splitting secrets out of `vars` means `Debug` on the spec never prints values, audit
/// logs record *which* secrets without values, and the backend picks its own injection
/// (on K8s, a Secret reference in the pod template). Nothing is inherited from the host
/// — inheritance is how host credentials end up inside sandboxes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxEnv {
    pub vars: BTreeMap<String, String>,
    pub secrets: BTreeMap<String, SecretRef>,
}

/// A reference to secret material, not the material itself — the backend resolves it
/// (a K8s Secret name/key, for instance).
#[derive(Clone, PartialEq, Eq)]
pub struct SecretRef {
    pub name: String,
    pub key: String,
}

impl fmt::Debug for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<redacted>")
    }
}

/// State volumes exist because K8s pause is `StatePreserving` — without one, pause
/// destroys everything and is indistinguishable from stop.
///
/// Lifetime rule: survives pause/resume, deleted on stop, never shared across
/// `SandboxId`s — sharing would reintroduce the cross-sandbox contamination this design
/// exists to prevent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxStorage {
    pub workspace_mb: u32,
    pub seed: Vec<SeedFile>,
    pub state_volume: Option<StateVolumeSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedFile {
    pub path: String,
    pub contents: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateVolumeSpec {
    pub size_mb: u32,
}

// ---------------------------------------------------------------------------
// Backend selection
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxBackendCapabilities {
    pub pause_fidelity: PauseFidelity,
    pub state_volumes: bool,
    pub warm_pool: bool,
    pub gpu: bool,
    pub max_memory_mb: u32,
}

/// Admission control. `available_slots` is `Option` because a backend like the K8s one
/// can't answer — cluster capacity is a scheduler question.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SandboxBackendHealth {
    Ready { available_slots: Option<u32> },
    Saturated,
    Degraded(String),
    Unavailable(String),
}

// ---------------------------------------------------------------------------
// Destroy
// ---------------------------------------------------------------------------

/// `destroy()` is idempotent and infallible: cleanup runs under failure, often twice,
/// and erroring on double-cleanup leaks resources exactly when leaks hurt most. This
/// reports which case happened rather than raising an error for either.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SandboxDestroyed {
    /// The sandbox was live on the backend and was torn down by this call.
    Destroyed,
    /// The backend had already lost track of the sandbox; this call reclaimed leaked
    /// resources. Consumed by the background reaper.
    Leaked,
}

// ---------------------------------------------------------------------------
// Reconciliation
// ---------------------------------------------------------------------------

/// Everything observable, whether or not the manager knows about it. `SandboxId` must
/// be recoverable from observed state, or `discover()` returns objects that can't be
/// mapped back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredSandbox {
    pub id: SandboxId,
    pub backend_id: SandboxBackendId,
    pub state: SandboxState,
    pub artifacts: SandboxObservedArtifacts,
}

/// Kept as plain optional fields rather than a per-backend enum: `sandbox-core` has no
/// backend deps, and this is the one place reconciliation needs to compare whatever a
/// backend happened to observe (pid, or crd_uid/pod_name/pvc_name) against its records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxObservedArtifacts {
    pub pid: Option<u32>,
    pub crd_uid: Option<String>,
    pub pod_name: Option<String>,
    pub pvc_name: Option<String>,
}

// ---------------------------------------------------------------------------
// Status and usage
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxStatus {
    pub id: SandboxId,
    pub backend_id: SandboxBackendId,
    pub state: SandboxState,
    /// Lets a caller holding a snapshot subscribe to `watch` from exactly this point:
    /// no gap, no duplicate.
    pub seq: u64,
    /// Lets a pump detect a stale `SandboxIo` handle without an extra call.
    pub io_generation: u64,
    pub created_at: Timestamp,
    /// Lets reaping express "paused longer than N" without consumers keeping their own
    /// timers.
    pub state_since: Timestamp,
    pub outcome: Option<SandboxOutcome>,
}

/// An enum, not an exit code: "exit code of a sandbox killed by timeout" isn't a
/// meaningful integer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SandboxOutcome {
    Exited(i32),
    TimedOut,
    OutOfMemory,
    Evicted,
    Killed,
    BackendFailure,
}

/// Resource consumption as observed by the backend. Whether this belongs to lifecycle
/// or metrics is unresolved (RFC open question 3); reaping heuristics are the only
/// declared consumer so far.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxUsage {
    pub memory_mb: u32,
    pub cpu_millis: u32,
    pub disk_mb: u32,
    pub pids: u32,
}

// ---------------------------------------------------------------------------
// Replay
// ---------------------------------------------------------------------------

/// `seq` makes the stream replayable — a late subscriber detects gaps instead of
/// silently missing a transition to `Failed`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxStatusEvent {
    pub id: SandboxId,
    pub seq: u64,
    pub at: Timestamp,
    pub state: SandboxState,
    pub cause: SandboxTransitionCause,
}

/// Separates "operator stopped it" from "hit its deadline" from "found gone during
/// reconciliation" — without it the log says the sandbox stopped but not why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxTransitionCause {
    Requested,
    DeadlineExceeded,
    BackendReported,
    Reconciled,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// `NotFound` is the cost of object safety with an ID-keyed trait: the handle converts
/// it away where it can, so consumers rarely see it. `Backend` boxes the source rather
/// than flattening to a string, so callers who care can downcast.
#[derive(Debug, Error)]
pub enum SandboxError {
    #[error("sandbox not found: {0}")]
    NotFound(SandboxId),

    #[error("sandbox is in terminal state {0:?}")]
    Terminal(SandboxState),

    #[error("invalid transition: {op} from {from:?}")]
    InvalidTransition {
        from: SandboxState,
        op: &'static str,
    },

    #[error("unsupported: {0}")]
    Unsupported(&'static str),

    #[error("stale io generation: have {stale}, current {current}")]
    StaleIoGeneration { stale: u64, current: u64 },

    #[error("timed out after {0:?}")]
    Timeout(Duration),

    #[error("cancelled")]
    Cancelled,

    #[error("backend error")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
}


