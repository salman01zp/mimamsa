use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use sandbox_core::{
    DiscoveredSandbox, ImageRef, PauseFidelity, SandboxBackend, SandboxBackendCapabilities,
    SandboxBackendHealth, SandboxBackendId, SandboxDestroyed, SandboxError, SandboxId, SandboxIo,
    SandboxObservedArtifacts, SandboxOutcome, SandboxSpec, SandboxStatus, SandboxStatusEvent,
    SandboxUsage, Timestamp,
};
use tokio::sync::Mutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::pipes::ProcessPipes;
use crate::record::{self, SandboxRecord, is_terminal};
use crate::resources;

fn image_path(image: &ImageRef) -> &str {
    match image {
        ImageRef::Digest(digest) => digest.as_str(),
        ImageRef::Tag(tag) => tag.as_str(),
    }
}

/// Dev/test backend over `std::process::Command`, per RFC 0001. `pause_fidelity` is
/// `None`: see `pause`/`resume` below and RFC open question 4.
///
/// Spec fields this backend deliberately does not act on: `image` is used as a literal
/// binary path (real image resolution is out of scope), `env.secrets` is never
/// injected (this backend has no secret store to resolve `SecretRef`s against),
/// `storage` (workspace materialization, seed files, state volumes) is not
/// provisioned, `labels` has nowhere to go in the RFC's output types, and `deadline`
/// is not auto-enforced (the RFC leaves whether that's the handle's job or the
/// manager's as an open question -- see open question 2 -- so this backend doesn't
/// pick an answer on its own).
pub struct LocalSandboxBackend {
    id: SandboxBackendId,
    /// `sandbox-core` mandates timestamps come from an injected clock, never
    /// `SystemTime::now()`, but defines no clock/injection type of its own. A
    /// monotonic `Instant` captured at construction, converted to elapsed
    /// milliseconds, is this backend's stand-in -- fine for a process-local dev
    /// backend that never needs to compare timestamps across a restart, but every
    /// backend will end up reinventing something like this unless `sandbox-core`
    /// eventually hoists a shared clock type.
    start: Instant,
    records: Arc<Mutex<HashMap<SandboxId, SandboxRecord>>>,
}

impl LocalSandboxBackend {
    pub fn new() -> Self {
        Self {
            id: SandboxBackendId::new("local"),
            start: Instant::now(),
            records: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn now(&self) -> Timestamp {
        Timestamp(self.start.elapsed().as_millis() as u64)
    }
}

impl Default for LocalSandboxBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SandboxBackend for LocalSandboxBackend {
    fn id(&self) -> SandboxBackendId {
        self.id.clone()
    }

    fn capabilities(&self) -> SandboxBackendCapabilities {
        SandboxBackendCapabilities {
            pause_fidelity: PauseFidelity::None,
            state_volumes: false,
            warm_pool: false,
            gpu: false,
            // Not measured from the host: a dev backend doesn't reserve or track
            // capacity the way a real one would.
            max_memory_mb: u32::MAX,
        }
    }

    /// `spawn()` is a single, effectively-atomic syscall sequence (fork+exec) -- it
    /// can't be interrupted partway through, so "cancelled mid-spawn" is handled by
    /// checking immediately before and immediately after it: if cancellation landed in
    /// that narrow window, the process may already exist, so it gets killed and reaped
    /// here rather than left running with nothing in the registry pointing at it.
    async fn create(
        &self,
        spec: SandboxSpec,
        cancel: CancellationToken,
    ) -> Result<SandboxId, SandboxError> {
        if cancel.is_cancelled() {
            return Err(SandboxError::Cancelled);
        }

        let mut command = std::process::Command::new(image_path(&spec.image));
        command.envs(spec.env.vars.iter());
        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());
        resources::apply(&mut command, spec.resources.memory_mb);

        let mut child = command
            .spawn()
            .map_err(|err| SandboxError::Backend(Box::new(err)))?;

        if cancel.is_cancelled() {
            let _ = child.kill();
            record::reap(&mut child).await;
            return Err(SandboxError::Cancelled);
        }

        let id = SandboxId::new(format!("local-{}", child.id()));

        let stdin = child.stdin.take().expect("stdin was piped at spawn");
        let stdout = child.stdout.take().expect("stdout was piped at spawn");
        let stderr = child.stderr.take().expect("stderr was piped at spawn");
        let pipes = ProcessPipes::spawn(stdin, stdout, stderr);

        let record = SandboxRecord::new(child, pipes, self.now());
        self.records.lock().await.insert(id.clone(), record);

        Ok(id)
    }

    /// Always `Unsupported`: `capabilities().pause_fidelity` is `None` for this
    /// backend. `SIGSTOP` would give `MemoryPreserving` fidelity here -- *stronger*
    /// than what's declared -- which would let tests pass against semantics a real
    /// backend (K8s: `StatePreserving` via a replica patch) doesn't offer. See RFC
    /// open question 4.
    async fn pause(&self, id: &SandboxId, cancel: CancellationToken) -> Result<(), SandboxError> {
        if cancel.is_cancelled() {
            return Err(SandboxError::Cancelled);
        }
        let now = self.now();
        let mut records = self.records.lock().await;
        let record = records
            .get_mut(id)
            .ok_or_else(|| SandboxError::NotFound(id.clone()))?;
        record.refresh(id, now);
        if is_terminal(&record.state) {
            return Err(SandboxError::Terminal(record.state.clone()));
        }
        Err(SandboxError::Unsupported(
            "sandbox-local: pause_fidelity is None (RFC open question 4)",
        ))
    }

    async fn resume(
        &self,
        id: &SandboxId,
        cancel: CancellationToken,
    ) -> Result<(), SandboxError> {
        if cancel.is_cancelled() {
            return Err(SandboxError::Cancelled);
        }
        let now = self.now();
        let mut records = self.records.lock().await;
        let record = records
            .get_mut(id)
            .ok_or_else(|| SandboxError::NotFound(id.clone()))?;
        record.refresh(id, now);
        if is_terminal(&record.state) {
            return Err(SandboxError::Terminal(record.state.clone()));
        }
        Err(SandboxError::Unsupported(
            "sandbox-local: pause_fidelity is None (RFC open question 4)",
        ))
    }

    async fn stop(&self, id: &SandboxId, cancel: CancellationToken) -> Result<(), SandboxError> {
        if cancel.is_cancelled() {
            return Err(SandboxError::Cancelled);
        }
        let now = self.now();
        let mut records = self.records.lock().await;
        let record = records
            .get_mut(id)
            .ok_or_else(|| SandboxError::NotFound(id.clone()))?;
        record.refresh(id, now);
        if is_terminal(&record.state) {
            // Idempotent: already stopped (or failed/destroyed), nothing to do.
            return Ok(());
        }
        let _ = record.child.kill();
        record::reap(&mut record.child).await;
        record.mark_stopped_by(id, SandboxOutcome::Killed, now);
        Ok(())
    }

    /// Removes the record entirely rather than leaving a `Destroyed` tombstone --
    /// mirrors a real backend deleting the underlying object (a subsequent `status()`
    /// on K8s after the CRD is deleted would also come back `NotFound`, not a
    /// lingering `Destroyed` snapshot).
    ///
    /// `Destroyed` vs `Leaked` is decided by whether a *record* was found, not by
    /// whether the process underneath it happened to still be running: a normal
    /// `stop()`-then-`destroy()` (or destroying an already self-exited sandbox) is a
    /// known record being deliberately torn down, which is `Destroyed`. `Leaked` is
    /// for when this call finds nothing at all -- the id was never known, or a prior
    /// `destroy()` already removed it -- matching the RFC's own use of it for a
    /// background reaper cleaning up resources the manager's bookkeeping lost track
    /// of.
    async fn destroy(&self, id: &SandboxId) -> SandboxDestroyed {
        let mut records = self.records.lock().await;
        match records.remove(id) {
            Some(mut record) => {
                if !is_terminal(&record.state) {
                    let _ = record.child.kill();
                    record::reap(&mut record.child).await;
                }
                SandboxDestroyed::Destroyed
            }
            None => SandboxDestroyed::Leaked,
        }
    }

    /// No once-only enforcement: every call gets a fresh, independent set of handles
    /// onto the same underlying pump threads. Whether that's safe to use concurrently
    /// is the caller's problem -- `sandbox-manager` is where that discipline belongs,
    /// per the RFC.
    async fn open_io(&self, id: &SandboxId) -> Result<SandboxIo, SandboxError> {
        let records = self.records.lock().await;
        let record = records
            .get(id)
            .ok_or_else(|| SandboxError::NotFound(id.clone()))?;
        let (stdin, stdout, stderr) = record.pipes.open().await;
        Ok(SandboxIo {
            // Always 0: this backend has no pause fidelity, so I/O handles never go
            // stale across a resume the way the K8s backend's do.
            generation: 0,
            stdin: Box::new(stdin),
            stdout: Box::new(stdout),
            stderr: Box::new(stderr),
        })
    }

    async fn status(&self, id: &SandboxId) -> Result<SandboxStatus, SandboxError> {
        let now = self.now();
        let mut records = self.records.lock().await;
        let record = records
            .get_mut(id)
            .ok_or_else(|| SandboxError::NotFound(id.clone()))?;
        record.refresh(id, now);
        Ok(SandboxStatus {
            id: id.clone(),
            backend_id: self.id.clone(),
            state: record.state.clone(),
            seq: record.seq,
            io_generation: 0,
            created_at: record.created_at,
            state_since: record.state_since,
            outcome: record.outcome.clone(),
        })
    }

    /// Best-effort and partial: `memory_mb` comes from `/proc/<pid>/status` on Linux
    /// (0 elsewhere, or if the process is already gone). `cpu_millis` and `disk_mb` are
    /// reported as 0 rather than estimated -- accurate versions of both need cgroup
    /// accounting this backend doesn't have, and a fabricated non-zero number would be
    /// worse than an honest zero. `pids` counts only the immediate child, not any
    /// descendants it may have spawned.
    async fn usage(&self, id: &SandboxId) -> Result<SandboxUsage, SandboxError> {
        let pid = {
            let records = self.records.lock().await;
            let record = records
                .get(id)
                .ok_or_else(|| SandboxError::NotFound(id.clone()))?;
            record.child.id()
        };
        Ok(SandboxUsage {
            memory_mb: crate::usage::read_memory_mb(pid).unwrap_or(0),
            cpu_millis: 0,
            disk_mb: 0,
            pids: 1,
        })
    }

    /// No historical replay: `since_seq` only filters events that arrive *after*
    /// subscribing, it doesn't reconstruct anything that happened before this call. A
    /// real backend with a persisted event log could do better; this one only forwards
    /// live transitions.
    fn watch(&self, id: &SandboxId, since_seq: u64) -> BoxStream<'static, SandboxStatusEvent> {
        let records = Arc::clone(&self.records);
        let id = id.clone();

        enum State {
            Init(Arc<Mutex<HashMap<SandboxId, SandboxRecord>>>, SandboxId),
            Streaming(tokio::sync::broadcast::Receiver<SandboxStatusEvent>),
        }

        stream::unfold(State::Init(records, id), move |mut state| async move {
            loop {
                state = match state {
                    State::Init(records, id) => {
                        let guard = records.lock().await;
                        let rx = guard.get(&id)?.subscribe();
                        State::Streaming(rx)
                    }
                    State::Streaming(mut rx) => {
                        use tokio::sync::broadcast::error::RecvError;
                        match rx.recv().await {
                            Ok(event) if event.seq > since_seq => {
                                return Some((event, State::Streaming(rx)));
                            }
                            Ok(_) => State::Streaming(rx),
                            Err(RecvError::Lagged(_)) => State::Streaming(rx),
                            Err(RecvError::Closed) => return None,
                        }
                    }
                };
            }
        })
        .boxed()
    }

    async fn discover(&self) -> Result<Vec<DiscoveredSandbox>, SandboxError> {
        let now = self.now();
        let backend_id = self.id.clone();
        let mut records = self.records.lock().await;
        Ok(records
            .iter_mut()
            .map(|(id, record)| {
                record.refresh(id, now);
                DiscoveredSandbox {
                    id: id.clone(),
                    backend_id: backend_id.clone(),
                    state: record.state.clone(),
                    artifacts: SandboxObservedArtifacts {
                        pid: Some(record.child.id()),
                        crd_uid: None,
                        pod_name: None,
                        pvc_name: None,
                    },
                }
            })
            .collect())
    }

    /// Always `Ready`: a dev backend with no scheduler behind it has no admission
    /// control to report, and `available_slots: None` is exactly how the RFC already
    /// expects a backend that can't answer capacity questions to respond.
    async fn health(&self) -> SandboxBackendHealth {
        SandboxBackendHealth::Ready {
            available_slots: None,
        }
    }
}
