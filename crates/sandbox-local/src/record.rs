use std::process::{Child, ExitStatus};
use std::time::Duration;

use sandbox_core::{
    SandboxFailure, SandboxId, SandboxOutcome, SandboxState, SandboxStatusEvent,
    SandboxTransitionCause, Timestamp,
};
use tokio::sync::broadcast;
use tokio::time::{sleep, timeout};

use crate::pipes::ProcessPipes;

const EVENTS_CAPACITY: usize = 64;
const REAP_TIMEOUT: Duration = Duration::from_secs(5);
const REAP_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Reaps a child after it's been sent a kill signal. `std::process::Child` has no
/// async `wait()`, so rather than blocking the executor thread on the synchronous one,
/// this polls `try_wait()` with a short sleep between attempts. Bounded by a timeout as
/// a defensive fallback only -- SIGKILL can't be caught or ignored, so in practice this
/// resolves on the first or second poll.
pub(crate) async fn reap(child: &mut Child) -> Option<ExitStatus> {
    if let Ok(Some(status)) = child.try_wait() {
        return Some(status);
    }
    timeout(REAP_TIMEOUT, async {
        loop {
            if let Ok(Some(status)) = child.try_wait() {
                return status;
            }
            sleep(REAP_POLL_INTERVAL).await;
        }
    })
    .await
    .ok()
}

pub(crate) struct SandboxRecord {
    pub(crate) child: Child,
    pub(crate) pipes: ProcessPipes,
    pub(crate) state: SandboxState,
    pub(crate) outcome: Option<SandboxOutcome>,
    pub(crate) created_at: Timestamp,
    pub(crate) state_since: Timestamp,
    pub(crate) seq: u64,
    events: broadcast::Sender<SandboxStatusEvent>,
}

impl SandboxRecord {
    pub(crate) fn new(child: Child, pipes: ProcessPipes, now: Timestamp) -> Self {
        let (events, _rx) = broadcast::channel(EVENTS_CAPACITY);
        Self {
            child,
            pipes,
            state: SandboxState::Running,
            outcome: None,
            created_at: now,
            state_since: now,
            seq: 0,
            events,
        }
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<SandboxStatusEvent> {
        self.events.subscribe()
    }

    fn transition(
        &mut self,
        id: &SandboxId,
        state: SandboxState,
        cause: SandboxTransitionCause,
        now: Timestamp,
    ) {
        self.state = state.clone();
        self.state_since = now;
        self.seq += 1;
        let event = SandboxStatusEvent {
            id: id.clone(),
            seq: self.seq,
            at: now,
            state,
            cause,
        };
        // Err just means nobody is currently subscribed via watch(); nothing to do.
        let _ = self.events.send(event);
    }

    /// Non-blocking: checks whether the child has exited since we last looked, without
    /// ever blocking on it. Call before reading `state` from any read-only method
    /// (`status`, `discover`) as well as before any operation that branches on
    /// terminal-ness (`pause`, `resume`, `stop`) — otherwise a self-exited process
    /// would still read back as `Running` until something else happened to notice.
    pub(crate) fn refresh(&mut self, id: &SandboxId, now: Timestamp) {
        if is_terminal(&self.state) {
            return;
        }
        match self.child.try_wait() {
            Ok(None) => {}
            Ok(Some(status)) => {
                self.outcome = Some(outcome_from_exit_status(status));
                self.transition(
                    id,
                    SandboxState::Stopped,
                    SandboxTransitionCause::BackendReported,
                    now,
                );
            }
            Err(err) => {
                // try_wait() itself erroring is not "the process exited badly" -- it's
                // this backend's own bookkeeping (the OS wait4 call) breaking, which is
                // the one realistic path to `Failed` for a backend this simple.
                self.transition(
                    id,
                    SandboxState::Failed(SandboxFailure::Backend(err.to_string())),
                    SandboxTransitionCause::BackendReported,
                    now,
                );
            }
        }
    }

    /// Used by `stop()`/`destroy()` after they've killed the child themselves.
    pub(crate) fn mark_stopped_by(
        &mut self,
        id: &SandboxId,
        outcome: SandboxOutcome,
        now: Timestamp,
    ) {
        self.outcome = Some(outcome);
        self.transition(id, SandboxState::Stopped, SandboxTransitionCause::Requested, now);
    }
}

pub(crate) fn is_terminal(state: &SandboxState) -> bool {
    matches!(
        state,
        SandboxState::Stopped | SandboxState::Destroyed | SandboxState::Failed(_)
    )
}

fn outcome_from_exit_status(status: std::process::ExitStatus) -> SandboxOutcome {
    match status.code() {
        Some(code) => SandboxOutcome::Exited(code),
        // No exit code means it died to a signal -- whether that was our own
        // stop()/destroy() or something external, `Killed` is the honest bucket for it.
        None => SandboxOutcome::Killed,
    }
}
