use crate::{SandboxId, SandboxState};
use std::time::Duration;
use thiserror::Error;

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
