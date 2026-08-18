use crate::types::SandboxId;
use thiserror::Error;

/// Errors returned by sandbox backend operations.
#[derive(Debug, Error)]
pub enum SandboxError {
    #[error("sandbox not found: {0}")]
    NotFound(SandboxId),

    #[error("backend error: {0}")]
    General(String),
}
