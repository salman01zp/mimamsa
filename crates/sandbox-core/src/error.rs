use crate::types::SandboxId;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SandboxError {
    #[error("sandbox not found: {0}")]
    NotFound(SandboxId),

    #[error("backend error: {0}")]
    General(String),
}
