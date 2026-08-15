use std::sync::Arc;

use futures::stream::BoxStream;
use tokio_util::sync::CancellationToken;

use crate::backend::SandboxBackend;
use crate::types::{
    SandboxBackendCapabilities, SandboxDestroyed, SandboxError, SandboxId, SandboxIo,
    SandboxSpec, SandboxStatus, SandboxStatusEvent, SandboxUsage,
};

/// One concrete struct, not a second trait: backend authors implement one thing, and
/// consumers get `&mut self` discipline, consuming `destroy`, and once-only `take_io`
/// without anyone re-implementing them per backend. The handle owns generation
/// tracking, `io_taken`, and the resume-bumps-generation rule; the backend owns
/// everything that touches the actual system.
pub struct SandboxHandle {
    #[allow(dead_code)]
    backend: Arc<dyn SandboxBackend>,
    id: SandboxId,
    generation: u64,
    io_taken: bool,
}

impl std::fmt::Debug for SandboxHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SandboxHandle")
            .field("id", &self.id)
            .field("generation", &self.generation)
            .field("io_taken", &self.io_taken)
            .finish_non_exhaustive()
    }
}

// Scaffold only: bodies are `todo!()` until a backend exists to drive them, so
// parameters go unused and clippy::todo would otherwise fire on every stub.
#[allow(unused_variables, clippy::todo)]
impl SandboxHandle {
    pub async fn create(
        backend: Arc<dyn SandboxBackend>,
        spec: SandboxSpec,
        cancel: CancellationToken,
    ) -> Result<Self, SandboxError> {
        todo!()
    }

    pub fn id(&self) -> &SandboxId {
        todo!()
    }

    pub fn capabilities(&self) -> SandboxBackendCapabilities {
        todo!()
    }

    pub async fn status(&self) -> Result<SandboxStatus, SandboxError> {
        todo!()
    }

    pub async fn usage(&self) -> Result<SandboxUsage, SandboxError> {
        todo!()
    }

    pub fn watch(&self, since_seq: u64) -> BoxStream<'static, SandboxStatusEvent> {
        todo!()
    }

    /// Once per generation. `None` if already taken.
    pub async fn take_io(&mut self) -> Result<Option<SandboxIo>, SandboxError> {
        todo!()
    }

    pub fn io_generation(&self) -> u64 {
        todo!()
    }

    pub async fn pause(&mut self, cancel: CancellationToken) -> Result<(), SandboxError> {
        todo!()
    }

    pub async fn resume(&mut self, cancel: CancellationToken) -> Result<(), SandboxError> {
        todo!()
    }

    pub async fn stop(&mut self, cancel: CancellationToken) -> Result<(), SandboxError> {
        todo!()
    }

    /// Consumes. Use-after-destroy is a compile error.
    pub async fn destroy(self) -> SandboxDestroyed {
        todo!()
    }
}
