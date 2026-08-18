
use async_trait::async_trait;

use tokio_util::sync::CancellationToken;

use crate::{error::SandboxError, types::{
    SandboxId, SandboxSpec, SandboxState
}};



/// Backend capable of creating, controlling, and querying sandboxes.
#[async_trait]
pub trait SandboxBackend: Send + Sync {
    
    async fn create(
        &self,
        spec: SandboxSpec,
        cancel: CancellationToken,
    ) -> Result<SandboxId, SandboxError>;


    async fn stop(&self, id: &SandboxId, cancel: CancellationToken) -> Result<(), SandboxError>;
    
    async fn destroy(&self, id: &SandboxId) -> Result<(), SandboxError>;

    async fn status(&self, id: &SandboxId) -> Result<SandboxState, SandboxError>;

    /// List all sandboxes this backend currently knows about.
    async fn list(&self) -> Result<Vec<SandboxId>, SandboxError>;

}
