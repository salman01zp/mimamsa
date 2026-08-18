
use async_trait::async_trait;

use tokio_util::sync::CancellationToken;

use crate::{error::SandboxError, types::{
    SandboxId, SandboxSpec, SandboxState
}};



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

/// `SandboxBackend` must stay object-safe: `Arc<dyn SandboxBackend>` and the backend
/// registry depend on it. An earlier draft used an associated type and did not compile
/// with a registry at all — this assertion is the regression test for that.
const _: fn() = || {
    fn assert_object_safe(_: &dyn SandboxBackend) {}
    let _: fn(&dyn SandboxBackend) = assert_object_safe;
};
