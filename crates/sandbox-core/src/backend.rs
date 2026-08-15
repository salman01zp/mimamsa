use async_trait::async_trait;
use futures::stream::BoxStream;
use tokio_util::sync::CancellationToken;

use crate::types::{
    DiscoveredSandbox, SandboxBackendCapabilities, SandboxBackendHealth, SandboxBackendId,
    SandboxDestroyed, SandboxError, SandboxId, SandboxIo, SandboxSpec, SandboxStatus,
    SandboxStatusEvent, SandboxUsage,
};

/// Object-safe, deliberately: no associated types, no generic methods, `watch` returns
/// `BoxStream` rather than `impl Stream`. This is what makes `Arc<dyn SandboxBackend>`
/// and a heterogeneous registry possible. ID-keyed, deliberately: mirrors how
/// Kubernetes and every container API actually work — a name and a client, no
/// client-side sandbox object.
///
/// Backends are stateless about ownership. That discipline (once-only I/O, consuming
/// `destroy`) lives only in `SandboxHandle`, not here.
#[async_trait]
pub trait SandboxBackend: Send + Sync {
    fn id(&self) -> SandboxBackendId;
    fn capabilities(&self) -> SandboxBackendCapabilities;

    async fn create(
        &self,
        spec: SandboxSpec,
        cancel: CancellationToken,
    ) -> Result<SandboxId, SandboxError>;

    async fn pause(&self, id: &SandboxId, cancel: CancellationToken) -> Result<(), SandboxError>;
    async fn resume(&self, id: &SandboxId, cancel: CancellationToken)
    -> Result<(), SandboxError>;
    async fn stop(&self, id: &SandboxId, cancel: CancellationToken) -> Result<(), SandboxError>;
    async fn destroy(&self, id: &SandboxId) -> SandboxDestroyed;

    async fn open_io(&self, id: &SandboxId) -> Result<SandboxIo, SandboxError>;
    async fn status(&self, id: &SandboxId) -> Result<SandboxStatus, SandboxError>;
    async fn usage(&self, id: &SandboxId) -> Result<SandboxUsage, SandboxError>;

    fn watch(&self, id: &SandboxId, since_seq: u64) -> BoxStream<'static, SandboxStatusEvent>;

    /// Everything observable, whether or not the manager knows about it.
    async fn discover(&self) -> Result<Vec<DiscoveredSandbox>, SandboxError>;

    /// Admission control.
    async fn health(&self) -> SandboxBackendHealth;
}

/// `SandboxBackend` must stay object-safe: `Arc<dyn SandboxBackend>` and the backend
/// registry depend on it. An earlier draft used an associated type and did not compile
/// with a registry at all — this assertion is the regression test for that.
const _: fn() = || {
    fn assert_object_safe(_: &dyn SandboxBackend) {}
    let _: fn(&dyn SandboxBackend) = assert_object_safe;
};
