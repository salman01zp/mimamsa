use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, DeleteParams, ListParams, PostParams};
use tokio_util::sync::CancellationToken;

use sandbox_core::SandboxBackend;
use sandbox_core::error::SandboxError;
use sandbox_core::types::{SandboxId, SandboxSpec, SandboxState};

use crate::pod::{MANAGED_BY_LABEL, MANAGED_BY_VALUE, build_pod};

/// `SandboxBackend` implementation backed by plain Kubernetes `Pod` objects
/// (no CRDs, no controller loop). Requires a real or `kind`-style cluster to
/// exercise beyond unit tests — the pod-building logic is unit-tested in
/// isolation in [`crate::pod`].
pub struct K8sBackend {
    client: kube::Client,
    namespace: String,
}

impl K8sBackend {
    /// Builds a client from the ambient kubeconfig / in-cluster config.
    pub async fn try_new(namespace: impl Into<String>) -> Result<Self, SandboxError> {
        let client = kube::Client::try_default()
            .await
            .map_err(|err| SandboxError::General(err.to_string()))?;
        Ok(Self {
            client,
            namespace: namespace.into(),
        })
    }

    fn pods(&self) -> Api<Pod> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }
}

fn map_kube_err(id: &SandboxId, err: kube::Error) -> SandboxError {
    match &err {
        kube::Error::Api(resp) if resp.code == 404 => SandboxError::NotFound(id.clone()),
        _ => SandboxError::General(err.to_string()),
    }
}

fn phase_to_state(phase: Option<&str>, terminating: bool) -> SandboxState {
    if terminating {
        return SandboxState::Stopping;
    }
    match phase {
        Some("Pending") => SandboxState::Creating,
        Some("Running") => SandboxState::Running,
        Some("Succeeded") => SandboxState::Stopped,
        _ => SandboxState::Failed,
    }
}

#[async_trait]
impl SandboxBackend for K8sBackend {
    async fn create(
        &self,
        spec: SandboxSpec,
        cancel: CancellationToken,
    ) -> Result<SandboxId, SandboxError> {
        let pod = build_pod(&spec);
        let name = pod
            .metadata
            .name
            .clone()
            .expect("build_pod always sets a name");

        let api = self.pods();
        let params = PostParams::default();
        tokio::select! {
            res = api.create(&params, &pod) => {
                res.map(|_| SandboxId::new(name))
                    .map_err(|err| SandboxError::General(err.to_string()))
            }
            _ = cancel.cancelled() => {
                Err(SandboxError::General("create cancelled".to_string()))
            }
        }
    }

    async fn stop(&self, _id: &SandboxId, _cancel: CancellationToken) -> Result<(), SandboxError> {
        Err(SandboxError::General(
            "stop is not supported: k8s pods cannot be paused/resumed".to_string(),
        ))
    }

    async fn destroy(&self, id: &SandboxId) -> Result<(), SandboxError> {
        let params = DeleteParams {
            grace_period_seconds: Some(0),
            ..Default::default()
        };
        match self.pods().delete(id.as_str(), &params).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(resp)) if resp.code == 404 => Ok(()),
            Err(err) => Err(SandboxError::General(err.to_string())),
        }
    }

    async fn status(&self, id: &SandboxId) -> Result<SandboxState, SandboxError> {
        let pod = self
            .pods()
            .get(id.as_str())
            .await
            .map_err(|err| map_kube_err(id, err))?;

        let terminating = pod.metadata.deletion_timestamp.is_some();
        let phase = pod
            .status
            .as_ref()
            .and_then(|status| status.phase.as_deref());

        Ok(phase_to_state(phase, terminating))
    }

    async fn list(&self) -> Result<Vec<SandboxId>, SandboxError> {
        let params =
            ListParams::default().labels(&format!("{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}"));
        let pods = self
            .pods()
            .list(&params)
            .await
            .map_err(|err| SandboxError::General(err.to_string()))?;

        Ok(pods
            .into_iter()
            .filter_map(|pod| pod.metadata.name)
            .map(SandboxId::new)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_404_to_not_found() {
        let id = SandboxId::new("missing");
        let err = kube::Error::Api(Box::new(kube::core::Status {
            code: 404,
            message: "not found".to_string(),
            reason: "NotFound".to_string(),
            ..Default::default()
        }));
        assert!(matches!(map_kube_err(&id, err), SandboxError::NotFound(_)));
    }

    #[test]
    fn maps_other_errors_to_general() {
        let id = SandboxId::new("whatever");
        let err = kube::Error::Api(Box::new(kube::core::Status {
            code: 500,
            message: "boom".to_string(),
            reason: "InternalError".to_string(),
            ..Default::default()
        }));
        assert!(matches!(map_kube_err(&id, err), SandboxError::General(_)));
    }

    #[test]
    fn phase_mapping() {
        assert_eq!(
            phase_to_state(Some("Pending"), false),
            SandboxState::Creating
        );
        assert_eq!(
            phase_to_state(Some("Running"), false),
            SandboxState::Running
        );
        assert_eq!(
            phase_to_state(Some("Succeeded"), false),
            SandboxState::Stopped
        );
        assert_eq!(phase_to_state(Some("Failed"), false), SandboxState::Failed);
        assert_eq!(
            phase_to_state(Some("Running"), true),
            SandboxState::Stopping
        );
        assert_eq!(phase_to_state(None, false), SandboxState::Failed);
    }
}
