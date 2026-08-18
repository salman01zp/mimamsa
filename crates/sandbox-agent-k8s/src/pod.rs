use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Container, EnvVar, Pod, PodSpec, ResourceRequirements};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use sandbox_core::types::{ImageRef, SandboxSpec};

/// Label applied to every pod this backend creates, used to scope `list()`.
pub(crate) const MANAGED_BY_LABEL: &str = "mimamsa.dev/managed-by";
pub(crate) const MANAGED_BY_VALUE: &str = "sandbox-agent-k8s";

/// Generates a fresh pod name and builds the `Pod` manifest for `spec`.
///
/// Only the core fields (image, cpu/memory, env vars, labels, deadline) are
/// wired up; secrets, seed files, state volumes, and max_pids are not yet
/// supported by this backend.
pub(crate) fn build_pod(spec: &SandboxSpec) -> Pod {
    let name = format!("sandbox-{}", uuid::Uuid::new_v4().simple());

    let mut labels: BTreeMap<String, String> = spec.labels.clone();
    labels.insert(MANAGED_BY_LABEL.to_string(), MANAGED_BY_VALUE.to_string());

    let image = match &spec.image {
        ImageRef::Tag(tag) => tag.clone(),
        ImageRef::Digest(digest) => digest.as_str().to_string(),
    };

    let env: Vec<EnvVar> = spec
        .env
        .vars
        .iter()
        .map(|(name, value)| EnvVar {
            name: name.clone(),
            value: Some(value.clone()),
            ..Default::default()
        })
        .collect();

    let mut resource_map = BTreeMap::new();
    resource_map.insert(
        "memory".to_string(),
        Quantity(format!("{}Mi", spec.resources.memory_mb)),
    );
    resource_map.insert(
        "cpu".to_string(),
        Quantity(format!("{}m", spec.resources.cpu_millis)),
    );

    let container = Container {
        name: "sandbox".to_string(),
        image: Some(image),
        env: Some(env),
        resources: Some(ResourceRequirements {
            requests: Some(resource_map.clone()),
            limits: Some(resource_map),
            ..Default::default()
        }),
        ..Default::default()
    };

    Pod {
        metadata: ObjectMeta {
            name: Some(name),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![container],
            restart_policy: Some("Never".to_string()),
            active_deadline_seconds: Some(spec.deadline.as_secs() as i64),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sandbox_core::types::{SandboxEnv, SandboxResources, SandboxStorage};
    use std::time::Duration;

    fn minimal_spec() -> SandboxSpec {
        let mut vars = BTreeMap::new();
        vars.insert("FOO".to_string(), "bar".to_string());

        let mut labels = BTreeMap::new();
        labels.insert("team".to_string(), "agents".to_string());

        SandboxSpec {
            image: ImageRef::Tag("busybox:latest".to_string()),
            resources: SandboxResources {
                memory_mb: 256,
                cpu_millis: 500,
                disk_mb: 1024,
                max_pids: 64,
            },
            env: SandboxEnv {
                vars,
                secrets: BTreeMap::new(),
            },
            storage: SandboxStorage {
                workspace_mb: 0,
                seed: Vec::new(),
                state_volume: None,
            },
            deadline: Duration::from_secs(120),
            labels,
        }
    }

    #[test]
    fn sets_managed_by_label_alongside_spec_labels() {
        let pod = build_pod(&minimal_spec());
        let labels = pod.metadata.labels.expect("labels set");
        assert_eq!(
            labels.get(MANAGED_BY_LABEL),
            Some(&MANAGED_BY_VALUE.to_string())
        );
        assert_eq!(labels.get("team"), Some(&"agents".to_string()));
    }

    #[test]
    fn maps_image_env_and_deadline() {
        let pod = build_pod(&minimal_spec());
        let pod_spec = pod.spec.expect("pod spec set");
        let container = &pod_spec.containers[0];

        assert_eq!(container.image.as_deref(), Some("busybox:latest"));
        assert_eq!(pod_spec.active_deadline_seconds, Some(120));
        assert_eq!(pod_spec.restart_policy.as_deref(), Some("Never"));

        let env = container.env.as_ref().expect("env set");
        assert!(
            env.iter()
                .any(|e| e.name == "FOO" && e.value.as_deref() == Some("bar"))
        );
    }

    #[test]
    fn maps_cpu_and_memory_resources() {
        let pod = build_pod(&minimal_spec());
        let container = &pod.spec.unwrap().containers[0];
        let resources = container.resources.as_ref().expect("resources set");
        let requests = resources.requests.as_ref().expect("requests set");

        assert_eq!(requests.get("memory"), Some(&Quantity("256Mi".to_string())));
        assert_eq!(requests.get("cpu"), Some(&Quantity("500m".to_string())));
    }

    #[test]
    fn generates_unique_names() {
        let spec = minimal_spec();
        let a = build_pod(&spec);
        let b = build_pod(&spec);
        assert_ne!(a.metadata.name, b.metadata.name);
        assert!(a.metadata.name.unwrap().starts_with("sandbox-"));
    }
}
