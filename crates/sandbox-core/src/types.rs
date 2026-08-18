use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use bytes::Bytes;



/// Opaque identifier for a sandbox instance, assigned by the backend on creation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SandboxId(String);

impl SandboxId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SandboxId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}



/// Lifecycle state of a sandbox, as reported by the backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SandboxState {
    Creating,
    Running,
    Paused,
    Stopping,
    Stopped,
    Destroyed,
    Failed
}


// ---------------------------------------------------------------------------
// Spec
// ---------------------------------------------------------------------------

/// Full specification for creating a sandbox: image, resources, environment,
/// storage, and deadline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxSpec {
    pub image: ImageRef,
    pub resources: SandboxResources,
    pub env: SandboxEnv,
    pub storage: SandboxStorage,
    pub deadline: Duration,
    pub labels: BTreeMap<String, String>,
}


/// Resource limits applied to a sandbox (memory, CPU, disk, process count).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxResources {
    pub memory_mb: u32,
    pub cpu_millis: u32,
    pub disk_mb: u32,
    pub max_pids: u32,
}


/// Reference to the container image to run, either pinned by digest or by tag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImageRef {
    Digest(SandboxImageDigest),
    Tag(String),
}

/// Content-addressed image digest, e.g. a `sha256:...` string.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SandboxImageDigest(String);

impl SandboxImageDigest {
    pub fn new(digest: impl Into<String>) -> Self {
        Self(digest.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}


/// Environment for a sandbox: plain variables plus references to secrets
/// resolved by the backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxEnv {
    pub vars: BTreeMap<String, String>,
    pub secrets: BTreeMap<String, SecretRef>,
}

/// Reference to secret material (name and key) that the backend resolves at runtime.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretRef {
    pub name: String,
    pub key: String,
}

impl fmt::Debug for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<redacted>")
    }
}

/// Storage configuration for a sandbox: workspace size, seed files, and an
/// optional persistent state volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxStorage {
    pub workspace_mb: u32,
    pub seed: Vec<SeedFile>,
    pub state_volume: Option<StateVolumeSpec>,
}

/// A file to be written into the sandbox workspace before it starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedFile {
    pub path: String,
    pub contents: Bytes,
}

/// Specification for an optional persistent state volume attached to the sandbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateVolumeSpec {
    pub size_mb: u32,
}


