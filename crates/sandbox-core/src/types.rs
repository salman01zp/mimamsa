use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use bytes::Bytes;



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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxSpec {
    pub image: ImageRef,
    pub resources: SandboxResources,
    pub env: SandboxEnv,
    pub storage: SandboxStorage,
    pub deadline: Duration,
    pub labels: BTreeMap<String, String>,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxResources {
    pub memory_mb: u32,
    pub cpu_millis: u32,
    pub disk_mb: u32,
    pub max_pids: u32,
}

/// Content-addressed, never a path. Digest over tag for the same reason you pin
/// dependencies: "which image ran this agent" must be answerable afterward.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImageRef {
    Digest(SandboxImageDigest),
    Tag(String),
}

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


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxEnv {
    pub vars: BTreeMap<String, String>,
    pub secrets: BTreeMap<String, SecretRef>,
}

/// A reference to secret material, not the material itself — the backend resolves it
/// (a K8s Secret name/key, for instance).
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxStorage {
    pub workspace_mb: u32,
    pub seed: Vec<SeedFile>,
    pub state_volume: Option<StateVolumeSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedFile {
    pub path: String,
    pub contents: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateVolumeSpec {
    pub size_mb: u32,
}


