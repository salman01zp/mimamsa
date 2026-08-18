use std::collections::BTreeMap;
use std::error::Error;
use std::time::Duration;

use clap::{Parser, Subcommand};
use sandbox_agent_k8s::K8sBackend;
use sandbox_core::types::{
    ImageRef, SandboxEnv, SandboxId, SandboxResources, SandboxSpec, SandboxState, SandboxStorage,
};
use sandbox_core::{SandboxBackend, SandboxError};
use tokio_util::sync::CancellationToken;

const DEFAULT_IMAGE: &str = "registry.k8s.io/pause:3.9";
const STATUS_POLL_ATTEMPTS: u32 = 30;
const STATUS_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Manual + automated driver for exercising `K8sBackend` against a live cluster.
#[derive(Parser)]
struct Cli {
    /// Kubernetes namespace to operate in.
    #[arg(long, global = true, env = "NAMESPACE", default_value = "default")]
    namespace: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the full automated create -> status -> list -> stop -> destroy flow.
    Run {
        #[arg(long, default_value = DEFAULT_IMAGE)]
        image: String,
    },
    /// Create a single sandbox pod and print its id.
    Create {
        #[arg(long, default_value = DEFAULT_IMAGE)]
        image: String,
        #[arg(long, default_value_t = 64)]
        memory_mb: u32,
        #[arg(long, default_value_t = 100)]
        cpu_millis: u32,
        #[arg(long, default_value_t = 300)]
        deadline_secs: u64,
        /// Repeatable `key=value` label.
        #[arg(long = "label", value_parser = parse_label)]
        labels: Vec<(String, String)>,
    },
    /// Print the status of a sandbox by id.
    Status {
        #[arg(long)]
        id: String,
    },
    /// List all sandbox ids managed by this backend.
    List,
    /// Destroy a sandbox by id.
    Destroy {
        #[arg(long)]
        id: String,
    },
    /// Attempt to stop a sandbox by id (expected to fail: unsupported).
    Stop {
        #[arg(long)]
        id: String,
    },
}

fn parse_label(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .ok_or_else(|| format!("label '{s}' must be in key=value form"))
}

fn build_spec(
    image: String,
    memory_mb: u32,
    cpu_millis: u32,
    deadline_secs: u64,
    labels: Vec<(String, String)>,
) -> SandboxSpec {
    SandboxSpec {
        image: ImageRef::Tag(image),
        resources: SandboxResources {
            memory_mb,
            cpu_millis,
            disk_mb: 0,
            max_pids: 0,
        },
        env: SandboxEnv {
            vars: BTreeMap::new(),
            secrets: BTreeMap::new(),
        },
        storage: SandboxStorage {
            workspace_mb: 0,
            seed: Vec::new(),
            state_volume: None,
        },
        deadline: Duration::from_secs(deadline_secs),
        labels: labels.into_iter().collect(),
    }
}

fn report(name: &str, ok: bool, detail: impl std::fmt::Display) {
    let tag = if ok { "PASS" } else { "FAIL" };
    println!("[{tag}] {name}: {detail}");
}

/// Runs the full create -> status -> list -> stop -> destroy flow, printing a
/// PASS/FAIL line per step. Returns `true` only if every step behaved as expected.
async fn run_e2e(backend: &K8sBackend, image: String) -> bool {
    let spec = build_spec(image, 64, 100, 300, Vec::new());

    let id = match backend.create(spec, CancellationToken::new()).await {
        Ok(id) => {
            report("create", true, &id);
            id
        }
        Err(err) => {
            report("create", false, err);
            return false;
        }
    };

    let mut all_passed = true;

    let mut reached_running = false;
    for _ in 0..STATUS_POLL_ATTEMPTS {
        match backend.status(&id).await {
            Ok(SandboxState::Running) => {
                reached_running = true;
                break;
            }
            Ok(_) => tokio::time::sleep(STATUS_POLL_INTERVAL).await,
            Err(err) => {
                report("status (poll to Running)", false, err);
                all_passed = false;
                break;
            }
        }
    }
    if reached_running {
        report("status (poll to Running)", true, "reached Running");
    } else if all_passed {
        report(
            "status (poll to Running)",
            false,
            "timed out before Running",
        );
        all_passed = false;
    }

    match backend.list().await {
        Ok(ids) if ids.contains(&id) => report("list (contains created id)", true, ids.len()),
        Ok(ids) => {
            report(
                "list (contains created id)",
                false,
                format!("{} ids, missing {id}", ids.len()),
            );
            all_passed = false;
        }
        Err(err) => {
            report("list", false, err);
            all_passed = false;
        }
    }

    match backend.stop(&id, CancellationToken::new()).await {
        Err(_) => report(
            "stop (expected unsupported)",
            true,
            "returned error as expected",
        ),
        Ok(()) => {
            report(
                "stop (expected unsupported)",
                false,
                "unexpectedly succeeded",
            );
            all_passed = false;
        }
    }

    match backend.status(&id).await {
        Ok(state) => report("status (unaffected by stop)", true, format!("{state:?}")),
        Err(err) => {
            report("status (unaffected by stop)", false, err);
            all_passed = false;
        }
    }

    match backend.destroy(&id).await {
        Ok(()) => report("destroy", true, "ok"),
        Err(err) => {
            report("destroy", false, err);
            all_passed = false;
        }
    }

    match backend.status(&id).await {
        Err(SandboxError::NotFound(_)) => report(
            "status (NotFound after destroy)",
            true,
            "not found as expected",
        ),
        Ok(state) => {
            report(
                "status (NotFound after destroy)",
                false,
                format!("still {state:?}"),
            );
            all_passed = false;
        }
        Err(err) => {
            report("status (NotFound after destroy)", false, err);
            all_passed = false;
        }
    }

    match backend.destroy(&id).await {
        Ok(()) => report("destroy (idempotent)", true, "ok"),
        Err(err) => {
            report("destroy (idempotent)", false, err);
            all_passed = false;
        }
    }

    all_passed
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let backend = K8sBackend::try_new(cli.namespace).await?;

    match cli.command {
        Command::Run { image } => {
            let ok = run_e2e(&backend, image).await;
            if !ok {
                std::process::exit(1);
            }
        }
        Command::Create {
            image,
            memory_mb,
            cpu_millis,
            deadline_secs,
            labels,
        } => {
            let spec = build_spec(image, memory_mb, cpu_millis, deadline_secs, labels);
            let id = backend.create(spec, CancellationToken::new()).await?;
            println!("{id}");
        }
        Command::Status { id } => {
            let state = backend.status(&SandboxId::new(id)).await?;
            println!("{state:?}");
        }
        Command::List => {
            for id in backend.list().await? {
                println!("{id}");
            }
        }
        Command::Destroy { id } => {
            backend.destroy(&SandboxId::new(id)).await?;
            println!("destroyed");
        }
        Command::Stop { id } => match backend
            .stop(&SandboxId::new(id), CancellationToken::new())
            .await
        {
            Ok(()) => println!("stopped (unexpected: this backend does not support stop)"),
            Err(err) => println!("stop returned expected error: {err}"),
        },
    }

    Ok(())
}
