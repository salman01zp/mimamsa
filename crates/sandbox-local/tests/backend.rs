use std::collections::BTreeMap;
use std::time::Duration;

use futures::io::{AsyncReadExt, AsyncWriteExt};
use sandbox_core::{
    ImageRef, SandboxBackend, SandboxDestroyed, SandboxEnv, SandboxError, SandboxOutcome,
    SandboxResources, SandboxSpec, SandboxState, SandboxStorage,
};
use sandbox_local::LocalSandboxBackend;
use tokio_util::sync::CancellationToken;

fn spec(binary: &str) -> SandboxSpec {
    SandboxSpec {
        image: ImageRef::Tag(binary.to_string()),
        resources: SandboxResources {
            // Generous on purpose: RLIMIT_AS bounds virtual address space, not
            // resident memory, and a dynamically-linked binary can reserve much more
            // address space than it ever touches. A tight limit here would make these
            // functional tests flaky for reasons unrelated to what they're testing.
            memory_mb: 1024,
            cpu_millis: 500,
            disk_mb: 64,
            max_pids: 16,
        },
        env: SandboxEnv {
            vars: BTreeMap::new(),
            secrets: BTreeMap::new(),
        },
        storage: SandboxStorage {
            workspace_mb: 16,
            seed: Vec::new(),
            state_volume: None,
        },
        deadline: Duration::from_secs(30),
        labels: BTreeMap::new(),
    }
}

async fn with_timeout<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(5), fut)
        .await
        .expect("test operation timed out")
}

#[tokio::test]
async fn stop_and_destroy_are_idempotent() {
    let backend = LocalSandboxBackend::new();
    let id = with_timeout(backend.create(spec("/usr/bin/cat"), CancellationToken::new()))
        .await
        .unwrap();

    with_timeout(backend.stop(&id, CancellationToken::new()))
        .await
        .unwrap();
    // Second stop on an already-stopped sandbox: idempotent, not an error.
    with_timeout(backend.stop(&id, CancellationToken::new()))
        .await
        .unwrap();

    let status = with_timeout(backend.status(&id)).await.unwrap();
    assert_eq!(status.state, SandboxState::Stopped);

    let first_destroy = with_timeout(backend.destroy(&id)).await;
    assert!(matches!(first_destroy, SandboxDestroyed::Destroyed));

    // Second destroy on an already-gone sandbox: idempotent, reports Leaked rather
    // than erroring (destroy() has no error variant to return in the first place).
    let second_destroy = with_timeout(backend.destroy(&id)).await;
    assert!(matches!(second_destroy, SandboxDestroyed::Leaked));

    let status_after = with_timeout(backend.status(&id)).await;
    assert!(matches!(status_after, Err(SandboxError::NotFound(_))));
}

#[tokio::test]
async fn pause_and_resume_on_a_terminal_sandbox_return_terminal_error() {
    let backend = LocalSandboxBackend::new();
    let id = with_timeout(backend.create(spec("/usr/bin/cat"), CancellationToken::new()))
        .await
        .unwrap();
    with_timeout(backend.stop(&id, CancellationToken::new()))
        .await
        .unwrap();

    let pause_result = with_timeout(backend.pause(&id, CancellationToken::new())).await;
    assert!(matches!(pause_result, Err(SandboxError::Terminal(_))));

    let resume_result = with_timeout(backend.resume(&id, CancellationToken::new())).await;
    assert!(matches!(resume_result, Err(SandboxError::Terminal(_))));

    with_timeout(backend.destroy(&id)).await;
}

#[tokio::test]
async fn pause_on_a_running_sandbox_returns_unsupported_not_terminal() {
    let backend = LocalSandboxBackend::new();
    let id = with_timeout(backend.create(spec("/usr/bin/cat"), CancellationToken::new()))
        .await
        .unwrap();

    let pause_result = with_timeout(backend.pause(&id, CancellationToken::new())).await;
    assert!(matches!(pause_result, Err(SandboxError::Unsupported(_))));

    with_timeout(backend.destroy(&id)).await;
}

#[tokio::test]
async fn cancel_before_create_leaves_no_orphaned_process() {
    let backend = LocalSandboxBackend::new();
    let cancel = CancellationToken::new();
    cancel.cancel();

    let result = with_timeout(backend.create(spec("/usr/bin/cat"), cancel)).await;
    assert!(matches!(result, Err(SandboxError::Cancelled)));

    let discovered = with_timeout(backend.discover()).await.unwrap();
    assert!(
        discovered.is_empty(),
        "cancellation must not leave a process behind: {discovered:?}"
    );
}

#[tokio::test]
async fn open_io_called_twice_returns_working_io_both_times() {
    let backend = LocalSandboxBackend::new();
    let id = with_timeout(backend.create(spec("/usr/bin/cat"), CancellationToken::new()))
        .await
        .unwrap();

    // The backend enforces no once-only discipline: both calls must succeed and both
    // must observe the same output, since stdout is fanned out to every subscriber
    // rather than split between them.
    let mut first = with_timeout(backend.open_io(&id)).await.unwrap();
    let mut second = with_timeout(backend.open_io(&id)).await.unwrap();

    with_timeout(first.stdin.write_all(b"ping\n"))
        .await
        .unwrap();
    with_timeout(first.stdin.flush()).await.unwrap();

    let mut first_buf = [0u8; 5];
    with_timeout(first.stdout.read_exact(&mut first_buf))
        .await
        .unwrap();
    assert_eq!(&first_buf, b"ping\n");

    let mut second_buf = [0u8; 5];
    with_timeout(second.stdout.read_exact(&mut second_buf))
        .await
        .unwrap();
    assert_eq!(&second_buf, b"ping\n");

    with_timeout(backend.destroy(&id)).await;
}

#[tokio::test]
async fn self_exiting_process_is_observed_as_stopped() {
    let backend = LocalSandboxBackend::new();
    let id = with_timeout(backend.create(spec("/usr/bin/true"), CancellationToken::new()))
        .await
        .unwrap();

    let status = with_timeout(async {
        loop {
            let status = backend.status(&id).await.unwrap();
            if status.state == SandboxState::Stopped {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    assert_eq!(status.outcome, Some(SandboxOutcome::Exited(0)));

    let destroyed = with_timeout(backend.destroy(&id)).await;
    // The record was found (just already self-stopped), so this is a normal
    // Destroyed, not the Leaked path -- that's reserved for when destroy() finds
    // nothing at all, e.g. a second call after the record is already removed.
    assert!(matches!(destroyed, SandboxDestroyed::Destroyed));
}

#[tokio::test]
async fn unknown_id_is_not_found_everywhere_it_should_be() {
    let backend = LocalSandboxBackend::new();
    let bogus = sandbox_core::SandboxId::new("local-does-not-exist");

    assert!(matches!(
        with_timeout(backend.status(&bogus)).await,
        Err(SandboxError::NotFound(_))
    ));
    assert!(matches!(
        with_timeout(backend.open_io(&bogus)).await,
        Err(SandboxError::NotFound(_))
    ));
    assert!(matches!(
        with_timeout(backend.stop(&bogus, CancellationToken::new())).await,
        Err(SandboxError::NotFound(_))
    ));
    assert!(matches!(
        with_timeout(backend.destroy(&bogus)).await,
        SandboxDestroyed::Leaked
    ));
}
