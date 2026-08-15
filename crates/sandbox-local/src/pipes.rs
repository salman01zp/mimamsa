//! Bridges the blocking pipes `std::process::Command` hands back into the
//! `futures::io::{AsyncRead, AsyncWrite}` handles `SandboxIo` requires.
//!
//! Each stream gets one long-lived OS thread (spawned once, at `create()` time, not
//! per `open_io()` call) that does the blocking read/write against the real pipe.
//! `open_io()` calls hand out lightweight channel endpoints onto that thread rather
//! than the pipe itself, which is what makes calling it more than once safe: nothing
//! here enforces once-only access (that's `sandbox-manager`'s job, per the RFC), so
//! every `SandboxIo` in circulation just gets its own fan-out/fan-in endpoint onto the
//! same underlying pump.
//!
//! stdout/stderr fan out: every `open_io()` call gets its own broadcast subscription,
//! so each caller sees a full independent copy of the output (not a competing read of
//! one copy — unlike a real dup'd fd, nothing here makes concurrent readers race for
//! the same bytes). Capacity is bounded, so a slow or absent subscriber causes old
//! chunks to be dropped rather than buffered without limit — that's this backend's
//! answer to "backpressure is the caller's": it drops instead of growing, it never
//! blocks the child.
//!
//! stdin fans in: every `open_io()` call gets a clone of the same unbounded sender, so
//! concurrent writers interleave into the child's real stdin in whatever order their
//! sends land. Unbounded because the risk "backpressure is the caller's" guards
//! against is untrusted *output*, not our own process writing input.

use std::io;
use std::pin::Pin;
use std::process::{ChildStderr, ChildStdin, ChildStdout};
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, broadcast, mpsc};

const BROADCAST_CAPACITY: usize = 256;
const READ_CHUNK: usize = 8 * 1024;

// ---------------------------------------------------------------------------
// stdout / stderr: one pump thread, fan-out broadcast
// ---------------------------------------------------------------------------

/// Shared with the pump thread. `sender` goes to `None` exactly once, the moment the
/// pump thread observes real EOF or a read error — guarded by the same mutex
/// `subscribe()` takes, so there's no window where a caller can subscribe successfully
/// but never see the channel close (a plain "check eof flag, then subscribe" without
/// this lock would have exactly that race).
///
/// `send`/`finish` run on the pump thread, which is a plain `std::thread`, not a tokio
/// task -- `blocking_lock()` is the tokio `Mutex` method meant for exactly that: a
/// synchronous caller outside any async execution context. `subscribe` runs from
/// `open_io()`, which is already async, so it awaits the lock normally.
struct BroadcastChannel {
    sender: Mutex<Option<broadcast::Sender<Bytes>>>,
}

impl BroadcastChannel {
    fn new() -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);
        Arc::new(Self {
            sender: Mutex::new(Some(tx)),
        })
    }

    fn send(&self, bytes: Bytes) {
        if let Some(tx) = self.sender.blocking_lock().as_ref() {
            // Err just means no receivers are currently subscribed; nothing to do.
            let _ = tx.send(bytes);
        }
    }

    /// Called exactly once by the pump thread when the real pipe is done.
    fn finish(&self) {
        *self.sender.blocking_lock() = None;
    }

    /// `None` means the stream already ended before this call — every read on the
    /// resulting handle should behave as already-at-EOF.
    async fn subscribe(&self) -> Option<broadcast::Receiver<Bytes>> {
        self.sender.lock().await.as_ref().map(|tx| tx.subscribe())
    }
}

type RecvOutcome = (broadcast::Receiver<Bytes>, Result<Bytes, broadcast::error::RecvError>);
type RecvFuture = Pin<Box<dyn futures::Future<Output = RecvOutcome> + Send>>;

enum RecvState {
    Idle(broadcast::Receiver<Bytes>),
    Pending(RecvFuture),
    Eof,
}

/// The `AsyncRead` half handed out by `open_io()` for stdout/stderr.
pub struct BroadcastAsyncRead {
    leftover: Option<(Bytes, usize)>,
    state: RecvState,
}

impl BroadcastAsyncRead {
    fn new(rx: Option<broadcast::Receiver<Bytes>>) -> Self {
        Self {
            leftover: None,
            state: match rx {
                Some(rx) => RecvState::Idle(rx),
                None => RecvState::Eof,
            },
        }
    }
}

async fn recv_once(
    mut rx: broadcast::Receiver<Bytes>,
) -> (
    broadcast::Receiver<Bytes>,
    Result<Bytes, broadcast::error::RecvError>,
) {
    let result = rx.recv().await;
    (rx, result)
}

impl AsyncRead for BroadcastAsyncRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            if let Some((chunk, offset)) = self.leftover.take() {
                let remaining = &chunk[offset..];
                let n = remaining.len().min(buf.len());
                buf[..n].copy_from_slice(&remaining[..n]);
                if offset + n < chunk.len() {
                    self.leftover = Some((chunk, offset + n));
                }
                return Poll::Ready(Ok(n));
            }

            match &mut self.state {
                RecvState::Idle(_) => {
                    let RecvState::Idle(rx) = std::mem::replace(&mut self.state, RecvState::Eof)
                    else {
                        unreachable!()
                    };
                    self.state = RecvState::Pending(Box::pin(recv_once(rx)));
                }
                RecvState::Pending(fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready((rx, Ok(bytes))) => {
                        self.state = RecvState::Idle(rx);
                        self.leftover = Some((bytes, 0));
                    }
                    Poll::Ready((rx, Err(broadcast::error::RecvError::Lagged(_)))) => {
                        // We fell behind and missed messages: documented data loss,
                        // not a hang. Keep going for whatever comes next.
                        self.state = RecvState::Idle(rx);
                    }
                    Poll::Ready((_, Err(broadcast::error::RecvError::Closed))) => {
                        self.state = RecvState::Eof;
                        return Poll::Ready(Ok(0));
                    }
                },
                RecvState::Eof => return Poll::Ready(Ok(0)),
            }
        }
    }
}

fn spawn_stdout_pump(mut stdout: ChildStdout, channel: Arc<BroadcastChannel>) {
    std::thread::spawn(move || {
        use std::io::Read;
        let mut buf = [0u8; READ_CHUNK];
        loop {
            match stdout.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => channel.send(Bytes::copy_from_slice(&buf[..n])),
            }
        }
        channel.finish();
    });
}

fn spawn_stderr_pump(mut stderr: ChildStderr, channel: Arc<BroadcastChannel>) {
    std::thread::spawn(move || {
        use std::io::Read;
        let mut buf = [0u8; READ_CHUNK];
        loop {
            match stderr.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => channel.send(Bytes::copy_from_slice(&buf[..n])),
            }
        }
        channel.finish();
    });
}

// ---------------------------------------------------------------------------
// stdin: one pump thread, fan-in mpsc
// ---------------------------------------------------------------------------

/// The `AsyncWrite` half handed out by `open_io()` for stdin. `tx` is `None` once this
/// specific handle has been closed; other outstanding handles from other `open_io()`
/// calls are unaffected, matching the "no ownership discipline here" rule — closing
/// one handle doesn't close the child's stdin while another handle might still write
/// to it.
pub struct MpscAsyncWrite {
    tx: Option<mpsc::UnboundedSender<Bytes>>,
}

impl AsyncWrite for MpscAsyncWrite {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.tx.as_ref() {
            Some(tx) => match tx.send(Bytes::copy_from_slice(buf)) {
                Ok(()) => Poll::Ready(Ok(buf.len())),
                Err(_) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "stdin pump thread is gone",
                ))),
            },
            None => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "this stdin handle was already closed",
            ))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Nothing is buffered here beyond the channel itself; each poll_write already
        // handed its bytes to the pump thread.
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.tx = None;
        Poll::Ready(Ok(()))
    }
}

fn spawn_stdin_pump(mut stdin: ChildStdin, mut rx: mpsc::UnboundedReceiver<Bytes>) {
    std::thread::spawn(move || {
        use std::io::Write;
        while let Some(bytes) = rx.blocking_recv() {
            if stdin.write_all(&bytes).is_err() {
                break;
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Per-sandbox handle: what the record keeps around to service open_io() calls
// ---------------------------------------------------------------------------

/// Owned by the sandbox record. Spawning the pump threads happens once, at `create()`
/// time; `open_io()` just calls the `subscribe`/`sender` methods below.
pub struct ProcessPipes {
    stdin_tx: mpsc::UnboundedSender<Bytes>,
    stdout: Arc<BroadcastChannel>,
    stderr: Arc<BroadcastChannel>,
}

impl ProcessPipes {
    pub fn spawn(stdin: ChildStdin, stdout: ChildStdout, stderr: ChildStderr) -> Self {
        let (stdin_tx, stdin_rx) = mpsc::unbounded_channel();
        spawn_stdin_pump(stdin, stdin_rx);

        let stdout_channel = BroadcastChannel::new();
        spawn_stdout_pump(stdout, stdout_channel.clone());

        let stderr_channel = BroadcastChannel::new();
        spawn_stderr_pump(stderr, stderr_channel.clone());

        Self {
            stdin_tx,
            stdout: stdout_channel,
            stderr: stderr_channel,
        }
    }

    pub async fn open(&self) -> (MpscAsyncWrite, BroadcastAsyncRead, BroadcastAsyncRead) {
        let stdin = MpscAsyncWrite {
            tx: Some(self.stdin_tx.clone()),
        };
        let stdout = BroadcastAsyncRead::new(self.stdout.subscribe().await);
        let stderr = BroadcastAsyncRead::new(self.stderr.subscribe().await);
        (stdin, stdout, stderr)
    }
}
