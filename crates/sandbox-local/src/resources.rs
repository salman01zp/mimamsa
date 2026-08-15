//! Best-effort enforcement of `SandboxResources` on Linux, using only what `std`
//! exposes (`CommandExt::pre_exec`) plus a hand-written `setrlimit(2)` binding — not a
//! new crate dependency, since libc is already linked into every Rust binary on this
//! platform.
//!
//! Only `memory_mb` gets enforced. The other three fields are documented, not
//! pretended:
//!
//! - `cpu_millis` is a *rate* (millicores), not a time budget. The only rlimit in this
//!   space, `RLIMIT_CPU`, bounds cumulative CPU *seconds* before SIGXCPU — a
//!   fundamentally different guarantee. Mapping one onto the other would misrepresent
//!   the field, so this backend doesn't attempt it; real rate limiting needs a cgroup
//!   (`cpu.max`).
//! - `disk_mb` has no rlimit equivalent for an arbitrary directory tree. Needs quotas
//!   or cgroups.
//! - `max_pids` deliberately does *not* use `RLIMIT_NPROC`. On Linux that limit is
//!   scoped to the real UID across the *entire host*, not to this process's
//!   descendants — setting it here would cap the number of processes the developer's
//!   own user account can run system-wide, not just this sandbox's children. That's a
//!   landmine, not a sandbox boundary, so it's left unenforced rather than enforced
//!   incorrectly.

#[cfg(target_os = "linux")]
mod linux {
    use std::io;

    #[repr(C)]
    struct RLimit {
        rlim_cur: u64,
        rlim_max: u64,
    }

    const RLIMIT_AS: i32 = 9;

    unsafe extern "C" {
        fn setrlimit(resource: i32, rlim: *const RLimit) -> i32;
    }

    /// Caps the *virtual address space* of the calling process. Must only be called
    /// from a `pre_exec` closure (after `fork`, before `exec`): it is async-signal-safe
    /// (no allocation, a single syscall) as `pre_exec`'s contract requires.
    ///
    /// This is an approximation of `memory_mb`, not an exact match: RLIMIT_AS bounds
    /// address space reservations, which can exceed resident memory (e.g. generous
    /// stack/heap reservations, shared library mappings), so a process can hit this
    /// limit before actually using `memory_mb` worth of physical memory. A cgroup
    /// memory controller would be the accurate version of this; this is what plain
    /// std/tokio can do without one.
    pub(super) fn set_address_space_limit(memory_mb: u32) -> io::Result<()> {
        let bytes = u64::from(memory_mb).saturating_mul(1024 * 1024);
        let limit = RLimit {
            rlim_cur: bytes,
            rlim_max: bytes,
        };
        let rc = unsafe { setrlimit(RLIMIT_AS, &limit) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

/// Installs the best-effort resource limits into `command`, to take effect in the
/// child after `fork` and before `exec`. On non-Linux targets this is a documented
/// no-op: the `pre_exec` hook it relies on is POSIX-only, and the specific rlimit used
/// here is Linux-specific besides.
pub(crate) fn apply(command: &mut std::process::Command, memory_mb: u32) {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt;
        unsafe {
            command.pre_exec(move || linux::set_address_space_limit(memory_mb));
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (command, memory_mb);
    }
}
