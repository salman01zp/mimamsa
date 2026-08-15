//! Best-effort process memory, read from `/proc` rather than tracked ourselves. See
//! `LocalSandboxBackend::usage` for why `cpu_millis` and `disk_mb` aren't attempted at
//! all.

#[cfg(target_os = "linux")]
pub(crate) fn read_memory_mb(pid: u32) -> Option<u32> {
    let contents = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    for line in contents.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some((kb / 1024) as u32);
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn read_memory_mb(_pid: u32) -> Option<u32> {
    None
}
