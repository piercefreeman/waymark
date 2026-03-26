use nix::sys::signal::Signal;

/// Set the parent-death signal of the child process we're going to spawn.
/// This is the signal that the calling process will get when its parent dies.
///
/// Effectively we request that the child processes we spawn get SIGTERM if we
/// die.
pub(crate) fn inject_sigterm_pdeathsig(command: &mut tokio::process::Command) {
    use std::os::unix::process::CommandExt as _;

    let our_pid = std::process::id();

    unsafe {
        command.as_std_mut().pre_exec(move || {
            if our_pid != std::os::unix::process::parent_id() {
                // If the parent has exited - we abort the launch because it
                // won't be terminated when the parent exists.
                return Err(std::io::Error::other(
                    "parent process already exited, won't start child with a set pdeathsig",
                ));
            }

            nix::sys::prctl::set_pdeathsig(Signal::SIGTERM).map_err(std::io::Error::other)
        });
    }
}
