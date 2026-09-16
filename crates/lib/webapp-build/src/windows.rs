//! Finding programs on Windows, where npm is usually a batch script that a bare
//! program name does not find.

use std::path::PathBuf;

/// The extensions a program may have, tried in order. A fixed list rather than
/// `PATHEXT`, so the environment cannot widen what gets run.
const PROGRAM_EXTENSIONS: &[&str] = &[".exe", ".cmd", ".bat"];

/// Search `PATH` for the program `name` directory by directory, trying each of
/// `PROGRAM_EXTENSIONS` in turn.
pub fn find_program(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;

    std::env::split_paths(&path).find_map(|directory| {
        PROGRAM_EXTENSIONS.iter().find_map(|extension| {
            let candidate = directory.join(format!("{name}{extension}"));
            candidate.is_file().then_some(candidate)
        })
    })
}
