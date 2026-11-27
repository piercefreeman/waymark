//! Tests of reading and rendering a crate capture, over a scratch
//! directory.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use super::{
    CRATE_CAPTURE_FILE, CrateCapture, ReadCrateCaptureError, read_crate_capture,
    render_crate_capture,
};

/// A scratch directory of its own for the test `test`, emptied first.
fn scratch_dir(test: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("waymark-migrations-rust-code-core-tests")
        .join(test);

    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("empty the scratch directory");
    }
    std::fs::create_dir_all(&dir).expect("create the scratch directory");

    dir
}

/// A capture of two files and two dependencies.
fn capture() -> CrateCapture {
    CrateCapture {
        files: BTreeMap::from([
            ("src/lib.rs".to_owned(), "sha256:aa".to_owned()),
            ("Cargo.toml".to_owned(), "sha256:bb".to_owned()),
        ]),
        dependencies: BTreeSet::from(["uuid 1.23.0".to_owned(), "rmpv 1.3.1".to_owned()]),
    }
}

#[test]
fn capture_is_rendered_with_its_entries_in_order_one_to_a_line() {
    assert_eq!(
        render_crate_capture(&capture()).expect("the capture is rendered"),
        "{\n  \"files\": {\n    \"Cargo.toml\": \"sha256:bb\",\n    \"src/lib.rs\": \
         \"sha256:aa\"\n  },\n  \"dependencies\": [\n    \"rmpv 1.3.1\",\n    \"uuid 1.23.0\"\n  \
         ]\n}\n"
    );
}

#[test]
fn rendered_capture_reads_back_as_the_same_capture() {
    let dir = scratch_dir("read-back");
    let rendered = render_crate_capture(&capture()).expect("the capture is rendered");
    std::fs::write(dir.join(CRATE_CAPTURE_FILE), rendered).expect("write the file");

    let read = read_crate_capture(&dir).expect("the capture is read");

    assert_eq!(read.files, capture().files);
    assert_eq!(read.dependencies, capture().dependencies);
}

#[test]
fn file_that_is_not_there_is_told_apart() {
    let dir = scratch_dir("not-found");

    let Err(error @ ReadCrateCaptureError::NotFound { .. }) = read_crate_capture(&dir) else {
        panic!("the file is not there");
    };

    assert_eq!(
        error.to_string(),
        format!("{} is not there", dir.join(CRATE_CAPTURE_FILE).display())
    );
}

#[test]
fn text_that_is_not_a_capture_is_refused() {
    let dir = scratch_dir("parse");

    for text in [
        "not json",
        "{}",
        "{\"files\": {}}",
        "{\"dependencies\": []}",
        "{\"files\": {\"src/lib.rs\": 7}, \"dependencies\": []}",
        "{\"files\": {}, \"dependencies\": [7]}",
        "{\"files\": {}, \"dependencies\": [], \"more\": true}",
    ] {
        std::fs::write(dir.join(CRATE_CAPTURE_FILE), text).expect("write the file");

        assert!(
            matches!(
                read_crate_capture(&dir),
                Err(ReadCrateCaptureError::Parse { .. })
            ),
            "{text} is refused"
        );
    }
}
