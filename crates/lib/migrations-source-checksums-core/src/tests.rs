//! Tests of the digest's spelling.

use super::source_digest;

#[test]
fn source_digest_is_the_sha_256_in_hex_after_its_label() {
    assert_eq!(
        source_digest(b"abc"),
        "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}
