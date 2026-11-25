//! What every kind of migration source shares about its checksums: the
//! spelling of a digest.

#![warn(missing_docs)]

use sha2::Digest as _;

/// The digest of `bytes` as the checksums and the migrations ledger have
/// it: the SHA-256 digest in hex, after `sha256:`.
pub fn source_digest(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(sha2::Sha256::digest(bytes)))
}

#[cfg(test)]
mod tests;
