//! The `now` feature, asked for by this crate's own dev-dependency on
//! itself, so the test runs under any feature configuration.

use waymark_timestamp::Timestamp;

#[test]
fn now_is_the_present() {
    let before = chrono::Utc::now();
    let now = Timestamp::now();
    let after = chrono::Utc::now();

    assert!(before <= now.get() && now.get() <= after);
}
