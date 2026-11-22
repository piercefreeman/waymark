//! Tests of the migrations lock holder id.

use super::HolderId;

#[test]
fn holder_id_gives_back_its_value() {
    let value = uuid::Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);

    let holder_id = HolderId::from_raw(value);

    assert_eq!(holder_id.as_raw(), &value);
}
