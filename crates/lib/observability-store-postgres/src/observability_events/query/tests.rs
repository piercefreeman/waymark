use waymark_cursor_core::{DecodeCursor as _, EncodeCursor as _};

use super::*;

#[test]
fn cursors_round_trip_through_their_wire_form() {
    let list = ListCursor {
        at: chrono::DateTime::from_timestamp_micros(1_700_000_000_123_456).unwrap(),
        node_id: waymark_ids::NodeId::new_uuid_v4(),
        node_sequence: 41,
    };
    let text = list.encode();
    let back = ListCursor::decode(&text).expect("a written cursor reads back");
    assert_eq!(back.encode(), text);

    let tail = TailCursor { node_sequence: 9 };
    let back = TailCursor::decode(&tail.encode()).expect("a written cursor reads back");
    assert_eq!(back.encode(), "9");

    let error = ListCursor::decode("1/2").expect_err("two parts are not a list cursor");
    assert_eq!(error.text, "1/2");
    let error = TailCursor::decode("x").expect_err("text is not a tail cursor");
    assert_eq!(error.text, "x");
}
