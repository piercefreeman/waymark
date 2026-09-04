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
    let error = ListCursor::decode("12").expect_err("one part is not a list cursor");
    assert_eq!(error.text, "12");
    let past_chrono = format!("{}/{}/1", i64::MAX, waymark_ids::NodeId::new_uuid_v4());
    let error = ListCursor::decode(&past_chrono).expect_err("an instant chrono can not hold");
    assert_eq!(error.text, past_chrono);
    let error = TailCursor::decode("x").expect_err("text is not a tail cursor");
    assert_eq!(error.text, "x");
}
