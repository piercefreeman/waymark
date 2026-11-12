use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, index_type::IndexType)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[index_type(error = index_type::GenericIndexTooBigError)]
struct TestId(usize);

#[test]
fn insert_hands_out_increasing_ids() {
    let mut map = IdMap::<TestId, &str>::new();

    let first = map.insert("first");
    let second = map.insert("second");
    let third = map.insert("third");

    assert_eq!(first, TestId(0));
    assert_eq!(second, TestId(1));
    assert_eq!(third, TestId(2));
    assert_eq!(map.get(first), Some(&"first"));
    assert_eq!(map.get(second), Some(&"second"));
    assert_eq!(map.get(third), Some(&"third"));
}

#[test]
fn removed_id_is_never_handed_out_again() {
    let mut map = IdMap::<TestId, &str>::new();

    let removed = map.insert("removed");
    assert_eq!(map.remove(removed), Some("removed"));
    assert_eq!(map.remove(removed), None);

    let fresh = map.insert("fresh");

    assert_ne!(fresh, removed);
    assert_eq!(map.get(removed), None);
    assert_eq!(map.get(fresh), Some(&"fresh"));
}

#[test]
fn len_tracks_present_entries() {
    let mut map = IdMap::<TestId, &str>::new();
    assert!(map.is_empty());

    let first = map.insert("first");
    let second = map.insert("second");
    assert_eq!(map.len(), 2);

    map.remove(first);
    assert_eq!(map.len(), 1);
    assert_eq!(map.iter().collect::<Vec<_>>(), vec![(second, &"second")]);

    map.remove(second);
    assert!(map.is_empty());
}

#[test]
fn get_mut_edits_in_place() {
    let mut map = IdMap::<TestId, u32>::new();

    let id = map.insert(1);
    *map.get_mut(id).unwrap() += 1;

    assert_eq!(map.get(id), Some(&2));
}

#[cfg(feature = "serde")]
#[test]
fn serde_round_trip_keeps_spent_ids_spent() {
    let mut map = IdMap::<TestId, &str>::new();
    let removed = map.insert("removed");
    let kept = map.insert("kept");
    map.remove(removed);

    let encoded = serde_json::to_string(&map).unwrap();
    let mut decoded: IdMap<TestId, &str> = serde_json::from_str(&encoded).unwrap();

    assert_eq!(decoded.get(kept), Some(&"kept"));
    assert_eq!(decoded.get(removed), None);
    assert_eq!(decoded.insert("fresh"), TestId(2));
}
