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

#[cfg(feature = "serde")]
#[test]
fn positional_round_trip_keeps_spent_ids_spent() {
    let mut map = IdMap::<TestId, String>::new();
    let removed = map.insert("removed".to_owned());
    let kept = map.insert("kept".to_owned());
    map.remove(removed);

    let encoded = rmp_serde::to_vec(&map).unwrap();
    assert_eq!(encoded[0] & 0xf0, 0x90, "the map is written as a sequence");
    let mut decoded: IdMap<TestId, String> = rmp_serde::from_slice(&encoded).unwrap();

    assert_eq!(decoded.get(kept).map(String::as_str), Some("kept"));
    assert_eq!(decoded.get(removed), None);
    assert_eq!(decoded.insert("fresh".to_owned()), TestId(2));
}

#[cfg(feature = "serde")]
#[test]
fn decode_refuses_an_entry_at_or_past_the_next_id() {
    let below =
        serde_json::from_str::<IdMap<TestId, &str>>(r#"{"next_id":2,"entries":{"1":"live"}}"#);
    assert!(below.is_ok(), "an entry below the next id decodes");

    for (next_id, key) in [(1, 1), (1, 2)] {
        let encoded = format!(r#"{{"next_id":{next_id},"entries":{{"{key}":"live"}}}}"#);

        let decoded = serde_json::from_str::<IdMap<TestId, &str>>(&encoded);

        let error = decoded
            .expect_err("an entry at or past the next id is refused")
            .to_string();
        assert!(
            error.contains(&format!("{:?}", TestId(key))),
            "the error carries the offending id: {error}"
        );
    }
}

/// Reads an entry as a string and pairs it with the id it was read under.
#[cfg(feature = "serde")]
struct PairWithId(TestId);

#[cfg(feature = "serde")]
impl<'de> serde::de::DeserializeSeed<'de> for PairWithId {
    type Value = (TestId, String);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let entry = <String as serde::Deserialize>::deserialize(deserializer)?;
        Ok((self.0, entry))
    }
}

#[cfg(feature = "serde")]
#[test]
fn deserialize_with_hands_each_seed_its_entry_id() {
    let mut map = IdMap::<TestId, String>::new();
    let removed = map.insert("removed".to_owned());
    let first = map.insert("first".to_owned());
    let second = map.insert("second".to_owned());
    map.remove(removed);
    let encoded = serde_json::to_string(&map).unwrap();

    let mut deserializer = serde_json::Deserializer::from_str(&encoded);
    let mut decoded =
        IdMap::<TestId, (TestId, String)>::deserialize_with(&mut deserializer, PairWithId).unwrap();

    assert_eq!(decoded.get(first), Some(&(first, "first".to_owned())));
    assert_eq!(decoded.get(second), Some(&(second, "second".to_owned())));
    assert_eq!(decoded.get(removed), None);
    assert_eq!(decoded.insert((TestId(0), String::new())), TestId(3));
}
