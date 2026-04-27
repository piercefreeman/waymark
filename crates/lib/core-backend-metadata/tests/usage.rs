use derive_where::derive_where;
use waymark_core_backend_metadata::{MetadataItems, MetadataValues};

#[allow(dead_code)]
#[derive_where(Debug; MetadataValues<Metadata>)]
struct MyDataBag<Metadata: MetadataItems> {
    pub myfield1: &'static str,
    pub metadata: MetadataValues<Metadata>,
}

mod no_metadata {
    use super::*;

    type Metadata = ();

    #[test]
    fn example() {
        let data_bag: MyDataBag<Metadata> = MyDataBag {
            myfield1: "my value",
            metadata: ().into(),
        };

        println!("{data_bag:?}");
    }
}

mod single_item_metadata {
    use super::*;

    /// A metadata item that references the creatior of the data bag.
    struct CreatorRef;

    impl waymark_core_backend_metadata::MetadataItem for CreatorRef {
        const KEY: &str = "creator_ref";
        type Value = &'static str;
    }

    type Metadata = (CreatorRef,);

    #[test]
    fn example() {
        let data_bag: MyDataBag<Metadata> = MyDataBag {
            myfield1: "my value",
            metadata: ("my creator ref",).into(),
        };

        println!("{data_bag:?}");
    }
}

mod multiple_items_metadata {
    use super::*;

    /// A metadata item that references the creatior of the data bag.
    struct CreatorRef;

    impl waymark_core_backend_metadata::MetadataItem for CreatorRef {
        const KEY: &str = "creator_ref";
        type Value = &'static str;
    }

    /// A metadata item that tracks some sort of latency for the data bag.
    struct Latency;

    impl waymark_core_backend_metadata::MetadataItem for Latency {
        const KEY: &str = "latency";
        type Value = std::time::Duration;
    }

    type Metadata = (CreatorRef, Latency);

    #[test]
    fn example() {
        let data_bag: MyDataBag<Metadata> = MyDataBag {
            myfield1: "my value",
            metadata: ("my creator ref", std::time::Duration::from_secs(1337)).into(),
        };

        println!("{data_bag:?}");
    }
}
