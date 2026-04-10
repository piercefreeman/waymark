pub mod action;
pub mod instance;

pub type Reader =
    waymark_jsonlines::Reader<tokio::io::BufReader<tokio::fs::File>, instance::LogItem>;
pub type Writer = waymark_jsonlines::Writer<tokio::fs::File, instance::LogItem>;
