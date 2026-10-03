//! An aggregate summing a fixed-length `bigint[]` column elementwise.

/// The text of an elementwise sum, written at compile time.
pub type Writer = const_format::StrWriter<[u8; 1024]>;

/// The elementwise sum of `column` over `buckets` positions, as a
/// `&'static str`.
///
/// Summing histogram counts across the rows of a time bucket is the whole
/// reason these are counts rather than a quantile, but Postgres has no
/// elementwise array aggregate. The length is fixed and known here, so
/// the sum is written out one position at a time rather than reached for
/// through `unnest` — or through a function installed in the database.
///
/// Use it in a const initializer: the text is written at compile time.
macro_rules! elementwise_sum {
    ($column:expr, $buckets:expr $(,)?) => {
        $crate::essential_metrics::common::elementwise_sum::write($column, $buckets)
            .unsize()
            .as_str()
    };
}

pub(crate) use elementwise_sum;

/// Write the elementwise sum of `column` over `buckets` positions.
///
/// A text longer than [`Writer`] holds fails the build.
pub const fn write(column: &str, buckets: usize) -> Writer {
    let mut writer = Writer::new([0; _]);
    if const_format::writec!(writer, "ARRAY[").is_err() {
        panic!("the elementwise sum outgrows its buffer");
    }

    let mut position = 1;
    while position <= buckets {
        let separator = if position < buckets { ", " } else { "" };
        if const_format::writec!(writer, "sum({}[{}])::bigint{}", column, position, separator)
            .is_err()
        {
            panic!("the elementwise sum outgrows its buffer");
        }
        position += 1;
    }

    if const_format::writec!(writer, "]").is_err() {
        panic!("the elementwise sum outgrows its buffer");
    }

    writer
}
