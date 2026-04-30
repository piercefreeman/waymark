use index_type::IndexType;

/// A table of items.
pub trait Table {
    /// An ID of the
    type Id: IndexType;
}

/// A constant table of items.
///
/// The size and set of items is fixed at compile-time.
pub trait ConstTable: Table {}

/// A runtime table of items.
///
///
/// The size and set of items is dynamic and can vary at runtime.
pub trait RuntimeTable: Table {}

/// A VM spec, captures the supported pure operations and extcalls.
pub trait Spec {
    /// A specification of supported pure operations.
    ///
    /// Provides a table of operations available on scalars and effectively
    /// specifies the set of supported scalar values.
    type PureOperations: ConstTable;

    /// A table of extcalls.
    type ExtCalls: ConstTable;
}

/// A trait specifying details of a single runtime instance for a specific
/// program.
pub trait InstanceSpec {
    type Spec: Spec;
    type Functions: RuntimeTable;
    type States: RuntimeTable;
}
