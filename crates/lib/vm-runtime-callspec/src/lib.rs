//! [`CallSpec`] — a specification of a VM function call.

#![warn(missing_docs)]

/// A specification of a VM function call.
#[derive(Debug, Clone)]
pub struct CallSpec<FunctionId, Arg> {
    /// A function to call.
    pub func: FunctionId,

    /// A list of arguments to pass to the function.
    pub args: Vec<Arg>,
}
