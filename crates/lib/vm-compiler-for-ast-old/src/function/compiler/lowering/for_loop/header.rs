//! Loop-header classification for `for` loops.

use waymark_vm_ast_old::{Expr, FunctionCall, GlobalFunction, Kwarg, Spanned};

use super::super::plan::call::UnsupportedFunctionCall;
use super::super::{Error, Unsupported};
use super::ErrorFor;

/// How a `for` loop binds values into its loop variables.
#[derive(Clone, Copy)]
pub(super) enum LoopBinding {
    /// Bind the current iteration value directly.
    Value,

    /// Bind an `enumerate(...)` pair of `[index, value]`.
    Enumerate,
}

/// Validated loop-source shapes that the lowering path knows how to execute.
pub(super) enum ResolvedForLoop<'expr> {
    /// Iterate an indexable iterable with an optional enumerate binding.
    Indexed {
        /// Source iterable expression.
        iterable: &'expr Spanned<Expr>,

        /// How iteration values bind to loop variables.
        binding: LoopBinding,
    },

    /// Iterate a validated `range(...)` header.
    Range {
        /// Parsed `range(...)` header.
        range: RangeLoop<'expr>,

        /// How iteration values bind to loop variables.
        binding: LoopBinding,
    },
}

/// Validated `range(...)` header shapes supported by the compiler.
pub(super) enum RangeLoop<'expr> {
    /// `range(stop)` or `range(start, stop)`.
    Positive {
        /// Optional starting value. When omitted the loop starts at `0`.
        start: Option<&'expr Spanned<Expr>>,

        /// Exclusive loop bound.
        end: &'expr Spanned<Expr>,
    },

    /// `range(start, stop, step)`.
    Stepped {
        /// Starting value.
        start: &'expr Spanned<Expr>,

        /// Exclusive loop bound.
        end: &'expr Spanned<Expr>,

        /// Per-iteration increment or decrement.
        step: &'expr Spanned<Expr>,
    },
}

impl<'expr> ResolvedForLoop<'expr> {
    /// Classifies the `for` loop header into one lowering strategy.
    pub(super) fn build<Spec, Lowering>(
        iterable: &'expr Spanned<Expr>,
    ) -> Result<Self, ErrorFor<Spec, Lowering>>
    where
        Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
        Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
    {
        match &iterable.value {
            Expr::FunctionCall { call } if call.global_function == Some(GlobalFunction::Range) => {
                Self::range::<Spec, Lowering>(call, LoopBinding::Value)
            }
            Expr::FunctionCall { call }
                if call.global_function == Some(GlobalFunction::Enumerate) =>
            {
                Self::enumerate::<Spec, Lowering>(call)
            }
            _ => Ok(Self::Indexed {
                iterable,
                binding: LoopBinding::Value,
            }),
        }
    }

    /// Validates and classifies a `range(...)` call.
    fn range<Spec, Lowering>(
        call: &'expr FunctionCall,
        binding: LoopBinding,
    ) -> Result<Self, ErrorFor<Spec, Lowering>>
    where
        Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
        Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
    {
        let function = builtin_call_name(call, "range");

        if !call.kwargs.is_empty() {
            return Err(Unsupported::FunctionCall {
                name: function,
                reason: UnsupportedFunctionCall::KeywordArguments,
            }
            .into());
        }

        let range = match call.args.as_slice() {
            [] => {
                return Err(Error::FunctionArityMismatch {
                    function,
                    expected: 1,
                    actual: 0,
                });
            }
            [end] => RangeLoop::Positive { start: None, end },
            [start, end] => RangeLoop::Positive {
                start: Some(start),
                end,
            },
            [start, end, step] => RangeLoop::Stepped { start, end, step },
            _ => {
                return Err(Error::FunctionArityMismatch {
                    function,
                    expected: 3,
                    actual: call.args.len(),
                });
            }
        };

        Ok(Self::Range { range, binding })
    }

    /// Validates and classifies an `enumerate(...)` call.
    fn enumerate<Spec, Lowering>(
        call: &'expr FunctionCall,
    ) -> Result<Self, ErrorFor<Spec, Lowering>>
    where
        Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
        Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
    {
        let function = builtin_call_name(call, "enumerate");

        match (call.args.as_slice(), call.kwargs.as_slice()) {
            ([iterable], []) => {
                Self::bind_iterable::<Spec, Lowering>(iterable, LoopBinding::Enumerate)
            }
            ([], [Kwarg { name, value }]) if name == "items" => {
                Self::bind_iterable::<Spec, Lowering>(value, LoopBinding::Enumerate)
            }
            (args, []) => Err(Error::FunctionArityMismatch {
                function,
                expected: 1,
                actual: args.len(),
            }),
            _ => Err(Unsupported::FunctionCall {
                name: function,
                reason: UnsupportedFunctionCall::KeywordArguments,
            }
            .into()),
        }
    }

    /// Reclassifies the wrapped iterable for `enumerate(...)` loop lowering.
    fn bind_iterable<Spec, Lowering>(
        iterable: &'expr Spanned<Expr>,
        binding: LoopBinding,
    ) -> Result<Self, ErrorFor<Spec, Lowering>>
    where
        Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
        Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
    {
        match &iterable.value {
            Expr::FunctionCall { call } if call.global_function == Some(GlobalFunction::Range) => {
                Self::range::<Spec, Lowering>(call, binding)
            }
            _ => Ok(Self::Indexed { iterable, binding }),
        }
    }
}

/// Returns a stable function name for built-ins parsed without a textual name.
fn builtin_call_name(call: &FunctionCall, fallback: &str) -> String {
    if call.name.is_empty() {
        return fallback.to_owned();
    }

    call.name.clone()
}
