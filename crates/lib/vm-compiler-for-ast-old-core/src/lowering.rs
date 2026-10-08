//! Lowering interfaces.

/// [`waymark_vm_instructions_extcallset`] lowering from [`waymark_vm_ast_old`]
/// specification.
pub trait ExtCallSet<Spec>
where
    Spec: waymark_vm_instructions_extcallset::Spec,
{
    /// Error returned when lowering an action call fails.
    type ActionError;

    /// Lowers one AST action call into the target spec's action reference.
    fn lower_action(
        call: &waymark_vm_ast_old::ActionCall,
    ) -> Result<<Spec as waymark_vm_instructions_extcallset::Spec>::ActionRef, Self::ActionError>;
}

/// [`waymark_vm_instructions_pureset`] lowering from [`waymark_vm_ast_old`]
/// specification.
pub trait PureSet<Spec>
where
    Spec: waymark_vm_instructions_pureset::Spec,
{
    /// Error returned when lowering a literal fails.
    type LiteralError;

    /// Lowers one AST literal into the target spec's constant representation.
    fn lower_literal(
        literal: &waymark_vm_ast_old::Literal,
    ) -> Result<<Spec as waymark_vm_instructions_pureset::Spec>::ConstValue, Self::LiteralError>;
}

/// An exception the compiler raises on its own, with no AST node behind it.
///
/// The compiler's vocabulary, the way [`waymark_vm_ast_old::Literal`] is
/// the AST's: the lowering turns each into the target spec's const
/// exception, and the VM never sees this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompilerEmittedException {
    /// An unpacking assignment's value does not have as many items as
    /// there are targets.
    UnpackMismatch,

    /// An action call's per-attempt timeout fired.
    ActionTimeout,
}

/// [`waymark_vm_instructions_excset`] lowering from [`waymark_vm_ast_old`]
/// specification.
///
/// The AST lists exception classes; what a handler lists in the bytecode
/// and what the compiler's own raises embed are the target spec's const
/// exception types, so the lowering spells them out.
pub trait ExcSet<Spec>
where
    Spec: waymark_vm_instructions_excset::Spec,
{
    /// Lowers the class names an `except` clause lists - none for a bare
    /// `except:` - into the target spec's const handler pattern.
    fn lower_exception_pattern(
        class_names: &[String],
    ) -> <Spec as waymark_vm_instructions_excset::Spec>::ConstExceptionPattern;

    /// Lowers one of the compiler's own raises into the target spec's
    /// const exception.
    fn lower_compiler_emitted_exception(
        exception: &CompilerEmittedException,
    ) -> <Spec as waymark_vm_instructions_excset::Spec>::ConstException;
}

/// Combined lowering for the full instruction set.
#[waymark_blanket_impl_macros::blanket_impl]
pub trait FullSet<Spec>: ExtCallSet<Spec> + PureSet<Spec> + ExcSet<Spec>
where
    Spec: waymark_vm_instructions_fullset::Spec,
{
}
