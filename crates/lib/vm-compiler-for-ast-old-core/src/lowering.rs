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

/// Combined lowering for the full instruction set.
pub trait FullSet<Spec>: ExtCallSet<Spec> + PureSet<Spec>
where
    Spec: waymark_vm_instructions_fullset::Spec,
{
}

impl<Spec, T> FullSet<Spec> for T
where
    T: ExtCallSet<Spec> + PureSet<Spec>,
    Spec: waymark_vm_instructions_fullset::Spec,
{
}
