//! Lowering interfaces.

/// [`waymark_vm_instructions_coreset`] lowering from [`waymark_vm_ast_old`]
/// specification.
pub trait CoreSet<Spec>
where
    Spec: waymark_vm_instructions_coreset::Spec,
{
    /// Error returned when lowering an action call fails.
    type ActionError;

    /// Lowers one AST action call into the target spec's extcall identifier.
    fn lower_action(
        call: &waymark_vm_ast_old::ActionCall,
    ) -> Result<<Spec as waymark_vm_instructions_coreset::Spec>::ExtCallId, Self::ActionError>;
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

/// [`waymark_vm_instructions_fullset`] lowering from [`waymark_vm_ast_old`]
/// specification.
pub trait FullSet<Spec>: CoreSet<Spec> + PureSet<Spec>
where
    Spec: waymark_vm_instructions_fullset::Spec,
{
}

impl<Spec, T> FullSet<Spec> for T
where
    T: CoreSet<Spec> + PureSet<Spec>,
    Spec: waymark_vm_instructions_fullset::Spec,
{
}
