//! Lowering interfaces.

/// Core instructions set lowering.
pub trait CoreSet<Spec>
where
    Spec: waymark_vm_instructions_coreset::Spec,
{
    type ActionError;

    fn lower_action(
        call: &waymark_vm_ast_old::ActionCall,
    ) -> Result<<Spec as waymark_vm_instructions_coreset::Spec>::ExtCallId, Self::ActionError>;
}

/// Pure instructions set lowering.
pub trait PureSet<Spec>
where
    Spec: waymark_vm_instructions_pureset::Spec,
{
    type LiteralError;

    fn lower_literal(
        literal: &waymark_vm_ast_old::Literal,
    ) -> Result<<Spec as waymark_vm_instructions_pureset::Spec>::ConstValue, Self::LiteralError>;
}

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
