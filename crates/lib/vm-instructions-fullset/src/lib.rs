//! The "full" instruction set for the VM.
//!
//! Merges together the "core", "exc", "extcall", and "pure" instruction
//! sets.

#![warn(missing_docs)]

use derive_where::derive_where;

/// The spec for required data types for the [`FullSet`].
pub trait Spec:
    waymark_vm_instructions_coreset::Spec
    + waymark_vm_instructions_excset::Spec<
        RegisterId = <Self as waymark_vm_instructions_coreset::Spec>::RegisterId,
    > + waymark_vm_instructions_extcallset::Spec<
        RegisterId = <Self as waymark_vm_instructions_coreset::Spec>::RegisterId,
        StateId = <Self as waymark_vm_instructions_coreset::Spec>::StateId,
    > + waymark_vm_instructions_pureset::Spec<
        RegisterId = <Self as waymark_vm_instructions_coreset::Spec>::RegisterId,
    >
{
}

impl<T> Spec for T where
    T: waymark_vm_instructions_coreset::Spec
        + waymark_vm_instructions_excset::Spec<
            RegisterId = <Self as waymark_vm_instructions_coreset::Spec>::RegisterId,
        > + waymark_vm_instructions_extcallset::Spec<
            RegisterId = <Self as waymark_vm_instructions_coreset::Spec>::RegisterId,
            StateId = <Self as waymark_vm_instructions_coreset::Spec>::StateId,
        > + waymark_vm_instructions_pureset::Spec<
            RegisterId = <Self as waymark_vm_instructions_coreset::Spec>::RegisterId,
        >
{
}

/// The full instructions set.
#[derive_where(Debug)]
pub enum FullSet<Spec: self::Spec> {
    /// Core instructions set.
    CoreSet(waymark_vm_instructions_coreset::CoreSet<Spec>),

    /// Exception instructions set.
    ExcSet(waymark_vm_instructions_excset::ExcSet<Spec>),

    /// External-call instructions set.
    ExtCallSet(waymark_vm_instructions_extcallset::ExtCallSet<Spec>),

    /// Pure instructions set.
    PureSet(waymark_vm_instructions_pureset::PureSet<Spec>),
}

impl<Spec: self::Spec> From<waymark_vm_instructions_coreset::CoreSet<Spec>> for FullSet<Spec> {
    fn from(value: waymark_vm_instructions_coreset::CoreSet<Spec>) -> Self {
        Self::CoreSet(value)
    }
}

impl<Spec: self::Spec> From<waymark_vm_instructions_excset::ExcSet<Spec>> for FullSet<Spec> {
    fn from(value: waymark_vm_instructions_excset::ExcSet<Spec>) -> Self {
        Self::ExcSet(value)
    }
}

impl<Spec: self::Spec> From<waymark_vm_instructions_extcallset::ExtCallSet<Spec>>
    for FullSet<Spec>
{
    fn from(value: waymark_vm_instructions_extcallset::ExtCallSet<Spec>) -> Self {
        Self::ExtCallSet(value)
    }
}

impl<Spec: self::Spec> From<waymark_vm_instructions_pureset::PureSet<Spec>> for FullSet<Spec> {
    fn from(value: waymark_vm_instructions_pureset::PureSet<Spec>) -> Self {
        Self::PureSet(value)
    }
}
