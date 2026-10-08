//! The "full" instruction set for the VM.
//!
//! Merges together the "core", "extcall", "pure", and "exception"
//! instruction sets.

#![warn(missing_docs)]

use derive_where::derive_where;

/// The spec for required data types for the [`FullSet`].
#[waymark_blanket_impl_macros::blanket_impl]
pub trait Spec:
    waymark_vm_instructions_coreset::Spec
    + waymark_vm_instructions_extcallset::Spec<
        RegisterId = <Self as waymark_vm_instructions_coreset::Spec>::RegisterId,
        StateId = <Self as waymark_vm_instructions_coreset::Spec>::StateId,
    > + waymark_vm_instructions_pureset::Spec<
        RegisterId = <Self as waymark_vm_instructions_coreset::Spec>::RegisterId,
    > + waymark_vm_instructions_excset::Spec<
        RegisterId = <Self as waymark_vm_instructions_coreset::Spec>::RegisterId,
        StateId = <Self as waymark_vm_instructions_coreset::Spec>::StateId,
    >
{
}

/// The full instructions set.
#[derive_where(Debug)]
#[derive(derive_more::From)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(bound(
        serialize = "
            waymark_vm_instructions_coreset::CoreSet<Spec>: serde::Serialize,
            waymark_vm_instructions_extcallset::ExtCallSet<Spec>: serde::Serialize,
            waymark_vm_instructions_pureset::PureSet<Spec>: serde::Serialize,
            waymark_vm_instructions_excset::ExcSet<Spec>: serde::Serialize,
        ",
        deserialize = "
            waymark_vm_instructions_coreset::CoreSet<Spec>: serde::Deserialize<'de>,
            waymark_vm_instructions_extcallset::ExtCallSet<Spec>: serde::Deserialize<'de>,
            waymark_vm_instructions_pureset::PureSet<Spec>: serde::Deserialize<'de>,
            waymark_vm_instructions_excset::ExcSet<Spec>: serde::Deserialize<'de>,
        ",
    ))
)]
pub enum FullSet<Spec: self::Spec> {
    /// Core instructions set.
    CoreSet(waymark_vm_instructions_coreset::CoreSet<Spec>),

    /// External-call instructions set.
    ExtCallSet(waymark_vm_instructions_extcallset::ExtCallSet<Spec>),

    /// Pure instructions set.
    PureSet(waymark_vm_instructions_pureset::PureSet<Spec>),

    /// Exception instructions set.
    ExcSet(waymark_vm_instructions_excset::ExcSet<Spec>),
}
