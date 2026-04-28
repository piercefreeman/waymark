pub trait Spec:
    waymark_vm_instructions_coreset::Spec
    + waymark_vm_instructions_pureset::Spec<
        RegisterId = <Self as waymark_vm_instructions_coreset::Spec>::RegisterId,
    >
{
}

impl<T> Spec for T where
    T: waymark_vm_instructions_coreset::Spec
        + waymark_vm_instructions_pureset::Spec<
            RegisterId = <Self as waymark_vm_instructions_coreset::Spec>::RegisterId,
        >
{
}

pub enum FullSet<Spec: self::Spec> {
    CoreSet(waymark_vm_instructions_coreset::CoreSet<Spec>),
    PureSet(waymark_vm_instructions_pureset::PureSet<Spec>),
}
