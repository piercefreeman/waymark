pub trait Spec: 'static {
    type Value;
    type ExtCallId;
}

pub struct Binding<T>(std::marker::PhantomData<T>, std::convert::Infallible);

impl<T: self::Spec> waymark_vm_instructions_coreset::Spec for Binding<T> {
    type RegisterId = waymark_vm_runtime_core::RegisterId;
    type FunctionId = waymark_vm_bytecode::FunctionId;
    type ExtCallId = <T as self::Spec>::ExtCallId;
    type StateId = waymark_vm_bytecode::StateId;
}

impl<T: self::Spec> waymark_vm_instructions_pureset::Spec for Binding<T> {
    type RegisterId = waymark_vm_runtime_core::RegisterId;
    type Value = <T as self::Spec>::Value;
}
