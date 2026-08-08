//! The interpreter for the "full" instructions set.

#![warn(missing_docs)]

pub mod operations;
pub mod value;

use derive_where::derive_where;

pub use self::operations::Operations;
pub use self::value::Value;

type FunctionIdFor<Spec> = <Spec as waymark_vm_instructions_coreset::Spec>::FunctionId;
type StateIdFor<Spec> = <Spec as waymark_vm_instructions_coreset::Spec>::StateId;
type ActionRefFor<Spec> = <Spec as waymark_vm_instructions_extcallset::Spec>::ActionRef;

/// The runtime view for the [`FullSetInterpreter`].
pub use waymark_vm_runtime_core::FullRuntimeView as RuntimeView;

/// An interpreter for the "full" instructions set.
#[derive_where(Default)]
#[derive(waymark_vm_interpreter_composite::Interpreter)]
#[interpreter(
    instruction = waymark_vm_instructions_fullset::FullSet<Spec>,
    frame = waymark_vm_runtime_core::Frame<FunctionIdFor<Spec>, StateIdFor<Spec>, Value>,
    view = waymark_vm_runtime_core::FullRuntimeView<
        'r,
        Executable,
        FunctionIdFor<Spec>,
        StateIdFor<Spec>,
        Value,
    >,
    bound(
        Executable: 'static,
        Executable: waymark_vm_executable::FunctionInfo<FunctionId = FunctionIdFor<Spec>>,
        Spec: waymark_vm_instructions_coreset::Spec<
            RegisterId = waymark_vm_runtime_core::RegisterId,
        >,
        Spec: waymark_vm_instructions_extcallset::Spec<
            RegisterId = waymark_vm_runtime_core::RegisterId,
            StateId = StateIdFor<Spec>,
        >,
        Spec: waymark_vm_instructions_pureset::Spec<
            RegisterId = waymark_vm_runtime_core::RegisterId,
        >,
        Spec: 'static,
        FunctionIdFor<Spec>: Copy,
        StateIdFor<Spec>: Copy + Default + PartialEq,
        ActionRefFor<Spec>: Clone,
        Operations: self::Operations<Value>,
        Operations: self::operations::Exceptions<Value>,
        Operations: 'static,
        Value: Clone + 'static,
        Value: waymark_vm_interpreter_coreset::Value,
        Value: waymark_vm_interpreter_extcallset::Value,
        Value: waymark_vm_interpreter_pureset::Value,
        Value: waymark_vm_runtime_exception::FromException<RootValue = Value>,
        Value: waymark_vm_runtime_exception::IntoException<RootValue = Value>,
        Value: for<'a> waymark_vm_interpreter_pureset::value::LoadConst<&'a Spec::ConstValue>,
        Value: waymark_vm_runtime_promise_core::Resolvable,
        Value: waymark_vm_runtime_promise_core::Suspendable,
        Value::ReadyValue: Clone,
    ),
)]
pub struct FullSetInterpreter<
    Spec: waymark_vm_instructions_fullset::Spec,
    Executable,
    Operations,
    Value,
> {
    /// The coreset interpreter used for core instructions.
    #[interpreter(
        variant = CoreSet,
        instruction = waymark_vm_instructions_coreset::CoreSet<Spec>,
    )]
    pub core_set:
        waymark_vm_interpreter_coreset::CoreSetInterpreter<Spec, Executable, Operations, Value>,

    /// The extcallset interpreter used for extcall instructions.
    #[interpreter(
        variant = ExtCallSet,
        instruction = waymark_vm_instructions_extcallset::ExtCallSet<Spec>,
    )]
    pub extcall_set: waymark_vm_interpreter_extcallset::ExtCallSetInterpreter<
        Spec,
        FunctionIdFor<Spec>,
        StateIdFor<Spec>,
        Operations,
        Value,
    >,

    /// The pureset interpreter used for pure instructions.
    #[interpreter(
        variant = PureSet,
        instruction = waymark_vm_instructions_pureset::PureSet<Spec>,
    )]
    pub pure_set: waymark_vm_interpreter_pureset::PureSetInterpreter<
        Spec,
        FunctionIdFor<Spec>,
        StateIdFor<Spec>,
        Operations,
        Value,
    >,
}
