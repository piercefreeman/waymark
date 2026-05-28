/// The error for the [`crate::FullSetInterpreter`].
#[derive(Debug, thiserror::Error)]
pub enum Error<Spec: waymark_vm_instructions_fullset::Spec, Value: crate::Value> {
    /// Executing a coreset instruction failed.
    #[error(transparent)]
    CoreSet(#[from] waymark_vm_interpreter_coreset::Error<Spec>),

    /// Executing an excset instruction failed.
    #[error(transparent)]
    ExcSet(#[from] waymark_vm_interpreter_excset::Error),

    /// Executing an extcallset instruction failed.
    #[error(transparent)]
    ExtCallSet(#[from] waymark_vm_interpreter_extcallset::Error<Value>),

    /// Executing a pureset instruction failed.
    #[error(transparent)]
    PureSet(#[from] waymark_vm_interpreter_pureset::Error),
}
