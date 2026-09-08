//! Effect handler for the fullset interpreter.

/// Handles effects emitted by the VM driver for the fullset interpreter.
pub struct EffectHandler<CoreEffectHandler, ExtcallEffectHandler> {
    /// Handles core effects
    pub core: CoreEffectHandler,

    /// Handles extcall effects.
    pub extcall: ExtcallEffectHandler,
}

/// Error returned when handling a fullset effect fails.
#[derive(Debug, thiserror::Error)]
pub enum Error<CoreError, ExtcallError> {
    #[error("coreset effect: {0}")]
    Core(CoreError),

    #[error("extcallset effect: {0}")]
    Extcall(ExtcallError),
}

impl<CoreEffectHandler, ExtcallEffectHandler, Value, ActionRef, ActionCallArgument>
    waymark_vm_driver_core::EffectHandler for EffectHandler<CoreEffectHandler, ExtcallEffectHandler>
where
    CoreEffectHandler: waymark_vm_driver_core::EffectHandler<
            Effect = waymark_vm_interpreter_coreset::Effect<Value>,
        >,
    CoreEffectHandler: Send,
    ExtcallEffectHandler: waymark_vm_driver_core::EffectHandler<
            Effect = waymark_vm_interpreter_extcallset::Effect<ActionRef, ActionCallArgument>,
        >,
    ExtcallEffectHandler: Send,
    Value: Send + 'static,
    ActionRef: Send + 'static,
    ActionCallArgument: Send + 'static,
{
    type Effect = waymark_vm_interpreter_fullset::Effect<
        waymark_vm_interpreter_coreset::Effect<Value>,
        waymark_vm_interpreter_extcallset::Effect<ActionRef, ActionCallArgument>,
        core::convert::Infallible,
    >;
    type Error = Error<CoreEffectHandler::Error, ExtcallEffectHandler::Error>;

    async fn handle_effect(
        &mut self,
        emitted_effect: waymark_vm_runtime_effect::EmittedEffect<Self::Effect>,
    ) -> Result<(), Self::Error> {
        let waymark_vm_runtime_effect::EmittedEffect { effect, number } = emitted_effect;
        match effect {
            waymark_vm_interpreter_fullset::Effect::CoreSet(core_effect) => self
                .core
                .handle_effect(waymark_vm_runtime_effect::EmittedEffect {
                    effect: core_effect,
                    number,
                })
                .await
                .map_err(Error::Core),
            waymark_vm_interpreter_fullset::Effect::ExtCallSet(extcall_effect) => self
                .extcall
                .handle_effect(waymark_vm_runtime_effect::EmittedEffect {
                    effect: extcall_effect,
                    number,
                })
                .await
                .map_err(Error::Extcall),
        }
    }
}
