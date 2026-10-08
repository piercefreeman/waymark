//! Effect handler for the fullset interpreter.

/// A terminal effect of the full instruction set: how the workflow ended.
///
/// The core set emits the completion and the exception set the unhandled
/// exception; both end the run, and both go to the one handler that
/// records how it ended.
#[derive(Debug)]
pub enum TerminalEffect<Value, RaisedException> {
    /// Program execution is complete.
    Complete(Value),

    /// Program execution terminated with an unhandled exception.
    UnhandledException(RaisedException),
}

/// Handles effects emitted by the VM driver for the fullset interpreter.
pub struct EffectHandler<TerminalEffectHandler, ExtcallEffectHandler> {
    /// Handles the terminal effects: the completion and the unhandled
    /// exception.
    pub terminal: TerminalEffectHandler,

    /// Handles extcall effects.
    pub extcall: ExtcallEffectHandler,
}

/// Error returned when handling a fullset effect fails.
#[derive(Debug, thiserror::Error)]
pub enum Error<TerminalError, ExtcallError> {
    #[error("terminal effect: {0}")]
    Terminal(TerminalError),

    #[error("extcallset effect: {0}")]
    Extcall(ExtcallError),
}

impl<
    TerminalEffectHandler,
    ExtcallEffectHandler,
    Value,
    RaisedException,
    ActionRef,
    ActionCallArgument,
> waymark_vm_driver_core::EffectHandler
    for EffectHandler<TerminalEffectHandler, ExtcallEffectHandler>
where
    TerminalEffectHandler:
        waymark_vm_driver_core::EffectHandler<Effect = TerminalEffect<Value, RaisedException>>,
    TerminalEffectHandler: Send,
    ExtcallEffectHandler: waymark_vm_driver_core::EffectHandler<
            Effect = waymark_vm_interpreter_extcallset::Effect<ActionRef, ActionCallArgument>,
        >,
    ExtcallEffectHandler: Send,
    Value: Send + 'static,
    RaisedException: Send + 'static,
    ActionRef: Send + 'static,
    ActionCallArgument: Send + 'static,
{
    type Effect = waymark_vm_interpreter_fullset::Effect<
        waymark_vm_interpreter_coreset::Effect<Value>,
        waymark_vm_interpreter_extcallset::Effect<ActionRef, ActionCallArgument>,
        core::convert::Infallible,
        waymark_vm_interpreter_excset::Effect<RaisedException>,
    >;
    type Error = Error<TerminalEffectHandler::Error, ExtcallEffectHandler::Error>;

    async fn handle_effect(
        &mut self,
        emitted_effect: waymark_vm_runtime_effect::EmittedEffect<Self::Effect>,
    ) -> Result<(), Self::Error> {
        let waymark_vm_runtime_effect::EmittedEffect { effect, number } = emitted_effect;
        match effect {
            waymark_vm_interpreter_fullset::Effect::CoreSet(
                waymark_vm_interpreter_coreset::Effect::Complete(value),
            ) => self
                .terminal
                .handle_effect(waymark_vm_runtime_effect::EmittedEffect {
                    effect: TerminalEffect::Complete(value),
                    number,
                })
                .await
                .map_err(Error::Terminal),
            waymark_vm_interpreter_fullset::Effect::ExcSet(
                waymark_vm_interpreter_excset::Effect::UnhandledException(exception),
            ) => self
                .terminal
                .handle_effect(waymark_vm_runtime_effect::EmittedEffect {
                    effect: TerminalEffect::UnhandledException(exception),
                    number,
                })
                .await
                .map_err(Error::Terminal),
            waymark_vm_interpreter_fullset::Effect::ExtCallSet(extcall_effect) => self
                .extcall
                .handle_effect(waymark_vm_runtime_effect::EmittedEffect {
                    effect: extcall_effect,
                    number,
                })
                .await
                .map_err(Error::Extcall),
            waymark_vm_interpreter_fullset::Effect::PureSet(effect) => match effect {},
        }
    }
}
