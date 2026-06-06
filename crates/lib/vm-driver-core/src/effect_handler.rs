use waymark_vm_runtime_effect::EmittedEffect;

/// Handles an effect emitted by a VM.
pub trait EffectHandler {
    /// The type of the effects to handle.
    type Effect;

    /// Error returned by [`EffectHandler::handle_effect`].
    ///
    /// Any error by an event handler will trigger a driver loop return with
    /// this error.
    type Error: std::fmt::Debug;

    /// Handle an effect emitted by the VM driver.
    fn handle_effect(
        &mut self,
        emitted_effect: EmittedEffect<Self::Effect>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + '_;
}

impl<A, B> EffectHandler for (A, B)
where
    A: EffectHandler,
{
    type Effect = A::Effect;
    type Error = A::Error;

    fn handle_effect(
        &mut self,
        emitted_effect: EmittedEffect<Self::Effect>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        self.0.handle_effect(emitted_effect)
    }
}

#[cfg(feature = "tokio")]
impl<Effect> EffectHandler for tokio::sync::mpsc::Sender<EmittedEffect<Effect>>
where
    Effect: Send,
{
    type Effect = Effect;
    type Error = tokio::sync::mpsc::error::SendError<EmittedEffect<Effect>>;

    async fn handle_effect(
        &mut self,
        emitted_effect: EmittedEffect<Self::Effect>,
    ) -> Result<(), Self::Error> {
        self.send(emitted_effect).await
    }
}
