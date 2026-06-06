/// Outcome returned by [`EffectHandler::handle_effect`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleEffectOutcome {
    /// Continue driving the VM.
    Continue,

    /// Persist (snapshot) the VM, then continue.
    Persist,

    /// Evict the VM entirely.
    Evict,
}

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
    ///
    /// Returns the recommended next action.
    fn handle_effect(
        &mut self,
        effect: Self::Effect,
    ) -> impl Future<Output = Result<HandleEffectOutcome, Self::Error>> + Send + '_;
}

#[cfg(feature = "tokio")]
impl<Effect, Other> EffectHandler for (tokio::sync::mpsc::Sender<Effect>, Other)
where
    Effect: Send,
    Other: Send,
{
    type Effect = Effect;
    type Error = tokio::sync::mpsc::error::SendError<Effect>;

    async fn handle_effect(
        &mut self,
        effect: Self::Effect,
    ) -> Result<HandleEffectOutcome, Self::Error> {
        self.0.send(effect).await?;
        Ok(HandleEffectOutcome::Persist)
    }
}
