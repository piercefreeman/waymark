pub trait Spec: 'static {
    type RegisterId;
    type FunctionId;
    type ExtCallId;
    type StateId;
}

pub enum CoreSet<Spec: self::Spec> {
    Call {
        dst: Spec::RegisterId,
        function_id: Spec::FunctionId,
        args: Vec<Spec::RegisterId>,
    },

    ExtCall {
        dst: Spec::RegisterId,
        extcall_id: Spec::ExtCallId,
        args: Vec<Spec::RegisterId>,
    },

    Await {
        dst: Spec::RegisterId,
        src: Spec::RegisterId,
        resume: Spec::StateId,
    },

    /// Jump to the specified state.
    Jump { target_state: Spec::StateId },

    /// Jump to the specified state if the cond is true.
    JumpIf {
        target_state: Spec::StateId,
        cond: Spec::RegisterId,
    },

    /// Return the value at the given registry.
    Return { src: Spec::RegisterId },
}
