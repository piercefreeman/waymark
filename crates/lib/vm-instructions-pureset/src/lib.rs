pub trait Spec: 'static {
    type RegisterId;
    type Value;
}

pub enum PureSet<Spec: self::Spec> {
    LoadConst {
        dst: Spec::RegisterId,
        value: Spec::Value,
    },

    Add {
        dst: Spec::RegisterId,
        a: Spec::RegisterId,
        b: Spec::RegisterId,
    },
}
