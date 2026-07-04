mod completions_provider;

pub use self::completions_provider::*;

type RoutingKeyForInner<Inner> = waymark_action_runtime_router_core::RoutingKeyFor<
    waymark_action_runtime_core::ActionCallMetadataFor<Inner>,
>;
