/// Which VM driver event.
///
/// Mirrors [`Observation`](super::Observation) without its data: one kind per
/// observation, and per summary variant where the observation carries a
/// summary. The tag is the
/// path, source first: `vm_driver.effect_emitted.action_call`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// The driver run has started.
    VmStarted,

    /// The runtime has emitted an effect.
    EffectEmitted(EffectKind),

    /// A promise settlement has reached the driver.
    PromiseSettled(SettlementKind),

    /// A snapshot has been persisted.
    SnapshotPersisted,

    /// The driver run has stopped.
    VmStopped(StopKind),
}

/// Which effect was emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectKind {
    /// The workflow completed with a value.
    Complete,

    /// The workflow raised an exception nothing caught.
    UnhandledException,

    /// An action was called.
    ActionCall,

    /// A sleep was requested.
    Sleep,
}

/// How a promise was settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementKind {
    /// Resolved with a value.
    Resolved,

    /// Rejected with an exception.
    Rejected,
}

/// Why a driver run stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopKind {
    /// A VM step failed while executing an instruction.
    Step,

    /// The runtime has no ready frames and no waiting promises.
    NoReadyFramesOrWaitingPromises,

    /// The snapshot serialization has failed.
    SnapshotSerialization,

    /// The snapshot persistence has failed.
    SnapshotPersistence,

    /// The effect handling has failed.
    EffectHandling,

    /// Getting promise settlements has failed.
    GettingPromiseSettlements,

    /// The run was cancelled.
    Cancelled,
}

impl Kind {
    /// The kind's tag: the stable text the store and the wire carry.
    pub const fn tag(self) -> &'static str {
        match self {
            Kind::VmStarted => "vm_driver.vm_started",
            Kind::EffectEmitted(EffectKind::Complete) => "vm_driver.effect_emitted.complete",
            Kind::EffectEmitted(EffectKind::UnhandledException) => {
                "vm_driver.effect_emitted.unhandled_exception"
            }
            Kind::EffectEmitted(EffectKind::ActionCall) => "vm_driver.effect_emitted.action_call",
            Kind::EffectEmitted(EffectKind::Sleep) => "vm_driver.effect_emitted.sleep",
            Kind::PromiseSettled(SettlementKind::Resolved) => "vm_driver.promise_settled.resolved",
            Kind::PromiseSettled(SettlementKind::Rejected) => "vm_driver.promise_settled.rejected",
            Kind::SnapshotPersisted => "vm_driver.snapshot_persisted",
            Kind::VmStopped(StopKind::Step) => "vm_driver.vm_stopped.step",
            Kind::VmStopped(StopKind::NoReadyFramesOrWaitingPromises) => {
                "vm_driver.vm_stopped.no_ready_frames_or_waiting_promises"
            }
            Kind::VmStopped(StopKind::SnapshotSerialization) => {
                "vm_driver.vm_stopped.snapshot_serialization"
            }
            Kind::VmStopped(StopKind::SnapshotPersistence) => {
                "vm_driver.vm_stopped.snapshot_persistence"
            }
            Kind::VmStopped(StopKind::EffectHandling) => "vm_driver.vm_stopped.effect_handling",
            Kind::VmStopped(StopKind::GettingPromiseSettlements) => {
                "vm_driver.vm_stopped.getting_promise_settlements"
            }
            Kind::VmStopped(StopKind::Cancelled) => "vm_driver.vm_stopped.cancelled",
        }
    }
}

// The kinds compose as subsets of one family, rooted at the event kind: a
// kind with its own kinds lists them through theirs, so a set is spelled
// once, where its variants are, and the whole is the composition. Each
// level converts to the root kind it is and back, through its parent.
impl waymark_observability_events_core::kind::Subset for Kind {
    type RootKind = crate::Kind;

    fn subset() -> impl Iterator<Item = Self> {
        [Kind::VmStarted]
            .into_iter()
            .chain(
                <EffectKind as waymark_observability_events_core::kind::Subset>::subset()
                    .map(Kind::EffectEmitted),
            )
            .chain(
                <SettlementKind as waymark_observability_events_core::kind::Subset>::subset()
                    .map(Kind::PromiseSettled),
            )
            .chain([Kind::SnapshotPersisted])
            .chain(
                <StopKind as waymark_observability_events_core::kind::Subset>::subset()
                    .map(Kind::VmStopped),
            )
    }

    fn root_kind(&self) -> crate::Kind {
        crate::Kind::VmDriver(*self)
    }

    fn from_root_kind(root: crate::Kind) -> Option<Self> {
        match root {
            crate::Kind::VmDriver(kind) => Some(kind),
        }
    }
}

impl waymark_observability_events_core::kind::Subset for EffectKind {
    type RootKind = crate::Kind;

    fn subset() -> impl Iterator<Item = Self> {
        [
            EffectKind::Complete,
            EffectKind::UnhandledException,
            EffectKind::ActionCall,
            EffectKind::Sleep,
        ]
        .into_iter()
    }

    fn root_kind(&self) -> crate::Kind {
        crate::Kind::VmDriver(Kind::EffectEmitted(*self))
    }

    fn from_root_kind(root: crate::Kind) -> Option<Self> {
        match root {
            crate::Kind::VmDriver(Kind::EffectEmitted(effect)) => Some(effect),
            _ => None,
        }
    }
}

impl waymark_observability_events_core::kind::Subset for SettlementKind {
    type RootKind = crate::Kind;

    fn subset() -> impl Iterator<Item = Self> {
        [SettlementKind::Resolved, SettlementKind::Rejected].into_iter()
    }

    fn root_kind(&self) -> crate::Kind {
        crate::Kind::VmDriver(Kind::PromiseSettled(*self))
    }

    fn from_root_kind(root: crate::Kind) -> Option<Self> {
        match root {
            crate::Kind::VmDriver(Kind::PromiseSettled(settlement)) => Some(settlement),
            _ => None,
        }
    }
}

impl waymark_observability_events_core::kind::Subset for StopKind {
    type RootKind = crate::Kind;

    fn subset() -> impl Iterator<Item = Self> {
        [
            StopKind::Step,
            StopKind::NoReadyFramesOrWaitingPromises,
            StopKind::SnapshotSerialization,
            StopKind::SnapshotPersistence,
            StopKind::EffectHandling,
            StopKind::GettingPromiseSettlements,
            StopKind::Cancelled,
        ]
        .into_iter()
    }

    fn root_kind(&self) -> crate::Kind {
        crate::Kind::VmDriver(Kind::VmStopped(*self))
    }

    fn from_root_kind(root: crate::Kind) -> Option<Self> {
        match root {
            crate::Kind::VmDriver(Kind::VmStopped(stop)) => Some(stop),
            _ => None,
        }
    }
}
