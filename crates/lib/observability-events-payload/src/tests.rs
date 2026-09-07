use std::collections::HashSet;

use waymark_observability_events_core::kind::{FromTag as _, Kind as _, Tagged as _};
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use super::*;

/// The lookup is built from the table, so every kind reads back from
/// its own tag — and no two kinds share one, or the lookup would have
/// dropped one of them.
#[test]
fn every_kind_reads_back_from_its_tag() {
    let mut tags = HashSet::new();

    for kind in Kind::all() {
        let tag = kind.tag();
        assert_eq!(Kind::from_tag(tag), Some(kind), "{tag}");
        assert!(tags.insert(tag), "tag {tag} names more than one kind");
    }

    assert_eq!(Kind::from_tag("not a kind"), None);
    assert_eq!(tags.len(), 15);
}

/// The tag is the path, source first, and it is the source's own tag.
#[test]
fn tags_are_source_prefixed_paths() {
    let kind = Kind::VmDriver(vm_driver::Kind::EffectEmitted(
        vm_driver::EffectKind::ActionCall,
    ));

    assert_eq!(kind.tag(), "vm_driver.effect_emitted.action_call");
    assert_eq!(
        Kind::VmDriver(vm_driver::Kind::VmStopped(
            vm_driver::StopKind::NoReadyFramesOrWaitingPromises,
        ))
        .tag(),
        "vm_driver.vm_stopped.no_ready_frames_or_waiting_promises"
    );
}

fn vm_driver_payloads() -> Vec<vm_driver::Payload> {
    let vm_id = waymark_ids::InstanceId::new_uuid_v4();
    let observations = [
        vm_driver::Observation::VmStarted,
        vm_driver::Observation::EffectEmitted {
            effect_number: EffectNumber(0),
            effect: vm_driver::EffectSummary::Complete,
        },
        vm_driver::Observation::EffectEmitted {
            effect_number: EffectNumber(1),
            effect: vm_driver::EffectSummary::UnhandledException {
                exception_type: "ValueError".to_owned(),
            },
        },
        vm_driver::Observation::EffectEmitted {
            effect_number: EffectNumber(2),
            effect: vm_driver::EffectSummary::ActionCall {
                promise_state_id: PromiseStateId(4),
                action_name: "fetch".to_owned(),
                module_name: Some("app.tasks".to_owned()),
            },
        },
        vm_driver::Observation::EffectEmitted {
            effect_number: EffectNumber(3),
            effect: vm_driver::EffectSummary::Sleep {
                promise_state_id: PromiseStateId(5),
                duration: core::time::Duration::from_millis(1500),
                skip_allowed: true,
            },
        },
        vm_driver::Observation::PromiseSettled {
            promise_state_id: PromiseStateId(4),
            settlement: vm_driver::Settlement::Resolved,
        },
        vm_driver::Observation::PromiseSettled {
            promise_state_id: PromiseStateId(5),
            settlement: vm_driver::Settlement::Rejected {
                exception_type: "TimeoutError".to_owned(),
            },
        },
        vm_driver::Observation::SnapshotPersisted {
            size_in_bytes: 4096,
        },
        vm_driver::Observation::VmStopped {
            reason: vm_driver::StopReason::Step {
                error: "boom".to_owned(),
            },
        },
        vm_driver::Observation::VmStopped {
            reason: vm_driver::StopReason::NoReadyFramesOrWaitingPromises,
        },
        vm_driver::Observation::VmStopped {
            reason: vm_driver::StopReason::SnapshotSerialization {
                error: "boom".to_owned(),
            },
        },
        vm_driver::Observation::VmStopped {
            reason: vm_driver::StopReason::SnapshotPersistence {
                error: "boom".to_owned(),
            },
        },
        vm_driver::Observation::VmStopped {
            reason: vm_driver::StopReason::EffectHandling {
                error: "boom".to_owned(),
            },
        },
        vm_driver::Observation::VmStopped {
            reason: vm_driver::StopReason::GettingPromiseSettlements {
                error: "boom".to_owned(),
            },
        },
        vm_driver::Observation::VmStopped {
            reason: vm_driver::StopReason::Cancelled,
        },
    ];

    observations
        .into_iter()
        .enumerate()
        .map(|(run_sequence, observation)| vm_driver::Payload {
            vm_id,
            run_sequence: u64::try_from(run_sequence).expect("a handful of observations"),
            observation,
        })
        .collect()
}

/// One payload per kind, and each knows its kind.
#[test]
fn vm_driver_payloads_cover_every_kind_once() {
    let kinds: Vec<_> = vm_driver_payloads()
        .iter()
        .map(vm_driver::Payload::kind)
        .map(Kind::VmDriver)
        .collect();
    let all: Vec<_> =
        <vm_driver::Kind as waymark_observability_events_core::kind::SubsetExt>::root_subset()
            .collect();

    assert_eq!(kinds, all);
}

/// The store writes and reads the payload as JSON: every variant survives
/// the trip unchanged, and the source is the top-level tag.
#[test]
fn payload_round_trips_through_json() {
    for payload in vm_driver_payloads() {
        let payload = Payload::VmDriver(payload);
        let json = serde_json::to_value(&payload).expect("serialize");

        assert_eq!(json["source"], "vm_driver", "{json}");

        let back: Payload = serde_json::from_value(json.clone()).expect("deserialize");
        let json_back = serde_json::to_value(&back).expect("serialize again");

        assert_eq!(json_back, json);
        assert_eq!(
            waymark_observability_events_core::Kinded::kind(&back),
            waymark_observability_events_core::Kinded::kind(&payload),
        );
    }
}

/// The JSON shape the store's access paths and the readers rely on.
#[test]
fn json_shape() {
    let vm_id = waymark_ids::InstanceId::new_uuid_v4();
    let payload = Payload::VmDriver(vm_driver::Payload {
        vm_id,
        run_sequence: 2,
        observation: vm_driver::Observation::EffectEmitted {
            effect_number: EffectNumber(7),
            effect: vm_driver::EffectSummary::ActionCall {
                promise_state_id: PromiseStateId(3),
                action_name: "fetch".to_owned(),
                module_name: None,
            },
        },
    });

    let json = serde_json::to_value(&payload).expect("serialize");

    assert_eq!(
        json,
        serde_json::json!({
            "source": "vm_driver",
            "vm_id": vm_id,
            "run_sequence": 2,
            "observation": {
                "kind": "effect_emitted",
                "effect_number": 7,
                "effect": {
                    "kind": "action_call",
                    "promise_state_id": 3,
                    "action_name": "fetch",
                    "module_name": null,
                },
            },
        })
    );
}

/// The payload describes itself: the schema follows the serde shape, with
/// the embedded ids and numbers described as what they serialize to.
#[test]
fn payload_has_a_schema() {
    let schema = schemars::schema_for!(Payload);
    let text = serde_json::to_string(&schema).expect("schema json");

    assert!(text.contains("\"vm_driver\""), "{text}");
    assert!(text.contains("\"format\":\"uuid\""), "{text}");
    assert!(text.contains("\"effect_number\""), "{text}");
}
