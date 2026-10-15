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
    assert_eq!(tags.len(), 16);
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
    assert_eq!(
        Kind::Bridge(bridge::Kind::ExecutableRegistered).tag(),
        "bridge.executable_registered"
    );
}

fn vm_driver_payloads() -> Vec<vm_driver::Payload> {
    let vm_id = waymark_ids::InstanceId::new_uuid_v4();
    let observations = [
        vm_driver::Observation::VmStarted {
            executable_id: waymark_ids::WorkflowVersionId::new_uuid_v4(),
            executable_name: "test_wf".to_owned(),
            executable_version: "v1".to_owned(),
        },
        vm_driver::Observation::EffectEmitted {
            effect_number: EffectNumber(0),
            effect: vm_driver::EffectSummary::Complete,
        },
        vm_driver::Observation::EffectEmitted {
            effect_number: EffectNumber(1),
            effect: vm_driver::EffectSummary::UnhandledException {
                exception: waymark_observability_vm_value_display::RaisedExceptionSummary {
                    exception_type: "ValueError".to_owned(),
                },
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
                exception: waymark_observability_vm_value_display::RaisedExceptionSummary {
                    exception_type: "TimeoutError".to_owned(),
                },
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

/// The bridge's event survives the trip too, under its own source tag,
/// with the executable's fields where the readers address them.
#[test]
fn bridge_payload_round_trips_through_json() {
    let executable_id = waymark_ids::WorkflowVersionId::new_uuid_v4();
    let payload = Payload::Bridge(bridge::Payload {
        observation: bridge::Observation::ExecutableRegistered {
            executable_id,
            executable_name: "test_wf".to_owned(),
            executable_version: "v1".to_owned(),
        },
    });

    let json = serde_json::to_value(&payload).expect("serialize");

    assert_eq!(
        json,
        serde_json::json!({
            "source": "bridge",
            "observation": {
                "kind": "executable_registered",
                "executable_id": executable_id,
                "executable_name": "test_wf",
                "executable_version": "v1",
            },
        })
    );

    let back: Payload = serde_json::from_value(json).expect("deserialize");
    assert_eq!(
        waymark_observability_events_core::Kinded::kind(&back),
        Kind::Bridge(bridge::Kind::ExecutableRegistered),
    );
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

/// The definition a `$ref` points at, within the root schema's `$defs`,
/// with the name it is defined under.
fn definition<'a>(
    schema: &'a serde_json::Value,
    referrer: &'a serde_json::Value,
) -> (&'a str, &'a serde_json::Value) {
    let reference = referrer["$ref"].as_str().expect("a $ref");
    let name = reference
        .strip_prefix("#/$defs/")
        .expect("a reference into $defs");
    let definition = &schema["$defs"][name];
    assert!(definition.is_object(), "{name} is not defined");
    (name, definition)
}

/// The variant of a tagged union whose tag has the given value.
fn variant<'a>(union: &'a serde_json::Value, tag: &str, value: &str) -> &'a serde_json::Value {
    union["oneOf"]
        .as_array()
        .expect("a oneOf")
        .iter()
        .find(|variant| variant["properties"][tag]["const"] == value)
        .unwrap_or_else(|| panic!("no variant with {tag} = {value}"))
}

/// The payload describes itself: the schema follows the serde shape for
/// both sources, with the embedded ids and numbers described as what
/// they serialize to, and every definition under the name its type
/// declares (a second `Observation` would otherwise come out numbered).
#[test]
fn payload_has_a_schema() {
    let schema = schemars::schema_for!(Payload).to_value();

    for name in schema["$defs"].as_object().expect("the definitions").keys() {
        assert!(
            !name.ends_with(|character: char| character.is_ascii_digit()),
            "{name}: a definition came out numbered"
        );
    }

    let (name, vm_driver) = definition(&schema, variant(&schema, "source", "vm_driver"));
    assert_eq!(name, "VmDriverPayload");
    let vm_id = &vm_driver["properties"]["vm_id"];
    assert_eq!(vm_id["type"], "string", "{vm_id}");
    assert_eq!(vm_id["format"], "uuid", "{vm_id}");

    let (name, observation) = definition(&schema, &vm_driver["properties"]["observation"]);
    assert_eq!(name, "VmDriverObservation");
    let effect_emitted = variant(observation, "kind", "effect_emitted");
    let effect_number = &effect_emitted["properties"]["effect_number"];
    assert_eq!(effect_number["type"], "integer", "{effect_number}");
    assert_eq!(effect_number["minimum"], 0, "{effect_number}");

    let (name, bridge) = definition(&schema, variant(&schema, "source", "bridge"));
    assert_eq!(name, "BridgePayload");

    let (name, observation) = definition(&schema, &bridge["properties"]["observation"]);
    assert_eq!(name, "BridgeObservation");
    let executable_registered = variant(observation, "kind", "executable_registered");
    let executable_id = &executable_registered["properties"]["executable_id"];
    assert_eq!(executable_id["type"], "string", "{executable_id}");
    assert_eq!(executable_id["format"], "uuid", "{executable_id}");
}
