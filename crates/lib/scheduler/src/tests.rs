use std::collections::VecDeque;
use std::num::{NonZeroU64, NonZeroUsize};

use chrono::TimeZone as _;
use nonempty_collections::{NEVec, nev};

use waymark_scheduler_backend::register_scheduled_vm_runtimes::Outcome;
use waymark_scheduler_backend::{PollDueSchedules, RegisterScheduledVmRuntimes};
use waymark_scheduler_core::{CronExpression, Schedule, ScheduleDefinition};

/// One recorded registration item, owned for assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedItem {
    schedule_name: String,
    expected_next_run_at: chrono::DateTime<chrono::Utc>,
    vm_id: u32,
    new_next_run_at: chrono::DateTime<chrono::Utc>,
    check_overlap: bool,
}

/// A scripted backend: registration calls are recorded and answered from
/// a queue of scripted outcome batches.
struct ScriptedBackend {
    register_calls: std::sync::Mutex<Vec<Vec<RecordedItem>>>,
    scripted_outcomes: std::sync::Mutex<VecDeque<NEVec<Outcome>>>,
}

impl ScriptedBackend {
    fn new(scripted_outcomes: impl IntoIterator<Item = NEVec<Outcome>>) -> Self {
        Self {
            register_calls: std::sync::Mutex::new(Vec::new()),
            scripted_outcomes: std::sync::Mutex::new(scripted_outcomes.into_iter().collect()),
        }
    }

    fn recorded_calls(&self) -> Vec<Vec<RecordedItem>> {
        self.register_calls.lock().unwrap().clone()
    }
}

impl waymark_scheduler_backend::HasVmId for ScriptedBackend {
    type VmId = u32;
}

impl waymark_scheduler_backend::HasTimestamp for ScriptedBackend {
    type Timestamp = chrono::DateTime<chrono::Utc>;
}

impl PollDueSchedules for ScriptedBackend {
    type Error = std::convert::Infallible;

    async fn poll_due_schedules(
        &self,
        _now: chrono::DateTime<chrono::Utc>,
        _max_items: NonZeroUsize,
    ) -> Result<
        Option<NEVec<waymark_scheduler_backend::poll_due_schedules::DueScheduleFor<Self>>>,
        Self::Error,
    > {
        unreachable!("these tests drive process_due_batch directly");
    }
}

impl RegisterScheduledVmRuntimes for ScriptedBackend {
    type Error = std::convert::Infallible;

    async fn register_scheduled_vm_runtimes<'a>(
        &'a self,
        items: nonempty_collections::NESlice<
            'a,
            waymark_scheduler_backend::register_scheduled_vm_runtimes::Item<
                'a,
                u32,
                chrono::DateTime<chrono::Utc>,
            >,
        >,
    ) -> Result<NEVec<Outcome>, Self::Error> {
        let recorded = items
            .iter()
            .map(|item| RecordedItem {
                schedule_name: item.schedule_name.to_string(),
                expected_next_run_at: *item.expected_next_run_at,
                vm_id: *item.vm_id,
                new_next_run_at: *item.new_next_run_at,
                check_overlap: item.check_overlap,
            })
            .collect();
        self.register_calls.lock().unwrap().push(recorded);
        Ok(self
            .scripted_outcomes
            .lock()
            .unwrap()
            .pop_front()
            .expect("a scripted outcome batch per registration call"))
    }
}

fn at(hour: u32, minute: u32) -> chrono::DateTime<chrono::Utc> {
    chrono::Utc
        .with_ymd_and_hms(2026, 1, 1, hour, minute, 0)
        .unwrap()
}

fn encode_definition(definition: &ScheduleDefinition) -> Vec<u8> {
    let codec = waymark_vm_codec_rmp::RmpCodec;
    let mut bytes = Vec::new();
    waymark_vm_codec_core::SerializerProvider::with_serializer(&codec, &mut bytes, |serializer| {
        serde::Serialize::serialize(definition, serializer)
    })
    .expect("serialize test definition");
    bytes
}

fn interval_definition(interval_seconds: u64, allow_duplicate: bool) -> Vec<u8> {
    encode_definition(&ScheduleDefinition {
        schedule: Schedule::IntervalSeconds(NonZeroU64::new(interval_seconds).unwrap()),
        jitter_seconds: 0,
        allow_duplicate,
    })
}

fn due_row(
    schedule_name: &str,
    definition: Vec<u8>,
    next_run_at: chrono::DateTime<chrono::Utc>,
) -> waymark_scheduler_backend::poll_due_schedules::DueScheduleFor<ScriptedBackend> {
    waymark_scheduler_backend::poll_due_schedules::DueSchedule {
        schedule_name: schedule_name.to_owned(),
        definition,
        next_run_at,
        last_instance_id: None,
    }
}

fn sequential_mint() -> impl Fn() -> u32 {
    let next = std::sync::atomic::AtomicU32::new(1);
    move || next.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

async fn process(
    backend: &ScriptedBackend,
    due: &NEVec<waymark_scheduler_backend::poll_due_schedules::DueScheduleFor<ScriptedBackend>>,
) {
    super::process_due_batch(
        backend,
        &waymark_vm_codec_rmp::RmpCodec,
        &sequential_mint(),
        at(12, 0),
        due,
    )
    .await
    .expect("infallible scripted backend");
}

#[tokio::test]
async fn registers_due_rows_with_fences_and_fresh_vm_ids() {
    let backend = ScriptedBackend::new([nev![Outcome::Registered, Outcome::Registered]]);
    let due = nev![
        due_row("first", interval_definition(3600, false), at(11, 0)),
        due_row("second", interval_definition(1800, true), at(11, 30)),
    ];

    process(&backend, &due).await;

    let calls = backend.recorded_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0],
        vec![
            RecordedItem {
                schedule_name: "first".to_owned(),
                expected_next_run_at: at(11, 0),
                vm_id: 1,
                new_next_run_at: at(13, 0),
                check_overlap: true,
            },
            RecordedItem {
                schedule_name: "second".to_owned(),
                expected_next_run_at: at(11, 30),
                vm_id: 2,
                new_next_run_at: at(12, 30),
                check_overlap: false,
            },
        ]
    );
}

#[tokio::test]
async fn skips_undecodable_definitions_and_registers_the_rest() {
    let backend = ScriptedBackend::new([nev![Outcome::Registered]]);
    let due = nev![
        due_row("broken", b"not a definition".to_vec(), at(11, 0)),
        due_row("healthy", interval_definition(3600, false), at(11, 0)),
    ];

    process(&backend, &due).await;

    let calls = backend.recorded_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].len(), 1);
    assert_eq!(calls[0][0].schedule_name, "healthy");
}

#[tokio::test]
async fn skips_rows_without_a_producible_next_run() {
    // February 30 never occurs: the definition decodes but yields no
    // next occurrence.
    let never = encode_definition(&ScheduleDefinition {
        schedule: Schedule::CronExpression(CronExpression::parse("0 0 30 2 *").unwrap()),
        jitter_seconds: 0,
        allow_duplicate: true,
    });
    let backend = ScriptedBackend::new([]);
    let due = nev![due_row("never", never, at(11, 0))];

    process(&backend, &due).await;

    assert!(backend.recorded_calls().is_empty());
}

#[tokio::test]
async fn an_all_skipped_batch_registers_nothing() {
    let backend = ScriptedBackend::new([]);
    let due = nev![due_row("broken", Vec::new(), at(11, 0))];

    process(&backend, &due).await;

    assert!(backend.recorded_calls().is_empty());
}

#[tokio::test]
async fn outcomes_are_consumed_in_input_order() {
    // All three outcome variants in one batch; the call succeeds and
    // every row keeps its position (observable via the recorded items).
    let backend = ScriptedBackend::new([nev![
        Outcome::Registered,
        Outcome::SkippedOverlap,
        Outcome::Superseded,
    ]]);
    let due = nev![
        due_row("a", interval_definition(60, false), at(11, 0)),
        due_row("b", interval_definition(60, false), at(11, 1)),
        due_row("c", interval_definition(60, false), at(11, 2)),
    ];

    process(&backend, &due).await;

    let calls = backend.recorded_calls();
    assert_eq!(calls.len(), 1);
    let names: Vec<_> = calls[0]
        .iter()
        .map(|item| item.schedule_name.as_str())
        .collect();
    assert_eq!(names, ["a", "b", "c"]);
}
