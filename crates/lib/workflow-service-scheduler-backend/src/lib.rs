//! Backend traits for the workflow-service scheduler surface.
//!
//! Defines the contract a database backend must fulfill for schedule
//! management: register-or-repoint (upsert), read back for get/list,
//! pause/resume (status), and hard delete. The schedule's definition and
//! initial snapshot cross these traits as opaque blobs — the backend
//! never interprets them.

#![warn(missing_docs)]

mod common;

pub mod delete_schedule;
pub mod get_schedule;
pub mod list_schedules;
pub mod update_schedule_status;
pub mod upsert_schedule;

pub use self::common::*;
pub use self::delete_schedule::DeleteSchedule;
pub use self::get_schedule::GetSchedule;
pub use self::list_schedules::ListSchedules;
pub use self::update_schedule_status::UpdateScheduleStatus;
pub use self::upsert_schedule::UpsertSchedule;
