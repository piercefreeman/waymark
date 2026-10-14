use super::Kind;

/// One bridge event.
///
/// The observation is tagged internally by `kind`, so a reader addresses
/// its fields directly (`observation ->> 'kind'`,
/// `observation ->> 'executable_id'`).
#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[schemars(rename = "BridgePayload")]
pub struct Payload {
    /// What was observed.
    pub observation: Observation,
}

impl Payload {
    /// The event's kind.
    pub fn kind(&self) -> Kind {
        self.observation.kind()
    }
}

/// What the bridge observed, one variant per observation.
#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[schemars(rename = "BridgeObservation")]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Observation {
    /// An executable was registered: stored under its name and version,
    /// whether for the first time or again.
    ExecutableRegistered {
        /// The executable.
        #[schemars(schema_with = "crate::schema::uuid")]
        executable_id: waymark_ids::WorkflowVersionId,

        /// The name the executable was registered under.
        executable_name: String,

        /// The version the executable was registered under.
        executable_version: String,
    },
}

impl Observation {
    /// The event's kind.
    pub fn kind(&self) -> Kind {
        match self {
            Observation::ExecutableRegistered { .. } => Kind::ExecutableRegistered,
        }
    }
}
