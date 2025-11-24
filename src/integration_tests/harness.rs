use std::{env, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use tempfile::TempDir;

use crate::{
    Database, PythonWorkerConfig, PythonWorkerPool, WorkflowInstanceId, db::WorkflowVersionDetail,
    worker::RoundTripMetrics,
};

use super::{
    TestServer, cleanup_database, common::run_in_env, dispatch_all_actions, encode_workflow_input,
    purge_empty_input_instances,
};

pub struct WorkflowHarnessConfig<'a> {
    pub files: &'a [(&'static str, &'static str)],
    pub entrypoint: &'static str,
    pub workflow_name: &'static str,
    pub user_module: &'static str,
    pub inputs: &'a [(&'static str, &'static str)],
}

pub struct WorkflowHarness {
    database: Database,
    server: TestServer,
    worker_server: Arc<crate::server_worker::WorkerBridgeServer>,
    pool: PythonWorkerPool,
    python_env: TempDir,
    version_detail: WorkflowVersionDetail,
    expected_actions: usize,
    instance_id: WorkflowInstanceId,
}

impl WorkflowHarness {
    pub async fn new(config: WorkflowHarnessConfig<'_>) -> Result<Option<Self>> {
        let database_url = match env::var("DATABASE_URL") {
            Ok(url) => url,
            Err(_) => {
                eprintln!("skipping integration test: DATABASE_URL not set");
                return Ok(None);
            }
        };
        let database = Database::connect(&database_url).await?;
        cleanup_database(&database).await?;

        let server = TestServer::spawn(database_url.clone()).await?;
        super::wait_for_health(server.http_addr).await?;

        let env_pairs = vec![
            ("CARABINER_GRPC_ADDR", server.grpc_addr.to_string()),
            ("CARABINER_SERVER_PORT", server.http_addr.port().to_string()),
            ("CARABINER_SERVER_HOST", server.http_addr.ip().to_string()),
            ("CARABINER_SKIP_WAIT_FOR_INSTANCE", "1".to_string()),
        ];
        let python_env = run_in_env(config.files, &[], &env_pairs, config.entrypoint).await?;
        purge_empty_input_instances(&database).await?;

        let versions = database.list_workflow_versions().await?;
        let version = versions
            .iter()
            .find(|v| v.workflow_name == config.workflow_name)
            .with_context(|| format!("{} missing", config.workflow_name))?;
        let version_detail = database
            .load_workflow_version(version.id)
            .await?
            .context("missing workflow version detail")?;
        let expected_actions = version_detail.dag.nodes.len();

        let workflow_input = encode_workflow_input(config.inputs);
        let instance_id = database
            .create_workflow_instance(&version.workflow_name, version.id, Some(&workflow_input))
            .await?;

        let worker_server: Arc<crate::server_worker::WorkerBridgeServer> =
            crate::server_worker::WorkerBridgeServer::start(None).await?;
        let worker_script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("python")
            .join(".venv")
            .join("bin")
            .join("rappel-worker");
        let worker_config = PythonWorkerConfig {
            script_path: worker_script,
            script_args: Vec::new(),
            user_module: config.user_module.to_string(),
            extra_python_paths: vec![python_env.path().to_path_buf()],
        };
        let pool = PythonWorkerPool::new(worker_config, 1, Arc::clone(&worker_server)).await?;

        Ok(Some(Self {
            database,
            server,
            worker_server,
            pool,
            python_env,
            version_detail,
            expected_actions,
            instance_id,
        }))
    }

    pub async fn dispatch_all(&self) -> Result<Vec<RoundTripMetrics>> {
        dispatch_all_actions(&self.database, &self.pool, self.expected_actions).await
    }

    pub async fn stored_result(&self) -> Result<Option<Vec<u8>>> {
        sqlx::query_scalar("SELECT result_payload FROM workflow_instances WHERE id = $1")
            .bind(self.instance_id)
            .fetch_one(self.database.pool())
            .await
            .map_err(Into::into)
    }

    pub fn version_detail(&self) -> &WorkflowVersionDetail {
        &self.version_detail
    }

    pub fn expected_actions(&self) -> usize {
        self.expected_actions
    }

    pub fn instance_id(&self) -> WorkflowInstanceId {
        self.instance_id
    }

    pub fn database(&self) -> &Database {
        &self.database
    }

    pub async fn shutdown(self) -> Result<()> {
        self.pool.shutdown().await?;
        self.worker_server.shutdown().await;
        self.server.shutdown().await;
        drop(self.python_env);
        Ok(())
    }
}
