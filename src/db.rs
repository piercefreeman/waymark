use anyhow::{Context, Result};
use sqlx::{FromRow, PgPool, postgres::PgPoolOptions};

#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

#[derive(Debug, Clone)]
pub struct CompletionRecord {
    pub action_id: i64,
    pub success: bool,
    pub delivery_id: u64,
    pub result_payload: Vec<u8>,
}

#[derive(Debug, Clone, FromRow)]
pub struct LedgerAction {
    pub id: i64,
    pub instance_id: i64,
    pub partition_id: i32,
    pub action_seq: i32,
    pub payload: Vec<u8>,
}

impl Database {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
            .with_context(|| "failed to connect to postgres")?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .with_context(|| "failed to run migrations")?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn reset_partition(&self, partition_id: i32) -> Result<()> {
        let span = tracing::info_span!("db.reset_partition", partition_id);
        let _guard = span.enter();
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM daemon_action_ledger WHERE partition_id = $1")
            .bind(partition_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM workflow_instances WHERE partition_id = $1")
            .bind(partition_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn seed_actions(
        &self,
        partition_id: i32,
        action_count: usize,
        payload: &[u8],
    ) -> Result<()> {
        let span = tracing::info_span!("db.seed_actions", partition_id, action_count);
        let _guard = span.enter();
        let mut tx = self.pool.begin().await?;
        let instance_id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO workflow_instances (partition_id, workflow_name, next_action_seq)
            VALUES ($1, $2, $3)
            RETURNING id
            "#,
        )
        .bind(partition_id)
        .bind("benchmark")
        .bind(action_count as i32)
        .fetch_one(&mut *tx)
        .await?;

        for seq in 0..action_count {
            sqlx::query(
                r#"
                INSERT INTO daemon_action_ledger (
                    instance_id,
                    partition_id,
                    action_seq,
                    status,
                    payload
                ) VALUES ($1, $2, $3, 'queued', $4)
                "#,
            )
            .bind(instance_id)
            .bind(partition_id)
            .bind(seq as i32)
            .bind(payload)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn dispatch_actions(
        &self,
        partition_id: i32,
        limit: i64,
    ) -> Result<Vec<LedgerAction>> {
        let span = tracing::info_span!("db.dispatch_actions", partition_id, limit);
        let _guard = span.enter();
        let records = sqlx::query_as::<_, LedgerAction>(
            r#"
            WITH next_actions AS (
                SELECT id
                FROM daemon_action_ledger
                WHERE partition_id = $1 AND status = 'queued'
                ORDER BY action_seq
                FOR UPDATE SKIP LOCKED
                LIMIT $2
            )
            UPDATE daemon_action_ledger AS dal
            SET status = 'dispatched', dispatched_at = NOW()
            FROM next_actions
            WHERE dal.id = next_actions.id
            RETURNING dal.id, dal.instance_id, dal.partition_id, dal.action_seq, dal.payload
            "#,
        )
        .bind(partition_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(records)
    }

    pub async fn mark_actions_batch(&self, records: &[CompletionRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let span = tracing::debug_span!("db.mark_actions_batch", count = records.len());
        let _guard = span.enter();
        let ids: Vec<i64> = records.iter().map(|r| r.action_id).collect();
        let successes: Vec<bool> = records.iter().map(|r| r.success).collect();
        let deliveries: Vec<i64> = records.iter().map(|r| r.delivery_id as i64).collect();
        let payloads: Vec<Vec<u8>> = records.iter().map(|r| r.result_payload.clone()).collect();
        sqlx::query(
            r#"
            WITH data AS (
                SELECT *
                FROM UNNEST($1::BIGINT[], $2::BOOL[], $3::BIGINT[], $4::BYTEA[])
                    AS t(action_id, success, delivery_id, result_payload)
            )
            UPDATE daemon_action_ledger AS dal
            SET status = 'completed',
                success = data.success,
                completed_at = NOW(),
                acked_at = COALESCE(dal.acked_at, NOW()),
                delivery_id = data.delivery_id,
                result_payload = data.result_payload
            FROM data
            WHERE dal.id = data.action_id
            "#,
        )
        .bind(ids)
        .bind(successes)
        .bind(deliveries)
        .bind(payloads)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
