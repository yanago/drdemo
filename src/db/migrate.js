require('dotenv').config();
const pool = require('./pool');
const logger = require('../utils/logger');

const MIGRATIONS = [
  {
    id: 1,
    name: 'create_jobs_table',
    sql: `
      CREATE TABLE IF NOT EXISTS replay_jobs (
        id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name        VARCHAR(255) NOT NULL,
        source_table VARCHAR(255) NOT NULL,
        start_date  DATE NOT NULL,
        end_date    DATE NOT NULL,
        customer_id UUID,
        priority    INTEGER NOT NULL DEFAULT 5,
        status      VARCHAR(50) NOT NULL DEFAULT 'pending',
        error_msg   TEXT,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_replay_jobs_status ON replay_jobs(status);
      CREATE INDEX IF NOT EXISTS idx_replay_jobs_customer ON replay_jobs(customer_id);
    `
  },
  {
    id: 2,
    name: 'create_partitions_table',
    sql: `
      CREATE TABLE IF NOT EXISTS job_partitions (
        id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        job_id      UUID NOT NULL REFERENCES replay_jobs(id) ON DELETE CASCADE,
        partition_key VARCHAR(255) NOT NULL,
        date_part   DATE NOT NULL,
        customer_id UUID,
        row_count   BIGINT,
        size_bytes  BIGINT,
        status      VARCHAR(50) NOT NULL DEFAULT 'pending',
        worker_id   VARCHAR(255),
        started_at  TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        error_msg   TEXT,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_partitions_job_id ON job_partitions(job_id);
      CREATE INDEX IF NOT EXISTS idx_partitions_status ON job_partitions(status);
    `
  },
  {
    id: 3,
    name: 'create_migrations_tracking',
    sql: `
      CREATE TABLE IF NOT EXISTS schema_migrations (
        id          INTEGER PRIMARY KEY,
        name        VARCHAR(255) NOT NULL,
        applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `
  }
];

async function migrate() {
  const client = await pool.connect();
  try {
    // Ensure migrations table exists first
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    const applied = await client.query('SELECT id FROM schema_migrations');
    const appliedIds = new Set(applied.rows.map(r => r.id));

    for (const migration of MIGRATIONS) {
      if (appliedIds.has(migration.id)) {
        logger.info(`Skipping migration ${migration.id}: ${migration.name}`);
        continue;
      }
      logger.info(`Applying migration ${migration.id}: ${migration.name}`);
      await client.query('BEGIN');
      await client.query(migration.sql);
      await client.query(
        'INSERT INTO schema_migrations(id, name) VALUES($1, $2)',
        [migration.id, migration.name]
      );
      await client.query('COMMIT');
      logger.info(`Migration ${migration.id} applied`);
    }

    logger.info('All migrations complete');
  } catch (err) {
    await client.query('ROLLBACK');
    logger.error('Migration failed', { error: err.message });
    throw err;
  } finally {
    client.release();
    await pool.end();
  }
}

migrate().catch(err => {
  logger.error('Fatal migration error', { error: err.message });
  process.exit(1);
});
