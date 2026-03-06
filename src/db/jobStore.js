const pool = require('./pool');

const JobStore = {
  async create(jobData) {
    const { id, name, sourceTable, startDate, endDate, customerId, priority } = jobData;
    const result = await pool.query(
      `INSERT INTO replay_jobs
        (id, name, source_table, start_date, end_date, customer_id, priority, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending')
       RETURNING *`,
      [id, name, sourceTable, startDate, endDate, customerId, priority]
    );
    return normalize(result.rows[0]);
  },

  async findById(id) {
    const result = await pool.query(
      'SELECT * FROM replay_jobs WHERE id = $1',
      [id]
    );
    return result.rows[0] ? normalize(result.rows[0]) : null;
  },

  async list({ status, limit = 50, offset = 0 } = {}) {
    let query = 'SELECT * FROM replay_jobs';
    const params = [];
    if (status) {
      params.push(status);
      query += ` WHERE status = $${params.length}`;
    }
    query += ' ORDER BY created_at DESC';
    params.push(limit);
    query += ` LIMIT $${params.length}`;
    params.push(offset);
    query += ` OFFSET $${params.length}`;

    const result = await pool.query(query, params);
    return result.rows.map(normalize);
  },

  async countByStatus(status) {
    const result = await pool.query(
      'SELECT COUNT(*) FROM replay_jobs WHERE status = $1',
      [status]
    );
    return parseInt(result.rows[0].count);
  },

  async updateStatus(id, status, errorMsg = null) {
    const result = await pool.query(
      `UPDATE replay_jobs
       SET status = $1, error_msg = $2, updated_at = NOW()
       WHERE id = $3
       RETURNING *`,
      [status, errorMsg, id]
    );
    return result.rows[0] ? normalize(result.rows[0]) : null;
  }
};

function normalize(row) {
  return {
    id: row.id,
    name: row.name,
    sourceTable: row.source_table,
    startDate: row.start_date,
    endDate: row.end_date,
    customerId: row.customer_id,
    priority: row.priority,
    status: row.status,
    errorMsg: row.error_msg,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}

module.exports = JobStore;
