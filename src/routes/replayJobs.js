const express = require('express');
const router = express.Router();
const { v4: uuidv4 } = require('uuid');
const logger = require('../utils/logger');
const { validate } = require('../middleware/validate');
const { createJobSchema } = require('../validation/jobSchema');

// In-memory store; will be replaced with PostgreSQL
const jobs = {};

/**
 * POST /api/v1/replay/jobs
 */
router.post('/', validate(createJobSchema), async (req, res) => {
  const { name, sourceTable, startDate, endDate, customerId, priority } = req.body;

  const job = {
    id: uuidv4(),
    name,
    sourceTable,
    startDate,
    endDate,
    customerId: customerId || null,
    priority,
    status: 'pending',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };

  jobs[job.id] = job;
  logger.info('Job created', { jobId: job.id, name });

  res.status(201).json(job);
});

module.exports = { router, jobs };
