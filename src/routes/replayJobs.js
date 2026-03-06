const express = require('express');
const router = express.Router();
const { v4: uuidv4 } = require('uuid');
const logger = require('../utils/logger');

// In-memory store for now; will be replaced with PostgreSQL
const jobs = {};

/**
 * POST /api/v1/replay/jobs
 * Create a new replay job
 */
router.post('/', async (req, res) => {
  const { name, sourceTable, startDate, endDate, customerId } = req.body;

  const job = {
    id: uuidv4(),
    name,
    sourceTable,
    startDate,
    endDate,
    customerId: customerId || null,
    status: 'pending',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };

  jobs[job.id] = job;
  logger.info('Job created', { jobId: job.id, name });

  res.status(201).json(job);
});

module.exports = { router, jobs };
