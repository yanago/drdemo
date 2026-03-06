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

/**
 * GET /api/v1/replay/jobs
 * List jobs with optional filtering by status
 */
router.get('/', (req, res) => {
  const { status, limit = 50, offset = 0 } = req.query;

  let list = Object.values(jobs);

  if (status) {
    list = list.filter(j => j.status === status);
  }

  // Sort by createdAt desc
  list.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

  const total = list.length;
  const page = list.slice(Number(offset), Number(offset) + Number(limit));

  res.json({
    total,
    limit: Number(limit),
    offset: Number(offset),
    jobs: page
  });
});

/**
 * GET /api/v1/replay/jobs/:id
 */
router.get('/:id', (req, res) => {
  const job = jobs[req.params.id];
  if (!job) return res.status(404).json({ error: 'Job not found' });
  res.json(job);
});

module.exports = { router, jobs };
