const express = require('express');
const router = express.Router();
const { v4: uuidv4 } = require('uuid');
const logger = require('../utils/logger');
const { validate } = require('../middleware/validate');
const { createJobSchema } = require('../validation/jobSchema');
const JobStore = require('../db/jobStore');

/**
 * POST /api/v1/replay/jobs
 */
router.post('/', validate(createJobSchema), async (req, res, next) => {
  try {
    const { name, sourceTable, startDate, endDate, customerId, priority } = req.body;
    const job = await JobStore.create({
      id: uuidv4(),
      name, sourceTable, startDate, endDate,
      customerId: customerId || null,
      priority
    });
    logger.info('Job created', { jobId: job.id });
    res.status(201).json(job);
  } catch (err) {
    next(err);
  }
});

/**
 * GET /api/v1/replay/jobs
 */
router.get('/', async (req, res, next) => {
  try {
    const { status, limit = 50, offset = 0 } = req.query;
    const jobs = await JobStore.list({ status, limit: Number(limit), offset: Number(offset) });
    res.json({ jobs });
  } catch (err) {
    next(err);
  }
});

/**
 * GET /api/v1/replay/jobs/:id
 */
router.get('/:id', async (req, res, next) => {
  try {
    const job = await JobStore.findById(req.params.id);
    if (!job) return res.status(404).json({ error: 'Job not found' });
    res.json(job);
  } catch (err) {
    next(err);
  }
});

module.exports = { router };
