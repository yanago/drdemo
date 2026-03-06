const Joi = require('joi');

const createJobSchema = Joi.object({
  name: Joi.string().min(1).max(255).required(),
  sourceTable: Joi.string().min(1).max(255).required(),
  startDate: Joi.string().isoDate().required(),
  endDate: Joi.string().isoDate().required(),
  customerId: Joi.string().uuid().optional().allow(null),
  priority: Joi.number().integer().min(1).max(10).default(5)
}).custom((value, helpers) => {
  if (new Date(value.endDate) <= new Date(value.startDate)) {
    return helpers.error('endDate must be after startDate');
  }
  return value;
});

module.exports = { createJobSchema };
