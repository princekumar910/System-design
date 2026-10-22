const { randomUUID } = require('crypto');
const { publishOrderCreated } = require('../producers/order.producer');

class ValidationError extends Error {
  constructor(message) {
    super(message);
    this.statusCode = 400;
  }
}

function validate({ userId, items, amount }) {
  if (!userId) throw new ValidationError('userId is required');
  if (!Array.isArray(items) || items.length === 0) {
    throw new ValidationError('items must be a non-empty array');
  }
  if (typeof amount !== 'number' || amount <= 0) {
    throw new ValidationError('amount must be a positive number');
  }
}

async function createOrder(input) {
  validate(input);

  const order = {
    orderId: randomUUID(),
    userId: input.userId,
    items: input.items,
    amount: input.amount,
    status: 'CREATED',
    createdAt: new Date().toISOString(),
  };

  // In a real app you would save the order to a database here first.
  const kafkaInfo = await publishOrderCreated(order);

  return { order, kafka: kafkaInfo };
}

module.exports = { createOrder, ValidationError };
