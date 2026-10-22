const kafka = require('../config/kafka');
const logger = require('../utils/logger');

const TOPIC = 'order-created';

const producer = kafka.producer({
  idempotent: true,             // retries will not create duplicates
  maxInFlightRequests: 1,       // required by idempotent mode in kafkajs
  allowAutoTopicCreation: false // topics must be created on purpose (npm run topics)
});

async function connectProducer() {
  await producer.connect();
  logger.info('Producer connected');
}

async function disconnectProducer() {
  await producer.disconnect();
  logger.info('Producer disconnected');
}

/**
 * Publish an "order created" event.
 * Key = userId, so all orders of one user go to the same partition (ordered).
 */
async function publishOrderCreated(order) {
  const result = await producer.send({
    topic: TOPIC,
    acks: -1, // wait for all in-sync replicas
    messages: [
      {
        key: String(order.userId),
        value: JSON.stringify(order),
        headers: { source: 'order-service', eventType: 'order.created' },
      },
    ],
  });

  const { partition, baseOffset } = result[0];
  logger.info(`Sent order ${order.orderId} -> partition ${partition}, offset ${baseOffset}`);
  return { topic: TOPIC, partition, offset: baseOffset };
}

module.exports = { connectProducer, disconnectProducer, publishOrderCreated };

// Quick manual test: npm run producer
if (require.main === module) {
  (async () => {
    await connectProducer();
    await publishOrderCreated({
      orderId: 'test-1',
      userId: 'user-42',
      amount: 500,
      items: [{ sku: 'demo', qty: 1 }],
    });
    await disconnectProducer();
  })().catch((err) => {
    logger.error(err.message);
    process.exit(1);
  });
}
