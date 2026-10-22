const kafka = require('../config/kafka');
const logger = require('../utils/logger');

const TOPICS = [
  { topic: 'order-created', numPartitions: 3, replicationFactor: 1 },
];

async function main() {
  const admin = kafka.admin();
  try {
    await admin.connect();
    for (const t of TOPICS) {
      const created = await admin.createTopics({
        waitForLeaders: true,
        topics: [t],
      });
      logger.info(
        created
          ? `Created topic "${t.topic}" (${t.numPartitions} partitions)`
          : `Topic "${t.topic}" already exists`
      );
    }
  } catch (err) {
    logger.error('Topic creation failed:', err.message);
    process.exitCode = 1;
  } finally {
    await admin.disconnect();
  }
}

main();
