const kafka = require('../config/kafka');
const logger = require('../utils/logger');

async function main() {
  const admin = kafka.admin();
  try {
    await admin.connect();
    const cluster = await admin.describeCluster();
    const topics = await admin.listTopics();

    logger.info('Connected to Kafka');
    logger.info('Cluster id :', cluster.clusterId);
    logger.info('Brokers    :', cluster.brokers.map((b) => `${b.host}:${b.port}`).join(', '));
    logger.info('Topics     :', topics.length ? topics.join(', ') : '(none yet)');
  } catch (err) {
    logger.error('Could not connect to Kafka:', err.message);
    process.exitCode = 1;
  } finally {
    await admin.disconnect();
  }
}

main();
