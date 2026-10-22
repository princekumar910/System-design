require('dotenv').config();
const express = require('express');
const logger = require('./utils/logger');
const { connectProducer, disconnectProducer } = require('./producers/order.producer');
const { createOrder } = require('./services/order.service');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.post('/orders', async (req, res, next) => {
  try {
    const { order, kafka } = await createOrder(req.body);
    // 202 Accepted: the order was accepted and the event is published.
    // Payment, email, etc. happen later in the consumers.
    res.status(202).json({ message: 'Order accepted', order, kafka });
  } catch (err) {
    next(err);
  }
});

// Central error handler
app.use((err, req, res, next) => {
  const status = err.statusCode || 500;
  if (status >= 500) logger.error(err);
  res.status(status).json({ error: err.message });
});

async function start() {
  await connectProducer();
  const server = app.listen(PORT, () => logger.info(`API listening on http://localhost:${PORT}`));

  // Graceful shutdown: stop HTTP first, then flush and close Kafka
  const shutdown = async (signal) => {
    logger.info(`${signal} received, shutting down...`);
    server.close(async () => {
      await disconnectProducer();
      process.exit(0);
    });
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

start().catch((err) => {
  logger.error('Failed to start:', err.message);
  process.exit(1);
});
