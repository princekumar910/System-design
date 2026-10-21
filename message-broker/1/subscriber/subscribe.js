import { connectRabbitMQ } from "./rabbitmq.js";

const QUEUE = "hello2";

export async function subscribe(handler) {
  const channel = await connectRabbitMQ();

  // Must match the publisher's queue options, otherwise RabbitMQ rejects the declare.
  // Consumers never talk to exchanges — they only read from queues. The queue is
  // already auto-bound to the default exchange ("") with key "hello2", so whatever
  // the publisher sends with sendToQueue("hello2") lands here.
  await channel.assertQueue(QUEUE, { durable: true });

  // Deliver one unacked message at a time to this consumer.
  await channel.prefetch(1);

  // Sends "basic.consume". The broker then PUSHES messages from the queue
  // as "basic.deliver" frames; ack/nack tell it to delete or drop/requeue them.
  await channel.consume(
    QUEUE,
    async (msg) => {
      if (!msg) {
        return;
      }

      try {
        await handler(msg.content.toString());
        channel.ack(msg);
      } catch (error) {
        console.error("Failed to process message:", error);
        channel.nack(msg, false, false);
      }
    },
    { noAck: false }
  );

  console.log(`Waiting for messages on "${QUEUE}". Press CTRL+C to exit.`);

  return channel;
}
