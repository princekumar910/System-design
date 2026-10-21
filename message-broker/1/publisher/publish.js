
import { getChannel } from "./rabbitmq.js";

// RabbitMQ rule: a producer NEVER writes directly into a queue.
// It always publishes to an EXCHANGE, and the exchange routes the message
// to zero or more queues using BINDINGS + a ROUTING KEY.
//
//   Producer --(publish: exchange, routingKey, body)--> Exchange --(bindings)--> Queue(s) --> Consumer
//
// Exchange types:
//   direct  -> deliver to queues whose binding key === routing key
//   fanout  -> deliver to every bound queue, routing key ignored (broadcast)
//   topic   -> pattern match on routing key ("order.*", "order.#")
//   headers -> match on message headers instead of routing key
const QUEUE = "hello2";

export async function publishMessage(message) {
  const channel = await getChannel();

  // Sends AMQP "queue.declare": creates the queue if missing, no-op if it already
  // exists with the same options. durable = queue definition survives broker restart.
  //
  // Under the hood the broker ALSO auto-binds every queue to the DEFAULT EXCHANGE
  // (named "" — a direct exchange) with binding key = queue name. That hidden
  // binding is what makes sendToQueue() below work.
  await channel.assertQueue(QUEUE, {
    durable: true,
  });

  // This is where we use an exchange, even though none is written here.
  // amqplib implements sendToQueue as:
  //
  //   channel.publish("", QUEUE, Buffer.from(message), { persistent: true });
  //                    ^   ^
  //                    |   routing key = queue name
  //                    default exchange
  //
  // Wire level: amqplib sends a "basic.publish" frame (exchange="", routingKey="hello2"),
  // then a content-header frame (properties, deliveryMode=2 because persistent: true),
  // then body frame(s) with the bytes. The broker's default exchange looks up the
  // binding "hello2" -> queue "hello2" and appends the message to that queue.
  //
  // If no queue matches the routing key, the message is silently DROPPED
  // (unless published with { mandatory: true }, which returns it to the producer).
  //
  // persistent: true -> message is written to disk; needs a durable queue to survive restarts.
  channel.sendToQueue(
    QUEUE,
    Buffer.from(message),
    { persistent: true }
  );

  console.log("Published:", message);
}
