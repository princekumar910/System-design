import amqp from "amqplib";

const RABBITMQ_URL =
  process.env.RABBITMQ_URL ??
  "amqp://prince:rPwCJ93Or9Ch@localhost:5672/";

let connection;
let channel;

export async function connectRabbitMQ() {
  if (channel) {
    return channel;
  }

  if (!connection) {
    console.log("Connecting to RabbitMQ...");
    connection = await amqp.connect(RABBITMQ_URL);
    console.log("RabbitMQ connected");

    connection.on("error", (err) => {
      console.error("RabbitMQ connection error:", err.message);
    });

    connection.on("close", () => {
      console.log("RabbitMQ connection closed");
      connection = null;
      channel = null;
    });
  }

  channel = await connection.createChannel();

  // Without an 'error' listener, a channel error would crash the process.
  channel.on("error", (err) => {
    console.error("RabbitMQ channel error:", err.message);
  });

  channel.on("close", () => {
    channel = null;
  });

  return channel;
}

export async function closeRabbitMQ() {
  const currentChannel = channel;
  const currentConnection = connection;

  channel = null;
  connection = null;

  // Ignore errors from a channel/connection that is already closed.
  if (currentChannel) {
    await currentChannel.close().catch(() => {});
  }

  if (currentConnection) {
    await currentConnection.close().catch(() => {});
  }
}
