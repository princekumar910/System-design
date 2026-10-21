
import amqp from "amqplib";

let connection;
let channel;

export async function connectRabbitMQ() {
    console.log("Connecting to RabbitMQ...");
  if (channel) {
    console.log("RabbitMQ channel already exists");
    return channel;
  }

  connection = await amqp.connect(
    "amqp://prince:rPwCJ93Or9Ch@localhost:5672/"
  );

  channel = await connection.createChannel();

  console.log("RabbitMQ connected");

  connection.on("error", (err) => {
    console.error("RabbitMQ connection error:", err.message);
  });

  connection.on("close", () => {
    console.log("RabbitMQ connection closed");
    connection = null;
    channel = null;
  });

  return channel;
}

export function getChannel() {
  if (!channel) {
    throw new Error("RabbitMQ is not connected");
  }

  return channel;
}

export async function closeRabbitMQ() {
  const currentChannel = channel;
  const currentConnection = connection;

  channel = null;
  connection = null;

  if (currentChannel) {
    await currentChannel.close();
  }

  if (currentConnection) {
    await currentConnection.close();
  }
}
