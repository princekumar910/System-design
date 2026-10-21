import { setTimeout as sleep } from "node:timers/promises";
import { subscribe } from "./subscribe.js";
import { closeRabbitMQ } from "./rabbitmq.js";

const RETRY_DELAY_MS = 5000;

let shuttingDown = false;

async function shutdown() {
  shuttingDown = true;
  console.log("Shutting down subscriber...");
  await closeRabbitMQ();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

async function handleMessage(message) {
  console.log("Received:", message);
}

while (!shuttingDown) {
  try {
    const channel = await subscribe(handleMessage);
    await new Promise((resolve) => channel.once("close", resolve));
  } catch (error) {
    console.error("Subscriber error:", error.message);
    await closeRabbitMQ();
  }

  if (!shuttingDown) {
    console.log(`Reconnecting in ${RETRY_DELAY_MS / 1000}s...`);
    await sleep(RETRY_DELAY_MS);
  }
}
