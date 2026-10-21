
import {
  connectRabbitMQ,
  closeRabbitMQ,
} from "./rabbitmq.js";

import { publishMessage } from "./publish.js";

async function main() {
  try {
    // Connect once during application startup
    await connectRabbitMQ();

    // Reuse the same connection and channel

    for (let i = 0; i < 1000000; i++) {
      await publishMessage(`Message ${i + 1}`);
    }

    console.log("All messages submitted");
  } catch (error) {
    console.error("Application error:", error);
    process.exitCode = 1;
  } finally {
    // Close only when the application is finished
    await closeRabbitMQ();
  }
}

main();

