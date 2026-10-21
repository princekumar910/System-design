# RabbitMQ in Node.js: Channels, Queues, Exchanges and Consumers

A beginner-friendly guide using the `amqplib` package.

```bash
npm install amqplib
```

---

## Table of Contents

1. [Connection vs Channel](#1-connection-vs-channel)
2. [Why use multiple channels](#2-why-use-multiple-channels)
3. [`assertQueue`](#3-assertqueue)
4. [`createChannel()` arguments](#4-createchannel-arguments)
5. [Exchanges](#5-exchanges)
6. [Many consumers on one queue (competing consumers)](#6-many-consumers-on-one-queue-competing-consumers)
7. [Every consumer must receive every message (fanout)](#7-every-consumer-must-receive-every-message-fanout)
8. [Cheat sheet](#8-cheat-sheet)

---

## 1. Connection vs Channel

- A **connection** is one real TCP connection to the RabbitMQ broker. It is expensive to open.
- A **channel** is a lightweight, virtual lane inside that connection. It is cheap to open.

```
Application
   ├── Channel 1 ─┐
   ├── Channel 2 ─┼── ONE TCP Connection ──► RabbitMQ Broker
   └── Channel 3 ─┘
```

All real work (declare queues, publish, consume, ack) happens on a **channel**.

### MongoDB analogy

| MongoDB | RabbitMQ | Same idea? |
|---|---|---|
| Connection | Connection | Yes, the TCP link |
| Collection | Queue | Yes, a named thing you use by name |
| (nothing) | **Channel** | A lane with its own settings and error state |

One channel **can** talk to many queues, just like one MongoDB connection can use many collections. Channels are not for reaching more queues. They give you **independent sessions** over one connection (similar to HTTP/2 streams).

---

## 2. Why use multiple channels

These things are **per channel**:

1. Errors (an error closes only that channel)
2. Prefetch (QoS)
3. Publisher confirms
4. Transactions
5. Unacknowledged message tracking

### Problem: an error kills the whole channel

```js
await channel.assertQueue('orders', { durable: true });
channel.consume('orders', handleOrder);
channel.consume('emails', handleEmail);

// Bug elsewhere: same queue, different options
await channel.assertQueue('orders', { durable: false });
// PRECONDITION_FAILED -> channel closes -> BOTH consumers stop
```

With separate channels only one lane dies.

### Problem: prefetch is shared

```js
const slowCh = await connection.createChannel();
await slowCh.prefetch(1);
slowCh.consume('slow-reports', handleSlow);

const fastCh = await connection.createChannel();
await fastCh.prefetch(100);
fastCh.consume('fast-logs', handleFast);
```

### Recommended pattern

```js
const amqp = require('amqplib');

async function main() {
  // 1 connection per app
  const connection = await amqp.connect('amqp://localhost');

  // 1 channel for publishing (with confirms)
  const publisher = await connection.createConfirmChannel();

  // 1 channel per consumer
  const orderConsumer = await connection.createChannel();
  await orderConsumer.prefetch(5);
  await orderConsumer.assertQueue('orders');
  orderConsumer.consume('orders', (msg) => {
    console.log('processing', msg.content.toString());
    orderConsumer.ack(msg);
  });

  publisher.sendToQueue('orders', Buffer.from('order #42'));
}

main();
```

**Rule of thumb:** one connection per app, one channel for publishing, one channel per consumer.

---

## 3. `assertQueue`

```js
await channel.assertQueue('orders');
```

Meaning: **"Make sure this queue exists. If it does not, create it."**

| Situation | Result |
|---|---|
| Queue does not exist | Created |
| Exists with same settings | Nothing happens, continues |
| Exists with different settings | Error `PRECONDITION_FAILED`, channel closes |

It is **idempotent**: calling it many times still gives one queue.

```
Your code                          RabbitMQ
   │ assertQueue('orders', opts)      │
   ├─────────────────────────────────►│
   │                                  ├── No  → create it
   │                                  ├── Yes, same options → OK
   │                                  └── Yes, different options → ERROR
   │◄─────────────────────────────────┤
   │ { queue, messageCount, consumerCount }
```

### Options

```js
await channel.assertQueue('orders', {
  durable: true,       // survives broker restart
  exclusive: false,    // true = only this connection, deleted when it closes
  autoDelete: false,   // true = deleted when last consumer disconnects
  arguments: {
    'x-message-ttl': 60000,
    'x-dead-letter-exchange': 'dlx',
    'x-max-length': 1000
  }
});
```

### Return value

```js
const info = await channel.assertQueue('orders');
// { queue: 'orders', messageCount: 5, consumerCount: 2 }
```

### Related methods

| Method | What it does |
|---|---|
| `assertQueue(name, opts)` | Create if missing, verify if present |
| `checkQueue(name)` | Only check; error if missing |
| `deleteQueue(name)` | Delete the queue |
| `purgeQueue(name)` | Remove all messages, keep the queue |

You **cannot change** a queue's properties after creation. Delete and recreate it, or use a new name.

---

## 4. `createChannel()` arguments

`connection.createChannel()` takes **no arguments**. It returns a Promise of a channel.

```js
const channel = await connection.createChannel();
await channel.prefetch(10);   // settings are applied after creation
```

| Function | Arguments | Returns |
|---|---|---|
| `amqp.connect(url or options)` | URL string or options object | Promise of `Connection` |
| `connection.createChannel()` | none | Promise of `Channel` |
| `connection.createConfirmChannel()` | none | Promise of `ConfirmChannel` |

Connection options:

```js
const connection = await amqp.connect({
  protocol: 'amqp',
  hostname: 'localhost',
  port: 5672,
  username: 'guest',
  password: 'guest',
  vhost: '/',
  heartbeat: 30
});
```

Confirm channel example:

```js
const confirmChannel = await connection.createConfirmChannel();
confirmChannel.sendToQueue('orders', Buffer.from('hi'), {}, (err, ok) => {
  if (err) console.log('message was nacked');
  else console.log('broker confirmed the message');
});
```

---

## 5. Exchanges

An **exchange** receives messages from the producer and **decides which queue(s)** they go to.

```
Producer ──► Exchange ──► Queue(s) ──► Consumer
```

Analogy: the exchange is a post office, the routing key is the address, a binding is a delivery rule, and the queue is a mailbox.

| Piece | Role |
|---|---|
| Exchange | Receives messages, decides where they go |
| Binding | Rule linking an exchange to a queue |
| Queue | Stores messages until a consumer reads them |

### `sendToQueue` uses the default exchange

```js
channel.sendToQueue('orders', Buffer.from('hello'));
// is the same as
channel.publish('', 'orders', Buffer.from('hello'));
//              ↑ default exchange, routing key = queue name
```

### Exchange types

**Direct** (exact routing key match)

```js
await channel.assertExchange('app', 'direct');
await channel.bindQueue('orders', 'app', 'order');
await channel.bindQueue('emails', 'app', 'email');

channel.publish('app', 'order', Buffer.from('order #1'));  // → orders only
```

**Fanout** (copy to all bound queues, key ignored)

```js
await channel.assertExchange('broadcast', 'fanout');
await channel.bindQueue('orders', 'broadcast', '');
await channel.bindQueue('emails', 'broadcast', '');

channel.publish('broadcast', '', Buffer.from('hello all'));
```

**Topic** (pattern matching: `*` = one word, `#` = zero or more words)

```js
await channel.assertExchange('events', 'topic');
await channel.bindQueue('orders', 'events', 'order.*');
await channel.bindQueue('logs', 'events', '#');

channel.publish('events', 'order.created', Buffer.from('new order'));
// → orders (matches order.*) and logs (matches #)
```

**Headers** routes on message headers instead of routing key (rarely used).

### Full example

```js
const amqp = require('amqplib');

async function main() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();

  await channel.assertExchange('events', 'topic', { durable: true });

  await channel.assertQueue('orders');
  await channel.assertQueue('emails');

  await channel.bindQueue('orders', 'events', 'order.*');
  await channel.bindQueue('emails', 'events', 'user.*');

  channel.consume('orders', (msg) => {
    console.log('order queue got:', msg.content.toString());
    channel.ack(msg);
  });

  channel.publish('events', 'order.created', Buffer.from('order #1'));
}

main();
```

### Why use exchanges

- **Decoupling:** the producer does not need to know which queues exist.
- **One message, many queues:** several services can receive the same event.
- **Flexible routing:** add a queue and binding without changing producer code.

---

## 6. Many consumers on one queue (competing consumers)

When several consumers attach to **one queue**, RabbitMQ **splits** the messages between them. Each message goes to **exactly one** consumer.

```
                         ┌──► Consumer A  gets msg 1, 4, 7
Queue: [1][2][3][4][5][6][7]──► Consumer B  gets msg 2, 5, 8
                         └──► Consumer C  gets msg 3, 6, 9
```

Use this to **scale work**: if one consumer is too slow, add more.

### Round-robin and prefetch

By default messages are handed out in turn. If jobs take different times, this is unfair. Fix it with prefetch:

```js
await channel.prefetch(1);   // "don't give me a new message until I ack the last one"
```

### Acknowledgements protect against crashes

```js
channel.consume('orders', async (msg) => {
  await processOrder(msg);
  channel.ack(msg);          // tell RabbitMQ it can delete the message
}, { noAck: false });
```

```
1. Queue delivers msg to Consumer A
2. Message stays in the queue marked "unacked"
3a. A finishes → ack → message deleted
3b. A crashes before ack → RabbitMQ puts it back → B or C gets it
```

### Handling failures

```js
channel.consume('orders', async (msg) => {
  try {
    await processOrder(msg);
    channel.ack(msg);
  } catch (err) {
    channel.nack(msg, false, false);  // requeue = false → dead-letter queue (if set)
  }
});
```

Careful: `requeue = true` on a message that always fails causes an infinite retry loop. Prefer a dead-letter queue.

### Empty queue

If the queue is empty, consumers just **wait**. No polling is needed. When a message arrives, RabbitMQ pushes it to the next free consumer.

### Example

**worker.js** (run in 3 terminals):

```js
const amqp = require('amqplib');

async function main() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();

  await channel.assertQueue('orders');
  await channel.prefetch(1);

  console.log('Worker waiting for messages...');

  channel.consume('orders', async (msg) => {
    const text = msg.content.toString();
    console.log('Processing:', text);
    await new Promise(r => setTimeout(r, 2000));
    channel.ack(msg);
    console.log('Done:', text);
  });
}

main();
```

**producer.js**:

```js
const amqp = require('amqplib');

async function main() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();
  await channel.assertQueue('orders');

  for (let i = 1; i <= 9; i++) {
    channel.sendToQueue('orders', Buffer.from(`order #${i}`));
  }

  setTimeout(() => connection.close(), 500);
}

main();
```

---

## 7. Every consumer must receive every message (fanout)

If **all** consumers must read **every** message, do **not** share a queue (the first ack would delete the message). Instead give **each consumer its own queue** and let a **fanout exchange copy** the message into each one.

```
                        ┌──► Queue A ──► Consumer A
Producer ──► [fanout] ──┼──► Queue B ──► Consumer B
                        └──► Queue C ──► Consumer C
```

- The exchange copies the message into every bound queue.
- When A acks, only A's copy is deleted. B's and C's copies stay until they ack.
- RabbitMQ tracks what each consumer has read, per queue, via acks. You track nothing yourself.

```
Queue A: [m1][m2]   ← A has acked nothing yet
Queue B: [m2]       ← B already acked m1
Queue C: []         ← C acked both
```

### publisher.js

```js
const amqp = require('amqplib');

async function main() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();

  await channel.assertExchange('broadcast', 'fanout', { durable: true });

  channel.publish('broadcast', '', Buffer.from('hello everyone'));
  console.log('sent');

  setTimeout(() => connection.close(), 500);
}

main();
```

### subscriber.js (run 3 times: A, B, C)

```js
const amqp = require('amqplib');

async function main() {
  const name = process.argv[2];   // node subscriber.js A

  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();

  await channel.assertExchange('broadcast', 'fanout', { durable: true });

  // Each consumer gets its OWN queue
  const q = await channel.assertQueue(`queue_${name}`, { durable: true });

  // Connect that queue to the exchange
  await channel.bindQueue(q.queue, 'broadcast', '');

  console.log(`Consumer ${name} waiting...`);

  channel.consume(q.queue, (msg) => {
    console.log(`Consumer ${name} got:`, msg.content.toString());
    channel.ack(msg);
  });
}

main();
```

Run:

```bash
node subscriber.js A
node subscriber.js B
node subscriber.js C
node publisher.js
```

### Named vs temporary queues

| Situation | Queue type |
|---|---|
| Consumer must not miss messages while offline | Named, durable queue (`queue_A`) |
| Consumer only cares while running | Temporary: `assertQueue('', { exclusive: true })` |

```js
const q = await channel.assertQueue('', { exclusive: true });
await channel.bindQueue(q.queue, 'broadcast', '');
```

### Bind before you publish

A fanout exchange only copies into queues that are **already bound**. If you publish before any queue is bound, the message is dropped.

```
Publish first, subscribers later  →  message lost
Subscribers first, then publish   →  everyone receives
```

Use named durable queues and start each subscriber at least once so the queue and binding exist.

### Combining both patterns

```
                    ┌──► Queue: email     ──► Email instance 1 ┐ share the work
Producer ──► [fanout]│                    ──► Email instance 2 ┘
                    └──► Queue: analytics ──► Analytics inst 1 ┐ share the work
                                          ──► Analytics inst 2 ┘
```

- **Between services:** every service gets every message (one queue per service).
- **Inside a service:** instances share the work (many consumers on that queue).

---

## 8. Cheat sheet

| Concept | Meaning |
|---|---|
| Connection | One TCP link to the broker (one per app) |
| Channel | Lightweight lane inside a connection |
| Queue | Named buffer that stores messages |
| Exchange | Receives messages and routes them to queues |
| Binding | Rule linking an exchange to a queue |
| `assertQueue` | Create the queue if missing, verify if present |
| `ack` | "I finished this message, delete it" |
| `prefetch(n)` | Max unacked messages per consumer |

| Goal | Setup |
|---|---|
| One message handled by only one worker | One queue, many consumers |
| Fair distribution among workers | `channel.prefetch(1)` |
| Do not lose messages if a worker crashes | Manual `ack` |
| Every consumer receives every message | Fanout exchange + one queue per consumer |
| Route by pattern | Topic exchange |
| Queue is empty | Consumers wait, no polling needed |

**Remember:** a queue is *consumed* (shared), an exchange is *copied* (broadcast).