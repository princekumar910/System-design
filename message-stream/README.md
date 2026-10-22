# Kafka from Scratch (Node.js)

A hands-on learning project. **Part 1** is theory (this README). **Part 2** is implementation with Node.js, `kafkajs`, and Docker.

---

## Table of Contents

1. [Why Kafka exists](#1-why-kafka-exists)
2. [What is Kafka?](#2-what-is-kafka)
3. [Core concepts](#3-core-concepts)
4. [Topics and partitions in depth](#4-topics-and-partitions-in-depth)
5. [Producers in depth](#5-producers-in-depth)
6. [Consumers and consumer groups in depth](#6-consumers-and-consumer-groups-in-depth)
7. [Offsets and delivery guarantees](#7-offsets-and-delivery-guarantees)
8. [Brokers, clusters and replication](#8-brokers-clusters-and-replication)
9. [Retention and log compaction](#9-retention-and-log-compaction)
10. [ZooKeeper vs KRaft](#10-zookeeper-vs-kraft)
11. [Error handling: retries and Dead Letter Queue](#11-error-handling-retries-and-dead-letter-queue)
12. [Kafka vs RabbitMQ](#12-kafka-vs-rabbitmq)
13. [When to use and not use Kafka](#13-when-to-use-and-not-use-kafka)
14. [Cheat sheet: rules to remember](#14-cheat-sheet-rules-to-remember)
15. [Glossary](#15-glossary)
16. [Implementation plan (Part 2)](#16-implementation-plan-part-2)

---

## 1. Why Kafka exists

Imagine an e-commerce app. When a user places an order you must:

- Charge the payment
- Send a confirmation email
- Update inventory
- Notify analytics
- Maybe trigger shipping

### The naive approach (synchronous calls)

```
Order Service
   ├── calls Payment Service   (wait)
   ├── calls Email Service     (wait)
   ├── calls Inventory Service (wait)
   └── calls Analytics Service (wait)
```

Problems:

| Problem | Explanation |
|---|---|
| **Tight coupling** | Order Service must know every other service and its API |
| **Fragility** | If Email Service is down, the whole order fails |
| **Slow** | Total latency is the sum of all calls |
| **Hard to extend** | Adding a new service means changing and redeploying Order Service |
| **Traffic spikes** | A burst of orders overloads downstream services directly |

### The Kafka approach (event-driven)

```
                        ┌──> Payment Service
Order Service ──> [ KAFKA ] ──> Email Service
  (producer)            ├──> Inventory Service
                        └──> Analytics Service
                              (consumers)
```

The Order Service only announces: **"an order was placed"** (an *event*). It does not know or care who listens. Each interested service reads the event and reacts independently, at its own speed.

Benefits: **decoupling**, **resilience**, **scalability**, **replayability**, easy to add new consumers.

---

## 2. What is Kafka?

Apache Kafka is a **distributed, durable, append-only commit log** used for streaming events.

Break that down:

- **Distributed**: runs on many servers (brokers) working together.
- **Durable**: events are written to disk and replicated, not kept only in memory.
- **Append-only**: new events are added to the end of a log. Existing events are never modified.
- **Log**: an ordered sequence of records.

```
Log:  [0] [1] [2] [3] [4] [5] ──> new events appended here
       ▲
   oldest
```

Kafka is not just a queue. A traditional queue deletes a message once consumed. Kafka **keeps** messages for a configured time, so many consumers can read them, and you can **replay** history.

### Typical use cases

- Microservice communication (event-driven architecture)
- Log and metrics aggregation
- Real-time analytics and dashboards
- Change data capture (syncing databases)
- Activity tracking (clicks, page views)
- Stream processing and ETL pipelines

---

## 3. Core concepts

| Concept | Meaning |
|---|---|
| **Event / Message / Record** | A single piece of data: `key`, `value`, `timestamp`, `headers` |
| **Topic** | A named stream of events (like a table or folder). e.g. `orders`, `payments` |
| **Partition** | A topic is split into partitions. Each is an ordered, append-only log |
| **Offset** | Sequential ID of a message **within one partition** (0, 1, 2, ...) |
| **Producer** | Client app that writes events to a topic |
| **Consumer** | Client app that reads events from a topic |
| **Consumer Group** | A set of consumers that share the work of reading a topic |
| **Broker** | A Kafka server that stores partitions and serves clients |
| **Cluster** | A group of brokers working together |
| **Leader / Follower** | Replica roles for a partition |
| **ISR** | In-Sync Replicas: followers caught up with the leader |

### Anatomy of a message

```json
{
  "key": "user-42",
  "value": "{\"orderId\": 101, \"amount\": 500}",
  "timestamp": 1735689600000,
  "headers": { "source": "order-service" },
  "partition": 1,
  "offset": 57
}
```

- **key**: optional. Decides the partition and keeps related events ordered.
- **value**: the payload (usually JSON, Avro or Protobuf). Kafka stores bytes and does not care about the format.
- **headers**: optional metadata (trace id, content type).
- **timestamp**: when the event was created or appended.

---

## 4. Topics and partitions in depth

### Why partitions?

A single log on a single machine cannot scale. Splitting a topic into **partitions** gives:

1. **Scalability**: partitions spread across brokers, so storage and traffic are distributed.
2. **Parallelism**: multiple consumers read different partitions at the same time.

```
Topic: orders (3 partitions)

Partition 0: [0][1][2][3][4]        ──> append
Partition 1: [0][1][2][3]           ──> append
Partition 2: [0][1][2][3][4][5]     ──> append
```

### Offsets are per partition

Offset `3` in Partition 0 and offset `3` in Partition 1 are **different messages**. A message's true address is:

```
(topic, partition, offset)
```

### Ordering guarantee

> **Kafka guarantees order only within a single partition, not across the whole topic.**

If order matters for a business entity (e.g. all events of one user or one order), they must land in the **same partition**. That is what the **key** is for.

### How the partition is chosen

| Situation | Behavior |
|---|---|
| Key provided | `partition = hash(key) % numberOfPartitions`. Same key always goes to the same partition |
| No key | Spread across partitions (round-robin / sticky batching) |
| Partition set explicitly | Producer can force a specific partition (rarely needed) |

Example:

```
key = "user-42"  ──hash──> partition 1   (always)
key = "user-7"   ──hash──> partition 0   (always)
```

So all events of `user-42` are in Partition 1, **in order**.

### Choosing the number of partitions

- More partitions means more parallelism (max consumers in a group = partitions).
- Too many partitions means more overhead (file handles, rebalance time, memory).
- You can **increase** partitions later but **never decrease** them.
- Increasing partitions changes `hash(key) % N`, so **key-to-partition mapping changes** for new messages. Plan ahead.

Rule of thumb for learning: start with **3 partitions**.

---

## 5. Producers in depth

A producer sends events to a topic.

### Producer flow

```
App ──> serialize ──> choose partition ──> batch ──> compress ──> send to leader broker
                                                                        │
                                                                   ack returned
```

### The `acks` setting (durability vs speed)

| acks | Meaning | Risk | Speed |
|---|---|---|---|
| `0` | Do not wait for any confirmation | May lose data silently | Fastest |
| `1` | Wait for the leader to write | Lost if leader dies before replication | Fast |
| `all` (`-1`) | Wait for all in-sync replicas | Safest | Slowest |

Production default for important data: `acks=all` with `min.insync.replicas=2`.

### Retries and idempotence

Network errors cause retries. Retries can create **duplicates**. The **idempotent producer** (`enable.idempotence=true`) gives each message a sequence number so the broker discards duplicates caused by retries.

### Batching and compression

Producers group messages into batches (`linger.ms`, `batch.size`) and may compress them (`gzip`, `snappy`, `lz4`, `zstd`). This massively improves throughput.

---

## 6. Consumers and consumer groups in depth

### Pull model

Consumers **pull** messages from brokers at their own pace (Kafka does not push). Slow consumers do not overwhelm themselves, and they can fall behind and catch up later.

### Consumer group

A **consumer group** is a set of consumers with the same `groupId` that cooperate to read a topic.

**Rules:**

1. Each partition is assigned to **exactly one consumer within a group**.
2. One consumer can read **multiple partitions**.
3. **Different groups are independent**. Each group receives **all** messages.

### Example: topic with 3 partitions

```
Group "payment-group" (3 consumers)
   C1 <── P0
   C2 <── P1
   C3 <── P2          Work is split in parallel

Group "email-group" (1 consumer)
   C1 <── P0, P1, P2  One consumer handles everything

Both groups receive every message independently.
```

### Scaling consumers

| Partitions | Consumers in group | Result |
|---|---|---|
| 3 | 1 | 1 consumer reads all 3 |
| 3 | 2 | One reads 2 partitions, other reads 1 |
| 3 | 3 | One partition each (ideal) |
| 3 | 5 | 3 active, **2 idle** |

> **Max parallelism of a group = number of partitions.**

### Queue vs pub/sub behavior

- **Same group** = queue behavior (each message processed by one consumer in the group).
- **Different groups** = pub/sub behavior (each group gets a copy).

### Rebalancing

When a consumer **joins, leaves, or crashes**, Kafka redistributes partitions among the remaining consumers. This is a **rebalance**.

```
Before:  C1<-P0   C2<-P1   C3<-P2
C2 crashes
After:   C1<-P0,P1         C3<-P2
```

During a rebalance, consumption may pause briefly. Frequent rebalances hurt performance. Common causes: slow processing exceeding `max.poll.interval.ms`, missed heartbeats, frequent deploys.

### Heartbeats

Consumers send heartbeats to the group coordinator (a broker). If heartbeats stop (`session.timeout.ms`), the consumer is considered dead and a rebalance starts.

---

## 7. Offsets and delivery guarantees

### Committing offsets

A consumer records its progress by **committing the offset** of the last processed message. Commits are stored in an internal topic called `__consumer_offsets`. On restart, the consumer resumes from the last committed offset.

```
Partition 0: [0][1][2][3][4][5][6]
                          ▲
                committed offset = 4
        (next read starts from the following message)
```

### Where does a new group start? (`auto.offset.reset`)

| Value | Behavior when no committed offset exists |
|---|---|
| `earliest` | Start from the beginning of the log |
| `latest` | Start from new messages only |

In kafkajs this is the `fromBeginning` option on `subscribe`.

### Delivery semantics

| Semantic | How | Outcome |
|---|---|---|
| **At-most-once** | Commit **before** processing | A crash can **lose** messages. No duplicates |
| **At-least-once** | Process, **then** commit | A crash can cause **duplicates**. No loss. **Most common** |
| **Exactly-once** | Idempotent producer + transactions | No loss, no duplicates. More complex, some overhead |

### Idempotent consumers (important!)

With at-least-once delivery, the same message may arrive twice. Make handlers **idempotent**: processing twice has the same effect as once.

Examples:
- Store a processed `eventId` and skip if seen.
- Use `INSERT ... ON CONFLICT DO NOTHING`.
- Use upserts instead of blind inserts.

---

## 8. Brokers, clusters and replication

### Broker and cluster

A **broker** is one Kafka server. A **cluster** is several brokers. Partitions of a topic are distributed across brokers.

```
Cluster of 3 brokers, topic "orders" (3 partitions, replication factor 3)

Broker 1: P0 (Leader)   P1 (Follower)  P2 (Follower)
Broker 2: P0 (Follower) P1 (Leader)    P2 (Follower)
Broker 3: P0 (Follower) P1 (Follower)  P2 (Leader)
```

### Replication

Each partition is copied to multiple brokers.

- **Replication factor (RF)**: number of copies. Production standard is **3**.
- **Leader**: the one replica that handles all reads and writes for the partition.
- **Followers**: replicate from the leader.
- **ISR (In-Sync Replicas)**: the set of replicas (including leader) fully caught up.

### Failure handling

If a leader's broker dies, Kafka promotes an **in-sync follower** to be the new leader. Clients automatically discover it. No data loss if `acks=all` and enough ISR members exist.

### Key durability settings

| Setting | Meaning |
|---|---|
| `replication.factor` | Number of copies of each partition |
| `min.insync.replicas` | Minimum ISR size required for a write with `acks=all` to succeed |
| `acks=all` | Producer waits for all ISR |

Recommended: `replication.factor=3`, `min.insync.replicas=2`, `acks=all`. This tolerates 1 broker failure with no data loss.

> In local development we use **1 broker**, so replication factor must be **1**.

---

## 9. Retention and log compaction

**Messages are not deleted when consumed.** They are removed by policy.

### Time/size-based retention

| Config | Meaning | Default |
|---|---|---|
| `retention.ms` | Keep messages for this long | 7 days |
| `retention.bytes` | Max size per partition | unlimited |

Old log segments are deleted when limits are exceeded.

### Log compaction

For topics that represent **latest state per key** (e.g. user profile), Kafka can keep only the **latest message per key**.

```
Before compaction: (k1,v1) (k2,v1) (k1,v2) (k1,v3) (k2,v2)
After compaction:  (k1,v3) (k2,v2)
```

Set with `cleanup.policy=compact`.

### Why Kafka is fast

- **Sequential disk I/O** (append-only), which is very fast even on disks.
- **OS page cache** is used instead of heavy application caching.
- **Zero-copy** transfer from disk to network.
- **Batching and compression**.
- **Partitioned parallelism**.

---

## 10. ZooKeeper vs KRaft

| | ZooKeeper (old) | KRaft (modern) |
|---|---|---|
| Metadata storage | External ZooKeeper cluster | Inside Kafka itself |
| Components to run | Kafka + ZooKeeper | Kafka only |
| Consensus | ZAB protocol | Raft protocol |
| Status | Removed in Kafka 4.0 | Standard |

Metadata (topics, partitions, leaders, configs) is managed by a **controller**. In KRaft, controller nodes use the Raft protocol to agree on metadata.

We use **KRaft**, so a single Docker container is enough.

---

## 11. Error handling: retries and Dead Letter Queue

What if a consumer fails to process a message?

### Types of failures

| Type | Example | Strategy |
|---|---|---|
| **Transient** | DB timeout, network blip | Retry with backoff |
| **Permanent** | Invalid JSON, bad data (a "poison pill") | Do not retry forever. Send to DLQ |

### Retry strategies

1. **In-place retry**: retry a few times with exponential backoff.
2. **Retry topics**: publish to `orders.retry-1`, `orders.retry-2` with delays.
3. **Dead Letter Queue (DLQ)**: after max retries, publish the message to `orders.dlq` for inspection and manual handling.

```
orders ──> consumer ──fail──> retry (3x) ──fail──> orders.dlq
                  │
                success ──> commit offset
```

> Never block a partition forever on one bad message. Retry a bounded number of times, then DLQ and move on.

---

## 12. Kafka vs RabbitMQ

| | Kafka | RabbitMQ |
|---|---|---|
| Model | Distributed commit log | Message broker (queues, exchanges) |
| Delivery | Consumers **pull** | Broker **pushes** |
| After consumption | Message **retained** | Message **removed** (acked) |
| Replay | Yes | Not by default |
| Ordering | Per partition | Per queue |
| Throughput | Very high (millions/sec) | Moderate (tens of thousands/sec) |
| Routing | Simple (topic + key) | Rich (direct, topic, fanout, headers) |
| Best for | Event streaming, analytics, logs, event sourcing | Task queues, complex routing, request/reply |

---

## 13. When to use and not use Kafka

### Use Kafka when

- You need high throughput event streaming
- Multiple independent consumers need the same data
- You need replay or an event history
- You are building event-driven microservices or data pipelines

### Avoid Kafka when

- You only need a simple background job queue (consider BullMQ/RabbitMQ/SQS)
- Low message volume and you want minimal ops overhead
- You need complex per-message routing or priority queues
- You need request/response style communication

---

## 14. Cheat sheet: rules to remember

1. **Order is guaranteed only within a partition.**
2. **Same key = same partition** (as long as partition count does not change).
3. **One partition is read by only one consumer per group.**
4. **Max parallelism = number of partitions.**
5. **Different groups each receive all messages.**
6. **Messages are retained**, not deleted on read. Retention policy deletes them.
7. **Offsets are per partition**, committed per group.
8. **At-least-once is the default** mindset. Write **idempotent** consumers.
9. **`acks=all` + RF=3 + `min.insync.replicas=2`** for durability.
10. **Partitions can be increased, never decreased.**
11. Use a **DLQ** for poison messages.
12. Consumers **pull**; brokers do not push.

---

## 15. Glossary

| Term | Definition |
|---|---|
| **Event** | A record of something that happened |
| **Topic** | Named stream of events |
| **Partition** | Ordered, append-only subdivision of a topic |
| **Offset** | Position of a message in a partition |
| **Producer** | Writes events |
| **Consumer** | Reads events |
| **Consumer group** | Consumers sharing a topic's workload |
| **Broker** | A Kafka server |
| **Cluster** | Multiple brokers |
| **Leader** | Replica serving reads/writes for a partition |
| **Follower** | Replica copying the leader |
| **ISR** | In-sync replicas |
| **Rebalance** | Reassignment of partitions among consumers |
| **Commit** | Saving consumer progress (offset) |
| **Retention** | How long messages are kept |
| **Compaction** | Keeping only the latest value per key |
| **DLQ** | Dead letter queue for failed messages |
| **KRaft** | Kafka's built-in Raft-based metadata management |
| **Idempotent** | Safe to run multiple times with the same result |
| **Lag** | How far a consumer is behind the latest offset |

---

## 16. Implementation plan (Part 2)

**Stack:** Node.js, `kafkajs`, Express, Docker (Kafka in KRaft mode + Kafka UI).

**Project:** a mini order system. An Order API produces `order-created` events. Payment and Email consumers (separate groups) react independently.

### Folder structure

```
kafka-learning/
├── docker-compose.yml          # Kafka broker (KRaft) + Kafka UI
├── package.json
├── .env
├── .gitignore
├── README.md
└── src/
    ├── config/
    │   └── kafka.js            # shared Kafka client setup
    ├── admin/
    │   └── createTopics.js     # create topics with partitions
    ├── producers/
    │   └── order.producer.js   # publishes order events
    ├── consumers/
    │   ├── payment.consumer.js # group: payment-group
    │   └── email.consumer.js   # group: email-group
    ├── services/
    │   └── order.service.js    # business logic
    ├── utils/
    │   └── logger.js
    └── server.js               # Express API: POST /orders
```

### Learning steps

| Step | Topic | What you will learn |
|---|---|---|
| 1 | Docker setup and project init | Run Kafka (KRaft) locally, Kafka UI |
| 2 | Create topics | Partitions, replication factor, admin client |
| 3 | First producer and consumer | Send and receive messages |
| 4 | Keys, partitions, ordering | See same key land in same partition |
| 5 | Consumer groups and rebalancing | Run multiple consumers, kill one, watch rebalance |
| 6 | Error handling | Retries, backoff, dead letter queue |
| 7 | Express API integration | Real-world flow: `POST /orders` -> Kafka -> consumers |
| 8 | Bonus | Idempotent consumers, graceful shutdown, consumer lag |

### Prerequisites

- Node.js 18+
- Docker and Docker Compose
- Basic JavaScript (async/await)

---

**Next:** Part 2, Step 1: Docker setup and project init.
