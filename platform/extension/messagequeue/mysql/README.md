# SQL Queue Implementation

MySQL-based distributed queue with partition leasing, delivery state tracking, and at-least-once delivery.

For design rationale, guarantees, and trade-offs, see the [RFC](../../../../doc/rfc/sql-queue-rfc.md).

## Quick Start

```go
import (
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
    extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
    entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
)

// Setup
db, _ := sql.Open("mysql", "user:pass@tcp(localhost:3306)/db")
q, _ := queueMySQL.NewQueue(queueMySQL.Params{
    DB:           db,
    Logger:       logger,
    MetricsScope: metrics,
})
defer q.Close()

// Publish
msg := entityqueue.NewMessage("msg-id", []byte(`{"data": "value"}`), "repo-123", nil)
q.Publisher().Publish(ctx, "land_events", msg)

// Subscribe with per-subscription config
subConfig := extqueue.DefaultSubscriptionConfig("worker-1", "orchestrator")
deliveryCh, _ := q.Subscriber().Subscribe(ctx, "land_events", subConfig)
for delivery := range deliveryCh {
    if err := process(delivery.Message()); err != nil {
        delivery.Nack(ctx)  // Retry
        continue
    }
    delivery.Ack(ctx)
}
```

## Configuration

Per-subscription configuration enables different settings for each topic:

```go
import extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"

subConfig := extqueue.DefaultSubscriptionConfig("worker-1", "consumer-group")

subConfig.PollIntervalMs = 50                         // Poll frequency (milliseconds)
subConfig.BatchSize = 20                              // Messages per poll
subConfig.VisibilityTimeoutMs = 60000                 // Retry delay (milliseconds)
subConfig.LeaseRenewalIntervalMs = 10000              // Lease renewal frequency (milliseconds)
subConfig.LeaseDurationMs = 30000                     // Lease timeout (milliseconds)
subConfig.Retry.MaxAttempts = 3                       // Max retries before DLQ
subConfig.Retry.InitialBackoffMs = 1000               // Initial retry backoff (milliseconds)
subConfig.Retry.MaxBackoffMs = 30000                  // Max retry backoff (milliseconds)
subConfig.Retry.BackoffMultiplier = 2.0               // Backoff multiplier for exponential backoff
subConfig.DLQ.Enabled = true                          // Enable dead letter queue
subConfig.DLQ.TopicSuffix = "_dlq"                    // DLQ topic suffix
```

**Key Configuration Fields:**

| Field | Description |
|-------|-------------|
| `SubscriberName` | Unique worker identifier for partition leasing (e.g., hostname, pod name) |
| `ConsumerGroup` | Consumer group for independent offset tracking |
| `PollIntervalMs` | How often to poll for new messages |
| `PartitionDiscoveryIntervalMs` | How often to discover partitions, attempt lease acquisition, and reconcile workers |
| `BatchSize` | Maximum messages to fetch per poll. Set to `1` for strict serialization |
| `VisibilityTimeoutMs` | How long messages are invisible after fetch. Must exceed max processing time for `BatchSize=1` |
| `LeaseRenewalIntervalMs` | How often to renew partition leases |
| `LeaseDurationMs` | How long leases remain valid without renewal |
| `Retry.MaxAttempts` | Maximum processing attempts before DLQ; zero retries indefinitely |
| `DLQ.TopicSuffix` | Suffix appended to topic name for DLQ (e.g., `"orders"` → `"orders_dlq"`) |

## Package Layout

```
platform/extension/messagequeue/mysql/
├── sql.go                          # NewQueue constructor, wires stores → publisher/subscriber
├── stores.go                       # Internal store interfaces (messageStore, offsetStore, etc.)
├── message_store.go                # queue_messages table operations (immutable log)
├── delivery_state_store.go         # queue_delivery_state table operations (per-consumer-group)
├── offset_store.go                 # queue_offsets table operations (watermark tracking)
├── partition_lease_store.go        # queue_partition_leases table operations
├── lease_guard.go                  # lease_generation fencing guard shared by owner-scoped writes
├── subscriber_heartbeat_store.go   # queue_subscriber_heartbeats table operations
├── publisher.go                    # Publisher implementation
├── subscriber.go                   # Subscriber, delivery, goroutine management
├── constants.go                    # Log key constants
├── errors.go                       # Error types
├── schema/                         # SQL schema files (one per table)
│   ├── queue_messages.sql
│   ├── queue_delivery_state.sql
│   ├── queue_offsets.sql
│   ├── queue_partition_leases.sql
│   └── queue_subscriber_heartbeats.sql
└── ctl/                            # Admin CLI (see ctl/README.md)
```

## Internal Architecture

### Database Tables

| Table | Purpose | Scoped To |
|-------|---------|-----------|
| `queue_messages` | Immutable append-only message log | `(topic, partition_key)` — shared across consumer groups |
| `queue_delivery_state` | Visibility, ack state, retry count, postponed flag | `(consumer_group, topic, partition_key, offset)` |
| `queue_offsets` | Contiguous acked watermark | `(consumer_group, topic, partition_key)` |
| `queue_partition_leases` | Partition lease coordination plus the `lease_generation` fencing epoch | `(consumer_group, topic, partition_key)` |
| `queue_subscriber_heartbeats` | Active subscriber tracking | `(consumer_group, topic, subscriber_name)` |

`queue_delivery_state` has a `postponed BOOLEAN NOT NULL DEFAULT FALSE` column that supports `Delivery.Postpone`. `MarkPostponed` sets `invisible_until = now + delay`, resets `retry_count` to 0, and sets the flag. While the flag is set and the row is invisible, the poll loop treats the message as a **barrier** — it stops scanning the partition instead of skipping past it (nacked rows keep skip-and-continue semantics, so a failed message never halts its partition). On the next `MarkDelivered` the flag is consumed: the `retry_count` increment is skipped and the flag cleared, so a postponed redelivery restarts as attempt 1 and only consecutive real failures count toward `Retry.MaxAttempts`. Default FALSE keeps existing rows back-compatible.

See `schema/` for full SQL definitions. See the [RFC](../../../../doc/rfc/sql-queue-rfc.md#database-schema) for field-level documentation.

### Store Architecture

Each table is backed by an internal store interface defined in `stores.go`. Stores:
- Query only their own table (no cross-table JOINs)
- Return errors via `fmt.Errorf` (no logging, no error classification)
- Use `metrics.Begin`/`Complete` for latency and success/failure tracking

The subscriber layer orchestrates cross-store operations (e.g., watermark advancement queries both `messageStore` and `deliveryStateStore`) and owns all logging and error classification.

### Goroutine Model

Each subscription has a **supervisor goroutine** (`managePartitions`) that discovers partitions, acquires leases, sends heartbeats, rebalances, and reconciles per-partition worker goroutines.

```
Subscribe()
  └── managePartitions (supervisor)       ← tracked by sub.wg
        ├── partitionWorker("part-1")     ← tracked by sub.workerWg
        ├── partitionWorker("part-2")
        └── partitionWorker("part-3")
```

Each partition worker runs independently — polls the DB on a ticker, checks deliverability via `GetDeliveryState` per message, and sends deliveries to the shared channel. A slow or blocked partition does not affect other partitions.

### Lease Fencing

Every partition lease carries a `lease_generation` fencing epoch: 1 on first acquisition, incremented by one when a stale lease is stolen by a different subscriber, and unchanged by renewal by the current owner. A partition worker captures its generation when it starts, and every owner-scoped write it makes — visibility claim, ack/nack/postpone, visibility extension, watermark advance, lease renewal and release, and the dead-letter move — is conditioned on `(leased_by, lease_generation)`. A write whose lease has moved matches no rows; a follow-up existence read distinguishes a legitimate no-op while still owning (e.g. re-acking) from a lost lease, and the latter returns `*ErrLeaseFenced`. A fenced worker exits without releasing the lease, the stale delivery stays governed by its visibility timeout, and the current owner redelivers it — a worker that lost a partition fails closed even with a delivery still in flight.

The dead-letter move and its ack share one transaction (`MoveToDLQAndAck`): the row is copied to the DLQ topic, deleted from the main topic, and the group's delivery state is marked acked, with the ack guarded by the same fencing token. A crash leaves either all three changes or none, so the move can never lose a message.

### Shutdown Sequence

Shutdown is an ordered drain rather than an immediate cancel. `Close()` and an explicit `ShutdownSubscription(ctx, topic, group)` (the `extension/messagequeue.DrainingSubscriber` interface) trigger the same idempotent run, and every concurrent caller joins it:

1. Stop claiming — signal drain start and cancel the subscription context and every partition worker, so fetching, lease renewal, and rebalance all stop.
2. Wait for in-flight deliveries to settle (ack/nack/postpone/reject) or until their visibility deadlines safely pass, bounded by the caller's deadline.
3. Wait briefly for the canceled workers to exit, then commit the final watermark for each fully drained partition and release its lease with the captured generation. A partition whose deliveries are still inside their visibility window when the deadline passes keeps its lease so a new owner can take it over.
4. Deregister the heartbeat. The delivery channel is closed only after every worker goroutine has exited, so a worker can never send on a closed channel.

The drain's cleanup steps run on their own bounded contexts: an expired caller deadline stops the wait and proceeds to cleanup, it does not strand an unbounded background goroutine; if cleanup cannot finish in its own budget the run exits with leases left for takeover.

The consumer framework drives this drain from `Stop()`: it stops routing new deliveries, lets in-flight `Process` calls finish, nack-abandons buffered deliveries that were never dispatched, and runs `ShutdownSubscription` concurrently with the wait for in-flight work.

### Worker Stop Behavior

When a partition worker is stopped (lease lost, fencing error, or shutdown):
- Immediately removed from the workers map and its context canceled; `workerWg` keeps tracking it
- On a lost lease or a fenced write it exits *without* releasing the lease — the row stays for the new owner, with the generation that made the old worker's writes fail closed
- During shutdown the drain waits briefly for exit before closing the delivery channel; a still-running worker blocks the channel close
- Reconciliation starts a replacement only while the lease is held at a current generation — brief overlap is harmless with at-least-once semantics

### Logger Hierarchy

`sql.go` creates a root `queue_mysql` logger and passes named children to each component:

```
queue_mysql
  ├── .publisher
  ├── .subscriber
  ├── .message_store
  ├── .delivery_state_store
  ├── .offset_store
  ├── .partition_lease_store
  └── .subscriber_heartbeat_store
```

Stores do not log errors — they return them. The subscriber propagates all errors to the top call site (`managePartitions` or `run`), which logs once with full context (`topic`, `consumer_group`, `subscriber_name`).

## Testing

### Unit Tests

```bash
bazel test //platform/extension/messagequeue/mysql:mysql_test --test_output=streamed
bazel test //platform/extension/messagequeue/mysql/ctl/...:all --test_output=streamed
```

### Integration Tests

Requires Docker running:

```bash
bazel test //test/integration/extension/messagequeue/... --test_output=streamed
```

Integration tests cover: publish/subscribe, partition isolation, ordering, visibility timeout, idempotent publish, concurrent publishers, crash recovery, multiple consumer groups, rebalancing, DLQ, graceful shutdown, non-blocking in-flight messages, the postpone barrier, strict serialization (`BatchSize=1`), and independent consumer group state.
