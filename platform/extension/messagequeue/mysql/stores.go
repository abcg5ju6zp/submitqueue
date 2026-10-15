// Copyright (c) 2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

//go:generate mockgen -source=stores.go -destination=mock_stores.go -package=mysql

import (
	"context"

	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
)

const (
	// Fixed table names for single-table design
	MessagesTableName             = "queue_messages"
	PartitionLeasesTableName      = "queue_partition_leases"
	OffsetsTableName              = "queue_offsets"
	SubscriberHeartbeatsTableName = "queue_subscriber_heartbeats"
	DeliveryStateTableName        = "queue_delivery_state"
)

// messageRow represents a row from the messages table (internal use only)
type messageRow struct {
	// Tenant is the shard isolation identity
	Tenant string
	// Offset is the auto-incrementing sequence number for message ordering within a partition
	Offset int64
	// ID is the unique message identifier
	ID string
	// Payload is the message body in bytes
	Payload []byte
	// Metadata contains key-value pairs for message attributes
	Metadata map[string]string
	// PartitionKey determines which partition this message belongs to for ordering guarantees
	PartitionKey string
	// PublishedAt is the Unix timestamp in milliseconds when message was published
	PublishedAt int64
	// FailedAt is the Unix timestamp in milliseconds when the message failed (0 for normal messages, >0 for DLQ)
	FailedAt int64
	// FailureCount tracks total failures before moving to DLQ (0 for normal messages, >0 for DLQ)
	FailureCount int
	// LastError contains the error message from the final failure ("" for normal messages)
	LastError string
	// OriginalTopic is the topic where the message originally failed ("" for normal messages)
	OriginalTopic string
	// FailureDetail is the encoded structured half of the failure — its
	// subjects and free-form context. Empty for normal messages, and for a DLQ
	// message whose failure recorded no structure.
	FailureDetail []byte
}

// messageStore handles message table operations (internal use only)
type messageStore interface {
	// Insert inserts messages into the topic table.
	Insert(ctx context.Context, tenant string, topic string, messages []entityqueue.Message) error

	// FetchByOffset fetches messages with offset > currentOffset for a specific partition.
	FetchByOffset(ctx context.Context, tenant string, topic string, partitionKey string, currentOffset int64, limit int) ([]messageRow, error)

	// MoveToDLQAndAck atomically moves a message to its DLQ topic and marks the
	// per-consumer-group delivery state acked. Both changes share one
	// transaction; the ack is conditioned on the lease token so a stale owner
	// rolls the move back and the message stays redeliverable. A message that
	// is already gone (moved by a retried call) is a no-op success, so
	// replaying the operation is safe.
	MoveToDLQAndAck(ctx context.Context, token partitionLeaseToken, consumerGroup, tenant, topic, partitionKey, messageID string, failureCount int, f failure.Failure, dlqTopicSuffix string) error

	// GarbageCollect deletes messages with offset <= minAckedOffset.
	GarbageCollect(ctx context.Context, tenant string, topic string, partitionKey string, minAckedOffset int64) (int64, error)

	// GetOffsetsAbove returns message offsets above afterOffset for a partition,
	// ordered ascending, up to limit rows.
	GetOffsetsAbove(ctx context.Context, tenant string, topic string, partitionKey string, afterOffset int64, limit int) ([]int64, error)
}

// offsetStore handles offset table operations for per-partition offset tracking (internal use only)
type offsetStore interface {
	// Initialize creates an offset entry for a tenant+topic+partition if it doesn't exist
	Initialize(ctx context.Context, tenant string, topic string, partitionKey string, consumerGroup string) error

	// GetAckedOffset returns the current acked offset for a tenant+topic+partition
	GetAckedOffset(ctx context.Context, tenant string, topic string, partitionKey string, consumerGroup string) (int64, error)

	// UpdateAckedOffset updates the offset_acked for a tenant+topic+partition (only if new offset is greater).
	// The write is conditioned on token: a caller that lost its lease gets ErrLeaseFenced and changes nothing.
	UpdateAckedOffset(ctx context.Context, token partitionLeaseToken, tenant string, topic string, partitionKey string, offset int64, consumerGroup string) error

	// GetMinAckedOffset returns the minimum offset_acked across all consumer groups
	// for a tenant+topic+partition. Returns (0, false, nil) if no offset rows exist.
	GetMinAckedOffset(ctx context.Context, tenant string, topic string, partitionKey string) (offset int64, found bool, err error)

	// DeleteOffset removes one consumer group's offset row for a partition.
	DeleteOffset(ctx context.Context, tenant string, topic string, partitionKey string, consumerGroup string) error
}

// partitionLeaseToken is the fencing identity an owner-scoped write carries:
// the subscriber name that acquired the lease and the generation in effect
// at acquisition. A conditional write matching (leased_by, lease_generation)
// rejects every write made by a stale owner after a steal.
type partitionLeaseToken struct {
	leasedBy   string
	generation int64
}

// leaseInfo describes one partition's current lease row (internal use only)
type leaseInfo struct {
	// PartitionKey is the partition this lease covers
	PartitionKey string
	// LeasedBy is the subscriber name currently holding the lease
	LeasedBy string
	// Generation is the fencing epoch of ownership: 1 on first acquisition,
	// +1 on every steal, unchanged by renewal. A worker's writes are
	// conditioned on the generation it acquired.
	Generation int64
	// LeaseRenewedAt is the epoch milliseconds of the last renewal; a lease
	// is stale (stealable) once this is older than the lease duration
	LeaseRenewedAt int64
}

// partitionLeaseStore handles partition lease operations (internal use only)
type partitionLeaseStore interface {
	// TryAcquireLease attempts to acquire or renew a lease for a partition.
	// The bool reports whether this subscriber owns the lease afterwards; the
	// int64 is the current lease generation (1 on first acquisition, +1 on
	// every steal, unchanged by renewal), valid only when acquired is true.
	TryAcquireLease(ctx context.Context, tenant string, topic string, partitionKey string, subscriberName string, consumerGroup string, leaseDurationMs int64) (bool, int64, error)

	// RenewLease refreshes lease_renewed_at on one partition, conditioned on
	// the fencing token (subscriberName, generation). The bool is false when
	// the row is owned by someone else, is gone, or carries another
	// generation — the lease has been lost and the caller must fence the
	// worker. A database failure is returned as an error (the lease may still
	// be held; the next tick retries).
	RenewLease(ctx context.Context, tenant string, topic string, partitionKey string, subscriberName string, consumerGroup string, generation int64) (bool, error)

	// ReleaseLease releases the lease for a partition this subscriber owns at
	// exactly generation. The int64 is rows deleted; 0 means the lease was
	// already gone (stolen or purged) or the generation no longer matches, so
	// a newer owner's lease is never deleted. Callers treat the partition as
	// released regardless of the count.
	ReleaseLease(ctx context.Context, tenant string, topic string, partitionKey string, subscriberName string, consumerGroup string, generation int64) (int64, error)

	// GetLeasedPartitionsForTenants returns partitions leased by this worker
	// across the given tenants (including each row's generation), keyed by
	// tenant. Tenants with no leases are absent.
	GetLeasedPartitionsForTenants(ctx context.Context, tenants []string, topic string, subscriberName string, consumerGroup string) (map[string][]leaseInfo, error)

	// GetAllLeasesForTenants returns every lease row under (topic, consumerGroup)
	// for the given tenants, keyed by tenant.
	GetAllLeasesForTenants(ctx context.Context, tenants []string, topic string, consumerGroup string) (map[string][]leaseInfo, error)

	// CheckLeaseOwnership reports whether the partition lease currently matches
	// token. A false result fences the caller without a write.
	CheckLeaseOwnership(ctx context.Context, token partitionLeaseToken, tenant, topic, partitionKey, consumerGroup string) (bool, error)

	// PurgeStaleForTenants deletes lease rows not renewed within olderThanMs
	// across the given tenants.
	PurgeStaleForTenants(ctx context.Context, tenants []string, topic string, consumerGroup string, olderThanMs int64) error

	// DiscoverPartitions returns distinct partition keys per tenant for topic.
	DiscoverPartitions(ctx context.Context, tenants []string, topic string) (map[string][]string, error)
}

// subscriberHeartbeatStore handles subscriber heartbeat operations for fair partition leasing (internal use only)
type subscriberHeartbeatStore interface {
	// HeartbeatForTenants registers or renews this subscriber's heartbeat for each tenant.
	HeartbeatForTenants(ctx context.Context, tenants []string, topic string, subscriberName string, consumerGroup string) error

	// ActiveSubscribersForTenants returns recent subscriber names keyed by tenant.
	ActiveSubscribersForTenants(ctx context.Context, tenants []string, topic string, consumerGroup string, staleDurationMs int64) (map[string][]string, error)

	// DeregisterForTenants removes this subscriber's heartbeat rows across tenants.
	DeregisterForTenants(ctx context.Context, tenants []string, topic string, subscriberName string, consumerGroup string) error

	// PurgeStaleForTenants deletes stale heartbeat rows across the given tenants.
	PurgeStaleForTenants(ctx context.Context, tenants []string, topic string, consumerGroup string, olderThanMs int64) error
}

// DeliveryState represents the full per-message delivery tracking state.
type DeliveryState struct {
	// Acked indicates whether this consumer group has processed the message
	Acked bool
	// InvisibleUntil is the epoch milliseconds until which the message is hidden
	InvisibleUntil int64
	// RetryCount tracks how many times the message has been delivered
	RetryCount int
	// Postponed indicates the last delivery was postponed (a deliberate wait,
	// not a failure). While set and invisible, the message is a partition
	// barrier and its next delivery is exempt from the retry_count increment.
	Postponed bool
}

// deliveryStateStore handles per-consumer-group delivery tracking (internal use only)
type deliveryStateStore interface {
	// MarkDelivered inserts a row marking message as in-flight for this consumer group.
	// Every mutation is conditioned on token: a stale owner's call returns ErrLeaseFenced.
	MarkDelivered(ctx context.Context, token partitionLeaseToken, consumerGroup, tenant, topic, partitionKey string, offset int64, visibilityTimeoutMs int64) (retryCount int, err error)

	// ExtendVisibility extends the visibility timeout for an in-flight message.
	// Conditioned on token; an already-acked message or a lost lease matches no rows
	// (the former is a no-op, the latter is ErrLeaseFenced).
	ExtendVisibility(ctx context.Context, token partitionLeaseToken, consumerGroup, tenant, topic, partitionKey string, offset int64, visibilityTimeoutMs int64) error

	// MarkAcked sets acked = TRUE to indicate this group has processed the message.
	MarkAcked(ctx context.Context, token partitionLeaseToken, consumerGroup, tenant, topic, partitionKey string, offset int64) error

	// MarkNacked makes the message eligible for redelivery after delayMs.
	MarkNacked(ctx context.Context, token partitionLeaseToken, consumerGroup, tenant, topic, partitionKey string, offset int64, delayMs int64) error

	// MarkPostponed sets invisible_until = now + delay, resets retry_count, and sets the postponed flag.
	MarkPostponed(ctx context.Context, token partitionLeaseToken, consumerGroup, tenant, topic, partitionKey string, offset int64, delayMs int64) error

	// GetDeliveryState returns the full delivery state for a message offset.
	GetDeliveryState(ctx context.Context, consumerGroup, tenant, topic, partitionKey string, offset int64) (DeliveryState, bool, error)

	// AdvanceWatermark computes the new contiguous acked watermark and cleans up
	// delivery state rows behind it. The cleanup write is conditioned on token;
	// a fenced caller leaves both state and offset rows for the new owner to
	// recompute. Reads (the watermark calculation itself) are not fenced.
	AdvanceWatermark(ctx context.Context, token partitionLeaseToken, consumerGroup, tenant, topic, partitionKey string, currentWatermark int64, offsets []int64) (int64, error)
}
