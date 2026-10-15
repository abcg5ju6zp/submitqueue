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

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"go.uber.org/zap"
)

// sqldeliveryStateStore is the SQL implementation of deliveryStateStore
type sqldeliveryStateStore struct {
	db     *sql.DB
	logger *zap.SugaredLogger
	scope  tally.Scope
}

// newDeliveryStateStore creates a new SQL delivery state store
func newDeliveryStateStore(db *sql.DB, logger *zap.SugaredLogger, scope tally.Scope) deliveryStateStore {
	return &sqldeliveryStateStore{
		db:     db,
		logger: logger.Named("delivery_state_store"),
		scope:  scope.SubScope("delivery_state_store"),
	}
}

// classifyZeroRows maps a zero-row guarded write to ErrLeaseFenced when the
// lease no longer matches, or nil when the caller is still the owner (a
// legitimate no-op, e.g. re-acking an already-acked row).
func (s *sqldeliveryStateStore) classifyZeroRows(ctx context.Context, token partitionLeaseToken, consumerGroup, tenant, topic, partitionKey string) error {
	owned, err := leaseStillOwned(ctx, s.db, token, tenant, consumerGroup, topic, partitionKey)
	if err != nil {
		return fmt.Errorf("recheck lease ownership tenant=%s topic=%s partition=%s: %w", tenant, topic, partitionKey, err)
	}
	if !owned {
		return fencedWriteError(token, tenant, topic, partitionKey)
	}
	return nil
}

// MarkDelivered inserts a row marking message as in-flight for this consumer group.
// Returns the resulting retry count after the operation.
//
// The INSERT ... SELECT ... WHERE EXISTS form makes the lease guard atomic:
// a stale owner produces zero rows (neither an insert nor an update), which is
// reported as ErrLeaseFenced. Assignment order in ON DUPLICATE KEY UPDATE
// preserves the existing retry/postpone semantics.
func (s *sqldeliveryStateStore) MarkDelivered(ctx context.Context, token partitionLeaseToken, consumerGroup, tenant, topic, partitionKey string, offset int64, visibilityTimeoutMs int64) (_ int, retErr error) {
	op := metrics.Begin(s.scope, "mark_delivered", metrics.StorageLatencyBuckets,
		metrics.NewTag("topic", topic),
		metrics.NewTag("consumer_group", consumerGroup))
	defer func() { op.Complete(retErr) }()

	now := time.Now().UnixMilli()
	invisibleUntil := now + visibilityTimeoutMs

	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (tenant, consumer_group, topic, partition_key, message_offset, acked, invisible_until, retry_count, postponed)
		SELECT ?, ?, ?, ?, ?, FALSE, ?, 0, FALSE
		WHERE %s
		ON DUPLICATE KEY UPDATE
			invisible_until = IF(acked = FALSE, VALUES(invisible_until), invisible_until),
			retry_count = IF(acked = FALSE AND postponed = FALSE, retry_count + 1, retry_count),
			postponed = IF(acked = FALSE, FALSE, postponed)
	`, DeliveryStateTableName, leaseGuardSQL),
		append([]any{tenant, consumerGroup, topic, partitionKey, offset, invisibleUntil},
			leaseGuardArgs(token, tenant, consumerGroup, topic, partitionKey)...)...)

	if err != nil {
		return 0, fmt.Errorf("mark delivered tenant=%s topic=%s partition=%s offset=%d: %w", tenant, topic, partitionKey, offset, err)
	}
	rows, err := result.RowsAffected()
	if err == nil && rows == 0 {
		return 0, s.classifyZeroRows(ctx, token, consumerGroup, tenant, topic, partitionKey)
	}

	// Read retry_count after INSERT/UPDATE to get the current value.
	// For new inserts, retry_count = 0. For updates (redelivery), retry_count was incremented.
	// If this SELECT fails, the caller (consumer lifecycle) nacks and redelivers the message,
	// which re-executes MarkDelivered idempotently — so the failure is safe to propagate.
	var retryCount int
	err = s.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT retry_count FROM %s
		WHERE tenant = ? AND consumer_group = ? AND topic = ? AND partition_key = ? AND message_offset = ?
	`, DeliveryStateTableName), tenant, consumerGroup, topic, partitionKey, offset).Scan(&retryCount)
	if err != nil {
		return 0, fmt.Errorf("get retry count after mark delivered tenant=%s topic=%s partition=%s offset=%d: %w", tenant, topic, partitionKey, offset, err)
	}

	return retryCount, nil
}

// ExtendVisibility extends the visibility timeout for an in-flight message
// without incrementing retry_count. Used by ExtendVisibilityTimeout.
func (s *sqldeliveryStateStore) ExtendVisibility(ctx context.Context, token partitionLeaseToken, consumerGroup, tenant, topic, partitionKey string, offset int64, visibilityTimeoutMs int64) (retErr error) {
	op := metrics.Begin(s.scope, "extend_visibility", metrics.StorageLatencyBuckets,
		metrics.NewTag("topic", topic),
		metrics.NewTag("consumer_group", consumerGroup))
	defer func() { op.Complete(retErr) }()

	now := time.Now().UnixMilli()
	invisibleUntil := now + visibilityTimeoutMs

	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET invisible_until = ?
		WHERE tenant = ? AND consumer_group = ? AND topic = ? AND partition_key = ? AND message_offset = ? AND acked = FALSE AND %s
	`, DeliveryStateTableName, leaseGuardSQL),
		append([]any{invisibleUntil, tenant, consumerGroup, topic, partitionKey, offset},
			leaseGuardArgs(token, tenant, consumerGroup, topic, partitionKey)...)...)

	if err != nil {
		return fmt.Errorf("extend visibility tenant=%s topic=%s partition=%s offset=%d: %w", tenant, topic, partitionKey, offset, err)
	}

	rowsAffected, raErr := result.RowsAffected()
	if raErr == nil && rowsAffected == 0 {
		if owned, ownErr := leaseStillOwned(ctx, s.db, token, tenant, consumerGroup, topic, partitionKey); ownErr == nil && owned {
			// Still the owner: the row was already acked or did not exist.
			s.logger.Warnw("extend visibility matched no rows, message already acked or never delivered",
				logTenant, tenant,
				logTopic, topic,
				logPartitionKey, partitionKey,
				"offset", offset,
			)
			return nil
		} else if ownErr != nil {
			return fmt.Errorf("recheck lease ownership tenant=%s topic=%s partition=%s: %w", tenant, topic, partitionKey, ownErr)
		}
		return fencedWriteError(token, tenant, topic, partitionKey)
	}

	return nil
}

// MarkAcked sets acked = TRUE to indicate this group has processed the message.
func (s *sqldeliveryStateStore) MarkAcked(ctx context.Context, token partitionLeaseToken, consumerGroup, tenant, topic, partitionKey string, offset int64) (retErr error) {
	op := metrics.Begin(s.scope, "mark_acked", metrics.StorageLatencyBuckets,
		metrics.NewTag("topic", topic),
		metrics.NewTag("consumer_group", consumerGroup))
	defer func() { op.Complete(retErr) }()

	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (tenant, consumer_group, topic, partition_key, message_offset, acked, invisible_until, retry_count)
		SELECT ?, ?, ?, ?, ?, TRUE, 0, 0
		WHERE %s
		ON DUPLICATE KEY UPDATE acked = TRUE
	`, DeliveryStateTableName, leaseGuardSQL),
		append([]any{tenant, consumerGroup, topic, partitionKey, offset},
			leaseGuardArgs(token, tenant, consumerGroup, topic, partitionKey)...)...)

	if err != nil {
		return fmt.Errorf("mark acked tenant=%s topic=%s partition=%s offset=%d: %w", tenant, topic, partitionKey, offset, err)
	}
	if rows, raErr := result.RowsAffected(); raErr == nil && rows == 0 {
		if fenceErr := s.classifyZeroRows(ctx, token, consumerGroup, tenant, topic, partitionKey); fenceErr != nil {
			return fmt.Errorf("mark acked tenant=%s topic=%s partition=%s offset=%d: %w", tenant, topic, partitionKey, offset, fenceErr)
		}
	}

	return nil
}

// MarkNacked makes the message eligible for redelivery after delayMs.
// retry_count is NOT incremented here — it is incremented by MarkDelivered on redelivery.
func (s *sqldeliveryStateStore) MarkNacked(ctx context.Context, token partitionLeaseToken, consumerGroup, tenant, topic, partitionKey string, offset int64, delayMs int64) (retErr error) {
	op := metrics.Begin(s.scope, "mark_nacked", metrics.StorageLatencyBuckets,
		metrics.NewTag("topic", topic),
		metrics.NewTag("consumer_group", consumerGroup))
	defer func() { op.Complete(retErr) }()

	if delayMs < 0 || delayMs > maxRetryBackoffMs {
		return fmt.Errorf("mark nacked tenant=%s topic=%s partition=%s offset=%d: retry delay %d is outside [0, %d]", tenant, topic, partitionKey, offset, delayMs, maxRetryBackoffMs)
	}
	invisibleUntil := time.Now().UnixMilli() + delayMs

	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (tenant, consumer_group, topic, partition_key, message_offset, acked, invisible_until, retry_count)
		SELECT ?, ?, ?, ?, ?, FALSE, ?, 0
		WHERE %s
		ON DUPLICATE KEY UPDATE
			invisible_until = IF(acked = FALSE, VALUES(invisible_until), invisible_until)
	`, DeliveryStateTableName, leaseGuardSQL),
		append([]any{tenant, consumerGroup, topic, partitionKey, offset, invisibleUntil},
			leaseGuardArgs(token, tenant, consumerGroup, topic, partitionKey)...)...)

	if err != nil {
		return fmt.Errorf("mark nacked tenant=%s topic=%s partition=%s offset=%d: %w", tenant, topic, partitionKey, offset, err)
	}
	if rows, raErr := result.RowsAffected(); raErr == nil && rows == 0 {
		if fenceErr := s.classifyZeroRows(ctx, token, consumerGroup, tenant, topic, partitionKey); fenceErr != nil {
			return fmt.Errorf("mark nacked tenant=%s topic=%s partition=%s offset=%d: %w", tenant, topic, partitionKey, offset, fenceErr)
		}
	}

	return nil
}

// MarkPostponed sets invisible_until = now + delay, resets retry_count, and sets
// the postponed flag. A postpone is a deliberate wait, not a failure: the flag
// makes the message a partition barrier while invisible, and exempts the next
// MarkDelivered from the retry_count increment. The reset restarts failure
// accounting — a completed delivery that chose to wait has demonstrated the
// message is processable.
func (s *sqldeliveryStateStore) MarkPostponed(ctx context.Context, token partitionLeaseToken, consumerGroup, tenant, topic, partitionKey string, offset int64, delayMs int64) (retErr error) {
	op := metrics.Begin(s.scope, "mark_postponed", metrics.StorageLatencyBuckets,
		metrics.NewTag("topic", topic),
		metrics.NewTag("consumer_group", consumerGroup))
	defer func() { op.Complete(retErr) }()

	now := time.Now().UnixMilli()
	invisibleUntil := now + delayMs

	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (tenant, consumer_group, topic, partition_key, message_offset, acked, invisible_until, retry_count, postponed)
		SELECT ?, ?, ?, ?, ?, FALSE, ?, 0, TRUE
		WHERE %s
		ON DUPLICATE KEY UPDATE
			invisible_until = IF(acked = FALSE, VALUES(invisible_until), invisible_until),
			retry_count = IF(acked = FALSE, 0, retry_count),
			postponed = IF(acked = FALSE, TRUE, postponed)
	`, DeliveryStateTableName, leaseGuardSQL),
		append([]any{tenant, consumerGroup, topic, partitionKey, offset, invisibleUntil},
			leaseGuardArgs(token, tenant, consumerGroup, topic, partitionKey)...)...)

	if err != nil {
		return fmt.Errorf("mark postponed tenant=%s topic=%s partition=%s offset=%d: %w", tenant, topic, partitionKey, offset, err)
	}
	if rows, raErr := result.RowsAffected(); raErr == nil && rows == 0 {
		if fenceErr := s.classifyZeroRows(ctx, token, consumerGroup, tenant, topic, partitionKey); fenceErr != nil {
			return fmt.Errorf("mark postponed tenant=%s topic=%s partition=%s offset=%d: %w", tenant, topic, partitionKey, offset, fenceErr)
		}
	}

	return nil
}

// GetDeliveryState returns the full delivery state for a message offset.
// Returns (state, found, error). found=false means no row (never delivered).
func (s *sqldeliveryStateStore) GetDeliveryState(ctx context.Context, consumerGroup, tenant, topic, partitionKey string, offset int64) (_ DeliveryState, _ bool, retErr error) {
	op := metrics.Begin(s.scope, "get_delivery_state", metrics.StorageLatencyBuckets,
		metrics.NewTag("topic", topic),
		metrics.NewTag("consumer_group", consumerGroup))
	defer func() { op.Complete(retErr) }()

	var state DeliveryState
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT acked, invisible_until, retry_count, postponed FROM %s
		WHERE tenant = ? AND consumer_group = ? AND topic = ? AND partition_key = ? AND message_offset = ?
	`, DeliveryStateTableName), tenant, consumerGroup, topic, partitionKey, offset).Scan(&state.Acked, &state.InvisibleUntil, &state.RetryCount, &state.Postponed)

	if err == sql.ErrNoRows {
		return DeliveryState{}, false, nil
	}
	if err != nil {
		return DeliveryState{}, false, fmt.Errorf("get delivery state tenant=%s topic=%s partition=%s offset=%d: %w", tenant, topic, partitionKey, offset, err)
	}

	return state, true, nil
}

// AdvanceWatermark computes the new contiguous acked watermark and cleans up
// delivery state rows that are behind it.
// offsets are the actual message offsets above the current watermark (from messageStore).
// Returns the new watermark (highest contiguous acked offset from currentWatermark).
//
// The watermark calculation is a read and needs no owner guard; the cleanup
// DELETE behind it is conditioned on the caller's fencing token so a stale
// owner never mutates a partition it lost. A fenced cleanup is swallowed — the
// new owner recomputes and cleans up on its next tick.
func (s *sqldeliveryStateStore) AdvanceWatermark(ctx context.Context, token partitionLeaseToken, consumerGroup, tenant, topic, partitionKey string, currentWatermark int64, offsets []int64) (_ int64, retErr error) {
	op := metrics.Begin(s.scope, "advance_watermark", metrics.StorageLatencyBuckets,
		metrics.NewTag("topic", topic),
		metrics.NewTag("consumer_group", consumerGroup))
	defer func() { op.Complete(retErr) }()

	if len(offsets) == 0 {
		return currentWatermark, nil
	}

	// Batch-fetch delivery state for the provided offsets.
	placeholders, ok := inListPlaceholders(len(offsets))
	if !ok {
		return currentWatermark, nil
	}
	args := make([]interface{}, 0, 4+len(offsets))
	args = append(args, tenant, consumerGroup, topic, partitionKey)
	for _, offset := range offsets {
		args = append(args, offset)
	}

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT message_offset, acked FROM %s
		WHERE tenant = ? AND consumer_group = ? AND topic = ? AND partition_key = ?
		AND message_offset IN (%s)
	`, DeliveryStateTableName, placeholders), args...)
	if err != nil {
		return currentWatermark, fmt.Errorf("query delivery state for watermark tenant=%s topic=%s partition=%s: %w", tenant, topic, partitionKey, err)
	}
	defer rows.Close()

	// Build lookup map: offset -> acked
	ackedMap := make(map[int64]bool, len(offsets))
	for rows.Next() {
		var offset int64
		var acked bool
		if err := rows.Scan(&offset, &acked); err != nil {
			return currentWatermark, fmt.Errorf("scan delivery state tenant=%s topic=%s partition=%s: %w", tenant, topic, partitionKey, err)
		}
		ackedMap[offset] = acked
	}
	if err := rows.Err(); err != nil {
		return currentWatermark, fmt.Errorf("delivery state iteration tenant=%s topic=%s partition=%s: %w", tenant, topic, partitionKey, err)
	}

	// Walk message offsets in order. Advance while contiguous acked.
	// Stop at first offset that is not acked (in-flight, nacked, or undelivered).
	newWatermark := currentWatermark
	for _, offset := range offsets {
		acked, exists := ackedMap[offset]
		if !exists || !acked {
			// No delivery state (undelivered) or not acked — stop
			break
		}
		newWatermark = offset
	}

	// Cleanup error is swallowed because the watermark was already computed and
	// will be returned to the caller. The stale delivery state rows behind the
	// watermark are harmless — they are never read again (all queries use
	// offset > watermark). Cleanup is retried on the next AdvanceWatermark call.
	if newWatermark > currentWatermark {
		_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
			DELETE FROM %s
			WHERE tenant = ? AND consumer_group = ? AND topic = ? AND partition_key = ? AND message_offset <= ? AND %s
		`, DeliveryStateTableName, leaseGuardSQL),
			append([]any{tenant, consumerGroup, topic, partitionKey, newWatermark},
				leaseGuardArgs(token, tenant, consumerGroup, topic, partitionKey)...)...)
		if err != nil {
			metrics.NamedCounter(s.scope, "advance_watermark", "cleanup_errors", 1,
				metrics.NewTag("topic", topic))
			s.logger.Warnw("failed to clean up delivery state behind watermark, will retry on next advance",
				logTenant, tenant,
				logTopic, topic,
				logPartitionKey, partitionKey,
				"watermark", newWatermark,
				logError, err,
			)
		}
	}

	return newWatermark, nil
}
