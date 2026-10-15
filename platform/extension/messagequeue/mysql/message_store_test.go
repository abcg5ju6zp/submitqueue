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
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap/zaptest"

	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
)

// testMetrics returns a test metrics scope for use in tests
func testMetrics() tally.Scope {
	return tally.NoopScope
}

func setupmessageStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, messageStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := newMessageStore(db, zaptest.NewLogger(t).Sugar(), testMetrics())

	return db, mock, store
}

func TestMessageStore_Insert(t *testing.T) {
	tests := []struct {
		name     string
		messages []entityqueue.Message
		setup    func(mock sqlmock.Sqlmock, messages []entityqueue.Message)
		wantErr  bool
	}{
		{
			name: "successful insert with multiple messages",
			messages: []entityqueue.Message{
				{Tenant: testTenant, ID: "msg1", Payload: []byte("payload1"), PartitionKey: "part1", PublishedAt: time.Now().UnixMilli()},
				{Tenant: testTenant, ID: "msg2", Payload: []byte("payload2"), PartitionKey: "part1", PublishedAt: time.Now().UnixMilli()},
			},
			setup: func(mock sqlmock.Sqlmock, messages []entityqueue.Message) {
				mock.ExpectBegin()
				mock.ExpectPrepare("INSERT INTO queue_messages")
				for range messages {
					mock.ExpectExec("INSERT INTO queue_messages").
						WillReturnResult(sqlmock.NewResult(1, 1))
				}
				mock.ExpectCommit()
			},
			wantErr: false,
		},
		{
			name:     "empty messages should succeed",
			messages: []entityqueue.Message{},
			setup:    func(mock sqlmock.Sqlmock, messages []entityqueue.Message) {},
			wantErr:  false,
		},
		{
			name: "conflicting queue metadata is rejected",
			messages: []entityqueue.Message{{
				Tenant:       testTenant,
				ID:           "msg-conflict",
				PartitionKey: "part1",
				Metadata:     map[string]string{entityqueue.MetadataKeyQueueName: "other-tenant"},
			}},
			setup: func(mock sqlmock.Sqlmock, messages []entityqueue.Message) {
				mock.ExpectBegin()
				mock.ExpectPrepare("INSERT INTO queue_messages")
				mock.ExpectRollback()
			},
			wantErr: true,
		},
		{
			// Regression: re-publishing the same (topic, partition_key, id) tuple
			// must succeed silently. sqlmock returns 0 affected rows to simulate
			// MySQL's ON DUPLICATE KEY UPDATE swallowing the unique-key collision.
			name: "duplicate publish is idempotent",
			messages: []entityqueue.Message{
				{Tenant: testTenant, ID: "msg-dup", Payload: []byte("payload"), PartitionKey: "part1", PublishedAt: time.Now().UnixMilli()},
			},
			setup: func(mock sqlmock.Sqlmock, messages []entityqueue.Message) {
				mock.ExpectBegin()
				mock.ExpectPrepare("INSERT INTO queue_messages")
				mock.ExpectExec("INSERT INTO queue_messages").
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectCommit()
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupmessageStoreTest(t)
			defer db.Close()

			tt.setup(mock, tt.messages)

			ctx := context.Background()
			err := store.Insert(ctx, testTenant, "test_topic", tt.messages)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestMessageStore_FetchByOffset(t *testing.T) {
	db, mock, store := setupmessageStoreTest(t)
	defer db.Close()

	ctx := context.Background()
	topic := "test_topic"
	partitionKey := "part1"
	currentOffset := int64(0)
	limit := 10

	// Mock query results (no transaction, simple SELECT)
	rows := sqlmock.NewRows([]string{"tenant", "offset", "id", "payload", "metadata", "partition_key", "published_at", "failed_at", "failure_count", "last_error", "original_topic", "failure_detail"}).
		AddRow(testTenant, int64(1), "msg1", []byte("payload1"), []byte("{}"), "part1", time.Now().UnixMilli(), int64(0), 0, "", "", nil)

	mock.ExpectQuery("SELECT (.+) FROM queue_messages").
		WithArgs(testTenant, topic, partitionKey, currentOffset, limit).
		WillReturnRows(rows)

	results, err := store.FetchByOffset(ctx, testTenant, topic, partitionKey, currentOffset, limit)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "msg1", results[0].ID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMessageStore_MoveToDLQAndAck(t *testing.T) {
	topic := "test_topic"
	partitionKey := "part1"
	messageID := "msg1"
	failureCount := 3
	lastError := "test error"
	dlqTopicSuffix := "_dlq"
	dlqTopic := topic + dlqTopicSuffix
	token := partitionLeaseToken{leasedBy: testSubscriberName, generation: 2}

	expectFetch := func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SELECT (.+) FROM queue_messages").
			WithArgs(testTenant, topic, partitionKey, messageID).
			WillReturnRows(sqlmock.NewRows([]string{"payload", "metadata", "partition_key", "created_at", "published_at", "offset"}).
				AddRow([]byte("payload1"), []byte(`{"key":"value"}`), "part1", time.Now().UnixMilli(), time.Now().UnixMilli(), int64(7)))
	}
	expectMoveAndAck := func(mock sqlmock.Sqlmock, lastErrorArg, detailArg any, ackRows int64) {
		mock.ExpectExec("INSERT INTO queue_messages").
			WithArgs(testTenant, dlqTopic, messageID, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), failureCount, lastErrorArg, topic, detailArg).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("DELETE FROM queue_messages").
			WithArgs(testTenant, topic, partitionKey, messageID).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("INSERT INTO queue_delivery_state").
			WithArgs(testTenant, testConsumerGroup, topic, partitionKey, int64(7),
				testTenant, testConsumerGroup, topic, partitionKey, testSubscriberName, int64(2)).
			WillReturnResult(sqlmock.NewResult(0, ackRows))
	}

	t.Run("moves and acks in one transaction", func(t *testing.T) {
		db, mock, store := setupmessageStoreTest(t)
		defer db.Close()

		mock.ExpectBegin()
		expectFetch(mock)
		expectMoveAndAck(mock, lastError, nil, 1)
		mock.ExpectCommit()

		err := store.MoveToDLQAndAck(context.Background(), token, testConsumerGroup, testTenant, topic, partitionKey, messageID, failureCount, failure.New(lastError), dlqTopicSuffix)
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	// The structured half of a failure is what lets a DLQ consumer act on the
	// right entity, so it has to reach the row rather than flattened prose.
	t.Run("writes structured failure detail", func(t *testing.T) {
		db, mock, store := setupmessageStoreTest(t)
		defer db.Close()

		f := failure.New("speculator failed", failure.Subject{Type: "queue", ID: "test-queue"})
		encoded, err := failure.Encode(f)
		require.NoError(t, err)

		mock.ExpectBegin()
		expectFetch(mock)
		expectMoveAndAck(mock, "speculator failed", encoded, 1)
		mock.ExpectCommit()

		require.NoError(t, store.MoveToDLQAndAck(context.Background(), token, testConsumerGroup, testTenant, topic, partitionKey, messageID, failureCount, f, dlqTopicSuffix))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("missing main row is an idempotent no-op", func(t *testing.T) {
		db, mock, store := setupmessageStoreTest(t)
		defer db.Close()

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT (.+) FROM queue_messages").
			WithArgs(testTenant, topic, partitionKey, messageID).
			WillReturnRows(sqlmock.NewRows([]string{"payload", "metadata", "partition_key", "created_at", "published_at", "offset"}))
		mock.ExpectCommit()

		err := store.MoveToDLQAndAck(context.Background(), token, testConsumerGroup, testTenant, topic, partitionKey, messageID, failureCount, failure.New(lastError), dlqTopicSuffix)
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("stale fencing token rolls the move back", func(t *testing.T) {
		db, mock, store := setupmessageStoreTest(t)
		defer db.Close()

		mock.ExpectBegin()
		expectFetch(mock)
		expectMoveAndAck(mock, lastError, nil, 0)
		// Guarded ack matches no rows and the lease no longer matches: rollback.
		mock.ExpectQuery("SELECT 1 FROM queue_partition_leases").
			WithArgs(testTenant, testConsumerGroup, topic, partitionKey, testSubscriberName, int64(2)).
			WillReturnRows(sqlmock.NewRows([]string{"1"}))
		mock.ExpectRollback()

		err := store.MoveToDLQAndAck(context.Background(), token, testConsumerGroup, testTenant, topic, partitionKey, messageID, failureCount, failure.New(lastError), dlqTopicSuffix)
		var fenced *ErrLeaseFenced
		require.ErrorAs(t, err, &fenced)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestMessageStore_GetOffsetsAbove(t *testing.T) {
	tests := []struct {
		name        string
		afterOffset int64
		limit       int
		offsets     []int64
		wantErr     bool
	}{
		{
			name:        "returns offsets in ascending order",
			afterOffset: 5,
			limit:       1000,
			offsets:     []int64{6, 7, 8},
		},
		{
			name:        "returns offsets with AUTO_INCREMENT gaps",
			afterOffset: 5,
			limit:       1000,
			offsets:     []int64{6, 9, 15},
		},
		{
			name:        "no messages above offset",
			afterOffset: 100,
			limit:       1000,
			offsets:     nil,
		},
		{
			name:        "db error",
			afterOffset: 0,
			limit:       1000,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupmessageStoreTest(t)
			defer db.Close()

			if tt.wantErr {
				mock.ExpectQuery("SELECT offset FROM queue_messages").
					WithArgs(testTenant, "test_topic", "part-1", tt.afterOffset, tt.limit).
					WillReturnError(fmt.Errorf("db error"))
			} else {
				rows := sqlmock.NewRows([]string{"offset"})
				for _, offset := range tt.offsets {
					rows.AddRow(offset)
				}
				mock.ExpectQuery("SELECT offset FROM queue_messages").
					WithArgs(testTenant, "test_topic", "part-1", tt.afterOffset, tt.limit).
					WillReturnRows(rows)
			}

			offsets, err := store.GetOffsetsAbove(context.Background(), testTenant, "test_topic", "part-1", tt.afterOffset, tt.limit)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.offsets, offsets)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestMessageStore_GarbageCollect(t *testing.T) {
	tests := []struct {
		name           string
		minAckedOffset int64
		deleteErr      bool
		wantDeleted    int64
		wantErr        bool
	}{
		{
			name:           "deletes messages up to min offset",
			minAckedOffset: 10,
			wantDeleted:    5,
		},
		{
			name:           "zero offset returns 0 deleted",
			minAckedOffset: 0,
			wantDeleted:    0,
		},
		{
			name:           "delete error",
			minAckedOffset: 10,
			deleteErr:      true,
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupmessageStoreTest(t)
			defer db.Close()

			if tt.minAckedOffset > 0 {
				if tt.deleteErr {
					mock.ExpectExec("DELETE FROM queue_messages").
						WithArgs(testTenant, "test_topic", "part-1", tt.minAckedOffset).
						WillReturnError(fmt.Errorf("db error"))
				} else {
					mock.ExpectExec("DELETE FROM queue_messages").
						WithArgs(testTenant, "test_topic", "part-1", tt.minAckedOffset).
						WillReturnResult(sqlmock.NewResult(0, tt.wantDeleted))
				}
			}

			deleted, err := store.GarbageCollect(context.Background(), testTenant, "test_topic", "part-1", tt.minAckedOffset)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantDeleted, deleted)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
