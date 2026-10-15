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
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap/zaptest"
)

type unixMillisAtLeast int64

func (minimum unixMillisAtLeast) Match(value driver.Value) bool {
	got, ok := value.(int64)
	return ok && got >= int64(minimum)
}

var testLeaseToken = partitionLeaseToken{leasedBy: testSubscriberName, generation: 1}

func leaseGuardArgsForTest() []driver.Value {
	return []driver.Value{testTenant, "group-1", "orders", "part-1", testSubscriberName, int64(1)}
}

func newTestDeliveryStateStoreWithMock(t *testing.T) (deliveryStateStore, *sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	store := newDeliveryStateStore(db, zaptest.NewLogger(t).Sugar(), tally.NoopScope)
	return store, db, mock
}

func TestDeliveryStateStore_MarkDelivered(t *testing.T) {
	tests := []struct {
		name           string
		execErr        bool
		fenced         bool
		queryErr       bool
		wantRetryCount int
		wantErr        bool
		wantFenced     bool
	}{
		{
			name:           "success new insert returns 0",
			wantRetryCount: 0,
		},
		{
			name:           "success redelivery returns incremented count",
			wantRetryCount: 3,
		},
		{
			name:    "exec error",
			execErr: true,
			wantErr: true,
		},
		{
			name:       "zero rows with lease lost is fenced",
			fenced:     true,
			wantFenced: true,
		},
		{
			name:     "query error on retry_count SELECT",
			queryErr: true,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, db, mock := newTestDeliveryStateStoreWithMock(t)
			defer db.Close()

			execArgs := append([]driver.Value{testTenant, "group-1", "orders", "part-1", int64(5), sqlmock.AnyArg()}, leaseGuardArgsForTest()...)
			switch {
			case tt.execErr:
				mock.ExpectExec("INSERT INTO queue_delivery_state").
					WithArgs(execArgs...).
					WillReturnError(assert.AnError)
			case tt.fenced:
				mock.ExpectExec("INSERT INTO queue_delivery_state").
					WithArgs(execArgs...).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery("SELECT 1 FROM queue_partition_leases").
					WithArgs(leaseGuardArgsForTest()...).
					WillReturnRows(sqlmock.NewRows([]string{"1"}))
			default:
				mock.ExpectExec("INSERT INTO queue_delivery_state").
					WithArgs(execArgs...).
					WillReturnResult(sqlmock.NewResult(1, 1))

				if tt.queryErr {
					mock.ExpectQuery("SELECT retry_count FROM queue_delivery_state").
						WithArgs(testTenant, "group-1", "orders", "part-1", int64(5)).
						WillReturnError(assert.AnError)
				} else {
					mock.ExpectQuery("SELECT retry_count FROM queue_delivery_state").
						WithArgs(testTenant, "group-1", "orders", "part-1", int64(5)).
						WillReturnRows(sqlmock.NewRows([]string{"retry_count"}).AddRow(tt.wantRetryCount))
				}
			}

			retryCount, err := store.MarkDelivered(context.Background(), testLeaseToken, "group-1", testTenant, "orders", "part-1", 5, 30000)

			switch {
			case tt.wantFenced:
				var fenced *ErrLeaseFenced
				require.ErrorAs(t, err, &fenced)
				assert.Equal(t, "part-1", fenced.PartitionKey)
				assert.Equal(t, int64(1), fenced.Generation)
			case tt.wantErr:
				require.Error(t, err)
			default:
				require.NoError(t, err)
				assert.Equal(t, tt.wantRetryCount, retryCount)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDeliveryStateStore_ZeroRowsWhileStillOwnedIsNoOp(t *testing.T) {
	store, db, mock := newTestDeliveryStateStoreWithMock(t)
	defer db.Close()

	// Re-acking an already acked row matches zero rows, but the lease still
	// belongs to the caller: the write is an idempotent no-op, not fenced.
	mock.ExpectExec("INSERT INTO queue_delivery_state").
		WithArgs(append([]driver.Value{testTenant, "group-1", "orders", "part-1", int64(5)}, leaseGuardArgsForTest()...)...).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT 1 FROM queue_partition_leases").
		WithArgs(leaseGuardArgsForTest()...).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))

	require.NoError(t, store.MarkAcked(context.Background(), testLeaseToken, "group-1", testTenant, "orders", "part-1", 5))
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDeliveryStateStore_ExtendVisibility(t *testing.T) {
	tests := []struct {
		name           string
		zeroRowsFenced bool
		zeroRowsNoOp   bool
		dbErr          bool
		wantErr        bool
		wantFenced     bool
	}{
		{name: "success"},
		{name: "db error", dbErr: true, wantErr: true},
		{name: "zero rows while lease lost is fenced", zeroRowsFenced: true, wantFenced: true},
		{name: "zero rows while still owner is no-op", zeroRowsNoOp: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, db, mock := newTestDeliveryStateStoreWithMock(t)
			defer db.Close()

			execArgs := append([]driver.Value{sqlmock.AnyArg(), testTenant, "group-1", "orders", "part-1", int64(5)}, leaseGuardArgsForTest()...)
			switch {
			case tt.dbErr:
				mock.ExpectExec("UPDATE queue_delivery_state").
					WithArgs(execArgs...).
					WillReturnError(errors.New("db down"))
			case tt.zeroRowsFenced:
				mock.ExpectExec("UPDATE queue_delivery_state").
					WithArgs(execArgs...).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery("SELECT 1 FROM queue_partition_leases").
					WithArgs(leaseGuardArgsForTest()...).
					WillReturnRows(sqlmock.NewRows([]string{"1"}))
			case tt.zeroRowsNoOp:
				mock.ExpectExec("UPDATE queue_delivery_state").
					WithArgs(execArgs...).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery("SELECT 1 FROM queue_partition_leases").
					WithArgs(leaseGuardArgsForTest()...).
					WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
			default:
				mock.ExpectExec("UPDATE queue_delivery_state").
					WithArgs(execArgs...).
					WillReturnResult(sqlmock.NewResult(0, 1))
			}

			err := store.ExtendVisibility(context.Background(), testLeaseToken, "group-1", testTenant, "orders", "part-1", 5, 60000)

			switch {
			case tt.wantFenced:
				var fenced *ErrLeaseFenced
				require.ErrorAs(t, err, &fenced)
			case tt.wantErr:
				require.Error(t, err)
			default:
				require.NoError(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDeliveryStateStore_MarkTerminalFenced(t *testing.T) {
	// Every terminal write (Ack/Nack/Postpone) reports ErrLeaseFenced on a
	// zero-row guarded statement when the lease no longer matches.
	cases := []struct {
		name string
		call func(t *testing.T, store deliveryStateStore) error
	}{
		{
			name: "mark acked fenced",
			call: func(t *testing.T, store deliveryStateStore) error {
				return store.MarkAcked(context.Background(), testLeaseToken, "group-1", testTenant, "orders", "part-1", 5)
			},
		},
		{
			name: "mark nacked fenced",
			call: func(t *testing.T, store deliveryStateStore) error {
				return store.MarkNacked(context.Background(), testLeaseToken, "group-1", testTenant, "orders", "part-1", 5, 1000)
			},
		},
		{
			name: "mark postponed fenced",
			call: func(t *testing.T, store deliveryStateStore) error {
				return store.MarkPostponed(context.Background(), testLeaseToken, "group-1", testTenant, "orders", "part-1", 5, 5000)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, db, mock := newTestDeliveryStateStoreWithMock(t)
			defer db.Close()

			mock.ExpectExec("INSERT INTO queue_delivery_state").
				WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery("SELECT 1 FROM queue_partition_leases").
				WithArgs(leaseGuardArgsForTest()...).
				WillReturnRows(sqlmock.NewRows([]string{"1"}))

			err := tc.call(t, store)
			var fenced *ErrLeaseFenced
			require.ErrorAs(t, err, &fenced)
			assert.Equal(t, testSubscriberName, fenced.SubscriberName)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDeliveryStateStore_MarkAcked(t *testing.T) {
	tests := []struct {
		name    string
		dbErr   bool
		wantErr bool
	}{
		{name: "success"},
		{name: "db error", dbErr: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, db, mock := newTestDeliveryStateStoreWithMock(t)
			defer db.Close()

			args := append([]driver.Value{testTenant, "group-1", "orders", "part-1", int64(5)}, leaseGuardArgsForTest()...)
			if tt.dbErr {
				mock.ExpectExec("INSERT INTO queue_delivery_state").
					WithArgs(args...).
					WillReturnError(assert.AnError)
			} else {
				mock.ExpectExec("INSERT INTO queue_delivery_state").
					WithArgs(args...).
					WillReturnResult(sqlmock.NewResult(1, 1))
			}

			err := store.MarkAcked(context.Background(), testLeaseToken, "group-1", testTenant, "orders", "part-1", 5)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDeliveryStateStore_MarkNacked(t *testing.T) {
	tests := []struct {
		name    string
		dbErr   bool
		wantErr bool
	}{
		{name: "success"},
		{name: "db error", dbErr: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, db, mock := newTestDeliveryStateStoreWithMock(t)
			defer db.Close()

			const retryDelayMs = int64(1000)
			invisibleUntil := unixMillisAtLeast(time.Now().UnixMilli() + retryDelayMs)
			args := append([]driver.Value{testTenant, "group-1", "orders", "part-1", int64(5), invisibleUntil}, leaseGuardArgsForTest()...)

			if tt.dbErr {
				mock.ExpectExec("INSERT INTO queue_delivery_state").
					WithArgs(args...).
					WillReturnError(assert.AnError)
			} else {
				mock.ExpectExec("INSERT INTO queue_delivery_state").
					WithArgs(args...).
					WillReturnResult(sqlmock.NewResult(1, 1))
			}

			err := store.MarkNacked(context.Background(), testLeaseToken, "group-1", testTenant, "orders", "part-1", 5, retryDelayMs)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDeliveryStateStore_MarkNackedRejectsInvalidDelay(t *testing.T) {
	tests := []struct {
		name    string
		delayMs int64
	}{
		{name: "negative", delayMs: -1},
		{name: "above safety ceiling", delayMs: maxRetryBackoffMs + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, db, mock := newTestDeliveryStateStoreWithMock(t)
			defer db.Close()

			err := store.MarkNacked(context.Background(), testLeaseToken, "group-1", testTenant, "orders", "part-1", 5, tt.delayMs)
			require.ErrorContains(t, err, "is outside")
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDeliveryStateStore_MarkPostponed(t *testing.T) {
	tests := []struct {
		name    string
		dbErr   bool
		wantErr bool
	}{
		{name: "success"},
		{name: "db error", dbErr: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, db, mock := newTestDeliveryStateStoreWithMock(t)
			defer db.Close()

			args := append([]driver.Value{testTenant, "group-1", "orders", "part-1", int64(5), sqlmock.AnyArg()}, leaseGuardArgsForTest()...)
			if tt.dbErr {
				mock.ExpectExec("INSERT INTO queue_delivery_state").
					WithArgs(args...).
					WillReturnError(assert.AnError)
			} else {
				mock.ExpectExec("INSERT INTO queue_delivery_state").
					WithArgs(args...).
					WillReturnResult(sqlmock.NewResult(1, 1))
			}

			err := store.MarkPostponed(context.Background(), testLeaseToken, "group-1", testTenant, "orders", "part-1", 5, 5000)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDeliveryStateStore_GetDeliveryState(t *testing.T) {
	tests := []struct {
		name           string
		acked          bool
		invisibleUntil int64
		retryCount     int
		postponed      bool
		noRows         bool
		wantErr        bool
		wantFound      bool
	}{
		{
			name:      "no delivery state row returns not found",
			noRows:    true,
			wantFound: false,
		},
		{
			name:           "acked message",
			acked:          true,
			invisibleUntil: 0,
			retryCount:     2,
			wantFound:      true,
		},
		{
			name:           "in-flight message",
			acked:          false,
			invisibleUntil: 9999999999999,
			retryCount:     1,
			wantFound:      true,
		},
		{
			name:           "expired visibility",
			acked:          false,
			invisibleUntil: 1000,
			retryCount:     3,
			wantFound:      true,
		},
		{
			name:           "postponed message",
			acked:          false,
			invisibleUntil: 9999999999999,
			retryCount:     0,
			postponed:      true,
			wantFound:      true,
		},
		{
			name:    "db error",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, db, mock := newTestDeliveryStateStoreWithMock(t)
			defer db.Close()

			if tt.wantErr {
				mock.ExpectQuery("SELECT acked, invisible_until, retry_count, postponed FROM queue_delivery_state").
					WithArgs(testTenant, "group-1", "orders", "part-1", int64(5)).
					WillReturnError(assert.AnError)
			} else if tt.noRows {
				mock.ExpectQuery("SELECT acked, invisible_until, retry_count, postponed FROM queue_delivery_state").
					WithArgs(testTenant, "group-1", "orders", "part-1", int64(5)).
					WillReturnRows(sqlmock.NewRows([]string{"acked", "invisible_until", "retry_count", "postponed"}))
			} else {
				mock.ExpectQuery("SELECT acked, invisible_until, retry_count, postponed FROM queue_delivery_state").
					WithArgs(testTenant, "group-1", "orders", "part-1", int64(5)).
					WillReturnRows(sqlmock.NewRows([]string{"acked", "invisible_until", "retry_count", "postponed"}).
						AddRow(tt.acked, tt.invisibleUntil, tt.retryCount, tt.postponed))
			}

			state, found, err := store.GetDeliveryState(context.Background(), "group-1", testTenant, "orders", "part-1", 5)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantFound, found)
				if found {
					assert.Equal(t, tt.acked, state.Acked)
					assert.Equal(t, tt.invisibleUntil, state.InvisibleUntil)
					assert.Equal(t, tt.retryCount, state.RetryCount)
					assert.Equal(t, tt.postponed, state.Postponed)
				}
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDeliveryStateStore_AdvanceWatermark(t *testing.T) {
	tests := []struct {
		name             string
		currentWatermark int64
		offsets          []int64 // message offsets passed in (from messageStore)
		dsRows           []struct {
			offset int64
			acked  bool
		}
		dsQueryErr      bool
		expectWatermark int64
		expectCleanup   bool
	}{
		{
			name:             "no offsets",
			currentWatermark: 5,
			offsets:          nil,
			expectWatermark:  5,
			expectCleanup:    false,
		},
		{
			name:             "acked offsets advance watermark",
			currentWatermark: 5,
			offsets:          []int64{6, 7, 8},
			dsRows: []struct {
				offset int64
				acked  bool
			}{
				{offset: 6, acked: true},
				{offset: 7, acked: true},
				{offset: 8, acked: true},
			},
			expectWatermark: 8,
			expectCleanup:   true,
		},
		{
			name:             "gap in offsets does not block advancement",
			currentWatermark: 5,
			offsets:          []int64{6, 8}, // gap: offset 7 does not exist (AUTO_INCREMENT gap)
			dsRows: []struct {
				offset int64
				acked  bool
			}{
				{offset: 6, acked: true},
				{offset: 8, acked: true},
			},
			expectWatermark: 8,
			expectCleanup:   true,
		},
		{
			name:             "non-acked offset stops advancement",
			currentWatermark: 5,
			offsets:          []int64{6, 7, 8},
			dsRows: []struct {
				offset int64
				acked  bool
			}{
				{offset: 6, acked: true},
				{offset: 7, acked: false}, // in-flight, not acked
				{offset: 8, acked: true},
			},
			expectWatermark: 6,
			expectCleanup:   true,
		},
		{
			name:             "undelivered message stops advancement",
			currentWatermark: 5,
			offsets:          []int64{6, 7},
			dsRows: []struct {
				offset int64
				acked  bool
			}{
				{offset: 6, acked: true},
				// offset 7 has no delivery state row (undelivered)
			},
			expectWatermark: 6,
			expectCleanup:   true,
		},
		{
			name:             "first offset not acked means no advancement",
			currentWatermark: 5,
			offsets:          []int64{6},
			dsRows: []struct {
				offset int64
				acked  bool
			}{
				{offset: 6, acked: false}, // in-flight
			},
			expectWatermark: 5,
			expectCleanup:   false,
		},
		{
			name:             "delivery state query error returns current watermark",
			currentWatermark: 5,
			offsets:          []int64{6, 7},
			dsQueryErr:       true,
			expectWatermark:  5,
			expectCleanup:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, db, mock := newTestDeliveryStateStoreWithMock(t)
			defer db.Close()

			// Delivery state query is only issued if there are offsets
			if len(tt.offsets) > 0 {
				dsArgs := make([]driver.Value, 0, 4+len(tt.offsets))
				dsArgs = append(dsArgs, testTenant, "group-1", "orders", "part-1")
				for _, offset := range tt.offsets {
					dsArgs = append(dsArgs, offset)
				}

				if tt.dsQueryErr {
					mock.ExpectQuery("SELECT message_offset, acked FROM queue_delivery_state").
						WithArgs(dsArgs...).
						WillReturnError(assert.AnError)
				} else {
					dsResultRows := sqlmock.NewRows([]string{"message_offset", "acked"})
					for _, r := range tt.dsRows {
						dsResultRows.AddRow(r.offset, r.acked)
					}
					mock.ExpectQuery("SELECT message_offset, acked FROM queue_delivery_state").
						WithArgs(dsArgs...).
						WillReturnRows(dsResultRows)
				}
			}

			if tt.expectCleanup {
				cleanupArgs := append([]driver.Value{testTenant, "group-1", "orders", "part-1", tt.expectWatermark}, leaseGuardArgsForTest()...)
				mock.ExpectExec("DELETE FROM queue_delivery_state").
					WithArgs(cleanupArgs...).
					WillReturnResult(sqlmock.NewResult(0, tt.expectWatermark-tt.currentWatermark))
			}

			watermark, err := store.AdvanceWatermark(context.Background(), testLeaseToken, "group-1", testTenant, "orders", "part-1", tt.currentWatermark, tt.offsets)

			if tt.dsQueryErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.expectWatermark, watermark)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
