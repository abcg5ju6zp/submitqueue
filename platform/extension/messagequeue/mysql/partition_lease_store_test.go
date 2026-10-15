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
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap/zaptest"
)

const testLeaseDurationMs = 30000 // 30 seconds in milliseconds

func setuppartitionLeaseStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, partitionLeaseStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := newPartitionLeaseStore(db, zaptest.NewLogger(t).Sugar(), tally.NoopScope)

	return db, mock, store
}

func TestPartitionLeaseStore_TryAcquireLease(t *testing.T) {
	tests := []struct {
		name           string
		setup          func(mock sqlmock.Sqlmock)
		acquired       bool
		wantGeneration int64
		wantErr        bool
	}{
		{
			name: "successfully acquire lease",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO queue_partition_leases").
					WithArgs(testTenant, testConsumerGroup, "test_topic", "part1", testSubscriberName, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(1, 1))
				rows := sqlmock.NewRows([]string{"leased_by", "lease_generation"}).AddRow(testSubscriberName, 1)
				mock.ExpectQuery("SELECT leased_by, lease_generation FROM queue_partition_leases").
					WithArgs(testTenant, testConsumerGroup, "test_topic", "part1").
					WillReturnRows(rows)
			},
			acquired:       true,
			wantGeneration: 1,
			wantErr:        false,
		},
		{
			name: "lease acquired by other worker",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO queue_partition_leases").
					WillReturnResult(sqlmock.NewResult(1, 1))
				rows := sqlmock.NewRows([]string{"leased_by", "lease_generation"}).AddRow("other-worker", 2)
				mock.ExpectQuery("SELECT leased_by, lease_generation FROM queue_partition_leases").
					WithArgs(testTenant, testConsumerGroup, "test_topic", "part1").
					WillReturnRows(rows)
			},
			acquired:       false,
			wantGeneration: 2,
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setuppartitionLeaseStoreTest(t)
			defer db.Close()

			ctx := context.Background()
			topic := "test_topic"
			partitionKey := "part1"

			tt.setup(mock)

			acquired, generation, err := store.TryAcquireLease(ctx, testTenant, topic, partitionKey, testSubscriberName, testConsumerGroup, testLeaseDurationMs)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.acquired, acquired)
				require.Equal(t, tt.wantGeneration, generation)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestPartitionLeaseStore_RenewLease(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(mock sqlmock.Sqlmock)
		wantRenew bool
		wantErr   bool
	}{
		{
			name: "renewal succeeds with matching fencing token",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE queue_partition_leases").
					WithArgs(sqlmock.AnyArg(), testTenant, testConsumerGroup, "test_topic", "part1", testSubscriberName, int64(3)).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			wantRenew: true,
		},
		{
			name: "zero rows means the lease was lost",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE queue_partition_leases").
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantRenew: false,
		},
		{
			name: "rows affected diagnostic failure is treated as renewed",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE queue_partition_leases").
					WillReturnResult(sqlmock.NewErrorResult(errors.New("unsupported")))
			},
			wantRenew: true,
		},
		{
			name: "database error is returned",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE queue_partition_leases").
					WillReturnError(errors.New("db unavailable"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setuppartitionLeaseStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			renewed, err := store.RenewLease(context.Background(), testTenant, "test_topic", "part1", testSubscriberName, testConsumerGroup, 3)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantRenew, renewed)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestPartitionLeaseStore_ReleaseLease(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(mock sqlmock.Sqlmock)
		wantRows int64
		wantErr  bool
	}{
		{
			name: "successfully release lease",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM queue_partition_leases").
					WithArgs(testTenant, testConsumerGroup, "test_topic", "part1", testSubscriberName, int64(2)).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			wantRows: 1,
			wantErr:  false,
		},
		{
			name: "stale generation or already released deletes nothing",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM queue_partition_leases").
					WithArgs(testTenant, testConsumerGroup, "test_topic", "part1", testSubscriberName, int64(2)).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantRows: 0,
			wantErr:  false,
		},
		{
			name: "rows affected unavailable",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM queue_partition_leases").
					WithArgs(testTenant, testConsumerGroup, "test_topic", "part1", testSubscriberName, int64(2)).
					WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected unsupported")))
			},
			wantRows: 0,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setuppartitionLeaseStoreTest(t)
			defer db.Close()

			ctx := context.Background()
			topic := "test_topic"
			partitionKey := "part1"

			tt.setup(mock)

			rows, err := store.ReleaseLease(ctx, testTenant, topic, partitionKey, testSubscriberName, testConsumerGroup, 2)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantRows, rows)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestPartitionLeaseStore_DiscoverPartitionsForTenants(t *testing.T) {
	db, mock, store := setuppartitionLeaseStoreTest(t)
	defer db.Close()

	tenants := []string{"alpha", "beta"}
	mock.ExpectQuery("SELECT DISTINCT tenant, partition_key FROM queue_messages").
		WithArgs("alpha", "beta", "test_topic").
		WillReturnRows(sqlmock.NewRows([]string{"tenant", "partition_key"}).
			AddRow("alpha", "p1").
			AddRow("beta", "p2").
			AddRow("beta", "p3"))

	got, err := store.DiscoverPartitions(context.Background(), tenants, "test_topic")
	require.NoError(t, err)
	require.Equal(t, map[string][]string{
		"alpha": {"p1"},
		"beta":  {"p2", "p3"},
	}, got)

	empty, err := store.DiscoverPartitions(context.Background(), nil, "test_topic")
	require.NoError(t, err)
	require.Empty(t, empty)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPartitionLeaseStore_GetLeasedPartitionsForTenants(t *testing.T) {
	db, mock, store := setuppartitionLeaseStoreTest(t)
	defer db.Close()

	tenants := []string{"alpha", "gamma"}
	mock.ExpectQuery("SELECT tenant, partition_key, lease_generation FROM queue_partition_leases").
		WithArgs("alpha", "gamma", testConsumerGroup, "test_topic", testSubscriberName).
		WillReturnRows(sqlmock.NewRows([]string{"tenant", "partition_key", "lease_generation"}).
			AddRow("alpha", "p1", 4))

	got, err := store.GetLeasedPartitionsForTenants(context.Background(), tenants, "test_topic", testSubscriberName, testConsumerGroup)
	require.NoError(t, err)
	require.Equal(t, map[string][]leaseInfo{"alpha": {{
		PartitionKey: "p1",
		LeasedBy:     testSubscriberName,
		Generation:   4,
	}}}, got)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPartitionLeaseStore_GetAllLeasesForTenants(t *testing.T) {
	db, mock, store := setuppartitionLeaseStoreTest(t)
	defer db.Close()

	tenants := []string{"alpha", "beta", "gamma"}
	mock.ExpectQuery("SELECT tenant, partition_key, leased_by, lease_generation, lease_renewed_at FROM queue_partition_leases").
		WithArgs("alpha", "beta", "gamma", testConsumerGroup, "test_topic").
		WillReturnRows(sqlmock.NewRows([]string{"tenant", "partition_key", "leased_by", "lease_generation", "lease_renewed_at"}).
			AddRow("alpha", "p1", testSubscriberName, int64(1), int64(1000)).
			AddRow("beta", "p2", "other-worker", int64(2), int64(2000)).
			AddRow("beta", "p3", testSubscriberName, int64(1), int64(3000)))

	got, err := store.GetAllLeasesForTenants(context.Background(), tenants, "test_topic", testConsumerGroup)
	require.NoError(t, err)
	require.Equal(t, map[string][]leaseInfo{
		"alpha": {
			{PartitionKey: "p1", LeasedBy: testSubscriberName, Generation: 1, LeaseRenewedAt: 1000},
		},
		"beta": {
			{PartitionKey: "p2", LeasedBy: "other-worker", Generation: 2, LeaseRenewedAt: 2000},
			{PartitionKey: "p3", LeasedBy: testSubscriberName, Generation: 1, LeaseRenewedAt: 3000},
		},
	}, got)

	empty, err := store.GetAllLeasesForTenants(context.Background(), nil, "test_topic", testConsumerGroup)
	require.NoError(t, err)
	require.Empty(t, empty)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPartitionLeaseStore_CheckLeaseOwnership(t *testing.T) {
	tests := []struct {
		name   string
		rows   *sqlmock.Rows
		expect bool
	}{
		{
			name:   "matching token is owned",
			rows:   sqlmock.NewRows([]string{"1"}).AddRow(1),
			expect: true,
		},
		{
			name:   "mismatching token is fenced",
			rows:   sqlmock.NewRows([]string{"1"}),
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setuppartitionLeaseStoreTest(t)
			defer db.Close()

			mock.ExpectQuery("SELECT 1 FROM queue_partition_leases").
				WithArgs(testTenant, testConsumerGroup, "test_topic", "part1", testSubscriberName, int64(1)).
				WillReturnRows(tt.rows)

			owned, err := store.CheckLeaseOwnership(context.Background(), partitionLeaseToken{leasedBy: testSubscriberName, generation: 1},
				testTenant, "test_topic", "part1", testConsumerGroup)
			require.NoError(t, err)
			assert.Equal(t, tt.expect, owned)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
