// Copyright (c) 2026 Uber Technologies, Inc.
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

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

func setupSpeculationReceiptStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.SpeculationReceiptStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := NewSpeculationReceiptStore(db, testMetrics(), testSpecQueue)

	return db, mock, store
}

func testReceipt() entity.SpeculationReceipt {
	return entity.SpeculationReceipt{
		Queue:       testSpecQueue,
		Head:        "2",
		Stage:       entity.SpeculationStageLandDispatch,
		Token:       "7",
		MessageID:   "2/land-dispatch",
		CreatedAtMs: 42000,
	}
}

func TestSpeculationReceiptStore_Get(t *testing.T) {
	want := testReceipt()

	tests := []struct {
		name      string
		token     string
		setup     func(mock sqlmock.Sqlmock)
		want      entity.SpeculationReceipt
		wantErr   bool
		wantErrIs error
	}{
		{
			name:  "found",
			token: want.Token,
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"queue", "head", "stage", "token", "message_id", "created_at_ms"}).
					AddRow(want.Queue, want.Head, string(want.Stage), want.Token, want.MessageID, want.CreatedAtMs)
				mock.ExpectQuery("SELECT queue, head, stage, token, message_id, created_at_ms FROM speculation_receipt").
					WithArgs(testSpecQueue, want.Head, string(want.Stage), want.Token).
					WillReturnRows(rows)
			},
			want: want,
		},
		{
			name:  "not found",
			token: "missing",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, head, stage, token, message_id, created_at_ms FROM speculation_receipt").
					WithArgs(testSpecQueue, want.Head, string(want.Stage), "missing").
					WillReturnError(sql.ErrNoRows)
			},
			wantErr:   true,
			wantErrIs: storage.ErrNotFound,
		},
		{
			name:  "query error",
			token: want.Token,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, head, stage, token, message_id, created_at_ms FROM speculation_receipt").
					WithArgs(testSpecQueue, want.Head, string(want.Stage), want.Token).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupSpeculationReceiptStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			got, err := store.Get(context.Background(), want.Head, want.Stage, tt.token)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					assert.ErrorIs(t, err, tt.wantErrIs)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSpeculationReceiptStore_Create(t *testing.T) {
	receipt := testReceipt()
	otherQueueReceipt := receipt
	otherQueueReceipt.Queue = "other-queue"

	tests := []struct {
		name      string
		receipt   entity.SpeculationReceipt
		setup     func(mock sqlmock.Sqlmock)
		wantErr   bool
		wantErrIs error
	}{
		{
			name:    "success",
			receipt: receipt,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO speculation_receipt").
					WithArgs(
						receipt.Queue, receipt.Head, string(receipt.Stage),
						receipt.Token, receipt.MessageID, receipt.CreatedAtMs,
					).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name:    "duplicate identity returns ErrAlreadyExists",
			receipt: receipt,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO speculation_receipt").
					WithArgs(
						receipt.Queue, receipt.Head, string(receipt.Stage),
						receipt.Token, receipt.MessageID, receipt.CreatedAtMs,
					).
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			wantErr:   true,
			wantErrIs: storage.ErrAlreadyExists,
		},
		{
			name:    "other exec error",
			receipt: receipt,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO speculation_receipt").
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
		{
			name:      "queue mismatch is rejected without touching the database",
			receipt:   otherQueueReceipt,
			setup:     func(sqlmock.Sqlmock) {},
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupSpeculationReceiptStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.Create(context.Background(), tt.receipt)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					assert.ErrorIs(t, err, tt.wantErrIs)
				}
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
