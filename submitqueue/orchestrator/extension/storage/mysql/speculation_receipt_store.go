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
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

type speculationReceiptStore struct {
	db    *sql.DB
	scope tally.Scope
	// queue is the queue name this store instance is bound to; every read and
	// write is scoped to it.
	queue string
}

// NewSpeculationReceiptStore creates a new MySQL-backed SpeculationReceiptStore.
func NewSpeculationReceiptStore(db *sql.DB, scope tally.Scope, queue string) storage.SpeculationReceiptStore {
	return &speculationReceiptStore{db: db, scope: scope, queue: queue}
}

// Get retrieves one stage receipt. Returns ErrNotFound if the stage has no
// receipt yet.
func (s *speculationReceiptStore) Get(ctx context.Context, head string, stage entity.SpeculationReceiptStage, token string) (ret entity.SpeculationReceipt, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	var receipt entity.SpeculationReceipt

	err := s.db.QueryRowContext(ctx,
		"SELECT queue, head, stage, token, message_id, created_at_ms FROM speculation_receipt WHERE queue = ? AND head = ? AND stage = ? AND token = ?",
		s.queue, head, string(stage), token,
	).Scan(&receipt.Queue, &receipt.Head, &receipt.Stage, &receipt.Token, &receipt.MessageID, &receipt.CreatedAtMs)

	if errors.Is(err, sql.ErrNoRows) {
		return entity.SpeculationReceipt{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.SpeculationReceipt{}, fmt.Errorf("failed to get speculation receipt entity queue=%s head=%s stage=%s token=%s from the database: %w", s.queue, head, stage, token, err)
	}

	return receipt, nil
}

// Create stores one stage receipt. Returns ErrAlreadyExists if a receipt with
// the same (head, stage, token) already exists.
func (s *speculationReceiptStore) Create(ctx context.Context, receipt entity.SpeculationReceipt) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if receipt.Queue != s.queue {
		return fmt.Errorf("speculation receipt head=%s stage=%s queue %q does not match the store's bound queue %q", receipt.Head, receipt.Stage, receipt.Queue, s.queue)
	}

	_, err := s.db.ExecContext(ctx,
		"INSERT INTO speculation_receipt (queue, head, stage, token, message_id, created_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
		s.queue, receipt.Head, string(receipt.Stage), receipt.Token, receipt.MessageID, receipt.CreatedAtMs,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry {
			return fmt.Errorf("speculation receipt entity queue=%s head=%s stage=%s token=%s: %w", s.queue, receipt.Head, receipt.Stage, receipt.Token, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert speculation receipt entity queue=%s head=%s stage=%s token=%s: %w", s.queue, receipt.Head, receipt.Stage, receipt.Token, err)
	}

	return nil
}
