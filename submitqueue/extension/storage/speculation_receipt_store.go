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

package storage

//go:generate mockgen -source=speculation_receipt_store.go -destination=mock/speculation_receipt_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// SpeculationReceiptStore records the conditional-stage receipts of a head's
// speculation.
//
// A receipt is keyed by the full identity of one stage action: (head, stage,
// token), where token is the stable generation the action belongs to (the
// committed batch version for a land dispatch, the build ID for a build
// signal). Records are immutable and write-once — Create is the idempotency
// point of the whole stage. A redelivery, a controller restart, or a dead-
// letter republish that finds the receipt re-publishes under its MessageID
// instead of enacting the stage a second time; ErrAlreadyExists is how it
// learns the stage was already committed. There is no lookup by anything but
// the full key: every caller names the head and the generation it is resuming.
type SpeculationReceiptStore interface {
	// Get retrieves one stage receipt. Returns ErrNotFound if the stage has no
	// receipt yet, which is the normal state before it has been committed.
	Get(ctx context.Context, head string, stage entity.SpeculationReceiptStage, token string) (entity.SpeculationReceipt, error)

	// Create stores a stage receipt. Returns ErrAlreadyExists if a receipt with
	// the same (head, stage, token) already exists, meaning the stage was
	// committed by an earlier attempt that owns it.
	Create(ctx context.Context, receipt entity.SpeculationReceipt) error
}
