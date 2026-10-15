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

// Package receipt holds the shared get-or-create primitive for a
// speculation stage's action receipt.
package receipt

import (
	"context"
	"errors"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// Ensure returns the stored receipt for want's identity, creating it when it is
// still absent. created reports whether this call wrote it: a receipt found on
// Get or lost to a concurrent writer's Create is reported as created=false, so
// callers publish once the receipt exists either way and count the two paths
// separately.
//
// This is the persistence half of persist-before-publish: the caller creates
// the receipt with the exact wire identity the stage must publish under, then
// publishes. A redelivery, a restart, or a dead-letter republish finds the
// receipt here and replays the publish under want.MessageID instead of
// enacting the stage a second time.
func Ensure(ctx context.Context, receipts storage.SpeculationReceiptStore, want entity.SpeculationReceipt) (entity.SpeculationReceipt, bool, error) {
	got, err := receipts.Get(ctx, want.Head, want.Stage, want.Token)
	if err == nil {
		return got, false, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return entity.SpeculationReceipt{}, false, err
	}

	if err := receipts.Create(ctx, want); err != nil {
		if !errors.Is(err, storage.ErrAlreadyExists) {
			return entity.SpeculationReceipt{}, false, err
		}
		// A concurrent delivery of the same stage committed first; adopt its
		// record rather than racing it.
		got, err := receipts.Get(ctx, want.Head, want.Stage, want.Token)
		if err != nil {
			return entity.SpeculationReceipt{}, false, err
		}
		return got, false, nil
	}
	return want, true, nil
}
