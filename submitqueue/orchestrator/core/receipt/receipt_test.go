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

package receipt

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

func wantReceipt() entity.SpeculationReceipt {
	return entity.SpeculationReceipt{
		Queue:       "q",
		Head:        "batch-1",
		Stage:       entity.SpeculationStageLandDispatch,
		Token:       "7",
		MessageID:   "batch-1/land-dispatch",
		CreatedAtMs: 1000,
	}
}

// An absent stage is created and reported as new.
func TestEnsure_CreatesWhenAbsent(t *testing.T) {
	ctrl := gomock.NewController(t)
	receipts := storagemock.NewMockSpeculationReceiptStore(ctrl)
	want := wantReceipt()

	gomock.InOrder(
		receipts.EXPECT().Get(gomock.Any(), "batch-1", entity.SpeculationStageLandDispatch, "7").
			Return(entity.SpeculationReceipt{}, storage.ErrNotFound),
		receipts.EXPECT().Create(gomock.Any(), want).Return(nil),
	)

	got, created, err := Ensure(context.Background(), receipts, want)
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, want, got)
}

// A stage a prior delivery already committed is adopted and not rewritten.
func TestEnsure_AdoptsExistingReceipt(t *testing.T) {
	ctrl := gomock.NewController(t)
	receipts := storagemock.NewMockSpeculationReceiptStore(ctrl)
	existing := wantReceipt()
	existing.CreatedAtMs = 900

	receipts.EXPECT().Get(gomock.Any(), "batch-1", entity.SpeculationStageLandDispatch, "7").
		Return(existing, nil)

	got, created, err := Ensure(context.Background(), receipts, wantReceipt())
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, existing, got)
}

// A concurrent delivery that wins the create race is followed: the loser
// reads the winner's record and reports it as pre-existing.
func TestEnsure_AdoptsTheWinnerAfterAnAlreadyExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	receipts := storagemock.NewMockSpeculationReceiptStore(ctrl)
	want := wantReceipt()
	winner := want
	winner.CreatedAtMs = 1

	gomock.InOrder(
		receipts.EXPECT().Get(gomock.Any(), "batch-1", entity.SpeculationStageLandDispatch, "7").
			Return(entity.SpeculationReceipt{}, storage.ErrNotFound),
		receipts.EXPECT().Create(gomock.Any(), want).Return(storage.ErrAlreadyExists),
		receipts.EXPECT().Get(gomock.Any(), "batch-1", entity.SpeculationStageLandDispatch, "7").
			Return(winner, nil),
	)

	got, created, err := Ensure(context.Background(), receipts, want)
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, winner, got)
}

// A genuine read error surfaces.
func TestEnsure_PropagatesReadError(t *testing.T) {
	ctrl := gomock.NewController(t)
	receipts := storagemock.NewMockSpeculationReceiptStore(ctrl)
	want := wantReceipt()
	readErr := errors.New("connection reset")

	receipts.EXPECT().Get(gomock.Any(), "batch-1", entity.SpeculationStageLandDispatch, "7").
		Return(entity.SpeculationReceipt{}, readErr)

	_, _, err := Ensure(context.Background(), receipts, want)
	assert.ErrorIs(t, err, readErr)
}

// A genuine write error surfaces rather than being treated as a race.
func TestEnsure_PropagatesCreateError(t *testing.T) {
	ctrl := gomock.NewController(t)
	receipts := storagemock.NewMockSpeculationReceiptStore(ctrl)
	want := wantReceipt()

	receipts.EXPECT().Get(gomock.Any(), "batch-1", entity.SpeculationStageLandDispatch, "7").
		Return(entity.SpeculationReceipt{}, storage.ErrNotFound)
	receipts.EXPECT().Create(gomock.Any(), want).Return(errors.New("connection reset"))

	_, created, err := Ensure(context.Background(), receipts, want)
	require.Error(t, err)
	assert.False(t, created)
}
