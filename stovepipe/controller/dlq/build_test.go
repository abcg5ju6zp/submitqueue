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

package dlq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	mqmock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	requestlogmock "github.com/uber/submitqueue/stovepipe/core/requestlog/mock"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func newBuildController(t *testing.T, ctrl *gomock.Controller) (consumer.Controller, dlqMocks) {
	t.Helper()

	scope := tally.NewTestScope("test", nil)
	m := dlqMocks{
		reqStore:     storagemock.NewMockRequestStore(ctrl),
		queueStore:   storagemock.NewMockQueueStore(ctrl),
		logStore:     storagemock.NewMockRequestLogStore(ctrl),
		buildStore:   storagemock.NewMockBuildStore(ctrl),
		store:        storagemock.NewMockStorage(ctrl),
		materializer: requestlogmock.NewMockMaterializer(ctrl),
		publisher:    mqmock.NewMockPublisher(ctrl),
		metricsScope: scope,
	}
	m.store.EXPECT().GetRequestStore().Return(m.reqStore).AnyTimes()
	m.store.EXPECT().GetQueueStore().Return(m.queueStore).AnyTimes()
	m.store.EXPECT().GetRequestLogStore().Return(m.logStore).AnyTimes()
	m.store.EXPECT().GetBuildStore().Return(m.buildStore).AnyTimes()

	queue := mqmock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(m.publisher).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: stovepipemq.TopicKeyBuildSignal, Name: "buildsignal", Queue: queue},
	})
	require.NoError(t, err)

	c := NewDLQBuildController(zap.NewNop().Sugar(), scope, staticStorageFactory{store: m.store}, m.materializer, registry, TopicKey(stovepipemq.TopicKeyBuild), "stovepipe-build-dlq")
	return c, m
}

// buildTriggerLogs returns the retained occurrences of a request whose build triggered.
func buildTriggerLogs() []entity.RequestLog {
	return []entity.RequestLog{requestlog.NewRequestEventLog(
		entity.Request{ID: testID, Queue: testQueue},
		entity.RequestEventBuildTriggered,
		testBuildID,
		map[string]string{requestlog.MetadataKeyBuildID: testBuildID},
	)}
}

// triggeredBuildRow returns the Build row of the durably triggered build.
func triggeredBuildRow() entity.Build {
	return entity.Build{
		ID:        testBuildID,
		RequestID: testID,
		Status:    entity.BuildStatusAccepted,
		Version:   1,
	}
}

func buildPayload(t *testing.T, id string) []byte {
	t.Helper()
	payload, err := stovepipemq.Marshal(&stovepipemq.BuildRequest{Id: id, QueueName: testQueue})
	require.NoError(t, err)
	return payload
}

func TestBuildProcess(t *testing.T) {
	tests := []struct {
		name       string
		payload    []byte
		tenant     string
		setup      func(m dlqMocks)
		wantErr    bool
		wantMetric string
	}{
		{
			name: "processing request without a durable trigger is failed",
			setup: func(m dlqMocks) {
				// failRequest reloads the request after the resume preload.
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil).Times(2)
				m.logStore.EXPECT().List(gomock.Any(), testID).Return(nil, storage.ErrNotFound)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{Name: testQueue, InFlightCount: 1, Version: 5}, nil)
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{Name: testQueue, Version: 5}, int32(5), int32(6)).Return(nil)
				updated := requestWithState(entity.RequestStateProcessing)
				updated.State = entity.RequestStateFailed
				updateCall := m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(2), int32(3)).Return(nil)
				expectFailureLog(m, entity.RequestOutcomeReasonProcessingFailed, 3).After(updateCall)
			},
			wantMetric: "test.build_dlq_controller.build_dlq.reconciled+queue=monorepo/main",
		},
		{
			// failRequest skips a non-failed terminal request: no log lookup, no write, no publish.
			name:       "terminal request is skipped without resume",
			wantMetric: "test.build_dlq_controller.build_dlq.reconciled+queue=monorepo/main",
			setup: func(m dlqMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateSucceeded), nil).Times(2)
			},
		},
		{
			name: "durable trigger with missing build row fails closed",
			setup: func(m dlqMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil).Times(2)
				m.logStore.EXPECT().List(gomock.Any(), testID).Return(buildTriggerLogs(), nil)
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{}, storage.ErrNotFound)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{Name: testQueue, InFlightCount: 1, Version: 5}, nil)
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{Name: testQueue, Version: 5}, int32(5), int32(6)).Return(nil)
				updated := requestWithState(entity.RequestStateProcessing)
				updated.State = entity.RequestStateFailed
				updateCall := m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(2), int32(3)).Return(nil)
				expectFailureLog(m, entity.RequestOutcomeReasonProcessingFailed, 3).After(updateCall)
			},
			wantMetric: "test.build_dlq_controller.build_dlq.reconciled+queue=monorepo/main",
		},
		{
			name: "durable trigger owned by another request fails closed",
			setup: func(m dlqMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil).Times(2)
				m.logStore.EXPECT().List(gomock.Any(), testID).Return(buildTriggerLogs(), nil)
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{
					ID: testBuildID, RequestID: "9", Status: entity.BuildStatusRunning, Version: 2,
				}, nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{Name: testQueue, InFlightCount: 1, Version: 5}, nil)
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{Name: testQueue, Version: 5}, int32(5), int32(6)).Return(nil)
				updated := requestWithState(entity.RequestStateProcessing)
				updated.State = entity.RequestStateFailed
				updateCall := m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(2), int32(3)).Return(nil)
				expectFailureLog(m, entity.RequestOutcomeReasonProcessingFailed, 3).After(updateCall)
			},
			wantMetric: "test.build_dlq_controller.build_dlq.reconciled+queue=monorepo/main",
		},
		{
			name:    "resume publish failure is returned",
			wantErr: true,
			setup: func(m dlqMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.logStore.EXPECT().List(gomock.Any(), testID).Return(buildTriggerLogs(), nil)
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(triggeredBuildRow(), nil)
				m.publisher.EXPECT().Publish(gomock.Any(), "buildsignal", gomock.Any()).Return(assert.AnError)
			},
		},
		{
			name:    "request log storage error is returned",
			wantErr: true,
			setup: func(m dlqMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.logStore.EXPECT().List(gomock.Any(), testID).Return(nil, assert.AnError)
			},
		},
		{name: "malformed payload is returned", payload: []byte("not-a-proto"), wantErr: true},
		{
			name:       "empty request id is returned",
			payload:    buildPayload(t, ""),
			wantErr:    true,
			wantMetric: "test.build_dlq_controller.build_dlq.empty_id_errors+queue=monorepo/main",
		},
		{
			name:       "payload queue mismatch is acked",
			tenant:     "monorepo/release",
			wantErr:    false,
			wantMetric: "test.build_dlq_controller.build_dlq.queue_identity_errors+queue=monorepo/main",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			controller, mocks := newBuildController(t, ctrl)
			if tt.setup != nil {
				tt.setup(mocks)
			}

			payload := tt.payload
			if payload == nil {
				payload = buildPayload(t, testID)
			}
			err := controller.Process(queueContext(), delivery(t, ctrl, payload, tt.tenant))
			if tt.wantMetric != "" {
				counter, ok := mocks.metricsScope.Snapshot().Counters()[tt.wantMetric]
				require.True(t, ok)
				assert.EqualValues(t, 1, counter.Value())
			}
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, consumer.TopicKey("build_dlq"), controller.TopicKey())
			assert.Equal(t, "build_dlq", controller.Name())
			assert.Equal(t, "stovepipe-build-dlq", controller.ConsumerGroup())
		})
	}
}

func TestBuildDLQResumesDurableBuild(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller, m := newBuildController(t, ctrl)

	m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
	m.logStore.EXPECT().List(gomock.Any(), testID).Return(buildTriggerLogs(), nil)
	m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(triggeredBuildRow(), nil)

	var got entityqueue.Message
	m.publisher.EXPECT().Publish(gomock.Any(), "buildsignal", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
			got = msg
			return nil
		})

	require.NoError(t, controller.Process(queueContext(), delivery(t, ctrl, buildPayload(t, testID))))
	assert.Equal(t, testBuildID, got.ID, "resumed signal keeps the build's stable message identity")
	assert.Equal(t, testBuildID, got.PartitionKey)
	assert.Equal(t, testQueue, got.Tenant)
	assert.Equal(t, testQueue, got.Metadata[entityqueue.MetadataKeyQueueName])
	sig := &stovepipemq.BuildSignal{}
	require.NoError(t, stovepipemq.Unmarshal(got.Payload, sig))
	assert.Equal(t, testBuildID, sig.GetId())
	assert.Equal(t, testQueue, sig.GetQueueName())

	counter, ok := m.metricsScope.Snapshot().Counters()["test.build_dlq_controller.build_dlq.build_resumed+queue=monorepo/main"]
	require.True(t, ok)
	assert.EqualValues(t, 1, counter.Value())
}
