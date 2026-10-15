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
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/platform/publish"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// _buildOpName is the metric operation name shared by every emit in this file.
const _buildOpName = "build_dlq"

// buildController reconciles build-stage dead letters. When the build trigger is already
// durable it resumes that stage from its persisted facts by re-announcing the build; otherwise
// it fails the request whose build could not be triggered, persisted, or handed to the poll loop.
type buildController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	materializer  requestlog.Materializer
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

var _ consumer.Controller = (*buildController)(nil)

// NewDLQBuildController creates a reconciler for the build dead-letter topic.
func NewDLQBuildController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	materializer requestlog.Materializer,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) consumer.Controller {
	name := string(topicKey) + "_controller"
	return &buildController{
		logger:        logger.Named(name),
		metricsScope:  scope.SubScope(name),
		stores:        stores,
		materializer:  materializer,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reconciles a dead-lettered BuildRequest from durable facts: a request whose trigger
// completed is resumed onto buildsignal, and every other case is failed closed.
func (c *buildController) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()
	buildRequest := &stovepipemq.BuildRequest{}
	if err := stovepipemq.Unmarshal(msg.Payload, buildRequest); err != nil {
		metrics.NamedCounter(c.metricsScope, _buildOpName, "deserialize_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("failed to decode dlq payload: %w", err)
	}
	if err := entityqueue.ValidatePayloadQueue(msg, buildRequest.GetQueueName()); err != nil {
		metrics.NamedCounter(c.metricsScope, _buildOpName, "queue_identity_errors", 1, metrics.TagsFromContext(ctx)...)
		return nil
	}
	if buildRequest.Id == "" {
		metrics.NamedCounter(c.metricsScope, _buildOpName, "empty_id_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("build dlq payload decoded to empty request id")
	}

	store, err := c.stores.For(storage.Config{QueueName: buildRequest.GetQueueName()})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _buildOpName, "storage_resolve_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("failed to resolve storage for queue %q: %w", buildRequest.GetQueueName(), err)
	}

	metadata := delivery.Metadata()
	c.logger.Warnw("dlq message received",
		"request_id", buildRequest.Id,
		"attempt", delivery.Attempt(),
		"dlq_original_topic", metadata["dlq.original_topic"],
		"dlq_failure_count", metadata["dlq.failure_count"],
		"dlq_last_error", metadata["dlq.last_error"],
	)

	resumed, err := c.resumeTriggeredBuild(ctx, store, buildRequest.Id)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _buildOpName, "resume_errors", 1, metrics.TagsFromContext(ctx)...)
		return err
	}
	if resumed {
		metrics.NamedCounter(c.metricsScope, _buildOpName, "build_resumed", 1, metrics.TagsFromContext(ctx)...)
		return nil
	}

	if err := failRequest(ctx, store, c.materializer, c.logger, buildRequest.Id, entity.RequestOutcomeReasonProcessingFailed); err != nil {
		metrics.NamedCounter(c.metricsScope, _buildOpName, "reconcile_errors", 1, metrics.TagsFromContext(ctx)...)
		return err
	}
	metrics.NamedCounter(c.metricsScope, _buildOpName, "reconciled", 1, metrics.TagsFromContext(ctx)...)
	return nil
}

// resumeTriggeredBuild continues the build stage from its durable facts when its trigger already
// completed, re-announcing the retained build on buildsignal so the poll loop resumes without
// touching the request's state or build slot. It returns true when fail-closed reconciliation is
// unneeded (the build was resumed, or the request is gone), and false when the caller must fail
// the request: a terminal request (failRequest skips or repairs it), no durable trigger
// occurrence, or trigger facts that contradict each other, where nothing can safely resume the
// build. Transient storage or publish errors return for redelivery — the DLQ consumer treats
// every error as retryable.
func (c *buildController) resumeTriggeredBuild(ctx context.Context, store storage.Storage, requestID string) (bool, error) {
	request, err := store.GetRequestStore().Get(ctx, requestID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			c.logger.Warnw("dlq reconcile: request not found, skipping", "request_id", requestID)
			return true, nil
		}
		return false, fmt.Errorf("failed to get request %s: %w", requestID, err)
	}
	if request.State.IsTerminal() {
		return false, nil
	}

	logs, err := store.GetRequestLogStore().List(ctx, requestID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("failed to list request log for request %s: %w", requestID, err)
	}
	buildID, ok := requestlog.TriggeredBuildID(logs)
	if !ok {
		return false, nil
	}

	build, err := store.GetBuildStore().Get(ctx, buildID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// The trigger occurrence is retained only after the Build row creates, so a miss
			// is a storage defect rather than a pollable build: fail closed instead of
			// re-announcing a build id the buildsignal stage cannot load.
			c.logger.Errorw("dlq reconcile: retained trigger names a missing build; failing the request closed",
				"request_id", requestID,
				"build_id", buildID,
			)
			return false, nil
		}
		return false, fmt.Errorf("failed to get build %s: %w", buildID, err)
	}
	if build.RequestID != requestID {
		c.logger.Errorw("dlq reconcile: retained trigger names a build owned by another request; failing the request closed",
			"request_id", requestID,
			"build_id", buildID,
			"build_request_id", build.RequestID,
		)
		return false, nil
	}

	payload, err := stovepipemq.Marshal(&stovepipemq.BuildSignal{Id: buildID, QueueName: request.Queue})
	if err != nil {
		return false, fmt.Errorf("failed to serialize build signal for build %s: %w", buildID, err)
	}
	if err := publish.Message(ctx, c.registry, stovepipemq.TopicKeyBuildSignal, publish.MessageParams{
		Tenant:       request.Queue,
		ID:           publish.IntentID(buildID),
		Payload:      payload,
		PartitionKey: buildID,
	}); err != nil {
		return false, fmt.Errorf("failed to republish build signal for build %s: %w", buildID, err)
	}
	c.logger.Warnw("dlq reconcile: resumed buildsignal for durably triggered build",
		"request_id", requestID,
		"build_id", buildID,
	)
	return true, nil
}

// Name returns the controller name.
func (c *buildController) Name() string { return string(c.topicKey) }

// TopicKey returns the dead-letter topic key.
func (c *buildController) TopicKey() consumer.TopicKey { return c.topicKey }

// ConsumerGroup returns the offset-tracking group.
func (c *buildController) ConsumerGroup() string { return c.consumerGroup }
