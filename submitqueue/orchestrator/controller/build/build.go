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

// Package build starts the builds a head batch's speculation paths have been
// funded for. A message names only the head; the path set is the instruction,
// and each pending entry in it is a build to start. Nothing about the action
// travels on the wire, so a dispatch cannot go stale between publish and
// delivery: a path refuted after its message was sent is simply no longer
// pending by the time the set is read.
//
// This stage only starts builds. Stopping them is the poll loop's job: every
// build started here is handed to the buildsignal stage, which follows it to a
// terminal state and stops it the moment its path no longer wants it. The
// split is what keeps this stage simple — it decides whether a build should
// exist, never whether one should die — and it holds together because of one
// invariant this stage maintains: every build that gets a link gets a signal
// (see ensureSignal for the crash case).
package build

import (
	"context"
	"errors"
	"fmt"
	"time"

	orchstorage "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/platform/publish"
	sqmq "github.com/uber/submitqueue/submitqueue/core/messagequeue"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	corereceipt "github.com/uber/submitqueue/submitqueue/orchestrator/core/receipt"
	"go.uber.org/zap"
)

// Controller handles build queue messages.
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        orchstorage.Factory
	buildRunners  buildrunner.Factory
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// opName is the metric operation name shared by every emit in this file.
const opName = "process"

// NewController creates a new build controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores orchstorage.Factory,
	buildRunners buildrunner.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("build_controller"),
		metricsScope:  scope.SubScope("build_controller"),
		stores:        stores,
		buildRunners:  buildRunners,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process starts a build for every pending path in the head's set.
// Returns nil to ack (success), or error to nack (retry).
//
// This controller never writes the path set. The set is the speculate run's
// state — what it decided to fund and what it wants stopped — and a second
// writer on it would make speculate lose compare-and-swap races across its own
// far longer read-decide-write window. What this stage knows is the build it
// started, and that goes in records of its own.
//
// A failure partway through nacks the whole message and redelivery re-runs all
// of it as conditional stages: a start is skipped when the link record already
// confirms the build, and the hand-off to buildsignal is resumed from its
// stage receipt for any live path already linked to a build, so a delivery
// that crashed between the link and the signal — or between the signal's
// receipt and its publish — re-publishes under the original message identity
// instead of forking a second poll chain (see ensureSignal).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	bid, err := sqmq.UnmarshalBatchID(c.topicKey, msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize batch ID: %w", err)
	}
	if err := entityqueue.ValidatePayloadQueue(msg, bid.Queue); err != nil {
		return fmt.Errorf("invalid message identity: %w", err)
	}

	store, err := c.stores.For(orchstorage.Config{QueueName: bid.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_resolve_errors", 1)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", bid.Queue, err)
	}

	// Fetch batch from storage
	batch, err := store.GetBatchStore().Get(ctx, bid.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get batch %s: %w", bid.ID, err)
	}

	// The payload's queue must match the batch's authoritative queue; a
	// mismatch is a malformed message. Non-retryable — reject to the DLQ.
	if bid.Queue != "" && bid.Queue != batch.Queue {
		metrics.NamedCounter(c.metricsScope, opName, "queue_mismatch", 1)
		return fmt.Errorf("payload queue %q does not match queue %q of batch %s", bid.Queue, batch.Queue, batch.ID)
	}

	set, err := store.GetSpeculationPathSetStore().Get(ctx, batch.ID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// No speculation run has funded this head yet, so there is nothing
			// to dispatch. A later run publishes again once it has.
			metrics.NamedCounter(c.metricsScope, opName, "no_path_set", 1)
			return nil
		}
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get path set for batch %s: %w", batch.ID, err)
	}

	c.logger.Debugw("received build event",
		"batch_id", batch.ID,
		"queue", batch.Queue,
		"state", string(batch.State),
		"paths", len(set.Paths),
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// halted covers terminal and cancelling batches, and gates only starts:
	// a halted batch gets no new builds, and its running ones are stopped by
	// the poll loop, which reads the batch state on every poll.
	halted := entity.IsBatchStateHalted(batch.State)

	for _, entry := range set.Paths {
		if entry.Status.IsTerminal() {
			continue
		}

		if entry.Status == entity.SpeculationPathStatusPending && !halted {
			if err := c.startPath(ctx, store, batch, entry); err != nil {
				return err
			}
			continue
		}

		if entry.Status == entity.SpeculationPathStatusPending {
			metrics.NamedCounter(c.metricsScope, opName, "skipped_halted", 1)
		}

		// Every live entry startPath did not just handle — one observed
		// building or cancelling, or pending on a halted batch — already names
		// a build through its link. The signal is a conditional stage: its
		// receipt makes the hand-off idempotent on every delivery, not just a
		// redelivery, so a first attempt that died between the link and the
		// receipt is repaired too.
		if err := c.ensureSignal(ctx, store, batch, entry); err != nil {
			return err
		}
	}

	return nil
}

// startPath triggers the build for a pending path and records it.
//
// The base is the path's own — the dependencies it assumes will succeed, in its
// order. This is the behavioral heart of speculation: dependencies the path
// assumes will fail are absent from the base, which is what lets the head be
// verified before they resolve.
//
// The write order is Trigger, then the Build record, then the link, then the
// signal — each write makes the previous one reachable. The Build record gives
// the runner's ID a home, the link makes the build findable from the path the
// caller holds, and the signal puts the poll loop behind it. The link is the
// idempotency point: a redelivery that finds it re-publishes the signal and
// stops, and a concurrent dispatch that loses the link's first-insert race
// hands both builds to the poll loop, which keeps the one the link names (see
// the lost-race branch below).
//
// A crash between the Trigger and the link orphans the build — redelivery
// cannot find it and triggers again — the accepted cost of not being able to
// name a build before the runner mints its ID.
//
// TODO: pass an idempotency key derived from (path ID, attempt) once
// BuildRunner.Trigger accepts one, so a retry re-attaches to the existing build
// instead of orphaning it. Only the runner can close this window, because the
// build exists before anything here can write it down.
func (c *Controller) startPath(ctx context.Context, store orchstorage.Storage, batch entity.Batch, entry entity.SpeculationPathEntry) error {
	existing, err := store.GetPathBuildStore().Get(ctx, entry.ID, entry.Attempt)
	switch {
	case err == nil:
		// Already dispatched and named; all that can be missing is the signal,
		// and a re-publish reuses its receipt and message identity.
		metrics.NamedCounter(c.metricsScope, opName, "already_dispatched", 1)
		return c.publishBuildSignal(ctx, store, batch, existing.BuildID)
	case !errors.Is(err, storage.ErrNotFound):
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to look up build for path %s attempt %d: %w", entry.ID, entry.Attempt, err)
	}

	base, err := c.loadBase(ctx, store, entry.Path)
	if err != nil {
		return err
	}

	buildRunner, err := c.buildRunners.For(buildrunner.Config{QueueName: batch.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "trigger_errors", 1)
		return fmt.Errorf("failed to build runner for batch %s: %w", batch.ID, err)
	}

	// metadata is nil until a caller-supplied source materializes (e.g.
	// requester / ticket pulled off the originating LandRequest).
	buildID, err := buildRunner.Trigger(ctx, base, batch, nil)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "trigger_errors", 1)
		return fmt.Errorf("failed to trigger build for path %s attempt %d: %w", entry.ID, entry.Attempt, err)
	}

	build := entity.Build{
		ID:      buildID.ID,
		BatchID: batch.ID,
		PathID:  entry.ID,
		Attempt: entry.Attempt,
		Status:  entity.BuildStatusAccepted,
	}

	// Persist the initial Build snapshot so the buildsignal poll loop has a
	// row to Update against. ErrAlreadyExists is benign — a redelivery
	// of this message after a previous successful Create.
	if err := store.GetBuildStore().Create(ctx, build); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to record build %s: %w", buildID.ID, err)
	}

	link := entity.PathBuild{Queue: batch.Queue, PathID: entry.ID, Attempt: entry.Attempt, BuildID: buildID.ID}
	if err := store.GetPathBuildStore().Create(ctx, link); err != nil {
		if errors.Is(err, storage.ErrAlreadyExists) {
			// Another dispatch named this attempt first. Its build is the one
			// the rest of the system will watch; ours is surplus. Hand both to
			// the poll loop: it keeps the build the link names and stops the
			// other one — and the winner's signal must be sent from here too,
			// because the dispatch that won may have died before sending it,
			// and acking this message would retire the redelivery that would
			// otherwise repair that.
			metrics.NamedCounter(c.metricsScope, opName, "lost_dispatch_race", 1)
			c.logger.Infow("lost the dispatch race; handing both builds to the poll loop",
				"batch_id", batch.ID,
				"path_id", entry.ID,
				"attempt", entry.Attempt,
				"surplus_build_id", buildID.ID,
			)
			if err := c.publishBuildSignal(ctx, store, batch, buildID.ID); err != nil {
				return err
			}
			winner, err := store.GetPathBuildStore().Get(ctx, entry.ID, entry.Attempt)
			if err != nil {
				metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
				return fmt.Errorf("failed to look up the winning build for path %s attempt %d: %w", entry.ID, entry.Attempt, err)
			}
			return c.publishBuildSignal(ctx, store, batch, winner.BuildID)
		}
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to link path %s attempt %d to build %s: %w", entry.ID, entry.Attempt, buildID.ID, err)
	}

	metrics.NamedCounter(c.metricsScope, opName, "build_triggered", 1)
	c.logger.Infow("triggered build for speculation path",
		"batch_id", batch.ID,
		"path_id", entry.ID,
		"attempt", entry.Attempt,
		"build_id", buildID.ID,
		"base_size", len(base),
	)

	return c.publishBuildSignal(ctx, store, batch, buildID.ID)
}

// ensureSignal drives the conditional build-signal stage for a live entry
// this delivery did not just start: one observed building or cancelling, or
// pending on a halted batch. It runs on every delivery rather than on
// redeliveries only.
//
// It closes the one crack in the "every linked build gets a signal" invariant:
// a dispatch that dies between writing the link and publishing the signal
// leaves a build no poll chain will ever watch. The link is the stage's
// generation key; the SpeculationReceipt is its confirmation: written before
// the publish and carrying the signal's message ID, so a restart or a
// dead-letter republish on a brand-new delivery still resumes here instead of
// depending on the queue's redelivery counter or deduplication window.
//
// An absent link is normal: nothing was dispatched for the attempt yet (for
// example a pending path on a halted batch), so there is no build to watch.
func (c *Controller) ensureSignal(ctx context.Context, store orchstorage.Storage, batch entity.Batch, entry entity.SpeculationPathEntry) error {
	link, err := store.GetPathBuildStore().Get(ctx, entry.ID, entry.Attempt)
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to look up build for path %s attempt %d: %w", entry.ID, entry.Attempt, err)
	}

	metrics.NamedCounter(c.metricsScope, opName, "signal_ensured", 1)
	c.logger.Debugw("ensured build signal from stage receipt",
		"batch_id", batch.ID,
		"path_id", entry.ID,
		"attempt", entry.Attempt,
		"build_id", link.BuildID,
	)
	return c.publishBuildSignal(ctx, store, batch, link.BuildID)
}

// loadBase loads the batches the path is stacked on top of.
//
// Which dependencies those are is the path's own to say — see
// SpeculationPath.Base — so this only resolves the IDs it is
// given. Nothing about assumptions is interpreted here.
func (c *Controller) loadBase(ctx context.Context, store orchstorage.Storage, path entity.SpeculationPath) ([]entity.Batch, error) {
	deps := path.Base()
	if len(deps) == 0 {
		return nil, nil
	}

	base := make([]entity.Batch, 0, len(deps))
	for _, depID := range deps {
		b, err := store.GetBatchStore().Get(ctx, depID)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return nil, fmt.Errorf("failed to get dependency batch %s of path %s: %w", depID, path.ID(), err)
		}
		base = append(base, b)
	}
	return base, nil
}

// publishBuildSignal hands a build off to the poll loop under its stage
// receipt.
//
// Only the runner's build ID travels, and it is also the partition key: the
// poll loop writes nothing but that build's own record, so there is nothing to
// serialize across builds, and successive polls of one build stay ordered
// because they share the key. Partitioning by batch instead would put every
// path of a head behind whichever of its builds polls slowest.
//
// The receipt is persisted before the publish: the build ID is both its token
// and the message's partition key, and the message ID is the same
// IntentID(buildID) a build has always been handed off under, so a recovery —
// redelivery, restart, or a dead-letter republish — replays the hand-off as
// the original message rather than forking a second poll chain, even after the
// queue has garbage-collected the first row.
func (c *Controller) publishBuildSignal(ctx context.Context, store orchstorage.Storage, batch entity.Batch, buildID string) error {
	receipt, err := c.ensureSignalReceipt(ctx, store, batch, buildID)
	if err != nil {
		return err
	}

	payload, err := sqmq.MarshalID(sqmq.TopicKeyBuildSignal, buildID, batch.Queue)
	if err != nil {
		return fmt.Errorf("failed to serialize build ID: %w", err)
	}

	if err := publish.Message(ctx, c.registry, topickey.TopicKeyBuildSignal, publish.MessageParams{
		Tenant:       batch.Queue,
		ID:           receipt.MessageID,
		Payload:      payload,
		PartitionKey: buildID,
	}); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to buildsignal: %w", err)
	}
	return nil
}

// ensureSignalReceipt returns the build-signal receipt for one build, creating
// it on the first hand-off. The build ID is both the generation token and the
// message identity, so every replay of this stage for the same build converges
// on one record and one wire message.
func (c *Controller) ensureSignalReceipt(ctx context.Context, store orchstorage.Storage, batch entity.Batch, buildID string) (entity.SpeculationReceipt, error) {
	want := entity.SpeculationReceipt{
		Queue:       batch.Queue,
		Head:        batch.ID,
		Stage:       entity.SpeculationStageBuildSignal,
		Token:       buildID,
		MessageID:   publish.IntentID(buildID),
		CreatedAtMs: time.Now().UnixMilli(),
	}
	got, created, err := corereceipt.Ensure(ctx, store.GetSpeculationReceiptStore(), want)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return entity.SpeculationReceipt{}, fmt.Errorf("failed to ensure build signal receipt for build %s: %w", buildID, err)
	}
	if created {
		metrics.NamedCounter(c.metricsScope, opName, "build_signal_receipt_created", 1)
	} else {
		metrics.NamedCounter(c.metricsScope, opName, "build_signal_receipt_reused", 1)
	}
	return got, nil
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "build"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
