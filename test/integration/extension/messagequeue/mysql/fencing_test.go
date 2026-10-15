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
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/platform/base/failure"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
)

// uniqueTopic isolates each fencing scenario into its own partition space.
func uniqueTopic(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()%1_000_000_000)
}

// leaseState is one row of queue_partition_leases joined to the heartbeat.
type leaseState struct {
	owner     string
	gen       int64
	renewed   int64
	heartbeat bool
}

func (s *SQLQueueIntegrationSuite) leaseState(topic, group string) (leaseState, bool) {
	var ls leaseState
	var heartbeatOwner sql.NullString
	query := `
		SELECT l.leased_by, l.lease_generation, l.lease_renewed_at, h.subscriber_name
		FROM queue_partition_leases l
		LEFT JOIN queue_subscriber_heartbeats h
		  ON h.tenant = l.tenant AND h.consumer_group = l.consumer_group
		 AND h.topic = l.topic AND h.subscriber_name = l.leased_by
		WHERE l.tenant = ? AND l.topic = ? AND l.consumer_group = ? AND l.partition_key = ?`
	err := s.db.QueryRowContext(s.ctx, query, testTenant, topic, group, "p1").
		Scan(&ls.owner, &ls.gen, &ls.renewed, &heartbeatOwner)
	if err == sql.ErrNoRows {
		return leaseState{}, false
	}
	require.NoError(s.T(), err)
	ls.heartbeat = heartbeatOwner.Valid
	return ls, true
}

func (s *SQLQueueIntegrationSuite) assertNoDelivery(ch <-chan extqueue.Delivery, within time.Duration) {
	select {
	case d, ok := <-ch:
		if ok && d != nil {
			s.T().Fatalf("unexpected redelivery of %s", d.Message().ID)
		}
	case <-time.After(within):
	}
}

// messageOffset reads the main-topic offset of a message by ID.
func (s *SQLQueueIntegrationSuite) messageOffset(topic, id string) int64 {
	var offset int64
	require.NoError(s.T(), s.db.QueryRowContext(s.ctx, `
		SELECT offset FROM queue_messages
		WHERE tenant = ? AND topic = ? AND id = ?`,
		testTenant, topic, id).Scan(&offset))
	return offset
}

func (s *SQLQueueIntegrationSuite) watermark(topic, group string) int64 {
	var offset sql.NullInt64
	_ = s.db.QueryRowContext(s.ctx, `
		SELECT offset_acked FROM queue_offsets
		WHERE tenant = ? AND topic = ? AND consumer_group = ? AND partition_key = ?`,
		testTenant, topic, group, "p1").Scan(&offset)
	return offset.Int64
}

// newSignalQueue returns a queue whose lifecycle signals are readable.
func (s *SQLQueueIntegrationSuite) newSignalQueue() (extqueue.Queue, chan queueMySQL.HookSignal) {
	sig := make(chan queueMySQL.HookSignal, 64)
	q, err := queueMySQL.NewQueue(s.testQueueParams(s.T(), func(p *queueMySQL.Params) { p.OnSignal = sig }))
	require.NoError(s.T(), err)
	return q, sig
}

// TestFencing_StaleAckRejectedAfterTakeover verifies the end-to-end rule:
// an in-flight delivery from a previous owner is rejected when acked after
// the partition lease moves, and the new owner redelivers and acknowledges
// it exactly once.
func (s *SQLQueueIntegrationSuite) TestFencing_StaleAckRejectedAfterTakeover() {
	t := s.T()
	topic := uniqueTopic("fence-ack")
	group := "fence-ack-cg"

	q1, err := queueMySQL.NewQueue(s.testQueueParams(t, nil))
	require.NoError(t, err)
	ch1, err := q1.Subscriber().Subscribe(s.ctx, topic, testSubConfig("worker-1", group))
	require.NoError(t, err)
	require.NoError(t, q1.Publisher().Publish(s.ctx, topic, testMessage("m1", []byte("p"), "p1", nil)))
	stale := receive(t, ch1)

	// worker-1 stops with the delivery still in flight and zero drain budget:
	// the lease is retained for takeover rather than released.
	ds, ok := q1.Subscriber().(extqueue.DrainingSubscriber)
	require.True(t, ok)
	drainCtx, cancel := context.WithTimeout(s.ctx, 50*time.Millisecond)
	defer cancel()
	require.Error(t, ds.ShutdownSubscription(drainCtx, topic, group))

	// The caller deadline only stops waiting: the bounded drain run keeps
	// going, so join it via Close before inspecting durable state. The
	// zero-budget drain intentionally reports the outstanding delivery.
	require.Error(t, q1.Close())

	ls, found := s.leaseState(topic, group)
	require.True(t, found, "undrained lease must be retained for takeover")
	assert.Equal(t, "worker-1", ls.owner)
	assert.False(t, ls.heartbeat, "heartbeat must be deregistered on drain")

	// worker-2 waits out the stale lease (3s), steals it, and receives the
	// visibility-expired message.
	q2, err := queueMySQL.NewQueue(s.testQueueParams(t, nil))
	require.NoError(t, err)
	defer q2.Close()
	ch2, err := q2.Subscriber().Subscribe(s.ctx, topic, testSubConfig("worker-2", group))
	require.NoError(t, err)

	redelivered := receive(t, ch2)
	assert.Equal(t, "m1", redelivered.Message().ID)

	// The stale delivery's ack is fenced server-side.
	var fenced *queueMySQL.ErrLeaseFenced
	require.ErrorAs(t, stale.Ack(s.ctx), &fenced)

	// The new owner's ack commits; no duplicate redelivery follows.
	require.NoError(t, redelivered.Ack(s.ctx))
	s.assertNoDelivery(ch2, 2500*time.Millisecond)
}

// TestFencing_LeaseGenerationIncrementsOnSteal checks the fencing epoch:
// stealing a stale lease moves generation 1 -> 2 and the old owner's
// guarded writes fail without moving the partition barrier.
func (s *SQLQueueIntegrationSuite) TestFencing_LeaseGenerationIncrementsOnSteal() {
	t := s.T()
	topic := uniqueTopic("fence-gen")
	group := "fence-gen-cg"

	q1, err := queueMySQL.NewQueue(s.testQueueParams(t, nil))
	require.NoError(t, err)
	defer q1.Close()
	ch1, err := q1.Subscriber().Subscribe(s.ctx, topic, testSubConfig("worker-1", group))
	require.NoError(t, err)
	require.NoError(t, q1.Publisher().Publish(s.ctx, topic, testMessage("m1", []byte("p"), "p1", nil)))
	d1 := receive(t, ch1)

	require.Eventually(t, func() bool {
		ls, ok := s.leaseState(topic, group)
		return ok && ls.owner == "worker-1" && ls.gen == 1
	}, 3*time.Second, 50*time.Millisecond, "worker-1 should acquire at generation 1")

	// Force worker-1's lease stale, then let worker-2 steal it.
	_, err = s.db.ExecContext(s.ctx, `
		UPDATE queue_partition_leases
		SET lease_renewed_at = UNIX_TIMESTAMP() * 1000 - 10000
		WHERE tenant = ? AND topic = ? AND consumer_group = ? AND partition_key = ?`,
		testTenant, topic, group, "p1")
	require.NoError(t, err)

	q2, err := queueMySQL.NewQueue(s.testQueueParams(t, nil))
	require.NoError(t, err)
	defer q2.Close()
	ch2, err := q2.Subscriber().Subscribe(s.ctx, topic, testSubConfig("worker-2", group))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		ls, ok := s.leaseState(topic, group)
		return ok && ls.owner == "worker-2" && ls.gen == 2
	}, 6*time.Second, 50*time.Millisecond, "worker-2 should steal at generation 2")
	// Close waits out d1's visibility deadline, then releases nothing (the
	// generation-guarded delete matches zero rows once worker-2 owns gen 2).
	require.NoError(t, q1.Close())

	// d1 carries the old token; even its postpone is rejected, leaving the
	// postponed barrier untouched for the current owner.
	var fenced *queueMySQL.ErrLeaseFenced
	require.ErrorAs(t, d1.Postpone(s.ctx, 1000), &fenced)

	// The rejected postpone left no barrier in durable delivery state.
	var postponed bool
	require.NoError(t, s.db.QueryRowContext(s.ctx, `
		SELECT postponed FROM queue_delivery_state
		WHERE tenant = ? AND topic = ? AND consumer_group = ? AND partition_key = ?`,
		testTenant, topic, group, "p1").Scan(&postponed))
	assert.False(t, postponed, "fenced postpone must not set the barrier")

	// The stale owner's postpone did not block the current owner: m1 is
	// redelivered after the visibility timeout and acked with generation 2.
	redelivered := receive(t, ch2)
	assert.Equal(t, "m1", redelivered.Message().ID)
	require.NoError(t, redelivered.Ack(s.ctx))
}

// TestOrderedShutdown_CommitsWatermarkAndReleases: a clean drain commits
// the watermark, releases every partition lease, and deregisters the
// heartbeat.
func (s *SQLQueueIntegrationSuite) TestOrderedShutdown_CommitsWatermarkAndReleases() {
	t := s.T()
	topic := uniqueTopic("ordered-shutdown")
	group := "ordered-shutdown-cg"

	q, sig := s.newSignalQueue()
	ch, err := q.Subscriber().Subscribe(s.ctx, topic, testSubConfig("worker-1", group))
	require.NoError(t, err)

	require.NoError(t, q.Publisher().Publish(s.ctx, topic, testMessage("m1", []byte("p"), "p1", nil)))
	require.NoError(t, q.Publisher().Publish(s.ctx, topic, testMessage("m2", []byte("p"), "p1", nil)))
	d1 := receive(t, ch)
	d2 := receive(t, ch)
	require.NoError(t, d1.Ack(s.ctx))
	require.NoError(t, d2.Ack(s.ctx))
	waitForSignal(t, sig, queueMySQL.SignalDeliveryCheck)

	// Clean drain: all in-flight deliveries settled, so Close returns
	// without error within its timeout.
	require.NoError(t, q.Close())

	var leaseCount int
	require.NoError(t, s.db.QueryRowContext(s.ctx, `
		SELECT COUNT(*) FROM queue_partition_leases
		WHERE tenant = ? AND topic = ? AND consumer_group = ?`,
		testTenant, topic, group).Scan(&leaseCount))
	assert.Equal(t, 0, leaseCount, "drained leases must be released")

	var heartbeatCount int
	require.NoError(t, s.db.QueryRowContext(s.ctx, `
		SELECT COUNT(*) FROM queue_subscriber_heartbeats
		WHERE tenant = ? AND topic = ? AND consumer_group = ?`,
		testTenant, topic, group).Scan(&heartbeatCount))
	assert.Equal(t, 0, heartbeatCount, "heartbeat must be deregistered")

	assert.GreaterOrEqual(t, s.watermark(topic, group), int64(1),
		"watermark must pass the last acked message")
}

// TestDLQMoveAndAckIsAtomic: after the dead-letter transaction, no split
// state survives — exactly one DLQ row, no main-topic row, and the group's
// delivery state for the moved offset is acked in the same commit. A sole
// moved message leaves no row behind in the main topic, so the watermark
// (computed from surviving main-topic offsets) cannot advance; the acked
// delivery state is the direct proof that ack committed with the move.
func (s *SQLQueueIntegrationSuite) TestDLQMoveAndAckIsAtomic() {
	t := s.T()
	topic := uniqueTopic("dlq-atomic")
	group := "dlq-atomic-cg"

	q, err := queueMySQL.NewQueue(s.testQueueParams(t, nil))
	require.NoError(t, err)
	defer q.Close()

	cfg := testSubConfig("worker-1", group)
	cfg.DLQ.Enabled = true
	cfg.DLQ.TopicSuffix = "_dlq"
	cfg.Retry.MaxAttempts = 1
	ch, err := q.Subscriber().Subscribe(s.ctx, topic, cfg)
	require.NoError(t, err)

	require.NoError(t, q.Publisher().Publish(s.ctx, topic, testMessage("m1", []byte("p"), "p1", nil)))
	d := receive(t, ch)
	offset := s.messageOffset(topic, "m1")
	require.NoError(t, d.Reject(s.ctx, failure.New("bad message")))

	var dlqRows int
	require.NoError(t, s.db.QueryRowContext(s.ctx, `
		SELECT COUNT(*) FROM queue_messages
		WHERE tenant = ? AND topic = ? AND id = ?`,
		testTenant, topic+"_dlq", "m1").Scan(&dlqRows))
	assert.Equal(t, 1, dlqRows, "exactly one DLQ row after the atomic move")

	var mainRows int
	require.NoError(t, s.db.QueryRowContext(s.ctx, `
		SELECT COUNT(*) FROM queue_messages
		WHERE tenant = ? AND topic = ? AND id = ?`,
		testTenant, topic, "m1").Scan(&mainRows))
	assert.Equal(t, 0, mainRows, "main topic row must be deleted in the same transaction")

	var acked bool
	require.NoError(t, s.db.QueryRowContext(s.ctx, `
		SELECT acked FROM queue_delivery_state
		WHERE tenant = ? AND topic = ? AND consumer_group = ? AND partition_key = ? AND message_offset = ?`,
		testTenant, topic, group, "p1", offset).Scan(&acked))
	assert.True(t, acked, "delivery state must be acked in the same transaction as the move")
}
