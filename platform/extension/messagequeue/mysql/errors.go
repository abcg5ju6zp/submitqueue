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
	"errors"
	"fmt"
)

// ErrPublisherClosed is returned when attempting to publish after the publisher has been closed.
// This is a graceful error, not a programming bug — concurrent goroutines may still hold
// references to the publisher when Close() is called, and their subsequent Publish calls
// return this error to signal they should stop.
var ErrPublisherClosed = errors.New("publisher is closed")

// ErrSubscriberClosed is returned when attempting to subscribe after the subscriber has been closed.
var ErrSubscriberClosed = errors.New("subscriber is closed")

// ErrInvalidConfig is returned when subscription configuration is invalid.
var ErrInvalidConfig = errors.New("invalid subscription config")

// ErrAlreadyAcknowledged is returned when attempting to ack/nack a delivery that was already processed
type ErrAlreadyAcknowledged struct {
	DeliveryID string
}

func (e *ErrAlreadyAcknowledged) Error() string {
	return fmt.Sprintf("delivery %s already acknowledged or nacked", e.DeliveryID)
}

// ErrLeaseExpired is returned when a lease renewal fails because the lease
// is no longer owned by this worker (rows affected == 0).
type ErrLeaseExpired struct {
	// Topic is the topic the lease was for.
	Topic string
	// PartitionKey is the partition the lease was for.
	PartitionKey string
}

func (e *ErrLeaseExpired) Error() string {
	return fmt.Sprintf("lease expired for topic=%s partition=%s", e.Topic, e.PartitionKey)
}

// ErrLeaseFenced is returned when an owner-scoped write matches no rows
// because the partition lease is no longer held at the caller's fencing
// token (it expired, was released, or was stolen and re-acquired with a new
// generation). The write has not taken effect; the delivery stays governed
// by its visibility timeout and is redelivered to the current owner.
type ErrLeaseFenced struct {
	// Topic is the topic the write was for.
	Topic string
	// PartitionKey is the partition the write was for.
	PartitionKey string
	// SubscriberName is the stale owner that attempted the write.
	SubscriberName string
	// Generation is the stale fencing generation the write carried.
	Generation int64
}

func (e *ErrLeaseFenced) Error() string {
	return fmt.Sprintf("lease fenced for topic=%s partition=%s subscriber=%s generation=%d",
		e.Topic, e.PartitionKey, e.SubscriberName, e.Generation)
}
