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

package messagequeue

import "context"

// DrainingSubscriber is a Subscriber that supports an explicit, ordered
// graceful shutdown per subscription. Backends that only guarantee the
// lifetime of a lease instead of a process (SQL subscribers, where a new
// process can take an old one's partitions) implement it; the consumer
// framework type-asserts it during Stop().
//
// ShutdownSubscription stops message claiming for (topic, consumerGroup),
// then waits for in-flight deliveries to settle (ack/nack/postpone/reject)
// or until their visibility deadlines safely pass, commits watermarks, and
// releases the partitions' leases. It is idempotent and safe to call
// concurrently with itself and with Close; every caller joins the same
// drain. On ctx deadline, leases with unsettled deliveries are kept so a
// new owner takes them over, and the method returns ctx.Err() while the
// drain itself finishes bounded by its own cleanup contexts.
type DrainingSubscriber interface {
	Subscriber

	ShutdownSubscription(ctx context.Context, topic, consumerGroup string) error
}
