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
)

// leaseGuardSQL is an EXISTS predicate that allows an owner-scoped write only
// while the partition lease is held at exactly the caller's fencing token.
// Append it to a statement's WHERE/SELECT and bind its six placeholders with
// leaseGuardArgs after the statement's own arguments.
const leaseGuardSQL = `EXISTS (
		SELECT 1 FROM queue_partition_leases
		WHERE tenant = ? AND consumer_group = ? AND topic = ? AND partition_key = ?
			AND leased_by = ? AND lease_generation = ?
	)`

// leaseGuardArgs returns the six bound values for leaseGuardSQL.
func leaseGuardArgs(token partitionLeaseToken, tenant, consumerGroup, topic, partitionKey string) []any {
	return []any{tenant, consumerGroup, topic, partitionKey, token.leasedBy, token.generation}
}

// leaseGuardQuerier is the subset of *sql.DB / *sql.Tx used by lease
// re-checks, so a zero-row guarded write can be classified inside the same
// transaction that made it.
type leaseGuardQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// leaseStillOwned re-checks lease ownership after a guarded statement matched
// no rows. It distinguishes "the write was a legitimate no-op while we still
// own the lease" (true — e.g. re-acking an already-acked row) from "the lease
// is gone or changed generation" (false — the caller must treat the write as
// fenced). The guarded statement itself remains the authority; this read only
// classifies its zero-row result.
func leaseStillOwned(ctx context.Context, q leaseGuardQuerier, token partitionLeaseToken, tenant, consumerGroup, topic, partitionKey string) (bool, error) {
	var one int
	err := q.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT 1 FROM %s
		WHERE tenant = ? AND consumer_group = ? AND topic = ? AND partition_key = ?
			AND leased_by = ? AND lease_generation = ?
	`, PartitionLeasesTableName),
		tenant, consumerGroup, topic, partitionKey, token.leasedBy, token.generation,
	).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// fencedWriteError builds the sentinel returned for a zero-row guarded write
// whose lease no longer matches.
func fencedWriteError(token partitionLeaseToken, tenant, topic, partitionKey string) *ErrLeaseFenced {
	return &ErrLeaseFenced{
		Topic:          topic,
		PartitionKey:   partitionKey,
		SubscriberName: token.leasedBy,
		Generation:     token.generation,
	}
}
