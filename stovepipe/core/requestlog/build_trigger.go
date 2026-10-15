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

package requestlog

import "github.com/uber/submitqueue/stovepipe/entity"

// TriggeredBuildID returns the id of the build whose trigger occurrence is retained for
// the request. logs must be in RequestLogStore.List order ((timestamp, id) ascending), so
// the first retained trigger is returned; false means the request has no durable trigger.
// The append-only log is the request -> triggered-build index: the runner mints the build id
// and BuildStore deliberately offers no list-by-request read. A trigger occurrence without
// its build id metadata cannot be produced by writers and contributes no result.
func TriggeredBuildID(logs []entity.RequestLog) (string, bool) {
	for _, log := range logs {
		if log.Event != entity.RequestEventBuildTriggered {
			continue
		}
		if buildID := log.Metadata[MetadataKeyBuildID]; buildID != "" {
			return buildID, true
		}
	}
	return "", false
}
