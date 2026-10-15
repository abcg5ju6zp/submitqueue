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

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/submitqueue/stovepipe/entity"
)

func triggerLogsForTest() []entity.RequestLog {
	request := entity.Request{ID: "7", Queue: "monorepo/main"}
	return []entity.RequestLog{
		NewRequestStateLog(entity.Request{
			ID: request.ID, Queue: request.Queue, State: entity.RequestStateAccepted, Version: 1,
		}, entity.RequestOutcomeReasonUnknown),
		NewRequestStateLog(entity.Request{
			ID: request.ID, Queue: request.Queue, State: entity.RequestStateProcessing, Version: 2,
		}, entity.RequestOutcomeReasonUnknown),
	}
}

func TestTriggeredBuildID(t *testing.T) {
	request := entity.Request{ID: "7", Queue: "monorepo/main"}
	trigger := func(buildID string) entity.RequestLog {
		var metadata map[string]string
		if buildID != "" {
			metadata = map[string]string{MetadataKeyBuildID: buildID}
		}
		return NewRequestEventLog(request, entity.RequestEventBuildTriggered, buildID, metadata)
	}
	finished := NewRequestEventLog(request, entity.RequestEventBuildFinished, "bk-1",
		map[string]string{MetadataKeyBuildID: "bk-1"})

	tests := []struct {
		name     string
		logs     []entity.RequestLog
		wantID   string
		wantFound bool
	}{
		{name: "no occurrences", logs: nil, wantID: "", wantFound: false},
		{name: "state occurrences only", logs: triggerLogsForTest(), wantID: "", wantFound: false},
		{
			name:      "one trigger occurrence",
			logs:      append(triggerLogsForTest(), trigger("bk-1")),
			wantID:    "bk-1",
			wantFound: true,
		},
		{
			name:      "earliest retained trigger wins",
			logs:      append(triggerLogsForTest(), trigger("bk-1"), finished, trigger("bk-2")),
			wantID:    "bk-1",
			wantFound: true,
		},
		{
			name:      "trigger occurrence without build id is ignored",
			logs:      append(triggerLogsForTest(), trigger(""), trigger("bk-3")),
			wantID:    "bk-3",
			wantFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotID, gotFound := TriggeredBuildID(tt.logs)
			assert.Equal(t, tt.wantID, gotID)
			assert.Equal(t, tt.wantFound, gotFound)
		})
	}
}
