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

package entity

// SpeculationReceiptStage names one conditional stage of a head's speculation
// whose action must be resumable after a failure, a restart, or a dead-letter
// republish. A receipt is written before the stage's message is published, so
// recovery replays the publish under the stage's original message identity
// instead of deciding or triggering anything a second time.
type SpeculationReceiptStage string

const (
	// SpeculationStageUnknown is the zero-value sentinel; it is never a valid
	// stage.
	SpeculationStageUnknown SpeculationReceiptStage = ""
	// SpeculationStageLandDispatch is the stage that hands a landed head to the
	// land stage. A head reaches it at most once, and only after its batch's
	// compare-and-swap into Landing has won.
	SpeculationStageLandDispatch SpeculationReceiptStage = "land-dispatch"
	// SpeculationStageBuildSignal is the stage that hands one started build to
	// the buildsignal poll chain. There is one receipt per build.
	SpeculationStageBuildSignal SpeculationReceiptStage = "build-signal"
)

// SpeculationReceipt is an immutable confirmation that one conditional stage
// was committed for a head, and the wire identity its action must be published
// under. Records are write-once and keyed by their complete identity
// (Queue, Head, Stage, Token): creating one is the idempotency point, so a
// redelivery finds the existing receipt and replays the publish with the same
// MessageID rather than enacting the stage twice.
//
// Token is the stable generation the stage belongs to: the batch version a
// land decision committed at for a land dispatch, so a decision an older batch
// version lost can never acquire a receipt on the new one; and the runner's
// build ID for a build signal. A receipt confirms the intent to publish, not
// the broker's acceptance — recovery re-publishes under MessageID and leaves
// deduplication to the queue and the idempotent downstream consumer.
type SpeculationReceipt struct {
	// Queue is the name of the queue the head batch belongs to. It leads the
	// record's primary key.
	Queue string
	// Head is the head batch ID the stage belongs to.
	Head string
	// Stage is the conditional stage this receipt confirms.
	Stage SpeculationReceiptStage
	// Token is the stable generation the stage action is bound to: the
	// committed batch version for a land dispatch, the build ID for a build
	// signal.
	Token string
	// MessageID is the exact wire message identity the stage's publish uses and
	// must reuse on every recovery, so an uncertain publish is replayed as the
	// same message instead of a second one.
	MessageID string
	// CreatedAtMs is the creation time in Unix epoch milliseconds.
	CreatedAtMs int64
}
