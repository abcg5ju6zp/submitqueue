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

// Package speculate plans a queue's speculative builds and finalizes each
// batch's outcome from their results. It is the orchestrator's decision
// stage: builds are started by the build stage and watched — and stopped —
// by the buildsignal stage, but what to build and what a finished build
// means are decided here.
//
// # Why speculation
//
// Batches in a queue depend on the batches ahead of them, so without
// speculation everything is serial: C waits for B, B waits for A. Speculation
// builds a batch against a guess about how its dependencies turn out. When
// the guess holds, the batch lands the moment the guessed-on dependencies
// land. If passed paths cover every possible outcome, the batch can land
// before those dependencies settle.
//
// # Paths
//
// The batch being speculated on is the head. One complete guess about it is a
// path: one assumption per dependency, each "succeeds" or "fails". A path's ID
// hashes the head and its assumptions, so a path *is* its guess; building the
// same guess again is a new attempt of the same path, and (path ID, attempt)
// names the resulting build.
//
// # A worked example
//
// A two-batch queue, where B depends on A and A is still building:
//
//	queue:  A ← B
//
//	B's speculation space is two paths:
//	  P1 = [A succeeds]   B built on top of A's result
//	  P2 = [A fails]      B built without A
//
// Fund both and every future is covered:
//
//   - While A is still unresolved, both P1 and P2 passing lets B bypass A and
//     land immediately: either possible future has already been validated.
//   - A succeeds and P1 passed: B lands the moment A lands. P2's guess
//     ("A fails") is broken — it can no longer come true — so its build is
//     cancelled to free the slot.
//   - A fails and P2 passed: B lands without A, again with no new build.
//     P1's guess is broken.
//   - A resolved either way, and every unbroken path failed: no future
//     remains in which B passes, so B fails.
//
// # The life of a path
//
// A path's status tracks its current attempt:
//
//	              funded            observed running       build finished
//	(no entry) ─────────► pending ─────────► building ──────┬──► passed
//	                         │                    │         └──► failed
//	           "stop this":  │                    │
//	           broken,       ▼                    ▼
//	           superseded, ─────► cancelling ◄────┘
//	           head cancelled,        │
//	           or preempted           │  build observed stopped, or
//	                                  │  nothing was ever dispatched
//	                                  ▼
//	                              cancelled
//
// A path is funded — given one of the slots under the queue's cap on
// concurrent builds, the build budget — when its guess is judged worth
// building, and every pending, building, and cancelling path holds its slot
// until its build stops. A path is broken once a dependency's actual result
// proves one of its assumptions wrong: its guess can no longer come true, so
// its build is cancelled to free the slot. A path is superseded when its head
// becomes landable, so any still-running siblings are cancelled too.
//
// Cancelling is intent, not fact: the build keeps its slot until CI actually
// stops it, and only an observation of that stop (or proof nothing was ever
// dispatched) moves the path to cancelled. The intent needs no dispatch of its
// own — the poll loop reads it off the set and asks the runner to stop the
// build. A terminal path can be resurrected by a new build proposal — status
// returns to pending and Attempt increments, the one backwards step in the
// diagram.
//
// # The life of a batch, as seen from here
//
//	Created ──admit──► Speculating ──┬── land or bypass ──► Landing
//	                                 └── fail ─────────────► Failed
//	user cancel (cancel stage):
//	   ... ──► Cancelling ── every path stopped ──► Cancelled
//
// A batch is admitted either by the message that names it or by the next run
// that finds it still in Created, whichever comes first. Created is
// dependency-eligible, so a batch left there would be a dependency nothing can
// resolve; admission cannot be left to rest on one message arriving.
//
// Failed and Cancelled fan out to the conclude stage, which reconciles the
// batch's requests.
//
// # Conditional stages and receipts
//
// A head moves through its stages conditionally, each with a stable
// generation and a durable confirmation, so a redelivery, a controller
// restart, or a dead-letter republish resumes instead of re-enacting:
//
//   - A path's run generation is its (ID, Attempt): the build link, the poll
//     chain, and the build-signal receipt are all correlated by it, so an old
//     signal can never act on a later attempt.
//   - Build start is confirmed by the write-once PathBuild link; a linked
//     build is never triggered again.
//   - The build-signal hand-off is a stage receipt: the build stage writes it
//     before publishing and re-publishes under the receipt's message ID on
//     recovery, instead of relying on a redelivery counter or the queue's
//     deduplication window.
//   - Cancel is the path status itself (cancelling then cancelled); the poll
//     loop is level-triggered off the set, so there is no cancel message to
//     go stale.
//   - Winner selection is WinnerPathID in the path set, written in the same
//     version that supersedes the winner's siblings.
//   - The batch transition is the only arbitration between a land and a
//     concurrent cancellation: the compare-and-swap on the batch version is
//     what decides which decision committed, and a decision read from an old
//     version cannot write the new one. A land that loses to a cancellation
//     has its winner cleared and its paths converged to cancelling; it
//     neither receives a receipt nor reaches land.
//   - The land hand-off is a stage receipt keyed by the batch version the
//     Landing transition committed at. It is created after that transition
//     wins and before the land message is published, so an uncertain publish
//     response is recovered with the original message identity and a batch is
//     never landed twice.
//
// Receipts are immutable create-once records of their own
// (SpeculationReceipt), separate from the versioned path set: a stage's
// confirmation is append-only history, while the set stays the replaced
// snapshot of the head's current paths.
//
// # How a run works
//
// Every message is only a dirty signal — "this queue changed" — naming the
// batch that changed. The run then re-plans the whole queue from a single
// read; nothing carries over from earlier runs, so duplicated, delayed, or
// reordered signals are harmless, and a later run repairs whatever an
// earlier one left half-done.
//
//	signal ──► read ──► admit ──► finalize ──► ask ──► check ──► dispatch
//	           one      every     enact the    the     filter     save changes,
//	           read of  batch     outcomes     Specu-  its        hand builds to
//	           queue +  still in  the facts    lator   proposals  the build stage
//	           paths    Created   already
//	                              decide
//
// The Speculator is the extension that proposes which paths to fund or
// preempt. It only ever proposes: check.go filters its answer, and outcomes
// are computed here, never by the extension. Finalize runs before ask so the
// Speculator reasons over facts as they now stand, not over a picture the run
// is about to invalidate.
//
// # Ownership
//
// The path set has exactly one writer: this controller's run. The build stage
// starts builds and the buildsignal stage watches and stops them; both write
// only per-build records of their own (the build link and the build's status),
// which read folds into the snapshot, and buildsignal reads the set only as
// the kill list for builds nothing wants any more. One writer is what lets a
// run hold one version of a head's paths across its whole decision without a
// poll invalidating it mid-thought.
package speculate
