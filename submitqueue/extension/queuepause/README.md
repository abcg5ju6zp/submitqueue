# Queue Pause Extension

Vendor-agnostic source of per-queue pause state. See [the pause and unpause RFC](../../../doc/rfc/submitqueue/queue-pause-and-unpause.md) for the design.

## Interfaces

### Store

Returns the pause state of a queue by name. The state is two independent switches: reject new land requests, and freeze processing of accepted requests. A queue with no entry is not paused, and an error means the backend could not be read.

The store only reports state. The gateway and orchestrator enforce it, so every deployment runs the same rejection and freezing logic and only the source of the state differs.

## Entities

`QueuePauseState` lives in `submitqueue/entity/queue_pause_state.go`.
