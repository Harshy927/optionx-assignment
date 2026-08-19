# NOTES.md

PROCESS FOLLOWED:
1. Started by using the Plan Agent to design the High level flow of the position engine, doing back and forth with AI for requirement Gathering and Planning
2. Specifically Discussed approach for cancel-vs-trigger race condition, idempotency handling and websocket backpressure before implementation to have the best implementation for current use case.
3. after all the discussion, AI did not steer from the discussed path as we had broken down the project into well defined tasks, so no need of correction from my side,  only decisions were made by me and implemented by AI.

## AI usage

This entire implementation was produced by an AI coding agent (Kiro), working
from a design the user and the agent worked out together in conversation
before any code was written: library choices, project layout, the
concurrency model (actor-per-instrument), the idempotency mechanism
(seq watermark), and the backpressure policy (per-client coalescing) were
all discussed and explicitly chosen — with reasoning and tradeoffs
against alternatives — before implementation began. The agent then
implemented, tested, and verified each of the 11 planned tasks in sequence,
running the real service against the real `tickgen` feed and a real local
Postgres instance after every task, not just running unit tests.

Given that scope, the honest log is not "here are the three lines the AI
wrote" — it's the reverse: essentially all of it is AI-authored, and what
follows is what a reviewer should actually interrogate: the real bugs the
agent found and fixed during implementation, the design tradeoffs it made
and why, and the places worth pressing on in a follow-up interview.


## Design choices worth defending in review

These weren't left to the AI's discretion — each was discussed with the
user first, with alternatives and tradeoffs, before being implemented. A
reviewer should expect the author (agent or human) to justify each one on
its own terms, not just "that's what was there":

- **Actor-per-instrument over per-row DB locking** for the cancel-vs-trigger
  race. Chosen because ticks arrive in bursts of hundreds/sec — a DB
  round-trip per tick to check for a crossing order wouldn't keep up. The
  tradeoff: correctness now depends on state living correctly in one Go
  process's memory, recovered from Postgres on restart, rather than being
  enforced by the database itself regardless of process count. Explored
  in `internal/instrument/actor.go` and `actor_state.go`.
- **Seq watermark over a full dedup log** for idempotency. Sufficient
  because the feed's replay model is "restart from an earlier point,"
  never arbitrary reordering — a monotonic per-instrument watermark is the
  minimal mechanism that satisfies the actual guarantee needed. A full
  dedup log (storing every seen seq) would be over-engineering for this
  feed's stated semantics, at the cost of unbounded table growth.
- **Coalescing/overwrite over queuing or disconnecting** for WebSocket
  backpressure. A position/P&L viewer needs current truth, not a replay of
  every intermediate tick, so overwriting a not-yet-sent update is strictly
  fine and directly implements the required 100ms throttle. Verified
  against a real stalled socket, not a mock, in
  `internal/ws/handler_test.go`.
- **Fill price is always the observed tick LTP**, never the order's own
  limit price, for both market and limit fills. A limit price is a
  worst-acceptable bound, not a promised execution price; filling at it
  would record a trade at a price the market never actually printed.
- **Bracket children persisted as plain columns on the existing `orders`
  table** (`entry_id`, `sibling_id`) rather than a separate bracket table.
  Chosen so the existing `Store` interface and its Postgres implementation
  needed zero changes to carry the linkage durably — the tradeoff is that
  `orders` now has two nullable-ish columns that are meaningless for a
  standalone order.