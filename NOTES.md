# NOTES.md

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

## Real bugs found and fixed during implementation

- **Postgres DSN bug (Task 1).** The connection-string builder initially
  always emitted `password=` even when the password was empty. pgx's DSN
  parser does not tolerate an empty unquoted value there — it silently
  misparses the *next* key, and `dbname` got dropped, causing a "database
  \"admin\" does not exist" error that had nothing obviously to do with the
  password field. Root-caused by writing a minimal reproduction against
  `pgx.ParseConfig` directly, not by guessing. Fixed by omitting the
  `password=` key entirely when the password is blank
  (`internal/storage/db.go`).
- **`CancelOrderHandler` using `Peek` instead of `Actor` (Task 8).** After
  adding boot-time seeding, a pre-restart order's ID gets indexed into the
  registry's `orderIx` map immediately at construction — before any actor
  for that instrument has actually been spawned. The cancel handler was
  still calling `Registry.Peek` (deliberately non-creating, to avoid
  spawning actors on read-only lookups), which meant a cancel for a
  rediscovered pre-restart order would incorrectly report "not found." Fixed
  by switching the cancel path to `Registry.Actor` (which lazily creates and
  seeds the actor if needed) — this is a case where two individually
  reasonable-looking design choices (index eagerly; look up non-creating)
  interacted badly, and it only surfaces once persistence and restart
  recovery exist, which is exactly why Task 8's live end-to-end restart
  test caught it before it shipped.

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