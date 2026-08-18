# OptionX Position Engine

A small position engine that ingests a live tick feed, aggregates 1-minute OHLC
candles, matches market/limit/bracket orders against the feed, maintains a
per-instrument position ledger, and streams position/P&L updates over
WebSocket. Built for the OptionX backend assignment.

## Running it

### Prerequisites

- Go 1.22+ (developed against 1.26)
- PostgreSQL running locally, reachable with no password (see
  [Database substitution](#database-mongodb--postgresql) below)

### 1. Create the database

```bash
createdb optionx
```

The server applies its own migrations on startup (`migrations/*.sql`, run in
filename order), so no separate migration step is needed.

### 2. Start the tick feed

```bash
cd tickgen
go run . -addr :9001
```

### 3. Start the server

```bash
go run ./cmd/server
```

Environment variables (all optional, sensible localhost defaults):

| Variable               | Default     | Purpose                          |
|-------------------------|-------------|-----------------------------------|
| `OPTIONX_DB_HOST`       | `localhost` | Postgres host                     |
| `OPTIONX_DB_USER`       | `$USER`     | Postgres user                     |
| `OPTIONX_DB_PASSWORD`   | *(empty)*   | Postgres password                 |
| `OPTIONX_DB_NAME`       | `optionx`   | Database name                     |
| `OPTIONX_DB_SSLMODE`    | `disable`   | Postgres SSL mode                 |
| `OPTIONX_HTTP_ADDR`     | `:8080`     | HTTP/WebSocket listen address     |
| `OPTIONX_FEED_ADDR`     | `localhost:9001` | tickgen address              |

The server connects to Postgres, applies migrations, seeds every instrument's
candle aggregator and order/position state from whatever was persisted by a
previous run, then connects to the tick feed and starts serving.

### 4. Exercise the API

```bash
# Place a market order (fills immediately at the last known price)
curl -X POST localhost:8080/orders -H "Content-Type: application/json" -d \
  '{"token":"NIFTY26AUG24800CE","side":"buy","type":"market","qty":10}'

# Place a limit order (rests until a tick crosses the limit price)
curl -X POST localhost:8080/orders -H "Content-Type: application/json" -d \
  '{"token":"NIFTY26AUG24800CE","side":"buy","type":"limit","qty":10,"limit_price":100.0}'

# Place a bracket order (entry + take-profit + stop-loss, OCO)
curl -X POST localhost:8080/orders/bracket -H "Content-Type: application/json" -d \
  '{"token":"NIFTY26AUG24800CE","side":"buy","qty":10,"entry_type":"market",
    "target_price":260.0,"stop_price":245.0}'

# Cancel an order (by ID, from any of the above responses)
curl -X DELETE localhost:8080/orders/<order-id>

# Check a position
curl localhost:8080/positions/NIFTY26AUG24800CE

# Health check
curl localhost:8080/health
```

WebSocket streaming:

```bash
# any WebSocket client; wscat/websocat both work, e.g.:
websocat ws://localhost:8080/ws
# then, to filter to specific instruments, send a control message:
{"action":"subscribe","tokens":["NIFTY26AUG24800CE"]}
```

A client that never sends a subscribe message receives updates for every
instrument (no filter) — convenient for a quick manual check.

### 5. Run the tests

```bash
go test ./... -race
```

Requires the same local Postgres instance (storage-package tests are
integration tests against a real database, not mocks — see
[Testing approach](#testing-approach)).

The two explicitly graded scenarios each have dedicated tests:

- **Cancel vs. trigger race:** `internal/instrument/actor_test.go`
  (`TestActor_CancelVsTrigger_Race_SingleDeterministicWinner`) and its bracket
  extension in `internal/instrument/bracket_test.go`
  (`TestActor_BracketCancelVsTrigger_Race_SingleDeterministicWinner`). Each
  runs 200 concurrent trials; run repeatedly under the race detector with:
  ```bash
  go test ./internal/instrument/... -race -count=50 -run CancelVsTrigger
  ```
- **Restart under replay:** `internal/storage/restart_recovery_test.go`
  (`TestRestartUnderReplay_RestingOrder_FillsExactlyOnce` and
  `TestRestartUnderReplay_AlreadyFilledOrder_NeverRefills`), plus the candle
  analog in `internal/storage/candle_store_test.go`
  (`TestApplyTick_RestartAndReplay_NoDoubleCount`).

## Design decisions

### Database: MongoDB → PostgreSQL

The assignment prefers MongoDB; this implementation uses PostgreSQL instead.
What would change to move to Mongo:

- The seq-watermark check-and-bump (idempotency) and the order/position write
  (durability) currently rely on Postgres transactions
  (`internal/storage/candle_store.go`'s `ApplyTick`,
  `internal/storage/order_store.go`'s `SaveFillTransition`) to make two writes
  atomic. Mongo would need either a multi-document transaction (available on
  replica sets) or a redesign around single-document atomic updates (e.g.
  storing watermark + candle in one document per instrument, or using
  `findAndModify` with an optimistic version field).
- `sqlx` scanning/`db:` tags (`internal/storage/*_store.go`) would become BSON
  struct tags and the Mongo Go driver's API.
- Migrations (`migrations/*.sql`) would become index-creation calls, since
  Mongo has no schema to migrate.
- The core domain logic (`internal/order`, `internal/instrument`,
  `internal/candle`) is untouched by this choice — it has no database
  dependency at all, by design.

### Idempotency: seq watermark

Every instrument has a persisted `last_seq` (`instrument_seq_watermark`
table). A tick is applied only if `tick.seq > last_seq`; the check and the
resulting write happen in the same transaction, so a crash between them is
never observable — either both happened or neither did. This is sufficient
because the feed only ever replays from an earlier point (never reorders), so
a simple watermark, not a full dedup log, is the right amount of mechanism.

Orders use a related but distinct guard: an order's `status` column is
itself idempotent — once `filled` or `cancelled`, `handleTick` only ever
re-evaluates orders still `resting`, so a replayed tick can re-observe an
already-filled order without re-filling it. See
`internal/instrument/actor_state.go`.

### Concurrency: one actor goroutine per instrument

Each instrument's resting orders and position ledger are owned exclusively
by a single goroutine (`internal/instrument/actor.go`). Every mutation —
ticks, order placement, cancellation — arrives as a message on that
goroutine's channel and is processed to completion before the next message
is read. This is what makes the cancel-vs-trigger race deterministic: a
cancel and a crossing tick for the same order can never be evaluated
concurrently, because only one can ever be "next" in the channel, and the
loser is told the truth (`already_filled` or the cancel simply wins) rather
than a guess.

This was chosen over per-row database locking because ticks arrive in
bursts of several hundred per second; a DB round-trip per tick just to check
for a crossing order would not keep up. The actor model resolves the race
entirely in memory and only touches Postgres to persist the outcome
afterward (write-through, not write-first).

### Backpressure: per-client coalescing, not queuing or disconnecting

The WebSocket hub (`internal/ws`) gives each client a buffer of "latest
update per instrument." Publishing into it is always non-blocking: a newer
update for an instrument overwrites the older one rather than queuing. A
100ms ticker flushes the buffer to the socket. Consequences:

- The tick loop never blocks on a slow client — `Hub.Publish` only ever does
  a map write, never I/O.
- A stalled client is never disconnected, either — it just sees stale-then-
  current state once it catches up, rather than a backlog of every value it
  missed. This fits a position/P&L feed: a viewer cares what's true *now*,
  not the full history of every intermediate tick.

Tested with two real WebSocket clients — one deliberately never reading —
in `internal/ws/handler_test.go`.

### Fill price

Both market and limit orders fill at the observed LTP of the tick that made
them marketable, not at the order's limit price. A limit price is a
worst-acceptable-price bound, not a promised execution price; filling at the
limit price itself would fabricate a price the market never actually traded
at. See `internal/order/order.go`'s `FillPrice`.



## Stretch goal: bracket (OCO) orders

`POST /orders/bracket` accepts an entry plus target (take-profit, a limit
order) and stop (stop-loss, a new `Stop` order type that triggers in the
opposite direction from a limit at the same side) on the opposite side of
the entry. Both children are validated and stored atomically with the
entry; when either fills, the other is cancelled in the same
message-processing step that filled it (`internal/instrument/bracket.go`),
so the same cancel-race guarantees proven for a single order extend to the
pair — see `TestActor_BracketCancelVsTrigger_Race_SingleDeterministicWinner`.
