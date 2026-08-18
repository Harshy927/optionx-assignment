# OptionX Backend Assignment — Position Engine

Build a small position engine on a live tick feed.

OptionX builds a real-time options trading terminal for Indian F&O markets. Our backend
(Go, MongoDB, Redis, WebSockets) sits between live market data and real money: it ingests
ticks, holds resting orders, computes live P&L, and streams it all back to traders. This
assignment is a miniature of that exact system. Nothing in it is artificial — every
requirement maps to a problem our production engine handles today, and a couple of them
map to bugs we have actually shipped and fixed.

**Expected effort:** ~6 focused hours over 3–4 days.
**Language:** Go for the core service (helper scripts in anything you like).

## The tick feed (provided)

`tickgen/` in this repo streams newline-delimited JSON ticks over TCP for 20 option
instruments. Rates are deliberately bursty — a calm few seconds, then bursts of several
hundred ticks per second.

```bash
cd tickgen
go run . -addr :9001          # serve the stream
nc localhost 9001 | head      # peek at it
```

Each tick looks like:

```json
{"seq": 184223, "token": "NIFTY26AUG24800CE", "ltp": 132.55, "ts": 1755500000123}
```

- `seq` is monotonically increasing **per instrument**.
- The stream is **deterministic per seed**: restarting `tickgen` with the same `-seed`
  (default 42) replays the identical tick content. `-from N` starts the stream at global
  event N, so you can replay history your engine has already seen. **Your engine must
  cope with seeing the same tick twice.**

## What you'll build

One Go service, three milestones.

### 1. Ingest & aggregate

- Consume the feed; maintain in-memory last price per instrument.
- Aggregate ticks into 1-minute OHLC candles and persist them (MongoDB preferred; if you
  use PostgreSQL instead, add a short note on what you'd change for Mongo).
- A replayed tick (same `seq`) must never distort a candle.

### 2. Orders & positions

- REST API: place and cancel orders — **market** (fills at last price immediately) and
  **limit** (rests; fills when a tick crosses its price).
- Maintain a position ledger per instrument: quantity, average price, realized and
  unrealized P&L updating off the feed.
- Orders and positions must survive a process restart: resting orders keep resting, and
  a restart + feed replay must not double-fill anything.

### 3. Stream it back

- A WebSocket endpoint where a client subscribes to instruments and receives position +
  P&L updates.
- Throttle to at most one update per instrument per 100 ms per client.
- A slow or stalled client must not slow the tick loop or other clients — decide your
  backpressure policy and write it down.

## The part we grade hardest

Two scenarios, both taken from our production history — **write tests for both**:

1. **Cancel vs. trigger race.** A cancel request arrives in the same instant a tick
   crosses a resting limit order. Exactly one outcome must win, and the API caller and
   the ledger must agree on which one it was.
2. **Restart under replay.** Kill the process mid-burst, restart it, replay the feed from
   an earlier `seq`. No duplicate fills, no distorted candles, no phantom positions.

## Stretch goal — optional, pick at most one

- **Bracket orders:** an entry with attached stop-loss and target children; when one
  child fills, the other cancels (OCO). The cancel-race rules above apply to the pair.
- **Risk check:** a per-instrument max-position limit that rejects violating orders —
  correctly, even under concurrent order placement.

## Ground rules

- **AI tools are allowed and encouraged** — we use them heavily ourselves. Keep a
  `NOTES.md` logging where AI wrote code and what you had to correct or reject. In the
  follow-up interview we'll pick lines from your code at random and ask you to defend
  them; "the AI wrote that" is a fine origin story and a failing explanation.
- **Timebox honestly.** ~6 focused hours. If milestone 3 is partial, a clear note on
  what's missing and how you'd finish beats a rushed implementation. We're hiring
  judgment, not stamina.
- **No UI.** `curl` examples or a tiny script exercising the API are enough.

## What to submit

- A repo (or zip) with the service, tests for the two graded scenarios, and a `README`:
  how to run it, your design decisions (especially the backpressure and idempotency
  choices), and known gaps.
- `NOTES.md` — the AI log plus anything you'd flag to a reviewer.
