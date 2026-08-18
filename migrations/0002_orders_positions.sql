-- 0002_orders_positions.sql
-- Durable storage for orders and positions, so an instrument actor's state
-- can be rebuilt on process restart before any replayed ticks are applied.
--
-- Idempotency for order fills does NOT rely on a separate seq watermark (as
-- candles do): an order's `status` column is itself the guard -- once an
-- order is 'filled' or 'cancelled', a resting-order match against a
-- replayed tick can never re-fire, because the actor only evaluates orders
-- still in 'resting' status (see internal/instrument/actor_state.go). What
-- durability provides here is making sure that guard survives a restart:
-- if a fill was applied in memory but the process crashed before this row
-- was written, the order is still 'resting' in Postgres, and the actor will
-- (correctly) re-evaluate and fill it once the replayed tick arrives again.

CREATE TABLE IF NOT EXISTS orders (
    id          TEXT PRIMARY KEY,
    token       TEXT NOT NULL,
    side        TEXT NOT NULL,
    type        TEXT NOT NULL,
    qty         BIGINT NOT NULL,
    limit_price DOUBLE PRECISION NOT NULL DEFAULT 0,
    status      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS orders_token_idx ON orders (token);

CREATE TABLE IF NOT EXISTS positions (
    token        TEXT PRIMARY KEY,
    qty          BIGINT NOT NULL DEFAULT 0,
    avg_price    DOUBLE PRECISION NOT NULL DEFAULT 0,
    realized_pnl DOUBLE PRECISION NOT NULL DEFAULT 0
);
