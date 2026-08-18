-- 0001_init.sql
-- Base schema: per-instrument seq watermark (idempotency) and 1-minute OHLC candles.
--
-- The watermark table is the single source of truth for "have we already applied
-- this tick" -- every state mutation driven by a tick (candle update, order fill)
-- must check-and-bump this watermark inside the SAME transaction as the mutation,
-- so a replayed/duplicate tick (same seq) is guaranteed to be a no-op.

CREATE TABLE IF NOT EXISTS instrument_seq_watermark (
    token    TEXT PRIMARY KEY,
    last_seq BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS candles (
    token      TEXT      NOT NULL,
    bucket_ts  TIMESTAMPTZ NOT NULL, -- start of the 1-minute bucket, UTC, truncated to the minute
    open       DOUBLE PRECISION NOT NULL,
    high       DOUBLE PRECISION NOT NULL,
    low        DOUBLE PRECISION NOT NULL,
    close      DOUBLE PRECISION NOT NULL,
    tick_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (token, bucket_ts)
);
