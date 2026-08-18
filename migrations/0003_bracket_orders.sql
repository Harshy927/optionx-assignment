-- 0003_bracket_orders.sql
-- Adds bracket (OCO) order linkage columns to the existing orders table.
--
-- entry_id: set on a bracket's target/stop children to the ID of the entry
-- order that spawns them; empty for a standalone order or an entry itself.
-- sibling_id: links a bracket's two children to each other, so filling one
-- can locate and cancel the other (see internal/instrument/bracket.go).
--
-- Uses ALTER TABLE ... ADD COLUMN IF NOT EXISTS rather than a fresh
-- CREATE TABLE, since orders already exists from migration 0002 and this
-- project's migration runner has no rollback/versioning -- each migration
-- must be additive and safe to re-run (see storage.Migrate's doc comment).

ALTER TABLE orders ADD COLUMN IF NOT EXISTS entry_id TEXT NOT NULL DEFAULT '';
ALTER TABLE orders ADD COLUMN IF NOT EXISTS sibling_id TEXT NOT NULL DEFAULT '';
