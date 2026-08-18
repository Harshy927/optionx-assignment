package storage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/optionx/backend-assignment/internal/instrument"
	"github.com/optionx/backend-assignment/internal/order"
)

// OrderStore implements instrument.Store against Postgres, giving each
// instrument actor durable write-through for orders and positions. It holds
// a plain *sqlx.DB (not a single *sqlx.Tx) because it is shared across every
// instrument actor in the process -- each call opens its own short-lived
// transaction, scoped to a single write.
type OrderStore struct {
	db *sqlx.DB
}

// NewOrderStore wraps db as an instrument.Store.
func NewOrderStore(db *sqlx.DB) *OrderStore {
	return &OrderStore{db: db}
}

// orderRow mirrors the orders table for scanning query results.
type orderRow struct {
	ID         string  `db:"id"`
	Token      string  `db:"token"`
	Side       string  `db:"side"`
	Type       string  `db:"type"`
	Qty        int64   `db:"qty"`
	LimitPrice float64 `db:"limit_price"`
	Status     string  `db:"status"`
	EntryID    string  `db:"entry_id"`
	SiblingID  string  `db:"sibling_id"`
}

func (r orderRow) toOrder() order.Order {
	return order.Order{
		ID: r.ID, Token: r.Token, Side: order.Side(r.Side), Type: order.Type(r.Type),
		Qty: r.Qty, LimitPrice: r.LimitPrice, Status: order.Status(r.Status),
		EntryID: r.EntryID, SiblingID: r.SiblingID,
	}
}

// positionRow mirrors the positions table.
type positionRow struct {
	Token       string  `db:"token"`
	Qty         int64   `db:"qty"`
	AvgPrice    float64 `db:"avg_price"`
	RealizedPnL float64 `db:"realized_pnl"`
}

func (r positionRow) toPosition() instrument.Position {
	return instrument.Position{Token: r.Token, Qty: r.Qty, AvgPrice: r.AvgPrice, RealizedPnL: r.RealizedPnL}
}

// SaveOrder implements instrument.Store. It is used for order-status-only
// transitions (a newly-resting order, or a cancellation) that do not touch
// the position ledger.
func (s *OrderStore) SaveOrder(ctx context.Context, o order.Order) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO orders (id, token, side, type, qty, limit_price, status, entry_id, sibling_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status`,
		o.ID, o.Token, string(o.Side), string(o.Type), o.Qty, o.LimitPrice, string(o.Status), o.EntryID, o.SiblingID)
	if err != nil {
		return fmt.Errorf("save order %s: %w", o.ID, err)
	}
	return nil
}

// SaveFillTransition implements instrument.Store. It persists the
// now-filled order and the resulting position in a single transaction, so a
// crash between the two writes is impossible to observe: either both are
// durable, or neither is (and the order is reloaded as still 'resting' on
// restart, which the actor can safely re-evaluate against a replayed tick).
func (s *OrderStore) SaveFillTransition(ctx context.Context, o order.Order, pos instrument.Position) error {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO orders (id, token, side, type, qty, limit_price, status, entry_id, sibling_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status`,
		o.ID, o.Token, string(o.Side), string(o.Type), o.Qty, o.LimitPrice, string(o.Status), o.EntryID, o.SiblingID); err != nil {
		return fmt.Errorf("save order %s: %w", o.ID, err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO positions (token, qty, avg_price, realized_pnl)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (token) DO UPDATE SET
			qty = EXCLUDED.qty, avg_price = EXCLUDED.avg_price, realized_pnl = EXCLUDED.realized_pnl`,
		pos.Token, pos.Qty, pos.AvgPrice, pos.RealizedPnL); err != nil {
		return fmt.Errorf("save position %s: %w", pos.Token, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit fill transition for order %s: %w", o.ID, err)
	}
	return nil
}

// LoadPosition returns the persisted position for token, or a zero Position
// (with Token set) if none has been recorded yet.
func LoadPosition(ctx context.Context, q sqlx.QueryerContext, token string) (instrument.Position, error) {
	var row positionRow
	err := sqlx.GetContext(ctx, q, &row, `SELECT token, qty, avg_price, realized_pnl FROM positions WHERE token = $1`, token)
	if err == sql.ErrNoRows {
		return instrument.Position{Token: token}, nil
	}
	if err != nil {
		return instrument.Position{}, fmt.Errorf("load position for %s: %w", token, err)
	}
	return row.toPosition(), nil
}

// LoadOrdersForToken returns every order previously persisted for token, any
// status.
func LoadOrdersForToken(ctx context.Context, q sqlx.QueryerContext, token string) ([]order.Order, error) {
	rows, err := q.QueryxContext(ctx, `
		SELECT id, token, side, type, qty, limit_price, status, entry_id, sibling_id FROM orders WHERE token = $1`, token)
	if err != nil {
		return nil, fmt.Errorf("load orders for %s: %w", token, err)
	}
	defer rows.Close()

	var out []order.Order
	for rows.Next() {
		var row orderRow
		if err := rows.StructScan(&row); err != nil {
			return nil, fmt.Errorf("scan order row: %w", err)
		}
		out = append(out, row.toOrder())
	}
	return out, rows.Err()
}

// LoadAllInstrumentState reconstructs an instrument.InitialState for every
// token that has a persisted position or order, so cmd/server can seed every
// instrument's actor on boot with a single pair of bulk queries rather than
// one query per instrument.
func LoadAllInstrumentState(ctx context.Context, q sqlx.QueryerContext) (map[string]instrument.InitialState, error) {
	out := make(map[string]instrument.InitialState)

	posRows, err := q.QueryxContext(ctx, `SELECT token, qty, avg_price, realized_pnl FROM positions`)
	if err != nil {
		return nil, fmt.Errorf("list positions: %w", err)
	}
	defer posRows.Close()
	for posRows.Next() {
		var row positionRow
		if err := posRows.StructScan(&row); err != nil {
			return nil, fmt.Errorf("scan position row: %w", err)
		}
		st := out[row.Token]
		st.Position = row.toPosition()
		out[row.Token] = st
	}
	if err := posRows.Err(); err != nil {
		return nil, err
	}

	orderRows, err := q.QueryxContext(ctx, `SELECT id, token, side, type, qty, limit_price, status, entry_id, sibling_id FROM orders`)
	if err != nil {
		return nil, fmt.Errorf("list orders: %w", err)
	}
	defer orderRows.Close()
	for orderRows.Next() {
		var row orderRow
		if err := orderRows.StructScan(&row); err != nil {
			return nil, fmt.Errorf("scan order row: %w", err)
		}
		st := out[row.Token]
		st.Orders = append(st.Orders, row.toOrder())
		out[row.Token] = st
	}
	if err := orderRows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}
