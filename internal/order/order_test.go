package order

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrder_Validate(t *testing.T) {
	tests := []struct {
		name    string
		order   Order
		wantErr bool
	}{
		{
			name:  "valid market order",
			order: Order{Token: "X", Side: Buy, Type: Market, Qty: 10},
		},
		{
			name:  "valid limit order",
			order: Order{Token: "X", Side: Sell, Type: Limit, Qty: 10, LimitPrice: 100.0},
		},
		{
			name:    "missing token",
			order:   Order{Side: Buy, Type: Market, Qty: 10},
			wantErr: true,
		},
		{
			name:    "invalid side",
			order:   Order{Token: "X", Side: "up", Type: Market, Qty: 10},
			wantErr: true,
		},
		{
			name:    "invalid type",
			order:   Order{Token: "X", Side: Buy, Type: "stop", Qty: 10},
			wantErr: true,
		},
		{
			name:    "zero qty",
			order:   Order{Token: "X", Side: Buy, Type: Market, Qty: 0},
			wantErr: true,
		},
		{
			name:    "negative qty",
			order:   Order{Token: "X", Side: Buy, Type: Market, Qty: -5},
			wantErr: true,
		},
		{
			name:    "limit order missing limit price",
			order:   Order{Token: "X", Side: Buy, Type: Limit, Qty: 10},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.order.Validate()
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestWouldFill_Market(t *testing.T) {
	buy := Order{Token: "X", Side: Buy, Type: Market, Qty: 10}
	sell := Order{Token: "X", Side: Sell, Type: Market, Qty: 10}

	// Market orders are always marketable, regardless of price.
	assert.True(t, WouldFill(buy, 100.0))
	assert.True(t, WouldFill(buy, 0.01))
	assert.True(t, WouldFill(sell, 100.0))
	assert.True(t, WouldFill(sell, 999999.0))
}

func TestWouldFill_LimitBuy(t *testing.T) {
	limitBuy := Order{Token: "X", Side: Buy, Type: Limit, Qty: 10, LimitPrice: 100.0}

	// A limit buy fills when ltp <= limit (willing to buy at 100 or cheaper).
	assert.True(t, WouldFill(limitBuy, 99.0), "ltp below limit must fill")
	assert.True(t, WouldFill(limitBuy, 100.0), "ltp exactly at limit must fill")
	assert.False(t, WouldFill(limitBuy, 100.01), "ltp above limit must not fill")
}

func TestWouldFill_LimitSell(t *testing.T) {
	limitSell := Order{Token: "X", Side: Sell, Type: Limit, Qty: 10, LimitPrice: 100.0}

	// A limit sell fills when ltp >= limit (willing to sell at 100 or higher).
	assert.True(t, WouldFill(limitSell, 101.0), "ltp above limit must fill")
	assert.True(t, WouldFill(limitSell, 100.0), "ltp exactly at limit must fill")
	assert.False(t, WouldFill(limitSell, 99.99), "ltp below limit must not fill")
}

func TestFillPrice_IsTheObservedLTP(t *testing.T) {
	assert.Equal(t, 123.45, FillPrice(123.45))
}
