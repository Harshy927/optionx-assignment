package feed

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseLine(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		want    Tick
		wantErr bool
	}{
		{
			name: "valid tick",
			line: `{"seq": 184223, "token": "NIFTY26AUG24800CE", "ltp": 132.55, "ts": 1755500000123}`,
			want: Tick{Seq: 184223, Token: "NIFTY26AUG24800CE", LTP: 132.55, Ts: 1755500000123},
		},
		{
			name: "zero seq is valid (first tick for instrument)",
			line: `{"seq": 0, "token": "BANKNIFTY26AUG55000PE", "ltp": 420.10, "ts": 1000}`,
			want: Tick{Seq: 0, Token: "BANKNIFTY26AUG55000PE", LTP: 420.10, Ts: 1000},
		},
		{
			name:    "malformed json",
			line:    `{"seq": 1, "token": "X"`,
			wantErr: true,
		},
		{
			name:    "empty line",
			line:    ``,
			wantErr: true,
		},
		{
			name:    "missing token",
			line:    `{"seq": 1, "ltp": 100.0, "ts": 1000}`,
			wantErr: true,
		},
		{
			name:    "wrong type for ltp",
			line:    `{"seq": 1, "token": "X", "ltp": "not-a-number", "ts": 1000}`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseLine([]byte(tc.line))
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
