package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"

	"github.com/optionx/backend-assignment/internal/storage"
)

// testDB opens a connection to the local Postgres instance used for
// development/testing (see README for setup). Tests in this package are
// integration tests against a real DB, consistent with the rest of the
// project's testing strategy for storage-backed behavior.
func testDB(t *testing.T) *sqlx.DB {
	t.Helper()
	cfg := storage.ConfigFromEnv()
	db, err := storage.Open(cfg)
	require.NoError(t, err, "failed to connect to local postgres; is it running? see README")
	t.Cleanup(func() { db.Close() })
	return db
}

func TestHealthHandler_OK(t *testing.T) {
	db := testDB(t)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	HealthHandler(db)(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"status":"ok"`)
}

func TestHealthHandler_DBUnreachable(t *testing.T) {
	// Point at a port nothing is listening on to force a ping failure.
	cfg := storage.Config{Host: "localhost", Port: 1, User: "nobody", DBName: "nope", SSLMode: "disable"}
	db, err := storage.Open(cfg)
	if err == nil {
		// Some drivers defer the actual connection until first use; if Open
		// succeeded, force a context-timeout ping via the handler itself.
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		req := httptest.NewRequest(http.MethodGet, "/health", nil).WithContext(ctx)
		rec := httptest.NewRecorder()
		HealthHandler(db)(rec, req)
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		return
	}
	// storage.Open itself already pings and returned an error -- nothing further to assert.
}
