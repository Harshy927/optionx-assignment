// Package httpapi wires together the REST API (chi router) for the position
// engine: health checks now, orders/positions endpoints in later tasks.
package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/jmoiron/sqlx"

	"github.com/optionx/backend-assignment/internal/storage"
)

// HealthResponse is the JSON body returned by GET /health.
type HealthResponse struct {
	Status string `json:"status"`
	DB     string `json:"db"`
}

// HealthHandler returns a handler that pings the DB and reports status.
// Returns 200 with {"status":"ok","db":"ok"} when Postgres is reachable, and
// 503 with {"status":"degraded","db":"<error>"} otherwise.
func HealthHandler(db *sqlx.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if err := storage.Ping(r.Context(), db); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(HealthResponse{Status: "degraded", DB: err.Error()})
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(HealthResponse{Status: "ok", DB: "ok"})
	}
}
