// Package storage owns the Postgres connection and schema migrations for the
// position engine. All SQL lives here (or in package-specific files within this
// package added by later tasks) -- we use sqlx directly rather than an ORM so the
// transactional check-and-update logic that idempotency depends on stays visible.
package storage

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/jmoiron/sqlx"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" driver with database/sql
)

// Config holds the connection parameters for Postgres. Values default to a local
// developer setup (see README) and can be overridden via environment variables.
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

// ConfigFromEnv builds a Config from environment variables, falling back to
// sensible local defaults for each field that isn't set. This project is
// local-only (no docker-compose), so the defaults assume a Postgres instance
// already running on localhost, as set up during development.
func ConfigFromEnv() Config {
	return Config{
		Host:     getEnv("OPTIONX_DB_HOST", "localhost"),
		Port:     5432,
		User:     getEnv("OPTIONX_DB_USER", currentOSUser()),
		Password: getEnv("OPTIONX_DB_PASSWORD", ""),
		DBName:   getEnv("OPTIONX_DB_NAME", "optionx"),
		SSLMode:  getEnv("OPTIONX_DB_SSLMODE", "disable"),
	}
}

func currentOSUser() string {
	if u := os.Getenv("USER"); u != "" {
		return u
	}
	return "postgres"
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// DSN renders the config as a libpq-style connection string for pgx.
//
// A blank password field is omitted rather than emitted as "password=" --
// pgx's connection-string parser does not tolerate an unquoted empty value
// there and will misparse the following key (observed: dbname gets dropped).
func (c Config) DSN() string {
	dsn := fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.DBName, c.SSLMode)
	if c.Password != "" {
		dsn += fmt.Sprintf(" password=%s", c.Password)
	}
	return dsn
}

// Open connects to Postgres via pgx (through database/sql/sqlx) and verifies the
// connection with a ping before returning.
func Open(cfg Config) (*sqlx.DB, error) {
	db, err := sqlx.Open("pgx", cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return db, nil
}

// Migrate applies every *.sql file found in dir, in filename order, inside its own
// transaction. It is intentionally simple (no migration-tracking table) since the
// migrations here are idempotent (CREATE TABLE IF NOT EXISTS) -- adequate for this
// assignment's local-only, single-developer scope. A real deployment would want a
// tracked migration history; noted in README as a known gap.
func Migrate(ctx context.Context, db *sqlx.DB, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read migrations dir %s: %w", dir, err)
	}

	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".sql" {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	for _, name := range files {
		path := dir + "/" + name
		contents, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", path, err)
		}

		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin tx for %s: %w", name, err)
		}
		if _, err := tx.ExecContext(ctx, string(contents)); err != nil {
			tx.Rollback()
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s: %w", name, err)
		}
	}
	return nil
}

// Ping is a thin wrapper used by the health endpoint.
func Ping(ctx context.Context, db *sqlx.DB) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return db.PingContext(ctx)
}
