// cmd/migrate applies the SQL files in migrations/ in order.
//
// Why a Go program instead of golang-migrate: the whole point of the compose
// stack is that a fresh clone comes up with `docker compose up`. That needs the
// schema applied without anyone installing a binary on the host, and it must not
// download a container image at demo time either. This is ~120 lines of standard
// library plus pgx, which the project already depends on.
//
// Applied versions are recorded in schema_migrations, so re-running is a no-op
// and a failed migration is never half-recorded.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	dir := flag.String("dir", "migrations", "directory holding the .up.sql files")
	databaseURL := flag.String("url", os.Getenv("DATABASE_URL"), "postgres connection string (default $DATABASE_URL)")
	wait := flag.Duration("wait", 60*time.Second, "how long to wait for Postgres to accept connections")
	flag.Parse()

	if *databaseURL == "" {
		log.Fatal("migrate: DATABASE_URL is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *wait)
	defer cancel()

	conn, err := connect(ctx, *databaseURL)
	if err != nil {
		log.Fatalf("migrate: %v", err)
	}
	defer conn.Close(context.Background())

	if err := applyAll(context.Background(), conn, *dir); err != nil {
		log.Fatalf("migrate: %v", err)
	}
}

// connect retries: in docker compose, `depends_on: service_healthy` already
// waits for Postgres, but a restart of the db container can still race us, and a
// migration that fails because the database was not ready yet is a bad first
// impression for anyone cloning the repo.
func connect(ctx context.Context, url string) (*pgx.Conn, error) {
	var lastErr error
	for {
		conn, err := pgx.Connect(ctx, url)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("database never became reachable: %w", lastErr)
		case <-time.After(time.Second):
		}
	}
}

func applyAll(ctx context.Context, conn *pgx.Conn, dir string) error {
	// gen_random_uuid() is built into Postgres 13+, so no extension is needed;
	// pgcrypto is created anyway because an older server would otherwise fail
	// with "function gen_random_uuid() does not exist" on the first CREATE TABLE.
	if _, err := conn.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pgcrypto`); err != nil {
		return fmt.Errorf("enable pgcrypto: %w", err)
	}

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	files, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no *.up.sql files in %s", dir)
	}
	sort.Strings(files)

	for _, f := range files {
		version := strings.TrimSuffix(filepath.Base(f), ".up.sql")

		var exists bool
		if err := conn.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, version,
		).Scan(&exists); err != nil {
			return fmt.Errorf("check %s: %w", version, err)
		}
		if exists {
			fmt.Printf("migrate: %-20s already applied\n", version)
			continue
		}

		sql, err := os.ReadFile(f)
		if err != nil {
			return err
		}

		// One transaction per migration: either the whole file lands and is
		// recorded, or nothing does. A partially applied schema is the worst
		// failure mode here because the next run would skip the file.
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1)`, version,
		); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record %s: %w", version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit %s: %w", version, err)
		}
		fmt.Printf("migrate: %-20s applied\n", version)
	}
	return nil
}
