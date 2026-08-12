// Package migrate applies the schema, once each, in order.
//
// The naive loop — psql over every *.up.sql — works exactly once. The second run
// fails on "relation already exists", which turns a routine restart into an
// outage and a quickstart into a bad first impression. A ledger of what has been
// applied is the difference.
package migrate

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nobledeveloper01/ReconSync/migrations"
)

// ledger records what has run. Created before anything else, and idempotently,
// because it is the one table that cannot be created by a migration.
const ledger = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// lockKey serialises migration runs across replicas. Two containers starting
// together would otherwise race the same DDL, and the failure would look like a
// crashloop rather than what it is.
const lockKey = 8891273744026107 // arbitrary, fixed

// Result reports what a run did.
type Result struct {
	Applied []string
	Skipped []string
}

// Up applies every pending migration, in filename order.
func Up(ctx context.Context, pool *pgxpool.Pool) (Result, error) {
	var res Result

	files, err := upFiles()
	if err != nil {
		return res, err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return res, fmt.Errorf("migrate: acquire connection: %w", err)
	}
	defer conn.Release()

	// Session-scoped, not transaction-scoped: each migration runs in its own
	// transaction, and the lock has to span all of them.
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, int64(lockKey)); err != nil {
		return res, fmt.Errorf("migrate: take lock: %w", err)
	}
	defer func() { _, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, int64(lockKey)) }()

	if _, err := conn.Exec(ctx, ledger); err != nil {
		return res, fmt.Errorf("migrate: create ledger: %w", err)
	}

	applied, err := appliedVersions(ctx, conn.Conn())
	if err != nil {
		return res, err
	}

	for _, name := range files {
		version := versionOf(name)
		if _, done := applied[version]; done {
			res.Skipped = append(res.Skipped, version)
			continue
		}

		sql, err := migrations.FS.ReadFile(name)
		if err != nil {
			return res, fmt.Errorf("migrate: read %s: %w", name, err)
		}

		// One transaction per migration: a failure leaves the schema at the
		// last good version rather than half-way through a broken one.
		tx, err := conn.Begin(ctx)
		if err != nil {
			return res, fmt.Errorf("migrate: begin %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			_ = tx.Rollback(ctx)
			return res, fmt.Errorf("migrate: apply %s: %w%s", version, err, baselineHint(version, applied))
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
			_ = tx.Rollback(ctx)
			return res, fmt.Errorf("migrate: record %s: %w", version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return res, fmt.Errorf("migrate: commit %s: %w", version, err)
		}
		res.Applied = append(res.Applied, version)
	}
	return res, nil
}

// Baseline marks every migration as applied without running any, for a database
// that was migrated before this ledger existed.
//
// Separate command and never automatic: guessing that an existing schema is
// current would silently skip a migration the database actually needs.
func Baseline(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	files, err := upFiles()
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, ledger); err != nil {
		return nil, fmt.Errorf("migrate: create ledger: %w", err)
	}

	var marked []string
	for _, name := range files {
		version := versionOf(name)
		tag, err := pool.Exec(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING`, version)
		if err != nil {
			return nil, fmt.Errorf("migrate: record %s: %w", version, err)
		}
		if tag.RowsAffected() > 0 {
			marked = append(marked, version)
		}
	}
	return marked, nil
}

// Status reports which migrations have run and which are pending.
func Status(ctx context.Context, pool *pgxpool.Pool) (applied, pending []string, err error) {
	files, err := upFiles()
	if err != nil {
		return nil, nil, err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("migrate: acquire connection: %w", err)
	}
	defer conn.Release()

	var done map[string]struct{}
	if _, err := conn.Exec(ctx, ledger); err != nil {
		return nil, nil, fmt.Errorf("migrate: create ledger: %w", err)
	}
	if done, err = appliedVersions(ctx, conn.Conn()); err != nil {
		return nil, nil, err
	}

	for _, name := range files {
		version := versionOf(name)
		if _, ok := done[version]; ok {
			applied = append(applied, version)
			continue
		}
		pending = append(pending, version)
	}
	return applied, pending, nil
}

func appliedVersions(ctx context.Context, conn *pgx.Conn) (map[string]struct{}, error) {
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("migrate: read ledger: %w", err)
	}
	defer rows.Close()

	out := map[string]struct{}{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("migrate: scan ledger: %w", err)
		}
		out[v] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("migrate: iterate ledger: %w", err)
	}
	return out, nil
}

// upFiles returns the .up.sql files in filename order, which is the order they
// must run in — the numeric prefix is what makes that true.
func upFiles() ([]string, error) {
	entries, err := fs.Glob(migrations.FS, "*.up.sql")
	if err != nil {
		return nil, fmt.Errorf("migrate: list migrations: %w", err)
	}
	if len(entries) == 0 {
		return nil, errors.New("migrate: no migrations are embedded")
	}
	sort.Strings(entries)
	return entries, nil
}

func versionOf(name string) string {
	return strings.TrimSuffix(name, ".up.sql")
}

// baselineHint turns the confusing failure — a fresh ledger against an existing
// schema — into the one instruction that fixes it.
func baselineHint(version string, applied map[string]struct{}) string {
	if len(applied) > 0 || !strings.HasPrefix(version, "0001") {
		return ""
	}
	return "\n  This looks like a database migrated before the ledger existed. " +
		"Run: reconsyncctl migrate baseline"
}
