package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

const migrationLockID int64 = 824019367

func RunMigrations(ctx context.Context, pool *pgxpool.Pool, directory string) error {
	if pool == nil {
		return fmt.Errorf("database pool is required")
	}
	if strings.TrimSpace(directory) == "" {
		return fmt.Errorf("migration directory is required")
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("read migration directory %q: %w", directory, err)
	}
	files := make([]string, 0, len(entries))
	versions := make(map[int64]string, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".up.sql") {
			version, err := migrationVersion(entry.Name())
			if err != nil {
				return err
			}
			if previous, exists := versions[version]; exists {
				return fmt.Errorf("migrations %q and %q share version %d", previous, entry.Name(), version)
			}
			versions[version] = entry.Name()
			files = append(files, entry.Name())
		}
	}
	if len(files) == 0 {
		return fmt.Errorf("no up migrations found in %q", directory)
	}
	sort.Strings(files)

	for _, name := range files {
		version, err := migrationVersion(name)
		if err != nil {
			return err
		}
		migration, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			return fmt.Errorf("read migration %q: %w", name, err)
		}
		if err := applyMigration(ctx, pool, version, name, string(migration)); err != nil {
			return err
		}
	}
	return nil
}

func applyMigration(ctx context.Context, pool *pgxpool.Pool, version int64, name, migration string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %q: %w", name, err)
	}
	defer tx.Rollback(context.Background())

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS message_schema_migrations (
			version BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("create migration table: %w", err)
	}
	var applied bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM message_schema_migrations WHERE version = $1)`, version).Scan(&applied); err != nil {
		return fmt.Errorf("check migration %q: %w", name, err)
	}
	if applied {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("finish migration check %q: %w", name, err)
		}
		return nil
	}

	for _, statement := range strings.Split(migration, ";") {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		if _, err := tx.Exec(ctx, statement); err != nil {
			return fmt.Errorf("apply migration %q: %w", name, err)
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO message_schema_migrations (version, name) VALUES ($1, $2)`, version, name); err != nil {
		return fmt.Errorf("record migration %q: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %q: %w", name, err)
	}
	return nil
}

func migrationVersion(name string) (int64, error) {
	prefix, _, ok := strings.Cut(name, "_")
	if !ok {
		return 0, fmt.Errorf("migration name %q must start with a numeric version and underscore", name)
	}
	version, err := strconv.ParseInt(prefix, 10, 64)
	if err != nil || version < 1 {
		return 0, fmt.Errorf("migration name %q has invalid version", name)
	}
	return version, nil
}
