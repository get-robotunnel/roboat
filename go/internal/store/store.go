// Package store is the tunnel's Postgres data-access layer. It uses pgx directly
// (no Supabase SDK) so the tunnel can run against any Postgres for self-hosting.
// It owns robot *connection identity* and connection runtime state; robot
// business metadata lives in the Operations database.
package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	tdb "github.com/get-robotunnel/robotunnel-tunnel/go/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when a requested row does not exist.
var ErrNotFound = errors.New("not found")

type Store struct {
	Pool *pgxpool.Pool
}

// New opens a pooled connection and verifies connectivity.
func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Close() { s.Pool.Close() }

// HashAPIKey returns the hex sha256 of a robot_api_key, the form stored in
// robot_conn.api_key_hash.
func HashAPIKey(apiKey string) string {
	sum := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(sum[:])
}

// LookupAPIKeyHash returns the stored api_key_hash for robotID and whether the
// robot is provisioned in the tunnel identity store. A false found value means
// the caller should fall back to the ops authority (transition behaviour).
func (s *Store) LookupAPIKeyHash(robotID string) (hash string, found bool, err error) {
	if robotID == "" {
		return "", false, nil
	}
	err = s.Pool.QueryRow(context.Background(),
		`SELECT api_key_hash FROM robot_conn WHERE robot_id = $1`, robotID).Scan(&hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return hash, true, nil
}

// LookupConn returns the stored api_key_hash and local_ip for robotID, and
// whether the robot is provisioned in the tunnel identity store.
func (s *Store) LookupConn(robotID string) (hash string, localIP *string, found bool, err error) {
	if robotID == "" {
		return "", nil, false, nil
	}
	err = s.Pool.QueryRow(context.Background(),
		`SELECT api_key_hash, local_ip FROM robot_conn WHERE robot_id = $1`, robotID).Scan(&hash, &localIP)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil, false, nil
	}
	if err != nil {
		return "", nil, false, err
	}
	return hash, localIP, true, nil
}

// RobotIP returns the last observed public IP for robotID (for the legacy TCP
// bootstrap fallback). Empty string if unknown.
func (s *Store) RobotIP(robotID string) (string, error) {
	var ip *string
	err := s.Pool.QueryRow(context.Background(),
		`SELECT robot_ip FROM robot_conn WHERE robot_id = $1`, robotID).Scan(&ip)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if ip == nil {
		return "", nil
	}
	return *ip, nil
}

// TouchLastSeen records control-plane liveness for robotID.
func (s *Store) TouchLastSeen(robotID string) error {
	_, err := s.Pool.Exec(context.Background(),
		`UPDATE robot_conn SET last_seen_at = NOW(), updated_at = NOW() WHERE robot_id = $1`, robotID)
	return err
}

// ProvisionRobot upserts a robot's connection identity. Called by ops via the
// internal API at agent-registration time. apiKey is hashed before storage.
func (s *Store) ProvisionRobot(ctx context.Context, robotID, agentID, apiKey string) error {
	_, err := s.Pool.Exec(ctx, `
		INSERT INTO robot_conn (robot_id, agent_id, api_key_hash)
		VALUES ($1, $2, $3)
		ON CONFLICT (robot_id) DO UPDATE
		  SET agent_id = EXCLUDED.agent_id,
		      api_key_hash = EXCLUDED.api_key_hash,
		      updated_at = NOW()`,
		robotID, agentID, HashAPIKey(apiKey))
	return err
}

// DeprovisionRobot removes a robot's connection identity (and cascades).
func (s *Store) DeprovisionRobot(ctx context.Context, robotID string) error {
	_, err := s.Pool.Exec(ctx, `DELETE FROM robot_conn WHERE robot_id = $1`, robotID)
	return err
}

// Migrate applies any embedded migrations not yet recorded in schema_migrations,
// in filename order, each in its own transaction.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.Pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`); err != nil {
		return err
	}

	entries, err := fs.ReadDir(tdb.Migrations, "migrations")
	if err != nil {
		return err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		var exists bool
		if err := s.Pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, name).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		sqlBytes, err := fs.ReadFile(tdb.Migrations, "migrations/"+name)
		if err != nil {
			return err
		}
		tx, err := s.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}
