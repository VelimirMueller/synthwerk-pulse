// Package store keeps the last status of each target in Postgres and writes the
// event dev.synthwerk.pulse.target.status_changed.v1 to the outbox in the same
// transaction (D-07). The outbox relay of the kit publishes it to JetStream.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/VelimirMueller/synthwerk-contracts/go/kit/cloudevents"
	"github.com/VelimirMueller/synthwerk-contracts/go/kit/outbox"

	"github.com/VelimirMueller/synthwerk-pulse/internal/check"
	"github.com/VelimirMueller/synthwerk-pulse/internal/status"
)

// EventType is the event pulse publishes when the stored status of a target changes.
const EventType = "dev.synthwerk.pulse.target.status_changed.v1"

// DataSchema is the $id of the data schema in synthwerk-contracts.
const DataSchema = "https://schemas.synthwerk.dev/events/pulse/target.status_changed.v1.json"

// Schema creates the status table. Status unknown is never stored.
const Schema = `
CREATE TABLE IF NOT EXISTS pulse_target_status (
    target      text        PRIMARY KEY,
    status      text        NOT NULL CHECK (status IN ('ok', 'degraded', 'down')),
    since       timestamptz NOT NULL,
    checks      jsonb       NOT NULL DEFAULT '[]',
    updated_at  timestamptz NOT NULL DEFAULT now()
);`

// Store writes status changes. Safe for concurrent use.
type Store struct {
	db     *pgxpool.Pool
	source string
}

// New returns a store. source is the CloudEvents source (cloudevents.Source).
func New(db *pgxpool.Pool, source string) *Store {
	return &Store{db: db, source: source}
}

// Migrate creates the status table and the outbox table.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.db.Exec(ctx, Schema); err != nil {
		return fmt.Errorf("store: migrate: %w", err)
	}
	return outbox.Migrate(ctx, s.db)
}

// Ping checks the database connection (readiness check).
func (s *Store) Ping(ctx context.Context) error { return s.db.Ping(ctx) }

// Record is the status of a target to store.
type Record struct {
	Target string
	Status status.Status
	Since  time.Time
	Checks []check.Item
}

type changed struct {
	Target string        `json:"target"`
	From   status.Status `json:"from"`
	To     status.Status `json:"to"`
	Checks []check.Item  `json:"checks"`
}

// Save stores r when its status differs from the stored one. Then it also writes the
// status event to the outbox, with the stored status as from (unknown when there is
// none). It reports whether it wrote an event. A status that did not change writes nothing,
// so a restart of pulse does not send the same event again.
func (s *Store) Save(ctx context.Context, r Record) (bool, error) {
	if !status.Valid(r.Status) {
		return false, fmt.Errorf("store: status %q cannot be stored", r.Status)
	}
	checks := r.Checks
	if checks == nil {
		checks = []check.Item{}
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	from := status.Unknown
	err = tx.QueryRow(ctx, `SELECT status FROM pulse_target_status WHERE target = $1 FOR UPDATE`, r.Target).Scan(&from)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("store: read status: %w", err)
	}
	if from == r.Status {
		return false, nil
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO pulse_target_status (target, status, since, checks) VALUES ($1, $2, $3, $4)
		ON CONFLICT (target) DO UPDATE
		SET status = EXCLUDED.status, since = EXCLUDED.since, checks = EXCLUDED.checks, updated_at = now()`,
		r.Target, r.Status, r.Since, checks); err != nil {
		return false, fmt.Errorf("store: write status: %w", err)
	}
	ev, err := cloudevents.New(ctx, EventType, s.source,
		changed{Target: r.Target, From: from, To: r.Status, Checks: checks},
		cloudevents.WithSubject(r.Target),
		cloudevents.WithActor("service:pulse"),
		cloudevents.WithDataSchema(DataSchema),
		cloudevents.WithTime(r.Since))
	if err != nil {
		return false, fmt.Errorf("store: event: %w", err)
	}
	if err := outbox.Enqueue(ctx, tx, ev); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("store: commit: %w", err)
	}
	return true, nil
}
