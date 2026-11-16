// Package uptime stores how long each target was up, per UTC day, for the public
// numbers (synthwerk#47, docs/features/public-stats.md).
//
// Once per interval (1 min) a Recorder reads the live status. Each target with a known
// status adds the interval to its total, and to its up time unless it is down. Degraded
// counts as up: the target still answers. One pulse process writes the table.
package uptime

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/VelimirMueller/synthwerk-pulse/internal/status"
	"github.com/VelimirMueller/synthwerk-pulse/internal/stream"
)

// Schema creates the uptime table.
const Schema = `
CREATE TABLE IF NOT EXISTS pulse_uptime_daily (
    day            date   NOT NULL,
    target         text   NOT NULL,
    up_seconds     bigint NOT NULL DEFAULT 0,
    total_seconds  bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (day, target)
);`

// keepDays is how long the day rows stay.
const keepDays = 400

// Sample is the time of one target in one interval.
type Sample struct {
	Target string
	Up     bool
}

// Samples turns the live status into samples. A target with status unknown (not
// checked yet) gives no sample.
func Samples(targets []stream.Target) []Sample {
	out := make([]Sample, 0, len(targets))
	for _, t := range targets {
		if t.Status == status.Unknown {
			continue
		}
		out = append(out, Sample{Target: t.Target, Up: t.Status != status.Down})
	}
	return out
}

// Store reads and writes the uptime table. Safe for concurrent use.
type Store struct {
	db  *pgxpool.Pool
	now func() time.Time
}

// NewStore returns a store. now nil: time.Now.
func NewStore(db *pgxpool.Pool, now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{db: db, now: now}
}

// Migrate creates the uptime table.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.db.Exec(ctx, Schema); err != nil {
		return fmt.Errorf("uptime: migrate: %w", err)
	}
	return nil
}

// Add adds one interval of seconds for each sample to the day of now, in one transaction.
func (s *Store) Add(ctx context.Context, samples []Sample, seconds int64) error {
	if len(samples) == 0 || seconds <= 0 {
		return nil
	}
	day := s.now().UTC().Truncate(24 * time.Hour)
	b := &pgx.Batch{}
	for _, smp := range samples {
		up := int64(0)
		if smp.Up {
			up = seconds
		}
		b.Queue(`
			INSERT INTO pulse_uptime_daily (day, target, up_seconds, total_seconds) VALUES ($1, $2, $3, $4)
			ON CONFLICT (day, target) DO UPDATE
			SET up_seconds = pulse_uptime_daily.up_seconds + EXCLUDED.up_seconds,
			    total_seconds = pulse_uptime_daily.total_seconds + EXCLUDED.total_seconds`,
			day, smp.Target, up, seconds)
	}
	b.Queue(`DELETE FROM pulse_uptime_daily WHERE day < $1`, day.AddDate(0, 0, -keepDays))
	if err := s.db.SendBatch(ctx, b).Close(); err != nil {
		return fmt.Errorf("uptime: add: %w", err)
	}
	return nil
}

// Ratio returns the up share of every target over the last days UTC days, today
// included. ok is false before the first sample.
func (s *Store) Ratio(ctx context.Context, days int) (float64, bool, error) {
	if days < 1 {
		return 0, false, fmt.Errorf("uptime: days must be 1 or more, got %d", days)
	}
	from := s.now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -(days - 1))
	var up, total int64
	err := s.db.QueryRow(ctx, `
		SELECT coalesce(sum(up_seconds), 0)::bigint, coalesce(sum(total_seconds), 0)::bigint
		FROM pulse_uptime_daily WHERE day >= $1`, from).Scan(&up, &total)
	if err != nil {
		return 0, false, fmt.Errorf("uptime: ratio: %w", err)
	}
	if total == 0 {
		return 0, false, nil
	}
	return float64(up) / float64(total), true, nil
}

// Adder stores samples. *Store is the real one.
type Adder interface {
	Add(ctx context.Context, samples []Sample, seconds int64) error
}

// Run stores the samples of targets() once per interval until ctx ends. It never
// returns an error: a failed write loses that interval only.
func Run(ctx context.Context, a Adder, targets func() []stream.Target, interval time.Duration, logger *slog.Logger) error {
	if interval <= 0 {
		interval = time.Minute
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
		if err := a.Add(ctx, Samples(targets()), max(1, int64(interval.Seconds()))); err != nil && ctx.Err() == nil {
			logger.WarnContext(ctx, "uptime not stored",
				slog.String("event.name", "pulse.uptime.write_failed"),
				slog.String("exception.message", err.Error()))
		}
	}
}
