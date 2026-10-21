// Package monitor checks every target on its own interval, keeps the status with
// hysteresis (package status), sends changes to the stream hub at once and stores
// them in Postgres.
//
// The live view never waits for the database: when Postgres is down, the stream still
// shows every change. The stored status catches up on the next check after Postgres
// is back. Changes in between are not stored as events, only the latest status.
package monitor

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/VelimirMueller/synthwerk-pulse/internal/check"
	"github.com/VelimirMueller/synthwerk-pulse/internal/status"
	"github.com/VelimirMueller/synthwerk-pulse/internal/store"
	"github.com/VelimirMueller/synthwerk-pulse/internal/stream"
	"github.com/VelimirMueller/synthwerk-pulse/internal/target"
)

// Saver stores a status (package store). nil means: store nothing.
type Saver interface {
	Save(ctx context.Context, r store.Record) (bool, error)
}

// Monitor runs the checks. Create it with New, then call Run once.
type Monitor struct {
	hub   *stream.Hub
	saver Saver
	log   *slog.Logger
	now   func() time.Time
	runs  []*run
}

type run struct {
	target  target.Target
	checker check.Checker

	mu        sync.Mutex
	tracker   *status.Tracker
	stored    status.Status // last status Save accepted
	saveError bool          // the last Save failed (log only the first failure)
}

// Option changes a monitor.
type Option func(*Monitor)

// WithClock sets the clock. Default time.Now.
func WithClock(now func() time.Time) Option { return func(m *Monitor) { m.now = now } }

// WithChecker replaces the checker of one target (tests).
func WithChecker(name string, c check.Checker) Option {
	return func(m *Monitor) {
		for _, r := range m.runs {
			if r.target.Name == name {
				r.checker = c
			}
		}
	}
}

// New returns a monitor for targets. Every target starts as unknown.
func New(targets []target.Target, hub *stream.Hub, saver Saver, logger *slog.Logger, opts ...Option) *Monitor {
	m := &Monitor{hub: hub, saver: saver, log: logger, now: time.Now}
	client := check.NewHTTPClient()
	for _, t := range targets {
		m.runs = append(m.runs, &run{target: t, checker: check.New(t, client)})
	}
	for _, o := range opts {
		o(m)
	}
	start := m.now()
	for _, r := range m.runs {
		r.tracker = status.NewTracker(start)
		r.stored = status.Unknown
	}
	return m
}

// Run checks every target until ctx ends. It always returns nil.
func (m *Monitor) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, r := range m.runs {
		wg.Go(func() { m.loop(ctx, r) })
	}
	wg.Wait()
	return nil
}

func (m *Monitor) loop(ctx context.Context, r *run) {
	// Spread the first checks so that all targets do not fire at the same moment.
	jitter := time.Duration(rand.Int64N(max(1, int64(r.target.Interval)/10))) //nolint:gosec // jitter, not security.
	select {
	case <-ctx.Done():
		return
	case <-time.After(jitter):
	}
	ticker := time.NewTicker(r.target.Interval)
	defer ticker.Stop()
	for {
		m.CheckOnce(ctx, r.target.Name)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// CheckOnce checks one target now and handles the result. Run calls it on every tick.
// Tests call it to step through checks without timers.
func (m *Monitor) CheckOnce(ctx context.Context, name string) {
	var r *run
	for _, x := range m.runs {
		if x.target.Name == name {
			r = x
		}
	}
	if r == nil {
		return
	}
	res := r.checker.Check(ctx)
	if ctx.Err() != nil {
		return // shutdown, the result means nothing
	}

	// One check per target at a time: the lock keeps the order of the results.
	r.mu.Lock()
	defer r.mu.Unlock()
	now := m.now()
	from, changed := r.tracker.Observe(res.Status, now)
	cur, since := r.tracker.Status()
	checkedAt := now.UTC()
	m.hub.Update(stream.Target{
		Target:    name,
		Status:    cur,
		Since:     since,
		CheckedAt: &checkedAt,
		LatencyMs: res.Latency.Milliseconds(),
		Checks:    res.Checks,
	}, from, changed)
	if changed {
		m.log.InfoContext(ctx, "target status changed",
			slog.String("event.name", "pulse.target.status_changed"),
			slog.String("pulse.target", name),
			slog.String("pulse.status.from", string(from)),
			slog.String("pulse.status.to", string(cur)))
	}
	m.save(ctx, r, cur, since, res.Checks)
}

func (m *Monitor) save(ctx context.Context, r *run, cur status.Status, since time.Time, checks []check.Item) {
	if m.saver == nil || cur == status.Unknown || cur == r.stored {
		return
	}
	if _, err := m.saver.Save(ctx, store.Record{Target: r.target.Name, Status: cur, Since: since, Checks: checks}); err != nil {
		if !r.saveError {
			m.log.WarnContext(ctx, "target status not stored, retry on the next check",
				slog.String("event.name", "pulse.status.store_failed"),
				slog.String("pulse.target", r.target.Name),
				slog.String("exception.message", err.Error()))
		}
		r.saveError = true
		return
	}
	r.stored, r.saveError = cur, false
}
