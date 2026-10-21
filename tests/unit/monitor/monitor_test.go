package monitor_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VelimirMueller/synthwerk-pulse/internal/check"
	"github.com/VelimirMueller/synthwerk-pulse/internal/monitor"
	"github.com/VelimirMueller/synthwerk-pulse/internal/status"
	"github.com/VelimirMueller/synthwerk-pulse/internal/store"
	"github.com/VelimirMueller/synthwerk-pulse/internal/stream"
	"github.com/VelimirMueller/synthwerk-pulse/internal/target"
)

// fakeChecker returns the status it holds.
type fakeChecker struct{ s atomic.Value }

// newFake returns a checker that reports ok.
func newFake() *fakeChecker {
	f := &fakeChecker{}
	f.s.Store(status.OK)
	return f
}

func (f *fakeChecker) set(s status.Status) { f.s.Store(s) }

func (f *fakeChecker) Check(context.Context) check.Result {
	return check.Result{Status: f.s.Load().(status.Status), Latency: time.Millisecond}
}

// fakeSaver records saves and fails while fail is set.
type fakeSaver struct {
	mu      sync.Mutex
	fail    bool
	records []store.Record
}

func (f *fakeSaver) Save(_ context.Context, r store.Record) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return false, errors.New("database down")
	}
	f.records = append(f.records, r)
	return true, nil
}

func (f *fakeSaver) statuses() []status.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]status.Status, 0, len(f.records))
	for _, r := range f.records {
		out = append(out, r.Status)
	}
	return out
}

var bus = target.Target{Name: "bus", Kind: target.TCP, Address: "bus:4222", Interval: time.Second, Timeout: time.Second}

func setup(t *testing.T, fc *fakeChecker, saver monitor.Saver) (*monitor.Monitor, *stream.Hub) {
	t.Helper()
	hub := stream.New([]string{"bus"}, time.Now(), stream.Config{})
	t.Cleanup(hub.Close)
	m := monitor.New([]target.Target{bus}, hub, saver, slog.New(slog.DiscardHandler), monitor.WithChecker("bus", fc))
	return m, hub
}

func current(hub *stream.Hub) status.Status { return hub.Snapshot().Targets[0].Status }

func TestCheckOnce(t *testing.T) {
	t.Run("CheckOnce - target stops and starts - down after 2 checks, ok after 3", func(t *testing.T) {
		fc := newFake()
		saver := &fakeSaver{}
		m, hub := setup(t, fc, saver)
		ctx := t.Context()

		m.CheckOnce(ctx, "bus")
		if current(hub) != status.OK {
			t.Fatalf("status %s after first ok, want ok", current(hub))
		}
		fc.set(status.Down)
		m.CheckOnce(ctx, "bus")
		if current(hub) != status.OK {
			t.Fatal("down after one failed check, want still ok")
		}
		m.CheckOnce(ctx, "bus")
		if current(hub) != status.Down {
			t.Fatalf("status %s after 2 failed checks, want down", current(hub))
		}
		fc.set(status.OK)
		m.CheckOnce(ctx, "bus")
		m.CheckOnce(ctx, "bus")
		if current(hub) != status.Down {
			t.Fatal("ok after 2 good checks, want still down")
		}
		m.CheckOnce(ctx, "bus")
		if current(hub) != status.OK {
			t.Fatalf("status %s after 3 good checks, want ok", current(hub))
		}
		got := saver.statuses()
		if len(got) != 3 || got[0] != status.OK || got[1] != status.Down || got[2] != status.OK {
			t.Fatalf("saved %v, want [ok down ok]", got)
		}
	})

	t.Run("CheckOnce - database down - stream still changes, store catches up later", func(t *testing.T) {
		fc := newFake()
		saver := &fakeSaver{fail: true}
		m, hub := setup(t, fc, saver)
		ctx := t.Context()

		m.CheckOnce(ctx, "bus")
		fc.set(status.Down)
		m.CheckOnce(ctx, "bus")
		m.CheckOnce(ctx, "bus")
		if current(hub) != status.Down || len(saver.statuses()) != 0 {
			t.Fatalf("status %s, saved %v; want down in the stream and nothing saved", current(hub), saver.statuses())
		}
		saver.mu.Lock()
		saver.fail = false
		saver.mu.Unlock()
		m.CheckOnce(ctx, "bus") // still down: the store gets the latest status
		if got := saver.statuses(); len(got) != 1 || got[0] != status.Down {
			t.Fatalf("saved %v, want [down]", got)
		}
		m.CheckOnce(ctx, "bus") // nothing new
		if got := saver.statuses(); len(got) != 1 {
			t.Fatalf("saved %v again, want one save per status", got)
		}
	})

	t.Run("CheckOnce - unknown target - nothing happens", func(t *testing.T) {
		m, hub := setup(t, newFake(), &fakeSaver{})
		m.CheckOnce(t.Context(), "nope")
		if current(hub) != status.Unknown {
			t.Fatalf("status %s, want unknown", current(hub))
		}
	})
}

func TestRun(t *testing.T) {
	t.Run("Run - context ends - returns", func(t *testing.T) {
		m, hub := setup(t, newFake(), nil)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- m.Run(ctx) }()
		deadline := time.Now().Add(3 * time.Second)
		for current(hub) != status.OK && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if current(hub) != status.OK {
			t.Fatal("no check ran within 3s")
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Run did not return after cancel")
		}
	})
}
