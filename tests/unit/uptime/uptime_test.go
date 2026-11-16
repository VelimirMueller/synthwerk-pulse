// Uptime samples (synthwerk#47). The Postgres part is in tests/integration.
package uptime_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/VelimirMueller/synthwerk-pulse/internal/status"
	"github.com/VelimirMueller/synthwerk-pulse/internal/stream"
	"github.com/VelimirMueller/synthwerk-pulse/internal/uptime"
)

func TestSamples(t *testing.T) {
	got := uptime.Samples([]stream.Target{
		{Target: "a", Status: status.OK},
		{Target: "b", Status: status.Degraded},
		{Target: "c", Status: status.Down},
		{Target: "d", Status: status.Unknown},
	})
	want := []uptime.Sample{{Target: "a", Up: true}, {Target: "b", Up: true}, {Target: "c", Up: false}}
	if len(got) != len(want) {
		t.Fatalf("got %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Samples - ok and degraded up, down not, unknown left out: got %+v", got)
		}
	}
}

type adder struct {
	mu    sync.Mutex
	calls int
	secs  int64
	fail  bool
}

func (a *adder) Add(_ context.Context, s []uptime.Sample, seconds int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	a.secs = seconds
	if a.fail {
		return errors.New("database down")
	}
	if len(s) != 1 {
		return errors.New("want one sample")
	}
	return nil
}

func TestRun(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("Run - write fails %v - every interval until the context ends", fail), func(t *testing.T) {
			a := &adder{fail: fail}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			targets := func() []stream.Target { return []stream.Target{{Target: "a", Status: status.OK}} }
			go func() { done <- uptime.Run(ctx, a, targets, 5*time.Millisecond, slog.New(slog.DiscardHandler)) }()
			for range 200 {
				a.mu.Lock()
				n := a.calls
				a.mu.Unlock()
				if n >= 3 {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			a.mu.Lock()
			defer a.mu.Unlock()
			if a.calls < 3 || a.secs != 1 {
				t.Fatalf("calls %d seconds %d", a.calls, a.secs)
			}
		})
	}
}
