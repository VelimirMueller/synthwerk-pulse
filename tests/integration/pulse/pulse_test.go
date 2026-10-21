//go:build integration

package pulse_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/sync/errgroup"

	"github.com/VelimirMueller/synthwerk-pulse/internal/app"
	"github.com/VelimirMueller/synthwerk-pulse/internal/check"
	"github.com/VelimirMueller/synthwerk-pulse/internal/status"
	"github.com/VelimirMueller/synthwerk-pulse/internal/store"
	"github.com/VelimirMueller/synthwerk-pulse/internal/stream"
	"github.com/VelimirMueller/synthwerk-pulse/internal/target"
	"github.com/VelimirMueller/synthwerk-pulse/tests/support/containers"
	"github.com/VelimirMueller/synthwerk-pulse/tests/support/sse"
)

var env *containers.Env

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	var err error
	env, err = containers.Start(ctx)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start containers:", err)
		os.Exit(1)
	}
	code := m.Run()
	env.Stop()
	os.Exit(code)
}

func schema(t *testing.T, path string) *jsonschema.Schema {
	t.Helper()
	abs, err := filepath.Abs(filepath.Join("..", "..", "fixtures", "contracts", path))
	if err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()
	s, err := c.Compile(abs)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// events reads every message on EVT_PULSE and checks each against the contract.
func events(t *testing.T, js jetstream.JetStream) []map[string]any {
	t.Helper()
	ctx := t.Context()
	c, err := js.OrderedConsumer(ctx, "EVT_PULSE", jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := c.FetchNoWait(100)
	if err != nil {
		t.Fatal(err)
	}
	envelope := schema(t, "envelope.schema.json")
	data := schema(t, "pulse/target.status_changed.v1.schema.json")
	var out []map[string]any
	for m := range batch.Messages() {
		if m.Subject() != "evt.pulse.target.status_changed.v1" {
			t.Fatalf("subject %s", m.Subject())
		}
		ev, err := jsonschema.UnmarshalJSON(bytes.NewReader(m.Data()))
		if err != nil {
			t.Fatal(err)
		}
		if err := envelope.Validate(ev); err != nil {
			t.Fatalf("envelope: %v", err)
		}
		if err := data.Validate(ev.(map[string]any)["data"]); err != nil {
			t.Fatalf("data: %v", err)
		}
		out = append(out, ev.(map[string]any))
	}
	if err := batch.Error(); err != nil {
		t.Fatal(err)
	}
	return out
}

func newApp(t *testing.T, targets []target.Target) *app.App {
	t.Helper()
	ctx := t.Context()
	a, err := app.New(env.Database(t), env.NATS(t), slog.New(slog.DiscardHandler), app.Config{
		Version: "0.1.0", Commit: "3f9c2e1", Env: "ci", Targets: targets,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := app.EnsureStream(ctx, a.JS); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.JS.DeleteStream(context.Background(), "EVT_PULSE") })
	return a
}

//nolint:contextcheck // the helpers take their context from t.
func TestStore(t *testing.T) {
	a := newApp(t, nil)
	ctx := t.Context()
	since := time.Date(2026, 10, 21, 8, 15, 30, 0, time.UTC)
	rec := store.Record{Target: "identity", Status: status.Down, Since: since,
		Checks: []check.Item{{Name: "postgres", Status: status.Down}, {Name: "nats", Status: status.OK}}}

	t.Run("Save - first status - one valid event from unknown", func(t *testing.T) {
		wrote, err := a.Store.Save(ctx, rec)
		if err != nil || !wrote {
			t.Fatalf("Save = %v, %v; want true, nil", wrote, err)
		}
		if n, err := a.Relay.RunOnce(ctx); err != nil || n != 1 {
			t.Fatalf("RunOnce = %d, %v; want 1, nil", n, err)
		}
		evs := events(t, a.JS)
		if len(evs) != 1 {
			t.Fatalf("%d events, want 1", len(evs))
		}
		ev := evs[0]
		data := ev["data"].(map[string]any)
		if ev["subject"] != "identity" || ev["source"] != "/synthwerk/pulse/local" || ev["actor"] != "service:pulse" ||
			ev["time"] != "2026-10-21T08:15:30Z" || data["from"] != "unknown" || data["to"] != "down" || len(data["checks"].([]any)) != 2 {
			t.Fatalf("event %v", ev)
		}
	})

	t.Run("Save - same status again, also from a new store - no event", func(t *testing.T) {
		wrote, err := a.Store.Save(ctx, rec)
		if err != nil || wrote {
			t.Fatalf("Save = %v, %v; want false, nil", wrote, err)
		}
		restarted := store.New(a.DB, "/synthwerk/pulse/local")
		if wrote, err := restarted.Save(ctx, rec); err != nil || wrote {
			t.Fatalf("Save after restart = %v, %v; want false, nil", wrote, err)
		}
		if n, err := a.Relay.RunOnce(ctx); err != nil || n != 0 {
			t.Fatalf("RunOnce = %d, %v; want 0, nil", n, err)
		}
	})

	t.Run("Save - status back up - event from down to ok", func(t *testing.T) {
		up := rec
		up.Status, up.Since, up.Checks = status.OK, since.Add(time.Minute), nil
		if wrote, err := a.Store.Save(ctx, up); err != nil || !wrote {
			t.Fatalf("Save = %v, %v; want true, nil", wrote, err)
		}
		if _, err := a.Relay.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		evs := events(t, a.JS)
		data := evs[len(evs)-1]["data"].(map[string]any)
		if len(evs) != 2 || data["from"] != "down" || data["to"] != "ok" || len(data["checks"].([]any)) != 0 {
			t.Fatalf("events %v, want a second one from down to ok with no checks", evs)
		}
	})

	t.Run("Save - status unknown - error, nothing stored", func(t *testing.T) {
		unknown := rec
		unknown.Target, unknown.Status = "llm", status.Unknown
		if _, err := a.Store.Save(ctx, unknown); err == nil {
			t.Fatal("Save stored status unknown")
		}
	})
}

// flipTarget is a fake service with a /readyz that answers ok or 503 down.
type flipTarget struct{ down atomic.Bool }

func (f *flipTarget) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if f.down.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"down","version":"1.0.0","commit":"3f9c2e1","checks":[{"name":"postgres","status":"down","latency_ms":1}]}`))
		return
	}
	_, _ = w.Write([]byte(`{"status":"ok","version":"1.0.0","commit":"3f9c2e1","checks":[{"name":"postgres","status":"ok","latency_ms":1}]}`))
}

func nextStatus(t *testing.T, s *sse.Stream, timeout time.Duration) stream.Change {
	t.Helper()
	ev, ok := s.Next(t, timeout)
	if !ok || ev.Event != "status" {
		t.Fatalf("event %+v (stream open %v), want a status event", ev, ok)
	}
	var ch stream.Change
	if err := json.Unmarshal([]byte(ev.Data), &ch); err != nil {
		t.Fatal(err)
	}
	return ch
}

//nolint:contextcheck // the helpers take their context from t.
func TestLiveStatus(t *testing.T) {
	fake := &flipTarget{}
	svc := httptest.NewServer(fake)
	t.Cleanup(svc.Close)
	interval := time.Second
	a := newApp(t, []target.Target{
		{Name: "identity", Kind: target.HTTP, URL: svc.URL + "/readyz", Interval: interval, Timeout: 500 * time.Millisecond},
	})
	srv := httptest.NewServer(a.Handler())
	t.Cleanup(srv.Close)
	t.Cleanup(a.Hub.Close)

	a.Health.MarkStarted()

	t.Run("readyz - postgres and nats up - 200", func(t *testing.T) {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/readyz", nil)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("status %d, want 200", res.StatusCode)
		}
	})

	// Open the stream before the first check, so the first change is an event.
	s := sse.Open(t, srv.URL+"/v1/health/stream")
	if ev, _ := s.Next(t, 5*time.Second); ev.Event != "snapshot" {
		t.Fatalf("first event %q, want snapshot", ev.Event)
	}
	ctx, cancel := context.WithCancel(t.Context())
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return a.Monitor.Run(gctx) })
	g.Go(func() error { return a.Relay.Run(gctx) })
	t.Cleanup(func() {
		cancel()
		_ = g.Wait()
	})

	t.Run("stream - target answers - status event unknown to ok", func(t *testing.T) {
		ch := nextStatus(t, s, 5*time.Second)
		if ch.Target != "identity" || ch.From != status.Unknown || ch.Status != status.OK || len(ch.Checks) != 1 {
			t.Fatalf("change %+v, want identity unknown -> ok with 1 check", ch)
		}
	})

	t.Run("stream - target stops - down within 3 intervals", func(t *testing.T) {
		start := time.Now()
		fake.down.Store(true)
		ch := nextStatus(t, s, 3*interval+2*time.Second)
		if ch.From != status.OK || ch.Status != status.Down || ch.Checks[0].Status != status.Down {
			t.Fatalf("change %+v, want ok -> down", ch)
		}
		t.Logf("down after %s", time.Since(start).Round(time.Millisecond))
	})

	t.Run("stream - target starts again - ok within 4 intervals", func(t *testing.T) {
		fake.down.Store(false)
		ch := nextStatus(t, s, 4*interval+2*time.Second)
		if ch.From != status.Down || ch.Status != status.OK {
			t.Fatalf("change %+v, want down -> ok", ch)
		}
	})

	t.Run("outbox - every change - one valid event each on EVT_PULSE", func(t *testing.T) {
		var evs []map[string]any
		for range 50 {
			if evs = events(t, a.JS); len(evs) == 3 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		var got []string
		for _, ev := range evs {
			d := ev["data"].(map[string]any)
			got = append(got, fmt.Sprintf("%s->%s", d["from"], d["to"]))
		}
		if fmt.Sprint(got) != "[unknown->ok ok->down down->ok]" {
			t.Fatalf("events %v, want [unknown->ok ok->down down->ok]", got)
		}
	})

	t.Run("GET /v1/health - after the changes - identity ok", func(t *testing.T) {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/v1/health", nil)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = res.Body.Close() }()
		var snap stream.Snapshot
		if err := json.NewDecoder(res.Body).Decode(&snap); err != nil {
			t.Fatal(err)
		}
		if len(snap.Targets) != 1 || snap.Targets[0].Status != status.OK || snap.Targets[0].CheckedAt == nil {
			t.Fatalf("snapshot %+v, want identity ok and checked", snap)
		}
	})
}
