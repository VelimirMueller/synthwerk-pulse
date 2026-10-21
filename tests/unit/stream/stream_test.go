package stream_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/VelimirMueller/synthwerk-pulse/internal/check"
	"github.com/VelimirMueller/synthwerk-pulse/internal/status"
	"github.com/VelimirMueller/synthwerk-pulse/internal/stream"
	"github.com/VelimirMueller/synthwerk-pulse/tests/support/sse"
)

var t0 = time.Date(2026, 10, 21, 8, 0, 0, 0, time.UTC)

func server(t *testing.T, hub *stream.Hub) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", hub.ServeSnapshot)
	mux.HandleFunc("GET /v1/health/stream", hub.ServeStream)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	t.Cleanup(hub.Close) // runs before srv.Close: open streams end first
	return srv
}

func waitClients(t *testing.T, hub *stream.Hub, n int) {
	t.Helper()
	for range 200 {
		if hub.Clients() == n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%d clients, want %d", hub.Clients(), n)
}

func TestStream(t *testing.T) {
	t.Run("stream - connect - snapshot with every target first", func(t *testing.T) {
		hub := stream.New([]string{"bus", "database"}, t0, stream.Config{})
		s := sse.Open(t, server(t, hub).URL+"/v1/health/stream")
		if ct := s.Res.Header.Get("Content-Type"); ct != "text/event-stream" {
			t.Fatalf("content type %q", ct)
		}
		ev, _ := s.Next(t, 2*time.Second)
		if ev.Event != "snapshot" || ev.ID != "0" {
			t.Fatalf("first event %q id %q, want snapshot id 0", ev.Event, ev.ID)
		}
		var snap stream.Snapshot
		if err := json.Unmarshal([]byte(ev.Data), &snap); err != nil {
			t.Fatal(err)
		}
		if len(snap.Targets) != 2 || snap.Targets[0].Target != "bus" || snap.Targets[1].Status != status.Unknown || snap.Targets[0].CheckedAt != nil {
			t.Fatalf("snapshot %+v, want bus and database unknown, not checked", snap)
		}
	})

	t.Run("stream - status changes - status event with from, to and checks", func(t *testing.T) {
		hub := stream.New([]string{"bus"}, t0, stream.Config{})
		s := sse.Open(t, server(t, hub).URL+"/v1/health/stream")
		s.Next(t, 2*time.Second) // snapshot
		waitClients(t, hub, 1)

		hub.Update(stream.Target{Target: "bus", Status: status.OK, Since: t0.Add(time.Second)}, status.Unknown, false) // no change: no event
		hub.Update(stream.Target{Target: "bus", Status: status.Down, Since: t0.Add(10 * time.Second),
			Checks: []check.Item{{Name: "jetstream", Status: status.Down}}}, status.OK, true)

		ev, _ := s.Next(t, 2*time.Second)
		if ev.Event != "status" || ev.ID != "1" {
			t.Fatalf("event %q id %q, want status id 1", ev.Event, ev.ID)
		}
		var ch stream.Change
		if err := json.Unmarshal([]byte(ev.Data), &ch); err != nil {
			t.Fatal(err)
		}
		if ch.Target != "bus" || ch.From != status.OK || ch.Status != status.Down || !ch.Since.Equal(t0.Add(10*time.Second)) ||
			len(ch.Checks) != 1 || ch.Checks[0].Name != "jetstream" {
			t.Fatalf("change %+v, want bus ok -> down with check jetstream", ch)
		}
	})

	t.Run("stream - reconnect after a change - snapshot has the new status", func(t *testing.T) {
		hub := stream.New([]string{"bus"}, t0, stream.Config{})
		srv := server(t, hub)
		hub.Update(stream.Target{Target: "bus", Status: status.Down, Since: t0}, status.OK, true)
		s := sse.Open(t, srv.URL+"/v1/health/stream")
		ev, _ := s.Next(t, 2*time.Second)
		var snap stream.Snapshot
		if err := json.Unmarshal([]byte(ev.Data), &snap); err != nil {
			t.Fatal(err)
		}
		if ev.ID != "1" || snap.Targets[0].Status != status.Down {
			t.Fatalf("snapshot id %s %+v, want id 1 with bus down", ev.ID, snap)
		}
	})

	t.Run("stream - idle - ping comment every heartbeat", func(t *testing.T) {
		hub := stream.New([]string{"bus"}, t0, stream.Config{Heartbeat: 50 * time.Millisecond})
		s := sse.Open(t, server(t, hub).URL+"/v1/health/stream")
		s.Next(t, 2*time.Second)
		ev, _ := s.NextAny(t, time.Second)
		if ev.Event != "comment" || ev.Data != "ping" {
			t.Fatalf("event %+v, want the ping comment", ev)
		}
	})

	t.Run("stream - max clients reached - 503 with Retry-After", func(t *testing.T) {
		hub := stream.New([]string{"bus"}, t0, stream.Config{MaxClients: 1})
		srv := server(t, hub)
		s := sse.Open(t, srv.URL+"/v1/health/stream")
		s.Next(t, 2*time.Second)
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/v1/health/stream", nil)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if res.StatusCode != http.StatusServiceUnavailable || res.Header.Get("Retry-After") == "" {
			t.Fatalf("status %d Retry-After %q, want 503 with Retry-After", res.StatusCode, res.Header.Get("Retry-After"))
		}
	})

	t.Run("stream - max duration over - stream ends", func(t *testing.T) {
		hub := stream.New([]string{"bus"}, t0, stream.Config{MaxDuration: 100 * time.Millisecond})
		s := sse.Open(t, server(t, hub).URL+"/v1/health/stream")
		s.Next(t, 2*time.Second)
		if !s.Ended(2 * time.Second) {
			t.Fatal("stream still open after max duration")
		}
		waitClients(t, hub, 0)
	})

	t.Run("stream - hub closed - open streams end, new ones get 503", func(t *testing.T) {
		hub := stream.New([]string{"bus"}, t0, stream.Config{})
		srv := server(t, hub)
		s := sse.Open(t, srv.URL+"/v1/health/stream")
		s.Next(t, 2*time.Second)
		hub.Close()
		if !s.Ended(2 * time.Second) {
			t.Fatal("stream still open after Close")
		}
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/v1/health/stream", nil)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if res.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("status %d, want 503", res.StatusCode)
		}
	})

	t.Run("stream - client too slow - stream closed, hub not blocked", func(t *testing.T) {
		hub := stream.New([]string{"bus"}, t0, stream.Config{Buffer: 1})
		srv := server(t, hub)
		s := sse.Open(t, srv.URL+"/v1/health/stream")
		s.Next(t, 2*time.Second)
		waitClients(t, hub, 1)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := range 20000 {
				st := status.OK
				if i%2 == 0 {
					st = status.Down
				}
				hub.Update(stream.Target{Target: "bus", Status: st, Since: t0}, status.Unknown, true)
			}
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Update blocked on a slow client")
		}
	})
}

func TestSnapshot(t *testing.T) {
	t.Run("GET /v1/health - after a check - JSON with status, checked_at and checks", func(t *testing.T) {
		hub := stream.New([]string{"bus", "database"}, t0, stream.Config{})
		at := t0.Add(3 * time.Second)
		hub.Update(stream.Target{Target: "database", Status: status.OK, Since: at, CheckedAt: &at, LatencyMs: 4}, status.Unknown, true)
		hub.Update(stream.Target{Target: "ghost", Status: status.OK, Since: at}, status.Unknown, true) // not a target: ignored

		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, server(t, hub).URL+"/v1/health", nil)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = res.Body.Close() }()
		var raw struct {
			Targets []map[string]any `json:"targets"`
		}
		if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusOK || len(raw.Targets) != 2 {
			t.Fatalf("status %d with %d targets, want 200 with 2", res.StatusCode, len(raw.Targets))
		}
		bus, db := raw.Targets[0], raw.Targets[1]
		if bus["status"] != "unknown" || bus["checked_at"] != nil || db["status"] != "ok" || db["checked_at"] != "2026-10-21T08:00:03Z" || db["latency_ms"] != 4.0 {
			t.Fatalf("targets %v", raw.Targets)
		}
		if checks, ok := db["checks"].([]any); !ok || len(checks) != 0 {
			t.Fatalf("checks %v, want an empty array (never null)", db["checks"])
		}
	})
}
