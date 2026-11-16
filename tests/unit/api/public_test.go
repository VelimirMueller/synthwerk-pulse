package api_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/VelimirMueller/synthwerk-contracts/go/kit/auth"
	"github.com/VelimirMueller/synthwerk-contracts/go/kit/auth/authtest"

	"github.com/VelimirMueller/synthwerk-pulse/internal/app"
)

// TestPublicStats: the whole app, with a database that does not answer. The public route
// needs no token and answers null numbers; the other /v1 routes still need one.
func TestPublicStats(t *testing.T) {
	ns, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(ns.Shutdown)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	// pgxpool connects lazily: port 1 never answers, so every query fails fast.
	db, err := pgxpool.New(t.Context(), "postgres://pulse@127.0.0.1:1/pulse?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	iss := authtest.NewIssuer(t)
	v, err := auth.NewVerifier(iss.Config())
	if err != nil {
		t.Fatal(err)
	}
	a, err := app.New(db, nc, slog.New(slog.DiscardHandler), app.Config{Version: "0.1.0", Commit: "3f9c2e1", Env: "ci", Verifier: v})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(a.Handler())
	t.Cleanup(srv.Close)

	get := func(t *testing.T, path string) (int, http.Header, map[string]any) {
		t.Helper()
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+path, nil)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = res.Body.Close() }()
		var body map[string]any
		_ = json.NewDecoder(res.Body).Decode(&body)
		return res.StatusCode, res.Header, body
	}

	t.Run("public stats - no token, database down - 200, null numbers, public cache", func(t *testing.T) {
		code, header, body := get(t, "/v1/public-stats")
		if code != http.StatusOK || header.Get("Cache-Control") != "public, max-age=60" {
			t.Fatalf("status %d headers %v", code, header)
		}
		if body["window_days"] != float64(30) || body["uptime_ratio"] != nil || body["p95_ms"] != nil || body["answered_chats"] != nil {
			t.Fatalf("body %v", body)
		}
	})

	t.Run("alerts - no token - still 401", func(t *testing.T) {
		if code, _, _ := get(t, "/v1/alerts"); code != http.StatusUnauthorized {
			t.Fatalf("status %d", code)
		}
	})
}
