//go:build integration

package pulse_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/VelimirMueller/synthwerk-contracts/go/kit/auth"
	"github.com/VelimirMueller/synthwerk-contracts/go/kit/auth/authtest"

	"github.com/VelimirMueller/synthwerk-pulse/internal/app"
	"github.com/VelimirMueller/synthwerk-pulse/internal/uptime"
)

// TestPublicStats: uptime and answered chats from real Postgres, through the public route.
//
//nolint:contextcheck // the helpers take their context from t.
func TestPublicStats(t *testing.T) {
	ctx := t.Context()
	iss := authtest.NewIssuer(t)
	v, err := auth.NewVerifier(iss.Config())
	if err != nil {
		t.Fatal(err)
	}
	a, err := app.New(env.Database(t), env.NATS(t), slog.New(slog.DiscardHandler), app.Config{
		Version: "0.1.0", Commit: "3f9c2e1", Env: "ci", Verifier: v,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	t.Run("Ratio - before the first sample - not available", func(t *testing.T) {
		if _, ok, err := a.Uptime.Ratio(ctx, 30); err != nil || ok {
			t.Fatalf("ok %v err %v", ok, err)
		}
	})

	now := time.Now().UTC()
	for _, s := range [][]uptime.Sample{
		{{Target: "llm", Up: true}, {Target: "vision", Up: true}},
		{{Target: "llm", Up: true}, {Target: "vision", Up: false}},
		{{Target: "llm", Up: true}, {Target: "vision", Up: true}},
		{{Target: "llm", Up: true}, {Target: "vision", Up: true}},
	} {
		if err := a.Uptime.Add(ctx, s, 60); err != nil {
			t.Fatal(err)
		}
	}
	for i, ev := range [][]byte{
		llmAnswer("0199f3b0-0000-7000-8000-0000000000a1", now.Add(-time.Minute), "org-a", "fake", "completed", "", 1, 1),
		llmAnswer("0199f3b0-0000-7000-8000-0000000000a2", now.AddDate(0, 0, -3), "org-b", "fake", "completed", "", 1, 1),
		llmAnswer("0199f3b0-0000-7000-8000-0000000000a3", now.Add(-time.Minute), "org-a", "fake", "failed", "", 1, 0),
		llmAnswer("0199f3b0-0000-7000-8000-0000000000a4", now.AddDate(0, 0, -31), "org-a", "fake", "completed", "", 1, 1),
	} {
		if _, err := a.Usage.Record(ctx, ev); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}

	srv := httptest.NewServer(a.Handler())
	t.Cleanup(srv.Close)

	t.Run("GET /v1/public-stats - no token - uptime of every target, completed answers of 30 days", func(t *testing.T) {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/v1/public-stats", nil)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = res.Body.Close() }()
		var body map[string]any
		if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		// 7 of 8 target minutes up; 2 completed answers inside 30 days (one failed, one too old).
		if res.StatusCode != http.StatusOK || body["uptime_ratio"] != 0.875 || body["answered_chats"] != float64(2) || body["p95_ms"] != nil {
			t.Fatalf("%d %v", res.StatusCode, body)
		}
		for _, k := range []string{"org_id", "orgs", "users"} {
			if _, ok := body[k]; ok {
				t.Fatalf("public body has %q", k)
			}
		}
	})
}
