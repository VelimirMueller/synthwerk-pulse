// Scope test per role (synthwerk#40): who may read the status and the metrics.
package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/VelimirMueller/synthwerk-contracts/go/kit/auth"
	"github.com/VelimirMueller/synthwerk-contracts/go/kit/auth/authtest"

	"github.com/VelimirMueller/synthwerk-pulse/internal/app"
	"github.com/VelimirMueller/synthwerk-pulse/internal/metrics"
	"github.com/VelimirMueller/synthwerk-pulse/internal/stream"
)

const platformOrg = "org-platform"

var t0 = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

func event(org string, tokens int) []byte {
	b, _ := json.Marshal(map[string]any{
		"specversion": "1.0", "id": "x", "source": "/synthwerk/llm/local", "type": "dev.synthwerk.llm.message.completed.v1",
		"time": t0.Add(-time.Minute), "orgid": org, "datacontenttype": "application/json",
		"data": map[string]any{"input_tokens": tokens, "output_tokens": 0, "latency_ms": 100, "status": "completed"},
	})
	return b
}

type fixture struct {
	srv *httptest.Server
	iss *authtest.Issuer
}

func setup(t *testing.T) fixture {
	t.Helper()
	iss := authtest.NewIssuer(t)
	cfg := iss.Config()
	cfg.Mapper = auth.ZitadelMapper{PlatformOrgID: platformOrg}
	v, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatal(err)
	}
	hub := stream.New([]string{"llm"}, t0, stream.Config{})
	col := metrics.New(metrics.Config{Now: func() time.Time { return t0 }}, nil)
	col.AI().SetConnected(true)
	for _, e := range [][]byte{event("org-a", 500), event("org-b", 7000)} {
		if err := col.AI().Add(e); err != nil {
			t.Fatal(err)
		}
	}
	srv := httptest.NewServer(app.API(v, hub, col))
	t.Cleanup(srv.Close)
	t.Cleanup(hub.Close)
	return fixture{srv: srv, iss: iss}
}

func (f fixture) get(t *testing.T, path, token string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, f.srv.URL+path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	var body map[string]any
	_ = json.NewDecoder(res.Body).Decode(&body)
	return res.StatusCode, body
}

func TestScopePerRole(t *testing.T) {
	f := setup(t)
	operator := f.iss.Mint(t, authtest.Claims("op-1", platformOrg, auth.RoleOperator))
	// An operator grant outside the platform org does not count.
	fakeOperator := f.iss.Mint(t, authtest.Claims("op-2", "org-a", auth.RoleOperator, auth.RoleMember))
	ownerA := f.iss.Mint(t, authtest.Claims("u-1", "org-a", auth.RoleOwner))
	adminA := f.iss.Mint(t, authtest.Claims("u-2", "org-a", auth.RoleAdmin))
	memberA := f.iss.Mint(t, authtest.Claims("u-3", "org-a", auth.RoleMember))
	// Admin in org-b by grant, but the token acts in org-a.
	crossAdmin := f.iss.Mint(t, map[string]any{
		"sub": "u-4", auth.ClaimOrgID: "org-a",
		auth.ClaimRoles: map[string]any{auth.RoleAdmin: map[string]any{"org-b": "b.test"}, auth.RoleMember: map[string]any{"org-a": "a.test"}},
	})

	cases := []struct {
		name, path, token string
		want              int
	}{
		{"status - no token - 401", "/v1/health", "", http.StatusUnauthorized},
		{"status - member - 403", "/v1/health", memberA, http.StatusForbidden},
		{"status - org admin - 200", "/v1/health", adminA, http.StatusOK},
		{"status - org owner - 200", "/v1/health", ownerA, http.StatusOK},
		{"status - operator - 200", "/v1/health", operator, http.StatusOK},
		{"stream - member - 403", "/v1/health/stream", memberA, http.StatusForbidden},
		{"platform metrics - org admin - 403", "/v1/metrics", adminA, http.StatusForbidden},
		{"platform metrics - operator grant outside the platform org - 403", "/v1/metrics", fakeOperator, http.StatusForbidden},
		{"platform metrics - operator - 200", "/v1/metrics", operator, http.StatusOK},
		{"org metrics - admin of the org - 200", "/v1/orgs/org-a/metrics", adminA, http.StatusOK},
		{"org metrics - owner of the org - 200", "/v1/orgs/org-a/metrics", ownerA, http.StatusOK},
		{"org metrics - admin of another org - 403", "/v1/orgs/org-b/metrics", adminA, http.StatusForbidden},
		{"org metrics - admin grant in another org - 403", "/v1/orgs/org-b/metrics", crossAdmin, http.StatusForbidden},
		{"org metrics - member of the org - 403", "/v1/orgs/org-a/metrics", memberA, http.StatusForbidden},
		{"org metrics - operator, any org - 200", "/v1/orgs/org-b/metrics", operator, http.StatusOK},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, body := f.get(t, c.path, c.token)
			if got != c.want {
				t.Fatalf("GET %s = %d, want %d (body %v)", c.path, got, c.want, body)
			}
		})
	}

	t.Run("org metrics - org admin - numbers of the own org only", func(t *testing.T) {
		_, body := f.get(t, "/v1/orgs/org-a/metrics", adminA)
		ai := body["ai"].(map[string]any)
		if body["org_id"] != "org-a" || ai["answers"] != float64(1) || ai["tokens_per_minute"] != float64(100) {
			t.Fatalf("body %v, want org-a with 1 answer and 100 tokens/min (org-b has 7000 tokens)", body)
		}
		for _, k := range []string{"services", "host", "database", "bus"} {
			if _, ok := body[k]; ok {
				t.Fatalf("org body has platform part %q", k)
			}
		}
	})

	t.Run("platform metrics - operator - AI of every org", func(t *testing.T) {
		_, body := f.get(t, "/v1/metrics", operator)
		ai := body["ai"].(map[string]any)
		if ai["answers"] != float64(2) || ai["tokens_per_minute"] != float64(1500) {
			t.Fatalf("ai %v, want 2 answers and 1500 tokens/min", ai)
		}
	})
}
