//go:build integration

package pulse_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/VelimirMueller/synthwerk-contracts/go/kit/auth"
	"github.com/VelimirMueller/synthwerk-contracts/go/kit/auth/authtest"

	"github.com/VelimirMueller/synthwerk-pulse/internal/alert"
	"github.com/VelimirMueller/synthwerk-pulse/internal/app"
	"github.com/VelimirMueller/synthwerk-pulse/internal/metrics"
	"github.com/VelimirMueller/synthwerk-pulse/internal/usage"
)

const platformOrg = "org-platform"

func llmAnswer(id string, at time.Time, org, provider, status, traceparent string, in, out int) []byte {
	return fmt.Appendf(nil, `{"specversion":"1.0","id":"e-%s","source":"/synthwerk/llm/ci","type":"dev.synthwerk.llm.message.completed.v1",`+
		`"time":%q,"orgid":%q,"traceparent":%q,"datacontenttype":"application/json","data":{"conversation_id":"6f1c2d3e-4a5b-4c6d-8e7f-9a0b1c2d3e4f",`+
		`"message_id":%q,"model":"default","provider":%q,"input_tokens":%d,"output_tokens":%d,"latency_ms":10,"status":%q}}`,
		id, at.Format(time.RFC3339Nano), org, traceparent, id, provider, in, out, status)
}

// TestForcedErrorAlert: a failed llm answer (the forced error) raises ai_failed, and the
// alert links the trace of that answer. The AI cost counts each answer once per day.
//
//nolint:contextcheck // the helpers take their context from t.
func TestForcedErrorAlert(t *testing.T) {
	ctx := t.Context()
	iss := authtest.NewIssuer(t)
	cfg := iss.Config()
	cfg.Mapper = auth.ZitadelMapper{PlatformOrgID: platformOrg}
	v, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatal(err)
	}
	prices, err := usage.LoadPrices(writeFile(t, "prices.yaml",
		"currency: USD\nprices:\n  - {provider: anthropic, model: default, input_per_million: 3, output_per_million: 15}\n"))
	if err != nil {
		t.Fatal(err)
	}
	a, err := app.New(env.Database(t), env.NATS(t), slog.New(slog.DiscardHandler), app.Config{
		Version: "0.1.0", Commit: "3f9c2e1", Env: "ci", Verifier: v, Prices: prices,
		Metrics: metrics.Config{Window: 5 * time.Minute, Interval: 100 * time.Millisecond},
		Alerts:  alert.Config{TraceURL: "http://localhost:3000/explore?orgId=1&trace={trace_id}"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := a.JS.CreateStream(ctx, jetstream.StreamConfig{Name: metrics.LLMStream, Subjects: []string{"evt.llm.>"}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.JS.DeleteStream(context.Background(), metrics.LLMStream) })

	now := time.Now().UTC()
	trace := "4bf92f3577b34da6a3ce929d0e0e4736"
	for _, ev := range [][]byte{
		llmAnswer("0199f3b0-0000-7000-8000-000000000001", now.Add(-time.Minute), "org-a", "anthropic", "completed", "", 1_000_000, 100_000),
		llmAnswer("0199f3b0-0000-7000-8000-000000000002", now.Add(-30*time.Second), "org-b", "openai_compat", "failed", "00-"+trace+"-00f067aa0ba902b7-01", 10, 0),
		// The same answer again: counted once.
		llmAnswer("0199f3b0-0000-7000-8000-000000000001", now.Add(-time.Minute), "org-a", "anthropic", "completed", "", 1_000_000, 100_000),
	} {
		if _, err := a.JS.Publish(ctx, metrics.LLMSubject, ev); err != nil {
			t.Fatal(err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{}, 3)
	for _, run := range []func(context.Context) error{a.Metrics.Run, a.Alerts.Run, a.RunUsage} {
		go func() {
			_ = run(runCtx)
			done <- struct{}{}
		}()
	}
	t.Cleanup(func() {
		cancel()
		for range 3 {
			<-done
		}
	})
	srv := httptest.NewServer(a.Handler())
	t.Cleanup(srv.Close)
	get := func(t *testing.T, path, token string, into any) int {
		t.Helper()
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = res.Body.Close() }()
		_ = json.NewDecoder(res.Body).Decode(into)
		return res.StatusCode
	}
	operator := iss.Mint(t, authtest.Claims("op-1", platformOrg, auth.RoleOperator))
	adminA := iss.Mint(t, authtest.Claims("u-1", "org-a", auth.RoleAdmin))

	t.Run("forced error - ai_failed alert with the trace link of the failed answer", func(t *testing.T) {
		var l alert.List
		for range 100 {
			l = alert.List{}
			if get(t, "/v1/alerts", operator, &l) == http.StatusOK && len(l.Alerts) == 1 && l.Alerts[0].TraceURL != nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if len(l.Alerts) != 1 {
			t.Fatalf("alerts %+v", l.Alerts)
		}
		al := l.Alerts[0]
		if al.ID != "ai_failed:llm" || al.TraceID == nil || *al.TraceID != trace ||
			al.TraceURL == nil || *al.TraceURL != "http://localhost:3000/explore?orgId=1&trace="+trace {
			t.Fatalf("alert %+v", al)
		}
	})

	t.Run("org admin - same alert without the trace link", func(t *testing.T) {
		var l alert.List
		if code := get(t, "/v1/alerts", adminA, &l); code != http.StatusOK || len(l.Alerts) != 1 || l.Alerts[0].TraceURL != nil || l.Alerts[0].TraceID != nil {
			t.Fatalf("%d %+v", code, l.Alerts)
		}
	})

	today := now.Format(time.DateOnly)
	t.Run("AI cost - real Postgres - each answer once, priced per org", func(t *testing.T) {
		var p metrics.Platform
		for range 100 {
			a.Metrics.Refresh(ctx)
			if p = a.Metrics.Platform(); p.AICost != nil && p.AICost.Days[6].Answers == 2 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if p.AICost == nil {
			t.Fatal("ai_cost null")
		}
		d := p.AICost.Days[6]
		if d.Date != today || d.Answers != 2 || d.Cost != 4.5 || d.UnpricedAnswers != 1 || d.InputTokens != 1_000_010 {
			t.Fatalf("today %+v, want 2 answers, cost 3 + 1.5, 1 unpriced", d)
		}
		var org metrics.Org
		if code := get(t, "/v1/orgs/org-b/metrics", operator, &org); code != http.StatusOK || org.AICost == nil {
			t.Fatalf("%d %+v", code, org)
		}
		if d := org.AICost.Days[6]; d.Answers != 1 || d.Cost != 0 || d.UnpricedAnswers != 1 {
			t.Fatalf("org-b today %+v, want only its own unpriced answer", d)
		}
	})

	t.Run("Record - same message id again - not counted", func(t *testing.T) {
		raw := llmAnswer("0199f3b0-0000-7000-8000-000000000001", now.Add(-time.Minute), "org-a", "anthropic", "completed", "", 1, 1)
		if fresh, err := a.Usage.Record(ctx, raw); err != nil || fresh {
			t.Fatalf("fresh %v err %v", fresh, err)
		}
		old := llmAnswer("0199f3b0-0000-7000-8000-000000000099", now.AddDate(0, 0, -usage.KeepDays-1), "org-a", "anthropic", "completed", "", 1, 1)
		if fresh, err := a.Usage.Record(ctx, old); err != nil || fresh {
			t.Fatalf("answer older than the dedupe window: fresh %v err %v", fresh, err)
		}
		if err := a.Usage.Prune(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("durable consumer - every answer acked", func(t *testing.T) {
		c, err := a.JS.Consumer(ctx, metrics.LLMStream, usage.Consumer)
		if err != nil {
			t.Fatal(err)
		}
		info, err := c.Info(ctx)
		if err != nil || info.NumPending != 0 || info.NumAckPending != 0 || !strings.HasPrefix(info.Config.FilterSubject, "evt.llm.") {
			t.Fatalf("info %+v err %v", info, err)
		}
	})
}

func writeFile(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
