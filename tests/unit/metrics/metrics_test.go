package metrics_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VelimirMueller/synthwerk-pulse/internal/metrics"
)

var t0 = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

func llmEvent(at time.Time, org string, in, out, latency int, status string) []byte {
	return fmt.Appendf(nil, `{"specversion":"1.0","id":"x","source":"/synthwerk/llm/local","type":"dev.synthwerk.llm.message.completed.v1",`+
		`"time":%q,"orgid":%q,"datacontenttype":"application/json","data":{"conversation_id":"c","message_id":"m","model":"default",`+
		`"provider":"fake","input_tokens":%d,"output_tokens":%d,"latency_ms":%d,"status":%q}}`,
		at.Format(time.RFC3339Nano), org, in, out, latency, status)
}

func TestAITracker(t *testing.T) {
	now := t0
	clock := func() time.Time { return now }

	t.Run("Stats - before the llm stream is read - not available", func(t *testing.T) {
		tr := metrics.NewAITracker(5*time.Minute, clock)
		if _, ok := tr.Stats(""); ok {
			t.Fatal("Stats ok before connect")
		}
	})

	tr := metrics.NewAITracker(5*time.Minute, clock)
	tr.SetConnected(true)
	for _, ev := range [][]byte{
		llmEvent(t0.Add(-time.Minute), "org-a", 100, 400, 200, "completed"),
		llmEvent(t0.Add(-2*time.Minute), "org-a", 50, 50, 900, "completed"),
		llmEvent(t0.Add(-3*time.Minute), "org-a", 10, 0, 50, "failed"),
		llmEvent(t0.Add(-time.Minute), "org-b", 1000, 1500, 3000, "completed"),
		llmEvent(t0.Add(-10*time.Minute), "org-a", 9999, 9999, 9999, "completed"), // outside the window
	} {
		if err := tr.Add(ev); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("Stats - one org - only the answers of that org", func(t *testing.T) {
		got, ok := tr.Stats("org-a")
		if !ok || got.Answers != 3 || got.Failed != 1 || got.TokensPerMinute != 122 || got.LatencyP95Ms == nil || *got.LatencyP95Ms != 900 {
			t.Fatalf("Stats(org-a) = %+v, %v; want 3 answers, 1 failed, 122 tokens/min, p95 900", got, ok)
		}
	})

	t.Run("Stats - all orgs - every answer in the window", func(t *testing.T) {
		got, _ := tr.Stats("")
		if got.Answers != 4 || got.TokensPerMinute != 622 || *got.LatencyP95Ms != 3000 {
			t.Fatalf("Stats() = %+v; want 4 answers, 622 tokens/min, p95 3000", got)
		}
	})

	t.Run("Stats - unknown org - zero, no latency", func(t *testing.T) {
		got, _ := tr.Stats("org-c")
		if got.Answers != 0 || got.TokensPerMinute != 0 || got.LatencyP95Ms != nil {
			t.Fatalf("Stats(org-c) = %+v; want zero", got)
		}
	})

	t.Run("Stats - window moves on - old answers drop out", func(t *testing.T) {
		now = t0.Add(3*time.Minute + 30*time.Second)
		defer func() { now = t0 }()
		got, _ := tr.Stats("org-a")
		if got.Answers != 1 || got.TokensPerMinute != 100 {
			t.Fatalf("Stats(org-a) = %+v; want only the newest answer", got)
		}
	})

	t.Run("Add - event without org id - dropped", func(t *testing.T) {
		if err := tr.Add(llmEvent(t0, "", 1, 1, 1, "completed")); err == nil {
			t.Fatal("Add accepted an event without org id")
		}
		if err := tr.Add([]byte("{not json")); err == nil {
			t.Fatal("Add accepted bad JSON")
		}
	})
}

func writeProc(t *testing.T, stat string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(stat), 0o600); err != nil {
		t.Fatal(err)
	}
	mem := "MemTotal:       16000000 kB\nMemFree:         1000000 kB\nMemAvailable:    4000000 kB\n"
	if err := os.WriteFile(filepath.Join(dir, "meminfo"), []byte(mem), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestHostSampler(t *testing.T) {
	t.Run("Sample - two samples - CPU share between them, memory and disk", func(t *testing.T) {
		dir := writeProc(t, "cpu  100 0 100 700 100 0 0 0 0 0\ncpu0 1 1 1 1 1 0 0 0 0 0\n")
		h := metrics.NewHostSampler(dir, dir)
		first, err := h.Sample()
		if err != nil {
			t.Fatal(err)
		}
		if first.CPURatio != nil {
			t.Fatalf("first CPU ratio %v, want null", *first.CPURatio)
		}
		if first.MemoryTotalBytes != 16000000*1024 || first.MemoryUsedBytes != 12000000*1024 {
			t.Fatalf("memory %d/%d", first.MemoryUsedBytes, first.MemoryTotalBytes)
		}
		if first.DiskTotalBytes == 0 || first.DiskUsedBytes > first.DiskTotalBytes {
			t.Fatalf("disk %d/%d", first.DiskUsedBytes, first.DiskTotalBytes)
		}
		// +300 busy, +100 idle: 75 % busy.
		if err := os.WriteFile(filepath.Join(dir, "stat"), []byte("cpu  250 0 250 800 100 0 0 0 0 0\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		second, err := h.Sample()
		if err != nil || second.CPURatio == nil || *second.CPURatio != 0.75 {
			t.Fatalf("second = %+v, %v; want CPU ratio 0.75", second, err)
		}
	})

	t.Run("Sample - no proc dir - error", func(t *testing.T) {
		if _, err := metrics.NewHostSampler(filepath.Join(t.TempDir(), "none"), "/").Sample(); err == nil {
			t.Fatal("Sample without /proc gave no error")
		}
	})

	t.Run("Sample - stat without cpu line - error", func(t *testing.T) {
		dir := writeProc(t, "intr 1 2 3\n")
		if _, err := metrics.NewHostSampler(dir, dir).Sample(); err == nil {
			t.Fatal("Sample accepted a stat without cpu line")
		}
	})
}

type queryLog struct {
	mu   sync.Mutex
	list []string
}

func (l *queryLog) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.list)
}

// fakeProm answers a query with the body of the answers key it contains, else an
// empty vector. Keys must match one query only.
func fakeProm(t *testing.T, answers map[string]string) (*httptest.Server, *queryLog) {
	t.Helper()
	queries := &queryLog{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query().Get("query")
		queries.mu.Lock()
		queries.list = append(queries.list, q)
		queries.mu.Unlock()
		body := `{"status":"success","data":{"resultType":"vector","result":[]}}`
		for key, b := range answers {
			if strings.Contains(q, key) {
				body = b
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, queries
}

func vector(pairs ...string) string {
	var items []string
	for i := 0; i+1 < len(pairs); i += 2 {
		items = append(items, fmt.Sprintf(`{"metric":{"service":%q},"value":[1761739200,%q]}`, pairs[i], pairs[i+1]))
	}
	return `{"status":"success","data":{"resultType":"vector","result":[` + strings.Join(items, ",") + `]}}`
}

func TestPrometheus(t *testing.T) {
	t.Run("Services - edge metrics - rate, error share and p95 per service", func(t *testing.T) {
		srv, queries := fakeProm(t, map[string]string{
			"(rate(traefik_service_requests_total[300s]))": vector("llm@file", "2", "identity@file", "0.5", "stub@file", "0"),
			`{code=~"5.."}`:           vector("llm@file", "0.5"),
			"histogram_quantile(0.95": vector("llm@file", "0.25", "identity@file", "NaN"),
		})
		p, err := metrics.NewPrometheus(srv.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		got, err := p.Services(t.Context(), 5*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 || got[0].Service != "identity" || got[1].Service != "llm" || got[2].Service != "stub" {
			t.Fatalf("services %+v, want identity, llm, stub", got)
		}
		llm := got[1]
		if llm.RequestsPerSecond != 2 || llm.ErrorRatio != 0.25 || llm.P95Ms == nil || *llm.P95Ms != 250 {
			t.Fatalf("llm %+v, want 2 rps, 0.25 errors, p95 250 ms", llm)
		}
		if got[0].P95Ms != nil || got[2].ErrorRatio != 0 {
			t.Fatalf("identity p95 %v (NaN = null), stub errors %v (no traffic = 0)", got[0].P95Ms, got[2].ErrorRatio)
		}
		if queries.len() != 3 {
			t.Fatalf("%d queries, want 3", queries.len())
		}
	})

	t.Run("Services - Prometheus answers 500 - error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		t.Cleanup(srv.Close)
		p, _ := metrics.NewPrometheus(srv.URL, nil)
		if _, err := p.Services(t.Context(), time.Minute); err == nil {
			t.Fatal("Services gave no error on 500")
		}
	})

	t.Run("NewPrometheus - url with password - error", func(t *testing.T) {
		for _, u := range []string{"http://u:p@telemetry:9090", "ftp://telemetry", "telemetry:9090", ""} {
			if _, err := metrics.NewPrometheus(u, nil); err == nil {
				t.Fatalf("NewPrometheus(%q) gave no error", u)
			}
		}
	})
}

func TestCollector(t *testing.T) {
	t.Run("Platform - no sources - every part null", func(t *testing.T) {
		c := metrics.New(metrics.Config{Now: func() time.Time { return t0 }}, nil)
		c.Refresh(t.Context())
		p := c.Platform()
		if p.Services != nil || p.Host != nil || p.Database != nil || p.Bus != nil || p.AI != nil {
			t.Fatalf("platform %+v, want every part null", p)
		}
		if p.WindowSeconds != 300 || !p.GeneratedAt.Equal(t0) {
			t.Fatalf("window %d, generated %s", p.WindowSeconds, p.GeneratedAt)
		}
	})

	t.Run("Platform - prometheus and host - services and host set", func(t *testing.T) {
		srv, _ := fakeProm(t, map[string]string{"(rate(traefik_service_requests_total[60s]))": vector("llm@file", "1")})
		prom, _ := metrics.NewPrometheus(srv.URL, nil)
		dir := writeProc(t, "cpu  1 0 1 8 0 0 0 0\n")
		c := metrics.New(metrics.Config{Window: time.Minute, Prom: prom, Host: metrics.NewHostSampler(dir, dir)}, nil)
		c.Refresh(t.Context())
		p := c.Platform()
		if len(p.Services) != 1 || p.Services[0].Service != "llm" || p.Host == nil || p.Host.MemoryTotalBytes == 0 {
			t.Fatalf("platform %+v, want llm and host numbers", p)
		}
	})
}
