// Alerts and trace links (synthwerk#57).
package alert_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VelimirMueller/synthwerk-pulse/internal/alert"
	"github.com/VelimirMueller/synthwerk-pulse/internal/metrics"
	"github.com/VelimirMueller/synthwerk-pulse/internal/status"
	"github.com/VelimirMueller/synthwerk-pulse/internal/stream"
)

var t0 = time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)

const grafana = "http://grafana.test/explore?orgId=1&panes=%7B%22a%22%3A%7B%22queries%22%3A%5B%7B%22query%22%3A%22{trace_id}%22%7D%5D%7D%7D"

// edge is a fake Prometheus (edge counts) and a fake Tempo (search and trace by id).
// The forced error: requests and errors of one service, one failed trace in Tempo.
type edge struct {
	mu        sync.Mutex
	requests  map[string]float64
	errors    map[string]float64
	promDown  bool
	traceID   string // as Tempo returns it, maybe without the leading zeros
	queries   []string
	tempoHits int
}

func (e *edge) set(service string, requests, errors float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.requests[service], e.errors[service] = requests, errors
}

func (e *edge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch {
	case r.URL.Path == "/api/v1/query":
		if e.promDown {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		src := e.requests
		if strings.Contains(r.URL.Query().Get("query"), `code=~"5.."`) {
			src = e.errors
		}
		var result []string
		for s, v := range src {
			result = append(result, fmt.Sprintf(`{"metric":{"service":"%s@file"},"value":[1,"%g"]}`, s, v))
		}
		_, _ = fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[%s]}}`, strings.Join(result, ","))
	case r.URL.Path == "/api/search":
		e.tempoHits++
		e.queries = append(e.queries, r.URL.Query().Get("q"))
		if e.traceID == "" {
			_, _ = w.Write([]byte(`{"traces":[]}`))
			return
		}
		_, _ = fmt.Fprintf(w, `{"traces":[{"traceID":"0000000000000000000000000000beef","startTimeUnixNano":"100"},{"traceID":%q,"startTimeUnixNano":"200"}]}`, e.traceID)
	case strings.HasPrefix(r.URL.Path, "/api/traces/"):
		// The trace UI reads the trace by its full id.
		if strings.TrimPrefix(r.URL.Path, "/api/traces/") == fmt.Sprintf("%032s", e.traceID) {
			_, _ = w.Write([]byte(`{"batches":[]}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

type fixture struct {
	edge    *edge
	srv     *httptest.Server
	engine  *alert.Engine
	ai      *metrics.AITracker
	targets []stream.Target
	now     time.Time
	logs    *bytes.Buffer
}

func setup(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{edge: &edge{requests: map[string]float64{}, errors: map[string]float64{}}, now: t0, logs: &bytes.Buffer{}}
	f.srv = httptest.NewServer(f.edge)
	t.Cleanup(f.srv.Close)
	prom, err := metrics.NewPrometheus(f.srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	tempo, err := alert.NewTempo(f.srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	clock := func() time.Time { return f.now }
	f.ai = metrics.NewAITracker(5*time.Minute, clock)
	f.engine = alert.New(alert.Config{
		Targets: func() []stream.Target { return f.targets }, Prom: prom, AI: f.ai, Tempo: tempo,
		TraceURL: grafana, Now: clock,
	}, slog.New(slog.NewJSONHandler(f.logs, nil)))
	return f
}

func (f *fixture) one(t *testing.T, operator bool) alert.Alert {
	t.Helper()
	l := f.engine.List(operator)
	if len(l.Alerts) != 1 {
		t.Fatalf("alerts %+v, want exactly one", l.Alerts)
	}
	return l.Alerts[0]
}

func TestForcedError(t *testing.T) {
	f := setup(t)
	f.edge.set("llm", 40, 0)
	f.edge.set("identity", 100, 0)
	f.edge.traceID = "d40c39905663a1bcaa36ad07693a23d8"
	f.engine.Evaluate(t.Context())

	t.Run("no 5xx - no alert", func(t *testing.T) {
		if l := f.engine.List(true); len(l.Alerts) != 0 {
			t.Fatalf("alerts %+v", l.Alerts)
		}
	})

	t.Run("forced 5xx at the edge - service_errors alert with a trace link that opens the trace", func(t *testing.T) {
		f.edge.set("llm", 43, 3)
		f.engine.Evaluate(t.Context())
		a := f.one(t, true)
		if a.ID != "service_errors:llm" || a.Rule != alert.ServiceErrors || a.Severity != alert.Warning || a.Subject != "llm" {
			t.Fatalf("alert %+v", a)
		}
		if a.Value == nil || *a.Value != 0.0698 || *a.Threshold != 0.05 || !a.Since.Equal(t0) {
			t.Fatalf("value %v threshold %v since %v", a.Value, a.Threshold, a.Since)
		}
		if a.Summary != "3 of 43 llm requests (7.0 %) failed at the edge in the last 5 min." {
			t.Fatalf("summary %q", a.Summary)
		}
		if a.TraceID == nil || *a.TraceID != f.edge.traceID || a.TraceURL == nil {
			t.Fatalf("trace %v %v, want the newest failed trace", a.TraceID, a.TraceURL)
		}
		link, err := url.Parse(*a.TraceURL)
		if err != nil || link.Host != "grafana.test" || !strings.Contains(link.Query().Get("panes"), `"query":"`+f.edge.traceID+`"`) {
			t.Fatalf("trace url %q does not hold the trace id", *a.TraceURL)
		}
		res, err := http.Get(f.srv.URL + "/api/traces/" + *a.TraceID) //nolint:noctx // test
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("trace %s not in Tempo: %d", *a.TraceID, res.StatusCode)
		}
		want := `{ resource.service.name = "synthwerk-edge" && kind = server && span.http.response.status_code >= 500 && span.url.path =~ "/llm(/.*)?" }`
		if len(f.edge.queries) != 1 || f.edge.queries[0] != want {
			t.Fatalf("tempo queries %q, want %q", f.edge.queries, want)
		}
		if !strings.Contains(f.logs.String(), `"event.name":"pulse.alert.raised"`) {
			t.Fatalf("no raised log: %s", f.logs)
		}
	})

	t.Run("still failing later - same id, first since, no new search", func(t *testing.T) {
		f.now = t0.Add(time.Minute)
		defer func() { f.now = t0 }()
		f.edge.set("llm", 50, 10)
		f.engine.Evaluate(t.Context())
		a := f.one(t, true)
		if !a.Since.Equal(t0) || *a.Value != 0.2 || f.edge.tempoHits != 1 {
			t.Fatalf("alert %+v, tempo hits %d", a, f.edge.tempoHits)
		}
	})

	t.Run("caller is not an operator - no trace fields", func(t *testing.T) {
		a := f.one(t, false)
		if a.TraceID != nil || a.TraceURL != nil {
			t.Fatalf("trace fields for a non-operator: %v %v", a.TraceID, a.TraceURL)
		}
		if b := f.one(t, true); b.TraceID == nil {
			t.Fatal("List(false) changed the stored alert")
		}
	})

	t.Run("prometheus down - alert stays", func(t *testing.T) {
		f.edge.promDown = true
		defer func() { f.edge.promDown = false }()
		f.engine.Evaluate(t.Context())
		f.one(t, true)
	})

	t.Run("errors gone - alert resolved", func(t *testing.T) {
		f.edge.set("llm", 50, 0)
		f.engine.Evaluate(t.Context())
		if l := f.engine.List(true); len(l.Alerts) != 0 {
			t.Fatalf("alerts %+v", l.Alerts)
		}
		if !strings.Contains(f.logs.String(), `"event.name":"pulse.alert.resolved"`) {
			t.Fatalf("no resolved log: %s", f.logs)
		}
	})
}

func TestTraceSearch(t *testing.T) {
	t.Run("no trace in Tempo - alert without link, search again after 30 s", func(t *testing.T) {
		f := setup(t)
		f.edge.set("vision", 2, 2)
		f.engine.Evaluate(t.Context())
		if a := f.one(t, true); a.TraceID != nil || a.TraceURL != nil {
			t.Fatalf("alert %+v", a)
		}
		f.engine.Evaluate(t.Context())
		if f.edge.tempoHits != 1 {
			t.Fatalf("tempo hits %d, want 1 inside 30 s", f.edge.tempoHits)
		}
		f.edge.traceID = "abc" // Tempo left out the leading zeros
		f.now = t0.Add(31 * time.Second)
		f.engine.Evaluate(t.Context())
		if a := f.one(t, true); a.TraceID == nil || *a.TraceID != "00000000000000000000000000000abc" {
			t.Fatalf("trace %v, want the padded id", a.TraceID)
		}
	})

	t.Run("one error below the share - no alert", func(t *testing.T) {
		f := setup(t)
		f.edge.set("llm", 100, 1)
		f.engine.Evaluate(t.Context())
		if l := f.engine.List(true); len(l.Alerts) != 0 {
			t.Fatalf("alerts %+v", l.Alerts)
		}
	})
}

func TestTargetDown(t *testing.T) {
	f := setup(t)
	downSince := t0.Add(-time.Minute)
	f.targets = []stream.Target{
		{Target: "identity", Status: status.OK, Since: t0},
		{Target: "database", Status: status.Down, Since: downSince},
	}
	f.edge.set("llm", 10, 10)
	f.engine.Evaluate(t.Context())

	t.Run("target down - critical alert first, since of the status", func(t *testing.T) {
		l := f.engine.List(true)
		if len(l.Alerts) != 2 {
			t.Fatalf("alerts %+v", l.Alerts)
		}
		a := l.Alerts[0]
		if a.ID != "target_down:database" || a.Severity != alert.Critical || !a.Since.Equal(downSince) ||
			a.Summary != "database is down." || a.Value != nil || a.TraceID != nil {
			t.Fatalf("alert %+v", a)
		}
	})

	t.Run("target up again - alert resolved", func(t *testing.T) {
		f.targets[1].Status = status.OK
		f.engine.Evaluate(t.Context())
		for _, a := range f.engine.List(true).Alerts {
			if a.Rule == alert.TargetDown {
				t.Fatalf("alert %+v still active", a)
			}
		}
	})
}

func llmEvent(at time.Time, status, traceparent string) []byte {
	b, _ := json.Marshal(map[string]any{
		"specversion": "1.0", "id": "x", "source": "/synthwerk/llm/ci", "type": "dev.synthwerk.llm.message.completed.v1",
		"time": at, "orgid": "org-a", "traceparent": traceparent,
		"data": map[string]any{"input_tokens": 1, "output_tokens": 1, "latency_ms": 10, "status": status},
	})
	return b
}

func TestAIFailed(t *testing.T) {
	f := setup(t)

	t.Run("llm stream not read - no alert", func(t *testing.T) {
		f.engine.Evaluate(t.Context())
		if l := f.engine.List(true); len(l.Alerts) != 0 {
			t.Fatalf("alerts %+v", l.Alerts)
		}
	})

	f.ai.SetConnected(true)
	for _, ev := range [][]byte{
		llmEvent(t0.Add(-3*time.Minute), "completed", ""),
		llmEvent(t0.Add(-2*time.Minute), "failed", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
		llmEvent(t0.Add(-time.Minute), "failed", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
		llmEvent(t0.Add(-30*time.Second), "failed", "not-a-traceparent"),
	} {
		if err := f.ai.Add(ev); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("failed answers - ai_failed alert with the trace of the newest failed answer", func(t *testing.T) {
		f.engine.Evaluate(t.Context())
		a := f.one(t, true)
		if a.ID != "ai_failed:llm" || a.Summary != "3 of 4 AI answers (75 %) failed in the last 5 min." || *a.Value != 0.75 {
			t.Fatalf("alert %+v", a)
		}
		if a.TraceID == nil || *a.TraceID != "0af7651916cd43dd8448eb211c80319c" || !strings.Contains(*a.TraceURL, "0af7651916cd43dd8448eb211c80319c") {
			t.Fatalf("trace %v %v", a.TraceID, a.TraceURL)
		}
	})

	t.Run("llm stream lost - alert stays", func(t *testing.T) {
		f.ai.SetConnected(false)
		defer f.ai.SetConnected(true)
		f.engine.Evaluate(t.Context())
		f.one(t, true)
	})

	t.Run("failed answers leave the window - alert resolved", func(t *testing.T) {
		f.now = t0.Add(10 * time.Minute)
		f.engine.Evaluate(t.Context())
		if l := f.engine.List(true); len(l.Alerts) != 0 {
			t.Fatalf("alerts %+v", l.Alerts)
		}
	})
}

func TestTraceHelpers(t *testing.T) {
	cases := []struct{ in, want string }{
		{"D40C39905663A1BCAA36AD07693A23D8", "d40c39905663a1bcaa36ad07693a23d8"},
		{"abc", "00000000000000000000000000000abc"},
		{"00000000000000000000000000000000", ""},
		{"xyz", ""},
		{strings.Repeat("a", 33), ""},
	}
	for _, c := range cases {
		t.Run("NormalizeTraceID - "+c.in, func(t *testing.T) {
			if got := alert.NormalizeTraceID(c.in); got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}

	t.Run("EdgeErrorQuery - name with a quote - refused", func(t *testing.T) {
		if _, err := alert.EdgeErrorQuery(`llm" || true || "`); err == nil {
			t.Fatal("unsafe name accepted")
		}
	})

	for name, tmpl := range map[string]string{
		"no placeholder": "http://grafana.test/explore",
		"not http":       "javascript:alert('{trace_id}')",
		"user info":      "http://user:" + "secret@grafana.test/{trace_id}",
	} {
		t.Run("CheckTraceURL - "+name+" - refused", func(t *testing.T) {
			if err := alert.CheckTraceURL(tmpl); err == nil {
				t.Fatalf("%q accepted", tmpl)
			}
		})
	}

	t.Run("CheckTraceURL - Grafana link or empty - accepted", func(t *testing.T) {
		if err := alert.CheckTraceURL(grafana); err != nil {
			t.Fatal(err)
		}
		if err := alert.CheckTraceURL(""); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("NewTempo - URL with user info - refused", func(t *testing.T) {
		if _, err := alert.NewTempo("http://u:p@tempo:3200", nil); err == nil {
			t.Fatal("accepted")
		}
	})
}

func TestRunAndServe(t *testing.T) {
	down := []stream.Target{{Target: "database", Status: status.Down, Since: t0}}
	e := alert.New(alert.Config{Interval: 10 * time.Millisecond, Targets: func() []stream.Target { return down }}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()

	t.Run("Run - checks on every interval until the context ends", func(t *testing.T) {
		for range 200 {
			if len(e.List(true).Alerts) == 1 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
		if err := <-done; err != nil || len(e.List(true).Alerts) != 1 {
			t.Fatalf("err %v, alerts %+v", err, e.List(true).Alerts)
		}
	})

	t.Run("ServeAlerts - no principal - no trace fields, no-store", func(t *testing.T) {
		rec := httptest.NewRecorder()
		e.ServeAlerts(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/alerts", nil))
		var body alert.List
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if rec.Header().Get("Cache-Control") != "no-store" || len(body.Alerts) != 1 || body.Alerts[0].ID != "target_down:database" {
			t.Fatalf("headers %v body %+v", rec.Header(), body)
		}
	})
}
