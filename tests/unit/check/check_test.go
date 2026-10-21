package check_test

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/VelimirMueller/synthwerk-pulse/internal/check"
	"github.com/VelimirMueller/synthwerk-pulse/internal/status"
	"github.com/VelimirMueller/synthwerk-pulse/internal/target"
)

func serve(t *testing.T, code int, contentType, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func httpTarget(url string) target.Target {
	return target.Target{Name: "svc", Kind: target.HTTP, URL: url, Interval: time.Second, Timeout: 500 * time.Millisecond}
}

func TestHTTPCheck(t *testing.T) {
	cases := []struct {
		name        string
		code        int
		contentType string
		body        string
		want        status.Status
		wantChecks  int
	}{
		{"Check - readyz 200 ok - ok with checks", 200, "application/json",
			`{"status":"ok","version":"0.1.0","commit":"3f9c2e1","checks":[{"name":"postgres","status":"ok","latency_ms":2}]}`, status.OK, 1},
		{"Check - readyz 200 degraded - degraded", 200, "application/json; charset=utf-8",
			`{"status":"degraded","checks":[{"name":"signoz","status":"degraded"}]}`, status.Degraded, 1},
		{"Check - readyz 503 down - down with checks", 503, "application/json",
			`{"status":"down","checks":[{"name":"postgres","status":"down"}]}`, status.Down, 1},
		{"Check - 503 that says ok - down", 503, "application/json", `{"status":"ok","checks":[]}`, status.Down, 0},
		{"Check - plain 200 - ok", 200, "text/plain", "OK", status.OK, 0},
		{"Check - 204 without body - ok", 204, "", "", status.OK, 0},
		{"Check - redirect - ok, not followed", 302, "text/html", "", status.OK, 0},
		{"Check - 500 - down", 500, "text/plain", "boom", status.Down, 0},
		{"Check - 404 - down", 404, "text/plain", "", status.Down, 0},
		{"Check - JSON without a known status - ok", 200, "application/json", `{"status":"green"}`, status.OK, 0},
		{"Check - bad check entries - dropped", 200, "application/json",
			`{"status":"ok","checks":[{"name":"Bad Name","status":"ok"},{"name":"nats","status":"maybe"},{"name":"postgres","status":"ok"}]}`, status.OK, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := check.New(httpTarget(serve(t, c.code, c.contentType, c.body)), nil).Check(t.Context())
			if res.Status != c.want || len(res.Checks) != c.wantChecks {
				t.Fatalf("status %s with %d checks, want %s with %d", res.Status, len(res.Checks), c.want, c.wantChecks)
			}
		})
	}

	t.Run("Check - target hangs - down within the timeout", func(t *testing.T) {
		release := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
		t.Cleanup(srv.Close)
		t.Cleanup(func() { close(release) })
		tgt := httpTarget(srv.URL)
		tgt.Timeout = 200 * time.Millisecond
		start := time.Now()
		res := check.New(tgt, nil).Check(t.Context())
		if res.Status != status.Down || time.Since(start) > time.Second {
			t.Fatalf("status %s after %s, want down within 1s", res.Status, time.Since(start))
		}
	})

	t.Run("Check - nothing listens - down", func(t *testing.T) {
		addr := closedAddr(t)
		res := check.New(httpTarget("http://"+addr+"/readyz"), nil).Check(t.Context())
		if res.Status != status.Down {
			t.Fatalf("status %s, want down", res.Status)
		}
	})
}

func TestTCPCheck(t *testing.T) {
	t.Run("Check - port open - ok", func(t *testing.T) {
		l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = l.Close() })
		go func() {
			for {
				c, err := l.Accept()
				if err != nil {
					return
				}
				_ = c.Close()
			}
		}()
		tgt := target.Target{Name: "db", Kind: target.TCP, Address: l.Addr().String(), Interval: time.Second, Timeout: 500 * time.Millisecond}
		if res := check.New(tgt, nil).Check(t.Context()); res.Status != status.OK {
			t.Fatalf("status %s, want ok", res.Status)
		}
	})

	t.Run("Check - port closed - down", func(t *testing.T) {
		tgt := target.Target{Name: "db", Kind: target.TCP, Address: closedAddr(t), Interval: time.Second, Timeout: 500 * time.Millisecond}
		if res := check.New(tgt, nil).Check(t.Context()); res.Status != status.Down {
			t.Fatalf("status %s, want down", res.Status)
		}
	})
}

// closedAddr returns a local address where nothing listens.
func closedAddr(t *testing.T) string {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func TestHTTPCheckLargeBody(t *testing.T) {
	t.Run("Check - readyz body over 64 KiB - not read as readyz, status from the code", func(t *testing.T) {
		pad := strings.Repeat(" ", 64<<10)
		url := serve(t, 200, "application/json", `{"status":"down","checks":[]}`+pad)
		res := check.New(httpTarget(url), nil).Check(t.Context())
		if res.Status != status.OK {
			t.Fatalf("status %s, want ok (body ignored)", res.Status)
		}
	})
}
