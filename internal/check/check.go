// Package check runs one check against one target and returns its raw result.
// Package status turns the raw results into the target status.
package check

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"regexp"
	"time"

	"github.com/VelimirMueller/synthwerk-pulse/internal/status"
	"github.com/VelimirMueller/synthwerk-pulse/internal/target"
)

const (
	maxBody   = 64 << 10
	maxChecks = 32
)

var checkNameRe = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

// Item is one check of a /readyz body: the dependency name and its status.
type Item struct {
	Name   string        `json:"name"`
	Status status.Status `json:"status"`
}

// Result of one check.
type Result struct {
	// Status is ok, degraded or down.
	Status status.Status
	// Checks are the checks of the /readyz body. Empty for tcp and for other bodies.
	Checks []Item
	// Latency is the time to the response headers (or to the failure).
	Latency time.Duration
}

// Checker checks one target. Check never blocks longer than the target timeout.
type Checker interface {
	Check(ctx context.Context) Result
}

// New returns the checker for t. client is used for kind http; nil means a client
// that follows no redirects.
func New(t target.Target, client *http.Client) Checker {
	if t.Kind == target.TCP {
		return tcpChecker{address: t.Address, timeout: t.Timeout}
	}
	if client == nil {
		client = NewHTTPClient()
	}
	return httpChecker{url: t.URL, timeout: t.Timeout, client: client}
}

// NewHTTPClient returns the client for http checks: no redirects, no cookies,
// no keep-alive between checks (a stopped target must not hide behind an old connection).
func NewHTTPClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DisableKeepAlives = true
	tr.Proxy = nil
	return &http.Client{
		Transport: tr,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

type httpChecker struct {
	url     string
	timeout time.Duration
	client  *http.Client
}

// readiness is the part of the /readyz body (openapi/_shared/health.yaml) pulse reads.
type readiness struct {
	Status status.Status `json:"status"`
	Checks []Item        `json:"checks"`
}

func (c httpChecker) Check(ctx context.Context) Result {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	start := time.Now()
	down := func() Result { return Result{Status: status.Down, Latency: time.Since(start)} }

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return down()
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "synthwerk-pulse")
	res, err := c.client.Do(req)
	if err != nil {
		return down()
	}
	defer func() { _ = res.Body.Close() }()
	latency := time.Since(start) // time to the response headers
	// One byte more than the limit: a longer body is cut, so it is not read as /readyz.
	body, err := io.ReadAll(io.LimitReader(res.Body, maxBody+1))
	if err != nil && !errors.Is(err, io.EOF) {
		return down()
	}
	if len(body) > maxBody {
		body = nil
	}
	if res.StatusCode >= http.StatusBadRequest {
		r := Result{Status: status.Down, Latency: latency}
		if rd, ok := parseReadiness(res.Header.Get("Content-Type"), body); ok {
			r.Checks = rd.Checks
		}
		return r
	}
	if rd, ok := parseReadiness(res.Header.Get("Content-Type"), body); ok {
		return Result{Status: rd.Status, Checks: rd.Checks, Latency: latency}
	}
	return Result{Status: status.OK, Latency: latency}
}

// parseReadiness reads a /readyz body. ok is false for any other body.
// Checks with a bad name or status are dropped; at most 32 are kept.
func parseReadiness(contentType string, body []byte) (readiness, bool) {
	mt, _, err := mime.ParseMediaType(contentType)
	if err != nil || mt != "application/json" {
		return readiness{}, false
	}
	var rd readiness
	if err := json.Unmarshal(body, &rd); err != nil || !status.Valid(rd.Status) {
		return readiness{}, false
	}
	checks := make([]Item, 0, min(len(rd.Checks), maxChecks))
	for _, c := range rd.Checks {
		if len(checks) == maxChecks {
			break
		}
		if checkNameRe.MatchString(c.Name) && status.Valid(c.Status) {
			checks = append(checks, c)
		}
	}
	rd.Checks = checks
	return rd, true
}

type tcpChecker struct {
	address string
	timeout time.Duration
}

func (c tcpChecker) Check(ctx context.Context) Result {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	start := time.Now()
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", c.address)
	if err != nil {
		return Result{Status: status.Down, Latency: time.Since(start)}
	}
	_ = conn.Close()
	return Result{Status: status.OK, Latency: time.Since(start)}
}
