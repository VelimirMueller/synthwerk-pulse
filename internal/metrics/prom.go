package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Service is the request rate, error share and p95 of one service, measured at the
// edge (Traefik metrics in Prometheus) over the window.
type Service struct {
	Service           string   `json:"service"`
	RequestsPerSecond float64  `json:"requests_per_second"`
	ErrorRatio        float64  `json:"error_ratio"`
	P95Ms             *float64 `json:"p95_ms"`
}

// Edge metric names as Prometheus stores the Traefik OTLP metrics.
const (
	edgeRequests = "traefik_service_requests_total"
	edgeDuration = "traefik_service_request_duration_seconds_bucket"
)

// Prometheus reads the edge metrics from the Prometheus HTTP API.
type Prometheus struct {
	base   *url.URL
	client *http.Client
}

// NewPrometheus returns a client for the Prometheus at base, for example
// http://telemetry:9090. The URL must be http or https without user info.
func NewPrometheus(base string, client *http.Client) (*Prometheus, error) {
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return nil, errors.New("metrics: prometheus url must be http(s)://host[:port] without user info")
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &Prometheus{base: u, client: client}, nil
}

// Services returns rate, errors and p95 per edge service over window, sorted by name.
func (p *Prometheus) Services(ctx context.Context, window time.Duration) ([]Service, error) {
	w := strconv.Itoa(int(window.Seconds())) + "s"
	rate, err := p.query(ctx, fmt.Sprintf(`sum by (service) (rate(%s[%s]))`, edgeRequests, w))
	if err != nil {
		return nil, err
	}
	errs, err := p.query(ctx, fmt.Sprintf(`sum by (service) (rate(%s{code=~"5.."}[%s]))`, edgeRequests, w))
	if err != nil {
		return nil, err
	}
	p95, err := p.query(ctx, fmt.Sprintf(`histogram_quantile(0.95, sum by (service, le) (rate(%s[%s])))`, edgeDuration, w))
	if err != nil {
		return nil, err
	}
	out := make([]Service, 0, len(rate))
	for name, r := range rate {
		s := Service{Service: name, RequestsPerSecond: r}
		if r > 0 {
			s.ErrorRatio = min(errs[name]/r, 1)
		}
		if v, ok := p95[name]; ok {
			ms := v * 1000
			s.P95Ms = &ms
		}
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b Service) int { return strings.Compare(a.Service, b.Service) })
	return out, nil
}

type promResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]any            `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// query runs an instant query and returns the value per service. NaN and Inf
// values (no traffic) are left out. The Traefik service name "llm@file" becomes "llm".
func (p *Prometheus) query(ctx context.Context, q string) (map[string]float64, error) {
	u := p.base.JoinPath("/api/v1/query")
	u.RawQuery = url.Values{"query": {q}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	res, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("metrics: prometheus: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics: prometheus: status %d", res.StatusCode)
	}
	var body promResponse
	if err := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&body); err != nil {
		return nil, fmt.Errorf("metrics: prometheus: %w", err)
	}
	if body.Status != "success" || body.Data.ResultType != "vector" {
		return nil, errors.New("metrics: prometheus: no vector result")
	}
	out := make(map[string]float64, len(body.Data.Result))
	for _, r := range body.Data.Result {
		name, _, _ := strings.Cut(r.Metric["service"], "@")
		raw, ok := r.Value[1].(string)
		if name == "" || !ok {
			continue
		}
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		out[name] += v
	}
	return out, nil
}
