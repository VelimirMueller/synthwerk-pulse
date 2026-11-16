package alert

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// EdgeService is the service.name of the edge (Traefik) spans in the synthwerk-infra stack.
const EdgeService = "synthwerk-edge"

var (
	serviceRE = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	traceRE   = regexp.MustCompile(`^[0-9a-f]{1,32}$`)
)

// Tempo finds the trace of a failed edge request with the Tempo search API.
type Tempo struct {
	base   *url.URL
	client *http.Client
}

// NewTempo returns a client for the Tempo API at base, for example http://telemetry:3200.
// The URL must be http or https without user info.
func NewTempo(base string, client *http.Client) (*Tempo, error) {
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return nil, errors.New("alert: tempo url must be http(s)://host[:port] without user info")
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &Tempo{base: u, client: client}, nil
}

// EdgeErrorQuery is the TraceQL query for a 5xx answer of service at the edge. The edge
// routes of a service start with /<service>/ (synthwerk-infra compose/traefik/dynamic).
func EdgeErrorQuery(service string) (string, error) {
	if !serviceRE.MatchString(service) {
		return "", fmt.Errorf("alert: service name %q is not safe in a query", service)
	}
	return fmt.Sprintf(`{ resource.service.name = %q && kind = server && span.http.response.status_code >= 500 && span.url.path =~ "/%s(/.*)?" }`,
		EdgeService, service), nil
}

// FailedEdgeTrace returns the id of the newest trace with a 5xx answer of service at the
// edge between from and to, or "" when there is none.
func (t *Tempo) FailedEdgeTrace(ctx context.Context, service string, from, to time.Time) (string, error) {
	q, err := EdgeErrorQuery(service)
	if err != nil {
		return "", err
	}
	u := t.base.JoinPath("/api/search")
	u.RawQuery = url.Values{
		"q":     {q},
		"limit": {"20"},
		"start": {strconv.FormatInt(from.Unix(), 10)},
		"end":   {strconv.FormatInt(to.Unix()+1, 10)},
	}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	res, err := t.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("alert: tempo: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("alert: tempo: status %d", res.StatusCode)
	}
	var body struct {
		Traces []struct {
			TraceID           string `json:"traceID"`
			StartTimeUnixNano string `json:"startTimeUnixNano"`
		} `json:"traces"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&body); err != nil {
		return "", fmt.Errorf("alert: tempo: %w", err)
	}
	var newest string
	var newestAt int64 = -1
	for _, tr := range body.Traces {
		id := NormalizeTraceID(tr.TraceID)
		at, err := strconv.ParseInt(tr.StartTimeUnixNano, 10, 64)
		if id == "" || err != nil {
			continue
		}
		if at > newestAt {
			newest, newestAt = id, at
		}
	}
	return newest, nil
}

// NormalizeTraceID returns id as 32 lower-case hex digits, or "" when it is not a trace
// id. Tempo may leave out the leading zeros. An all-zero id is not valid.
func NormalizeTraceID(id string) string {
	id = strings.ToLower(id)
	if !traceRE.MatchString(id) || strings.Trim(id, "0") == "" {
		return ""
	}
	return strings.Repeat("0", 32-len(id)) + id
}

// Placeholder is the part of the trace URL template that pulse replaces with the trace id.
const Placeholder = "{trace_id}"

// CheckTraceURL checks a trace URL template: an http(s) URL with the placeholder
// {trace_id}, for example a Grafana Explore link. Empty is allowed: no links.
func CheckTraceURL(template string) error {
	if template == "" {
		return nil
	}
	if !strings.Contains(template, Placeholder) {
		return errors.New("alert: trace url template must contain {trace_id}")
	}
	u, err := url.Parse(strings.ReplaceAll(template, Placeholder, strings.Repeat("0", 31)+"1"))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return errors.New("alert: trace url template must be an http(s) URL without user info")
	}
	return nil
}

// TraceURL fills the template with a normalized trace id. "" when either is empty.
func TraceURL(template, traceID string) string {
	if template == "" || traceID == "" {
		return ""
	}
	return strings.ReplaceAll(template, Placeholder, traceID)
}
