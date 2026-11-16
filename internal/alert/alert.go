// Package alert raises and resolves the alerts of the health view (synthwerk#57,
// docs/features/alerts-and-cost.md). An Engine checks three rules on every interval:
//
//   - target_down: a target of the live status is down.
//   - service_errors: the share of 5xx answers of a service at the edge is at or above
//     the threshold, with at least one 5xx in the window. The trace link comes from Tempo.
//   - ai_failed: the share of failed llm answers is at or above the threshold, with at
//     least one failed answer in the window. The trace link comes from the traceparent of
//     the newest failed answer.
//
// Alerts live in memory. After a restart, the first check raises the active ones again.
package alert

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/VelimirMueller/synthwerk-contracts/go/kit/auth"

	"github.com/VelimirMueller/synthwerk-pulse/internal/metrics"
	"github.com/VelimirMueller/synthwerk-pulse/internal/status"
	"github.com/VelimirMueller/synthwerk-pulse/internal/stream"
)

// Rule is the kind of an alert.
type Rule string

// The rules.
const (
	TargetDown    Rule = "target_down"
	ServiceErrors Rule = "service_errors"
	AIFailed      Rule = "ai_failed"
)

// Severity of an alert.
type Severity string

// The severities.
const (
	Critical Severity = "critical"
	Warning  Severity = "warning"
)

// MaxAlerts caps the answer of GET /v1/alerts.
const MaxAlerts = 100

// traceRetry is the time between two trace searches of one alert without a trace.
const traceRetry = 30 * time.Second

// Alert is one active alert.
type Alert struct {
	// ID is "<rule>:<subject>". It stays the same while the alert is active.
	ID       string   `json:"id"`
	Rule     Rule     `json:"rule"`
	Severity Severity `json:"severity"`
	// Subject is the target or service name.
	Subject string `json:"subject"`
	Summary string `json:"summary"`
	// Value and Threshold are shares from 0 to 1. Null for target_down.
	Value     *float64  `json:"value"`
	Threshold *float64  `json:"threshold"`
	Since     time.Time `json:"since"`
	// TraceID and TraceURL are null when no trace is known, and for callers who are not
	// operators: a trace can hold data of every org.
	TraceID  *string `json:"trace_id"`
	TraceURL *string `json:"trace_url"`
}

// List is the body of GET /v1/alerts.
type List struct {
	GeneratedAt time.Time `json:"generated_at"`
	Alerts      []Alert   `json:"alerts"`
}

// Config of an Engine.
type Config struct {
	// Window of the edge and AI numbers. Default 5 min.
	Window time.Duration
	// Interval between two checks. Default 10 s.
	Interval time.Duration
	// ErrorRatio is the threshold of service_errors and ai_failed. Default 0.05.
	ErrorRatio float64
	// Targets returns the live status. Nil: no target_down alerts.
	Targets func() []stream.Target
	// Prom gives the edge numbers. Nil: no service_errors alerts.
	Prom *metrics.Prometheus
	// AI gives the llm answers. Nil: no ai_failed alerts.
	AI *metrics.AITracker
	// Tempo finds the trace of a service_errors alert. Nil: no trace for that rule.
	Tempo *Tempo
	// TraceURL is the link template with {trace_id}. Empty: no links.
	TraceURL string
	Now      func() time.Time
}

type state struct {
	alert      Alert
	traceTried time.Time
}

// Engine is safe for concurrent use. Create it with New, then call Run once.
type Engine struct {
	cfg Config
	log *slog.Logger

	eval   sync.Mutex // one Evaluate at a time; guards active
	active map[string]*state

	mu        sync.Mutex
	published List
}

// New returns an engine. logger nil: no log.
func New(cfg Config, logger *slog.Logger) *Engine {
	if cfg.Window <= 0 {
		cfg.Window = 5 * time.Minute
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Second
	}
	if cfg.ErrorRatio <= 0 {
		cfg.ErrorRatio = 0.05
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Engine{cfg: cfg, log: logger, active: map[string]*state{},
		published: List{GeneratedAt: cfg.Now().UTC(), Alerts: []Alert{}}}
}

// Run checks the rules on every interval until ctx ends.
func (e *Engine) Run(ctx context.Context) error {
	tick := time.NewTicker(e.cfg.Interval)
	defer tick.Stop()
	for {
		e.Evaluate(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

// Evaluate checks every rule once, raises new alerts and resolves the ones whose
// condition is gone. A source that cannot be read keeps its alerts as they are.
func (e *Engine) Evaluate(ctx context.Context) {
	e.eval.Lock()
	defer e.eval.Unlock()
	now := e.cfg.Now().UTC()
	want := map[string]Alert{}
	keep := map[Rule]bool{} // rules whose source failed: their alerts stay

	if e.cfg.Targets != nil {
		for _, t := range e.cfg.Targets() {
			if t.Status != status.Down {
				continue
			}
			a := Alert{Rule: TargetDown, Severity: Critical, Subject: t.Target, Since: t.Since.UTC(),
				Summary: fmt.Sprintf("%s is down.", t.Target)}
			want[id(a)] = a
		}
	}
	if e.cfg.Prom != nil {
		cctx, cancel := context.WithTimeout(ctx, min(e.cfg.Interval, 5*time.Second))
		counts, err := e.cfg.Prom.EdgeCounts(cctx, e.cfg.Window)
		cancel()
		if err != nil {
			keep[ServiceErrors] = true
			if ctx.Err() == nil {
				e.log.DebugContext(ctx, "edge numbers not readable for alerts",
					slog.String("event.name", "pulse.alert.source_failed"),
					slog.String("exception.message", err.Error()))
			}
		}
		for name, c := range counts { // counts is nil when err is set
			if c.Requests <= 0 || c.Errors < 0.5 || c.Errors/c.Requests < e.cfg.ErrorRatio {
				continue
			}
			ratio := min(c.Errors/c.Requests, 1)
			a := Alert{Rule: ServiceErrors, Severity: Warning, Subject: name, Value: ptr(round(ratio)),
				Threshold: ptr(e.cfg.ErrorRatio), Since: now,
				Summary: fmt.Sprintf("%.0f of %.0f %s requests (%s) failed at the edge in the last %s.",
					math.Round(c.Errors), math.Max(math.Round(c.Requests), 1), name, percent(ratio), minutes(e.cfg.Window))}
			want[id(a)] = a
		}
	}
	var aiTrace string
	if e.cfg.AI != nil {
		ai, ok := e.cfg.AI.Stats("")
		if !ok {
			keep[AIFailed] = true
		} else if ai.Failed > 0 && float64(ai.Failed)/float64(ai.Answers) >= e.cfg.ErrorRatio {
			ratio := float64(ai.Failed) / float64(ai.Answers)
			a := Alert{Rule: AIFailed, Severity: Warning, Subject: "llm", Value: ptr(round(ratio)),
				Threshold: ptr(e.cfg.ErrorRatio), Since: now,
				Summary: fmt.Sprintf("%d of %d AI answers (%s) failed in the last %s.",
					ai.Failed, ai.Answers, percent(ratio), minutes(e.cfg.Window))}
			want[id(a)] = a
			aiTrace = e.cfg.AI.LastFailedTrace()
		}
	}

	for key, st := range e.active {
		if _, ok := want[key]; ok || keep[st.alert.Rule] {
			continue
		}
		delete(e.active, key)
		e.log.InfoContext(ctx, "alert resolved",
			slog.String("event.name", "pulse.alert.resolved"),
			slog.String("pulse.alert.rule", string(st.alert.Rule)),
			slog.String("pulse.alert.subject", st.alert.Subject),
			slog.Duration("pulse.alert.duration", now.Sub(st.alert.Since)))
	}
	for key, a := range want {
		st, ok := e.active[key]
		if !ok {
			a.ID = key
			st = &state{alert: a}
			e.active[key] = st
			e.log.WarnContext(ctx, "alert raised",
				slog.String("event.name", "pulse.alert.raised"),
				slog.String("pulse.alert.rule", string(a.Rule)),
				slog.String("pulse.alert.subject", a.Subject),
				slog.String("pulse.alert.summary", a.Summary))
		} else {
			since := st.alert.Since
			if a.Rule == TargetDown {
				since = a.Since
			}
			a.ID, a.Since, a.TraceID = key, since, st.alert.TraceID
			st.alert = a
		}
		switch a.Rule {
		case AIFailed:
			if aiTrace != "" {
				st.alert.TraceID = &aiTrace
			}
		case ServiceErrors:
			e.findTrace(ctx, st, now)
		case TargetDown:
			// A down target has no request, so no trace.
		}
	}
	e.publish(now)
}

// findTrace searches the trace of a service_errors alert at most every traceRetry.
func (e *Engine) findTrace(ctx context.Context, st *state, now time.Time) {
	if e.cfg.Tempo == nil || st.alert.TraceID != nil || now.Sub(st.traceTried) < traceRetry {
		return
	}
	st.traceTried = now
	cctx, cancel := context.WithTimeout(ctx, min(e.cfg.Interval, 5*time.Second))
	defer cancel()
	tid, err := e.cfg.Tempo.FailedEdgeTrace(cctx, st.alert.Subject, now.Add(-e.cfg.Window), now)
	if err != nil {
		if ctx.Err() == nil {
			e.log.DebugContext(ctx, "trace of the alert not found",
				slog.String("event.name", "pulse.alert.trace_failed"),
				slog.String("pulse.alert.subject", st.alert.Subject),
				slog.String("exception.message", err.Error()))
		}
		return
	}
	if tid != "" {
		st.alert.TraceID = &tid
	}
}

func (e *Engine) publish(now time.Time) {
	out := make([]Alert, 0, len(e.active))
	for _, st := range e.active {
		a := st.alert
		if a.TraceID != nil {
			if u := TraceURL(e.cfg.TraceURL, *a.TraceID); u != "" {
				a.TraceURL = &u
			}
		}
		out = append(out, a)
	}
	slices.SortFunc(out, func(a, b Alert) int {
		if a.Severity != b.Severity {
			if a.Severity == Critical {
				return -1
			}
			return 1
		}
		return cmp.Or(a.Since.Compare(b.Since), strings.Compare(a.ID, b.ID))
	})
	if len(out) > MaxAlerts {
		out = out[:MaxAlerts]
	}
	e.mu.Lock()
	e.published = List{GeneratedAt: now, Alerts: out}
	e.mu.Unlock()
}

// List returns the active alerts. Without operator, the trace fields are null.
func (e *Engine) List(operator bool) List {
	e.mu.Lock()
	l := List{GeneratedAt: e.published.GeneratedAt, Alerts: slices.Clone(e.published.Alerts)}
	e.mu.Unlock()
	if !operator {
		for i := range l.Alerts {
			l.Alerts[i].TraceID, l.Alerts[i].TraceURL = nil, nil
		}
	}
	return l
}

// ServeAlerts serves GET /v1/alerts. Guard it with auth.RequireRole(metrics.ViewerRoles...).
// Only operators get the trace fields.
func (e *Engine) ServeAlerts(w http.ResponseWriter, r *http.Request) {
	p, ok := auth.FromContext(r.Context())
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(e.List(ok && p.IsOperator()))
}

func id(a Alert) string { return string(a.Rule) + ":" + a.Subject }

func ptr[T any](v T) *T { return &v }

func round(v float64) float64 { return math.Round(v*1e4) / 1e4 }

func percent(ratio float64) string {
	if ratio < 0.1 {
		return fmt.Sprintf("%.1f %%", ratio*100)
	}
	return fmt.Sprintf("%.0f %%", ratio*100)
}

func minutes(d time.Duration) string {
	return fmt.Sprintf("%d min", int(math.Round(d.Minutes())))
}
