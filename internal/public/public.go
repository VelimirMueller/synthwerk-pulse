// Package public serves the public numbers of the site (synthwerk#47,
// docs/features/public-stats.md): uptime, p95 and answered chats. No token, no
// personal data, only aggregates over every org. The answer is cached for one minute,
// so the load does not grow with the callers.
package public

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"net/http"
	"sync"
	"time"
)

// WindowDays is the window of the uptime and of the answered chats.
const WindowDays = 30

// P95Window is the window of the p95.
const P95Window = 24 * time.Hour

// CacheFor is how long one answer stays: also the max-age of the answer.
const CacheFor = time.Minute

// Stats is the body of GET /v1/public-stats. A number pulse cannot read is null.
type Stats struct {
	GeneratedAt time.Time `json:"generated_at"`
	WindowDays  int       `json:"window_days"`
	// UptimeRatio is the share of target minutes that were not down, 0 to 1.
	UptimeRatio *float64 `json:"uptime_ratio"`
	// P95Ms is the p95 of the edge requests in the last 24 h, without the pulse stream.
	P95Ms *float64 `json:"p95_ms"`
	// AnsweredChats is the number of completed AI answers of every org.
	AnsweredChats *int64 `json:"answered_chats"`
}

// Sources of the numbers. A nil func gives null.
type Sources struct {
	Uptime   func(ctx context.Context, days int) (float64, bool, error)
	P95      func(ctx context.Context, window time.Duration) (float64, bool, error)
	Answered func(ctx context.Context, days int) (int64, error)
	Now      func() time.Time
}

// Service caches the numbers. Safe for concurrent use.
type Service struct {
	src Sources
	log *slog.Logger

	refresh sync.Mutex // one refresh at a time

	mu      sync.Mutex // guards last, expires and have
	last    Stats
	expires time.Time
	have    bool
}

// New returns a service. logger nil: no log.
func New(src Sources, logger *slog.Logger) *Service {
	if src.Now == nil {
		src.Now = time.Now
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Service{src: src, log: logger}
}

// Stats returns the cached numbers, or reads them again when the cache is older than
// CacheFor. During a refresh, other callers get the last answer at once. Only callers
// before the first answer wait for it.
func (s *Service) Stats(ctx context.Context) Stats {
	if st, fresh := s.cached(); fresh {
		return st
	}
	if !s.refresh.TryLock() {
		if st, have := s.stale(); have {
			return st
		}
		s.refresh.Lock() // no answer yet: wait for the running refresh
	}
	defer s.refresh.Unlock()
	if st, fresh := s.cached(); fresh {
		return st
	}
	now := s.src.Now()
	// The refresh must not die with one caller's request.
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	out := Stats{GeneratedAt: now.UTC(), WindowDays: WindowDays}
	if s.src.Uptime != nil {
		if v, ok, err := s.src.Uptime(rctx, WindowDays); err != nil {
			s.failed(ctx, "uptime", err)
		} else if ok {
			r := math.Round(v*1e4) / 1e4
			out.UptimeRatio = &r
		}
	}
	if s.src.P95 != nil {
		if v, ok, err := s.src.P95(rctx, P95Window); err != nil {
			s.failed(ctx, "p95", err)
		} else if ok && !math.IsNaN(v) && !math.IsInf(v, 0) {
			ms := math.Round(v*10) / 10
			out.P95Ms = &ms
		}
	}
	if s.src.Answered != nil {
		if n, err := s.src.Answered(rctx, WindowDays); err != nil {
			s.failed(ctx, "answered_chats", err)
		} else {
			out.AnsweredChats = &n
		}
	}
	s.mu.Lock()
	s.last, s.expires, s.have = out, now.Add(CacheFor), true
	s.mu.Unlock()
	return out
}

func (s *Service) cached() (Stats, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last, s.have && s.src.Now().Before(s.expires)
}

func (s *Service) stale() (Stats, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last, s.have
}

func (s *Service) failed(ctx context.Context, part string, err error) {
	s.log.DebugContext(ctx, "public number not readable",
		slog.String("event.name", "pulse.public.part_failed"),
		slog.String("pulse.part", part),
		slog.String("exception.message", err.Error()))
}

// ServeHTTP serves GET /v1/public-stats. It needs no token.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	st := s.Stats(r.Context())
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=60")
	_ = json.NewEncoder(w).Encode(st)
}
