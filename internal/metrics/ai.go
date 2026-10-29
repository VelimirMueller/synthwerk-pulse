package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// LLM event the AI numbers come from (synthwerk-contracts events/llm/message.completed.v1).
const (
	LLMStream  = "EVT_LLM"
	LLMSubject = "evt.llm.message.completed.v1"
)

// maxSamples caps the memory of the AI window. Older samples go first.
const maxSamples = 50_000

// AI is the tokens and model latency of one scope (all orgs or one org) in the window.
type AI struct {
	// TokensPerMinute is input plus output tokens of every answer, per minute of the window.
	TokensPerMinute float64 `json:"tokens_per_minute"`
	// Answers is the number of answers in the window, in every end state.
	Answers int `json:"answers"`
	// Failed is the number of answers with status failed.
	Failed int `json:"failed"`
	// LatencyP95Ms is the p95 of latency_ms of completed answers. Null without one.
	LatencyP95Ms *int64 `json:"latency_p95_ms"`
}

type sample struct {
	at      time.Time
	org     string
	tokens  int64
	latency int64
	status  string
}

// AITracker keeps the llm answers of the last window in memory. Safe for concurrent use.
type AITracker struct {
	window time.Duration
	now    func() time.Time

	mu        sync.Mutex
	samples   []sample // ordered by arrival
	connected bool
}

// NewAITracker returns a tracker for window (default 5 min). now nil: time.Now.
func NewAITracker(window time.Duration, now func() time.Time) *AITracker {
	if window <= 0 {
		window = 5 * time.Minute
	}
	if now == nil {
		now = time.Now
	}
	return &AITracker{window: window, now: now}
}

// Window is the time span of the numbers.
func (t *AITracker) Window() time.Duration { return t.window }

// event is the part of the CloudEvent pulse reads.
type event struct {
	Time  time.Time `json:"time"`
	OrgID string    `json:"orgid"`
	Data  struct {
		InputTokens  int64  `json:"input_tokens"`
		OutputTokens int64  `json:"output_tokens"`
		LatencyMs    int64  `json:"latency_ms"`
		Status       string `json:"status"`
	} `json:"data"`
}

// Add reads one llm.message.completed.v1 CloudEvent. An event without org id or
// with a bad shape is dropped: it cannot be scoped to an org.
func (t *AITracker) Add(raw []byte) error {
	var e event
	if err := json.Unmarshal(raw, &e); err != nil {
		return err
	}
	if e.OrgID == "" || e.Time.IsZero() || e.Data.InputTokens < 0 || e.Data.OutputTokens < 0 || e.Data.LatencyMs < 0 {
		return errors.New("metrics: event without org id, time or valid numbers")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.samples = append(t.samples, sample{
		at: e.Time, org: e.OrgID, tokens: e.Data.InputTokens + e.Data.OutputTokens,
		latency: e.Data.LatencyMs, status: e.Data.Status,
	})
	t.trim()
	return nil
}

// trim drops samples past the window and keeps at most maxSamples. Caller holds mu.
func (t *AITracker) trim() {
	from := t.now().Add(-t.window)
	keep := t.samples[:0]
	for _, s := range t.samples {
		if !s.at.Before(from) {
			keep = append(keep, s)
		}
	}
	if over := len(keep) - maxSamples; over > 0 {
		keep = keep[over:]
	}
	clear(t.samples[len(keep):])
	t.samples = keep
}

func (t *AITracker) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	clear(t.samples)
	t.samples = t.samples[:0]
}

// SetConnected marks whether pulse reads the llm stream. RunAI calls it; tests may too.
func (t *AITracker) SetConnected(v bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.connected = v
}

// Stats returns the numbers of org, or of every org when org is empty. ok is false
// while pulse does not read the llm stream.
func (t *AITracker) Stats(org string) (AI, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.trim()
	if !t.connected {
		return AI{}, false
	}
	var out AI
	var tokens int64
	var latencies []int64
	for _, s := range t.samples {
		if org != "" && s.org != org {
			continue
		}
		out.Answers++
		tokens += s.tokens
		switch s.status {
		case "failed":
			out.Failed++
		case "completed":
			latencies = append(latencies, s.latency)
		}
	}
	out.TokensPerMinute = float64(tokens) / t.window.Minutes()
	if len(latencies) > 0 {
		p := percentile(latencies, 0.95)
		out.LatencyP95Ms = &p
	}
	return out, true
}

// percentile returns the nearest-rank percentile q (0 < q <= 1) of values.
func percentile(values []int64, q float64) int64 {
	s := slices.Clone(values)
	slices.Sort(s)
	rank := int(q*float64(len(s)) + 0.999999)
	rank = max(1, min(rank, len(s)))
	return s[rank-1]
}

// RunAI reads llm.message.completed.v1 from the stream EVT_LLM into t, starting one
// window back, until ctx ends. A missing stream is retried every retry: the llm
// service creates it. It never returns an error: the AI numbers are then unavailable.
func RunAI(ctx context.Context, js jetstream.JetStream, t *AITracker, logger *slog.Logger, retry time.Duration) {
	for {
		err := consume(ctx, js, t, logger)
		t.SetConnected(false)
		if ctx.Err() != nil {
			return
		}
		logger.WarnContext(ctx, "llm stream not readable, retrying",
			slog.String("event.name", "pulse.ai.unavailable"),
			slog.String("exception.message", err.Error()))
		select {
		case <-ctx.Done():
			return
		case <-time.After(retry):
		}
	}
}

func consume(ctx context.Context, js jetstream.JetStream, t *AITracker, logger *slog.Logger) error {
	start := t.now().Add(-t.window)
	c, err := js.OrderedConsumer(ctx, LLMStream, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{LLMSubject},
		DeliverPolicy:  jetstream.DeliverByStartTimePolicy,
		OptStartTime:   &start,
	})
	if err != nil {
		return err
	}
	// The new consumer replays the whole window: drop what an earlier consumer read, so
	// no answer counts twice. Inside one Consume the ordered consumer resumes after the
	// last sequence it delivered, without a replay.
	t.reset()
	failed := make(chan error, 1)
	cc, err := c.Consume(func(m jetstream.Msg) {
		if err := t.Add(m.Data()); err != nil {
			logger.DebugContext(ctx, "llm event dropped", slog.String("event.name", "pulse.ai.dropped"))
		}
	}, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		// The ordered consumer recreates itself on most errors. A deleted stream ends it.
		if errors.Is(err, jetstream.ErrStreamNotFound) || errors.Is(err, jetstream.ErrConsumerDeleted) {
			select {
			case failed <- err:
			default:
			}
		}
	}))
	if err != nil {
		return err
	}
	defer cc.Stop()
	t.SetConnected(true)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-failed:
		return err
	case <-cc.Closed():
		return errors.New("metrics: llm consumer closed")
	}
}
