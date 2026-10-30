// Package metrics collects the numbers of the health view (synthwerk#40,
// docs/features/health-metrics.md):
//
//   - per service: request rate, error share and p95, from the edge metrics in Prometheus
//   - host: CPU, memory and disk, from /proc and statfs
//   - database connections (pg_stat_activity) and pending bus messages (JetStream)
//   - tokens per minute and model latency, from llm.message.completed.v1 on EVT_LLM
//
// A Collector refreshes the platform numbers on one interval, so the load does not grow
// with the number of viewers. The AI numbers are live from the events and can be read
// per org. A part that cannot be read is null in the answer; the others stay.
package metrics

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"
)

// Platform is the body of GET /v1/metrics. Only operators read it.
type Platform struct {
	GeneratedAt   time.Time `json:"generated_at"`
	WindowSeconds int       `json:"window_seconds"`
	// Services is null without a Prometheus URL or when the query fails.
	Services []Service `json:"services"`
	Host     *Host     `json:"host"`
	Database *Database `json:"database"`
	Bus      *Bus      `json:"bus"`
	// AI covers every org. Null while pulse does not read the llm stream.
	AI *AI `json:"ai"`
}

// Org is the body of GET /v1/orgs/{org}/metrics: the numbers of one org only.
type Org struct {
	GeneratedAt   time.Time `json:"generated_at"`
	WindowSeconds int       `json:"window_seconds"`
	OrgID         string    `json:"org_id"`
	AI            *AI       `json:"ai"`
}

// Config of a Collector. Nil sources give null parts.
type Config struct {
	// Window of the rates, the p95 values and the AI numbers. Default 5 min.
	Window time.Duration
	// Interval between two refreshes of the platform numbers. Default 10 s.
	Interval time.Duration
	DB       *pgxpool.Pool
	JS       jetstream.JetStream
	Prom     *Prometheus
	Host     *HostSampler
	AI       *AITracker
	Now      func() time.Time
}

// Collector is safe for concurrent use. Create it with New, then call Run once.
type Collector struct {
	cfg Config
	log *slog.Logger

	refresh sync.Mutex // one Refresh at a time

	mu   sync.Mutex
	last Platform
}

// New returns a collector. logger nil: no log. The first refresh happens when Run starts.
func New(cfg Config, logger *slog.Logger) *Collector {
	if cfg.Window <= 0 {
		cfg.Window = 5 * time.Minute
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.AI == nil {
		cfg.AI = NewAITracker(cfg.Window, cfg.Now)
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Collector{cfg: cfg, log: logger, last: Platform{WindowSeconds: int(cfg.Window.Seconds())}}
}

// AI is the tracker the llm consumer writes to.
func (c *Collector) AI() *AITracker { return c.cfg.AI }

// Run refreshes the platform numbers on every interval until ctx ends. It also
// reads the llm stream when a JetStream is set.
func (c *Collector) Run(ctx context.Context) error {
	if c.cfg.JS != nil {
		go RunAI(ctx, c.cfg.JS, c.cfg.AI, c.log, 15*time.Second)
	}
	c.Refresh(ctx)
	tick := time.NewTicker(c.cfg.Interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			c.Refresh(ctx)
		}
	}
}

// Refresh reads every platform source once. Each source has its own timeout and
// writes only its own field of next.
func (c *Collector) Refresh(ctx context.Context) {
	c.refresh.Lock()
	defer c.refresh.Unlock()
	timeout := min(c.cfg.Interval, 5*time.Second)
	next := Platform{WindowSeconds: int(c.cfg.Window.Seconds())}
	var wg sync.WaitGroup
	run := func(part string, f func(context.Context) error) {
		wg.Go(func() {
			cctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			if err := f(cctx); err != nil && ctx.Err() == nil {
				c.log.DebugContext(ctx, "metrics part not readable",
					slog.String("event.name", "pulse.metrics.part_failed"),
					slog.String("pulse.part", part),
					slog.String("exception.message", err.Error()))
			}
		})
	}
	if c.cfg.Prom != nil {
		run("services", func(ctx context.Context) error {
			s, err := c.cfg.Prom.Services(ctx, c.cfg.Window)
			next.Services = s
			return err
		})
	}
	if c.cfg.Host != nil {
		run("host", func(context.Context) error {
			h, err := c.cfg.Host.Sample()
			if err == nil {
				next.Host = &h
			}
			return err
		})
	}
	if c.cfg.DB != nil {
		run("database", func(ctx context.Context) error {
			d, err := ReadDatabase(ctx, c.cfg.DB)
			if err == nil {
				next.Database = &d
			}
			return err
		})
	}
	if c.cfg.JS != nil {
		run("bus", func(ctx context.Context) error {
			b, err := ReadBus(ctx, c.cfg.JS)
			if err == nil {
				next.Bus = &b
			}
			return err
		})
	}
	wg.Wait()
	next.GeneratedAt = c.cfg.Now().UTC()
	c.mu.Lock()
	c.last = next
	c.mu.Unlock()
}

// Platform returns the last platform numbers with the live AI numbers of every org.
func (c *Collector) Platform() Platform {
	c.mu.Lock()
	p := c.last
	c.mu.Unlock()
	if ai, ok := c.cfg.AI.Stats(""); ok {
		p.AI = &ai
	}
	return p
}

// Org returns the numbers of one org. It never reads data of another org.
func (c *Collector) Org(orgID string) Org {
	o := Org{GeneratedAt: c.cfg.Now().UTC(), WindowSeconds: int(c.cfg.Window.Seconds()), OrgID: orgID}
	if ai, ok := c.cfg.AI.Stats(orgID); ok {
		o.AI = &ai
	}
	return o
}
