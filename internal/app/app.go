// Package app wires pulse: health probes, the live status, metrics and alerts endpoints
// behind the token check, the monitor, the metrics collector, the alert engine, the AI
// usage store, the store and the outbox relay. It starts from the example service of the
// synthwerk-contracts Go kit (go/examples/service).
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/VelimirMueller/synthwerk-contracts/go/kit/auth"
	"github.com/VelimirMueller/synthwerk-contracts/go/kit/cloudevents"
	"github.com/VelimirMueller/synthwerk-contracts/go/kit/health"
	kitlog "github.com/VelimirMueller/synthwerk-contracts/go/kit/log"
	"github.com/VelimirMueller/synthwerk-contracts/go/kit/outbox"

	"github.com/VelimirMueller/synthwerk-pulse/internal/alert"
	"github.com/VelimirMueller/synthwerk-pulse/internal/metrics"
	"github.com/VelimirMueller/synthwerk-pulse/internal/monitor"
	"github.com/VelimirMueller/synthwerk-pulse/internal/public"
	"github.com/VelimirMueller/synthwerk-pulse/internal/store"
	"github.com/VelimirMueller/synthwerk-pulse/internal/stream"
	"github.com/VelimirMueller/synthwerk-pulse/internal/target"
	"github.com/VelimirMueller/synthwerk-pulse/internal/uptime"
	"github.com/VelimirMueller/synthwerk-pulse/internal/usage"
)

// Service is the short service name: env prefix SYNTHWERK_PULSE_, source /synthwerk/pulse/<env>.
const Service = "pulse"

// Config of the app.
type Config struct {
	Version string
	Commit  string
	// Env is SYNTHWERK_PLATFORM_ENV.
	Env     string
	Targets []target.Target
	Stream  stream.Config
	// Monitor options (tests).
	MonitorOptions []monitor.Option
	// Verifier checks the Bearer token of every /v1 request. Required.
	Verifier *auth.Verifier
	// Metrics sources and timing. DB, JS and Cost are set by New.
	Metrics metrics.Config
	// Alerts: threshold, Tempo and trace link. Window, Interval and the sources are set by New.
	Alerts alert.Config
	// Prices of the AI cost. Zero value: usage.DefaultPrices.
	Prices usage.Prices
}

// App holds the dependencies of the service.
type App struct {
	DB      *pgxpool.Pool
	NC      *nats.Conn
	JS      jetstream.JetStream
	Log     *slog.Logger
	Health  *health.Handler
	Store   *store.Store
	Hub     *stream.Hub
	Monitor *monitor.Monitor
	Metrics *metrics.Collector
	Alerts  *alert.Engine
	Usage   *usage.Store
	Uptime  *uptime.Store
	Public  *public.Service
	Relay   *outbox.Relay
	handler http.Handler
}

// New wires the app. Call Migrate, then Health.MarkStarted, then run Monitor, Metrics,
// Alerts, RunUsage and Relay.
func New(db *pgxpool.Pool, nc *nats.Conn, logger *slog.Logger, cfg Config) (*App, error) {
	if cfg.Verifier == nil {
		return nil, errors.New("app: a token verifier is required")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("app: jetstream: %w", err)
	}
	source, err := cloudevents.Source(Service, cfg.Env)
	if err != nil {
		return nil, err
	}
	h, err := health.New(cfg.Version, cfg.Commit,
		health.Check{Name: "postgres", Critical: true, Run: db.Ping, Detail: "postgres unreachable"},
		health.Check{Name: "nats", Critical: true, Detail: "nats not connected", Run: func(context.Context) error {
			if !nc.IsConnected() {
				return errors.New("not connected")
			}
			return nil
		}},
		health.Check{Name: "jwks", Run: cfg.Verifier.Ready, Detail: "token keys unavailable"},
	)
	if err != nil {
		return nil, err
	}
	relay, err := outbox.NewRelay(db, js, logger, outbox.Config{})
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(cfg.Targets))
	for _, t := range cfg.Targets {
		names = append(names, t.Name)
	}
	st := store.New(db, source)
	hub := stream.New(names, time.Now(), cfg.Stream)
	mon := monitor.New(cfg.Targets, hub, st, logger, cfg.MonitorOptions...)
	prices := cfg.Prices
	if prices.Currency == "" {
		prices = usage.DefaultPrices()
	}
	use := usage.NewStore(db, prices, cfg.Metrics.Now)
	mcfg := cfg.Metrics
	mcfg.DB, mcfg.JS, mcfg.Cost = db, js, use
	col := metrics.New(mcfg, logger)
	acfg := cfg.Alerts
	acfg.Window, acfg.Interval = cfg.Metrics.Window, cfg.Metrics.Interval
	acfg.Targets = func() []stream.Target { return hub.Snapshot().Targets }
	acfg.Prom, acfg.AI = cfg.Metrics.Prom, col.AI()
	alerts := alert.New(acfg, logger)
	up := uptime.NewStore(db, cfg.Metrics.Now)
	src := public.Sources{Uptime: up.Ratio, Answered: use.AnsweredChats, Now: cfg.Metrics.Now}
	if prom := cfg.Metrics.Prom; prom != nil {
		src.P95 = func(ctx context.Context, w time.Duration) (float64, bool, error) {
			return prom.EdgeP95(ctx, w, Service)
		}
	}
	pub := public.New(src, logger)
	a := &App{DB: db, NC: nc, JS: js, Log: logger, Health: h, Store: st, Hub: hub, Monitor: mon,
		Metrics: col, Alerts: alerts, Usage: use, Uptime: up, Public: pub, Relay: relay}

	mux := http.NewServeMux()
	h.Register(mux)
	// Public numbers of the site (synthwerk#47): no token, cached, aggregates only. The more
	// specific pattern wins over "/v1/", so the token check does not apply.
	mux.Handle("GET /v1/public-stats", pub)
	mux.Handle("/v1/", API(cfg.Verifier, hub, col, alerts))
	// The log line sits inside the span. Probes and the long-lived stream create no span.
	a.handler = otelhttp.NewHandler(kitlog.HTTP(logger, mux), Service,
		otelhttp.WithFilter(func(r *http.Request) bool {
			switch r.URL.Path {
			case "/healthz", "/readyz", "/v1/health/stream":
				return false
			}
			return true
		}))
	return a, nil
}

// API returns the /v1 routes. Every route needs a token (docs/features/health-metrics.md).
//
//   - GET /v1/health and /v1/health/stream: operators and the owners and admins of any
//     org. The status of platform parts holds no org data.
//   - GET /v1/metrics: operators only. The numbers cover every org.
//   - GET /v1/orgs/{org}/metrics: the owners and admins of that org, or operators.
//   - GET /v1/public-stats (registered in New, outside this handler): no token.
//   - GET /v1/alerts: operators and the owners and admins of any org. Only operators get
//     the trace links (docs/features/alerts-and-cost.md).
func API(v *auth.Verifier, hub *stream.Hub, col *metrics.Collector, alerts *alert.Engine) http.Handler {
	viewer := auth.RequireRole(metrics.ViewerRoles...)
	api := http.NewServeMux()
	api.Handle("GET /v1/health", viewer(http.HandlerFunc(hub.ServeSnapshot)))
	api.Handle("GET /v1/alerts", viewer(http.HandlerFunc(alerts.ServeAlerts)))
	api.Handle("GET /v1/health/stream", viewer(http.HandlerFunc(hub.ServeStream)))
	api.Handle("GET /v1/metrics", auth.RequireRole(auth.RoleOperator)(http.HandlerFunc(col.ServePlatform)))
	api.HandleFunc("GET /v1/orgs/{org}/metrics", col.ServeOrg)
	return auth.Middleware(v, api)
}

// Handler is the HTTP handler of the service.
func (a *App) Handler() http.Handler { return a.handler }

// Migrate creates the tables of pulse, of the AI usage and of the outbox.
func (a *App) Migrate(ctx context.Context) error {
	if err := a.Store.Migrate(ctx); err != nil {
		return err
	}
	if err := a.Uptime.Migrate(ctx); err != nil {
		return err
	}
	return a.Usage.Migrate(ctx)
}

// RunUptime stores the uptime of every target once per minute until ctx ends.
func (a *App) RunUptime(ctx context.Context) error {
	return uptime.Run(ctx, a.Uptime, func() []stream.Target { return a.Hub.Snapshot().Targets }, time.Minute, a.Log)
}

// RunUsage counts the llm answers for the AI cost per day until ctx ends.
func (a *App) RunUsage(ctx context.Context) error {
	usage.Run(ctx, a.JS, a.Usage, a.Log, 15*time.Second)
	return nil
}

// EnsureStream creates the stream EVT_PULSE when it is missing. Only for local runs
// and tests: in a deployment the infra module owns the stream config.
func EnsureStream(ctx context.Context, js jetstream.JetStream) error {
	_, err := js.Stream(ctx, "EVT_PULSE")
	if err == nil {
		return nil
	}
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("app: stream: %w", err)
	}
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:       "EVT_PULSE",
		Subjects:   []string{"evt.pulse.>"},
		Duplicates: 2 * time.Minute, // >= relay restart time, see the kit package outbox
	})
	if err != nil && !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		return fmt.Errorf("app: create stream: %w", err)
	}
	return nil
}
