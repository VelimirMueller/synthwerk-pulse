// Package app wires pulse: health probes, the live status and metrics endpoints behind
// the token check, the monitor, the metrics collector, the store and the outbox relay. It starts from the example service of the
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

	"github.com/VelimirMueller/synthwerk-pulse/internal/metrics"
	"github.com/VelimirMueller/synthwerk-pulse/internal/monitor"
	"github.com/VelimirMueller/synthwerk-pulse/internal/store"
	"github.com/VelimirMueller/synthwerk-pulse/internal/stream"
	"github.com/VelimirMueller/synthwerk-pulse/internal/target"
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
	// Metrics sources and timing. DB and JS are set by New.
	Metrics metrics.Config
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
	Relay   *outbox.Relay
	handler http.Handler
}

// New wires the app. Call Migrate, then Health.MarkStarted, then run Monitor, Metrics and Relay.
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
	mcfg := cfg.Metrics
	mcfg.DB, mcfg.JS = db, js
	col := metrics.New(mcfg, logger)
	a := &App{DB: db, NC: nc, JS: js, Log: logger, Health: h, Store: st, Hub: hub, Monitor: mon, Metrics: col, Relay: relay}

	mux := http.NewServeMux()
	h.Register(mux)
	mux.Handle("/v1/", API(cfg.Verifier, hub, col))
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
func API(v *auth.Verifier, hub *stream.Hub, col *metrics.Collector) http.Handler {
	viewer := auth.RequireRole(metrics.ViewerRoles...)
	api := http.NewServeMux()
	api.Handle("GET /v1/health", viewer(http.HandlerFunc(hub.ServeSnapshot)))
	api.Handle("GET /v1/health/stream", viewer(http.HandlerFunc(hub.ServeStream)))
	api.Handle("GET /v1/metrics", auth.RequireRole(auth.RoleOperator)(http.HandlerFunc(col.ServePlatform)))
	api.HandleFunc("GET /v1/orgs/{org}/metrics", col.ServeOrg)
	return auth.Middleware(v, api)
}

// Handler is the HTTP handler of the service.
func (a *App) Handler() http.Handler { return a.handler }

// Migrate creates the tables of pulse and of the outbox.
func (a *App) Migrate(ctx context.Context) error { return a.Store.Migrate(ctx) }

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
