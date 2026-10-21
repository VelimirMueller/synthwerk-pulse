// Package app wires pulse: health probes, the live status endpoints, the monitor,
// the store and the outbox relay. It starts from the example service of the
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

	"github.com/VelimirMueller/synthwerk-contracts/go/kit/cloudevents"
	"github.com/VelimirMueller/synthwerk-contracts/go/kit/health"
	kitlog "github.com/VelimirMueller/synthwerk-contracts/go/kit/log"
	"github.com/VelimirMueller/synthwerk-contracts/go/kit/outbox"

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
	Relay   *outbox.Relay
	handler http.Handler
}

// New wires the app. Call Migrate, then Health.MarkStarted, then run Monitor and Relay.
func New(db *pgxpool.Pool, nc *nats.Conn, logger *slog.Logger, cfg Config) (*App, error) {
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
	a := &App{DB: db, NC: nc, JS: js, Log: logger, Health: h, Store: st, Hub: hub, Monitor: mon, Relay: relay}

	mux := http.NewServeMux()
	h.Register(mux)
	mux.HandleFunc("GET /v1/health", hub.ServeSnapshot)
	mux.HandleFunc("GET /v1/health/stream", hub.ServeStream)
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
