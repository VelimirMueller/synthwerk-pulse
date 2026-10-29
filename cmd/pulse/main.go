// Command pulse checks the health of every Synthwerk service and infra part and
// streams the status over Server-Sent Events. See docs/features/live-status.md.
//
//	pulse              run the service
//	pulse healthcheck  exit 0 when /readyz answers 200 (container health check)
//
// Environment:
//
//	SYNTHWERK_PLATFORM_ENV               local (default), ci, dev, stg, prd
//	SYNTHWERK_PULSE_HTTP_ADDR            listen address, default :8080
//	SYNTHWERK_PULSE_DATABASE_URL         Postgres URL (required, secret)
//	SYNTHWERK_PULSE_NATS_URL             NATS URL, default nats://127.0.0.1:4222 (may hold a password)
//	SYNTHWERK_PULSE_TARGETS_FILE         targets.yaml. Unset = no targets.
//	SYNTHWERK_PULSE_DEFAULT_INTERVAL     check interval of a target that sets none, default 15s
//	SYNTHWERK_PULSE_DEFAULT_TIMEOUT      check timeout of a target that sets none, default 3s
//	SYNTHWERK_PULSE_SSE_MAX_CLIENTS      max open streams, default 50
//	SYNTHWERK_PULSE_OIDC_ISSUER          token issuer (required), for example http://auth.localhost:8080
//	SYNTHWERK_PULSE_OIDC_AUDIENCE        accepted audiences, comma separated, default synthwerk-api
//	SYNTHWERK_PULSE_JWKS_URL             key set URL; empty = OIDC discovery on the issuer
//	SYNTHWERK_PULSE_PLATFORM_ORG_ID      Zitadel org that grants operator. Empty = nobody is operator.
//	SYNTHWERK_PULSE_PROMETHEUS_URL       Prometheus with the edge metrics, for example http://telemetry:9090. Empty = no service numbers.
//	SYNTHWERK_PULSE_PROC_DIR             proc dir for host CPU and memory, default /proc
//	SYNTHWERK_PULSE_DISK_PATH            path whose file system is the host disk, default /
//	SYNTHWERK_PULSE_METRICS_WINDOW       window of rates, p95 and AI numbers, default 5m
//	SYNTHWERK_PULSE_METRICS_INTERVAL     refresh of the platform numbers, default 10s
//	SYNTHWERK_PULSE_LOG_LEVEL            DEBUG, INFO (default), WARN, ERROR
//	SYNTHWERK_PULSE_LOG_FORMAT           json (default) or console (local only)
//	OTEL_EXPORTER_OTLP_ENDPOINT          collector (gRPC), for example http://telemetry:4317. Unset = no export.
//
// Build with the release data:
//
//	go build -ldflags "-X main.version=0.1.0 -X main.commit=$(git rev-parse HEAD)" ./cmd/pulse
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"

	"github.com/VelimirMueller/synthwerk-contracts/go/kit/auth"
	"github.com/VelimirMueller/synthwerk-contracts/go/kit/env"
	kitlog "github.com/VelimirMueller/synthwerk-contracts/go/kit/log"
	kitotel "github.com/VelimirMueller/synthwerk-contracts/go/kit/otel"

	"github.com/VelimirMueller/synthwerk-pulse/internal/app"
	"github.com/VelimirMueller/synthwerk-pulse/internal/metrics"
	"github.com/VelimirMueller/synthwerk-pulse/internal/stream"
	"github.com/VelimirMueller/synthwerk-pulse/internal/target"
)

// Set by -ldflags at build time.
var (
	version = "0.1.0-dev"
	commit  = "0000000"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		if err := healthcheck(env.Get(app.Service, "HTTP_ADDR", ":8080")); err != nil {
			fmt.Fprintln(os.Stderr, "healthcheck:", err)
			os.Exit(1)
		}
		return
	}
	if err := start(); err != nil {
		slog.Error("service stopped", slog.String("event.name", "service.failed"), slog.String("exception.message", err.Error()))
		os.Exit(1)
	}
}

func start() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx)
}

func run(ctx context.Context) error {
	// 1. Logger without the OTel bridge, for the setup lines.
	logOpts, err := kitlog.OptionsFromEnv(app.Service, version)
	if err != nil {
		return err
	}
	logger, err := kitlog.New(logOpts)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)

	// 2. Telemetry, then the final logger that also writes to OTLP.
	tel, err := kitotel.Setup(ctx, kitotel.Config{ServiceName: logOpts.ServiceName, Version: version, Env: logOpts.Env}, logger)
	if err != nil {
		return err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tel.Shutdown(sctx)
	}()
	logOpts.LoggerProvider = tel.LoggerProvider
	if logger, err = kitlog.New(logOpts); err != nil {
		return err
	}
	slog.SetDefault(logger)

	// 3. Config.
	cfg, err := config(logOpts.Env)
	if err != nil {
		return err
	}
	if len(cfg.Targets) == 0 {
		logger.WarnContext(ctx, "no targets to check",
			slog.String("event.name", "pulse.targets.empty"),
			slog.String("synthwerk.reason", env.Key(app.Service, "TARGETS_FILE")+" is not set or lists no target"))
	}

	// 4. Dependencies.
	dbURL := env.Get(app.Service, "DATABASE_URL", "")
	if dbURL == "" {
		return errors.New(env.Key(app.Service, "DATABASE_URL") + " is not set")
	}
	db, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return errors.New("database url does not parse") // the error text may hold the password
	}
	defer db.Close()
	nc, err := nats.Connect(env.Get(app.Service, "NATS_URL", nats.DefaultURL),
		nats.Name("synthwerk-"+app.Service), nats.MaxReconnects(-1))
	if err != nil {
		return fmt.Errorf("nats: %w", err)
	}
	defer nc.Close()

	a, err := app.New(db, nc, logger, cfg)
	if err != nil {
		return err
	}
	if err := a.Migrate(ctx); err != nil {
		return err
	}
	if logOpts.Env == "local" || logOpts.Env == "ci" {
		if err := app.EnsureStream(ctx, a.JS); err != nil {
			return err
		}
	}

	// 5. HTTP server, monitor and relay. /readyz answers 503 until here.
	// No WriteTimeout: the stream stays open up to one hour.
	srv := &http.Server{
		Addr:              env.Get(app.Service, "HTTP_ADDR", ":8080"),
		Handler:           a.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	srv.RegisterOnShutdown(a.Hub.Close)
	a.Health.MarkStarted()
	logger.InfoContext(ctx, "service started",
		slog.String("event.name", "service.started"),
		slog.String("server.address", srv.Addr),
		slog.Int("pulse.targets", len(cfg.Targets)))

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return a.Relay.Run(gctx) })
	g.Go(func() error { return a.Monitor.Run(gctx) })
	g.Go(func() error { return a.Metrics.Run(gctx) })
	g.Go(func() error {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		sctx, cancel := context.WithTimeout(context.WithoutCancel(gctx), 10*time.Second)
		defer cancel()
		return srv.Shutdown(sctx)
	})
	return g.Wait()
}

func config(platform string) (app.Config, error) {
	d := target.DefaultDefaults
	var err error
	if d.Interval, err = duration("DEFAULT_INTERVAL", d.Interval); err != nil {
		return app.Config{}, err
	}
	if d.Timeout, err = duration("DEFAULT_TIMEOUT", d.Timeout); err != nil {
		return app.Config{}, err
	}
	var targets []target.Target
	if path := env.Get(app.Service, "TARGETS_FILE", ""); path != "" {
		if targets, err = target.Load(path, d); err != nil {
			return app.Config{}, err
		}
	}
	maxClients, err := strconv.Atoi(env.Get(app.Service, "SSE_MAX_CLIENTS", "50"))
	if err != nil || maxClients < 1 || maxClients > 10000 {
		return app.Config{}, errors.New(env.Key(app.Service, "SSE_MAX_CLIENTS") + " must be 1 to 10000")
	}
	verifier, err := verifierConfig()
	if err != nil {
		return app.Config{}, err
	}
	mcfg, err := metricsConfig()
	if err != nil {
		return app.Config{}, err
	}
	return app.Config{
		Version:  version,
		Commit:   commit,
		Env:      platform,
		Targets:  targets,
		Stream:   stream.Config{MaxClients: maxClients},
		Verifier: verifier,
		Metrics:  mcfg,
	}, nil
}

func verifierConfig() (*auth.Verifier, error) {
	issuer := env.Get(app.Service, "OIDC_ISSUER", "")
	if issuer == "" {
		return nil, errors.New(env.Key(app.Service, "OIDC_ISSUER") + " is required")
	}
	var audiences []string
	for a := range strings.SplitSeq(env.Get(app.Service, "OIDC_AUDIENCE", "synthwerk-api"), ",") {
		if a = strings.TrimSpace(a); a != "" {
			audiences = append(audiences, a)
		}
	}
	return auth.NewVerifier(auth.Config{
		Issuer:    issuer,
		Audiences: audiences,
		JWKSURL:   env.Get(app.Service, "JWKS_URL", ""),
		Mapper:    auth.ZitadelMapper{PlatformOrgID: env.Get(app.Service, "PLATFORM_ORG_ID", "")},
	})
}

func metricsConfig() (metrics.Config, error) {
	var c metrics.Config
	var err error
	if c.Window, err = duration("METRICS_WINDOW", 5*time.Minute); err != nil {
		return c, err
	}
	if c.Window < time.Minute || c.Window > time.Hour {
		return c, errors.New(env.Key(app.Service, "METRICS_WINDOW") + " must be 1m to 1h")
	}
	if c.Interval, err = duration("METRICS_INTERVAL", 10*time.Second); err != nil {
		return c, err
	}
	if c.Interval < time.Second || c.Interval > 5*time.Minute {
		return c, errors.New(env.Key(app.Service, "METRICS_INTERVAL") + " must be 1s to 5m")
	}
	if u := env.Get(app.Service, "PROMETHEUS_URL", ""); u != "" {
		if c.Prom, err = metrics.NewPrometheus(u, nil); err != nil {
			return c, err
		}
	}
	c.Host = metrics.NewHostSampler(env.Get(app.Service, "PROC_DIR", "/proc"), env.Get(app.Service, "DISK_PATH", "/"))
	return c, nil
}

func duration(name string, def time.Duration) (time.Duration, error) {
	raw := env.Get(app.Service, name, "")
	if raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s=%q is not a positive duration such as 15s", env.Key(app.Service, name), raw)
	}
	return d, nil
}

func healthcheck(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("address %q: %w", addr, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	url := "http://" + net.JoinHostPort("127.0.0.1", port) + "/readyz"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("readyz: status %d", res.StatusCode)
	}
	return nil
}
