// Package containers starts the real dependencies of the integration tests:
// Postgres and NATS JetStream in Testcontainers (D-15, plan 05-testing-strategy.md).
//
// Copied from synthwerk-contracts tests/support/containers. The images are the ones
// of the synthwerk-infra compose stack, pinned by digest.
// OrbStack: if Testcontainers cannot find Docker, set
// TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock.
package containers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	natsgo "github.com/nats-io/nats.go"
	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Images, the same as synthwerk-infra compose.
const (
	PostgresImage = "postgres:18.6-alpine3.24@sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873"
	NATSImage     = "nats:2.15.1-alpine@sha256:2ae57dfba3063d5e25fecb75256118c0882a1a2d2cf7d0fba17a7c701df956f6"
)

// Env holds one running Postgres and one running NATS for a test package.
type Env struct {
	pg      *postgres.PostgresContainer
	nats    *tcnats.NATSContainer
	pgURL   string
	NATSURL string
}

// Start runs both containers. Call Stop when the package tests end (TestMain).
func Start(ctx context.Context) (*Env, error) {
	pg, err := postgres.Run(ctx, PostgresImage, postgres.BasicWaitStrategies())
	if err != nil {
		_ = testcontainers.TerminateContainer(pg)
		return nil, err
	}
	pgURL, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = testcontainers.TerminateContainer(pg)
		return nil, err
	}
	nc, err := tcnats.Run(ctx, NATSImage)
	if err != nil {
		_ = testcontainers.TerminateContainer(pg)
		_ = testcontainers.TerminateContainer(nc)
		return nil, err
	}
	natsURL, err := nc.ConnectionString(ctx)
	if err != nil {
		_ = testcontainers.TerminateContainer(pg)
		_ = testcontainers.TerminateContainer(nc)
		return nil, err
	}
	return &Env{pg: pg, nats: nc, pgURL: pgURL, NATSURL: natsURL}, nil
}

// Stop removes both containers.
func (e *Env) Stop() {
	_ = testcontainers.TerminateContainer(e.pg)
	_ = testcontainers.TerminateContainer(e.nats)
}

// Database creates an empty database for one test and returns a pool to it.
func (e *Env) Database(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := t.Context()
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "t_" + hex.EncodeToString(b)

	admin, err := pgx.Connect(ctx, e.pgURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close(ctx) }()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(e.pgURL)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	pool, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// NATS connects to the NATS container. The connection closes when the test ends.
func (e *Env) NATS(t *testing.T) *natsgo.Conn {
	t.Helper()
	nc, err := natsgo.Connect(e.NATSURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}
