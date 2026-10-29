package metrics

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"
)

// Database is the connection use of the shared Postgres.
type Database struct {
	// Connections is the number of client connections (pg_stat_activity).
	Connections    int `json:"connections"`
	MaxConnections int `json:"max_connections"`
}

// ReadDatabase counts the client connections of every database in the cluster.
func ReadDatabase(ctx context.Context, db *pgxpool.Pool) (Database, error) {
	var d Database
	err := db.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend')::int,
		       current_setting('max_connections')::int`).Scan(&d.Connections, &d.MaxConnections)
	if err != nil {
		return Database{}, fmt.Errorf("metrics: database: %w", err)
	}
	return d, nil
}

// Stream is one JetStream stream and the messages its consumers have not finished.
type Stream struct {
	Stream   string `json:"stream"`
	Messages uint64 `json:"messages"`
	// Pending is the sum over the consumers: not yet delivered plus not yet acked.
	Pending uint64 `json:"pending"`
}

// Bus is the pending work of the event bus.
type Bus struct {
	PendingMessages uint64   `json:"pending_messages"`
	Streams         []Stream `json:"streams"`
}

// ReadBus lists every stream and the pending messages of its consumers.
func ReadBus(ctx context.Context, js jetstream.JetStream) (Bus, error) {
	out := Bus{Streams: []Stream{}}
	streams := js.ListStreams(ctx)
	for info := range streams.Info() {
		s := Stream{Stream: info.Config.Name, Messages: info.State.Msgs}
		st, err := js.Stream(ctx, info.Config.Name)
		if err != nil {
			return Bus{}, fmt.Errorf("metrics: bus: %w", err)
		}
		consumers := st.ListConsumers(ctx)
		for c := range consumers.Info() {
			s.Pending += c.NumPending + uint64(max(c.NumAckPending, 0)) //nolint:gosec // checked >= 0
		}
		if err := consumers.Err(); err != nil {
			return Bus{}, fmt.Errorf("metrics: bus: %w", err)
		}
		out.PendingMessages += s.Pending
		out.Streams = append(out.Streams, s)
	}
	if err := streams.Err(); err != nil {
		return Bus{}, fmt.Errorf("metrics: bus: %w", err)
	}
	slices.SortFunc(out.Streams, func(a, b Stream) int { return strings.Compare(a.Stream, b.Stream) })
	return out, nil
}
