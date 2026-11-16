package usage

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/VelimirMueller/synthwerk-pulse/internal/metrics"
)

// Consumer is the name of the durable consumer on EVT_LLM.
const Consumer = "pulse-ai-usage"

// Recorder counts the answers. *Store is the real one.
type Recorder interface {
	Record(ctx context.Context, raw []byte) (bool, error)
	Prune(ctx context.Context) error
}

// Run counts every llm.message.completed.v1 on EVT_LLM into s with the durable consumer
// Consumer, until ctx ends. A missing stream is retried every retry: the llm service
// creates it. It prunes old rows once an hour. It never returns an error.
func Run(ctx context.Context, js jetstream.JetStream, s Recorder, logger *slog.Logger, retry time.Duration) {
	go prune(ctx, s, logger)
	for {
		err := consume(ctx, js, s, logger)
		if ctx.Err() != nil {
			return
		}
		logger.WarnContext(ctx, "llm stream not readable for the AI cost, retrying",
			slog.String("event.name", "pulse.usage.unavailable"),
			slog.String("exception.message", err.Error()))
		select {
		case <-ctx.Done():
			return
		case <-time.After(retry):
		}
	}
}

func prune(ctx context.Context, s Recorder, logger *slog.Logger) {
	tick := time.NewTicker(time.Hour)
	defer tick.Stop()
	for {
		if err := s.Prune(ctx); err != nil && ctx.Err() == nil {
			logger.WarnContext(ctx, "usage rows not pruned",
				slog.String("event.name", "pulse.usage.prune_failed"),
				slog.String("exception.message", err.Error()))
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func consume(ctx context.Context, js jetstream.JetStream, s Recorder, logger *slog.Logger) error {
	c, err := js.CreateOrUpdateConsumer(ctx, metrics.LLMStream, jetstream.ConsumerConfig{
		Durable:       Consumer,
		Description:   "pulse: AI tokens and cost per day (synthwerk#57)",
		FilterSubject: metrics.LLMSubject,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxAckPending: 256,
	})
	if err != nil {
		return err
	}
	failed := make(chan error, 1)
	cc, err := c.Consume(func(m jetstream.Msg) {
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := s.Record(rctx, m.Data())
		cancel()
		switch {
		case errors.Is(err, ErrBadEvent):
			logger.DebugContext(ctx, "llm event not counted", slog.String("event.name", "pulse.usage.dropped"))
			_ = m.Term()
		case err != nil:
			if ctx.Err() == nil {
				logger.WarnContext(ctx, "llm event not counted, retrying",
					slog.String("event.name", "pulse.usage.record_failed"),
					slog.String("exception.message", err.Error()))
			}
			_ = m.NakWithDelay(5 * time.Second)
		default:
			_ = m.Ack()
		}
	}, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		// The consumer recovers from most errors by itself (missed heartbeats, reconnects).
		logger.DebugContext(ctx, "llm consumer error", slog.String("event.name", "pulse.usage.consume_error"),
			slog.String("exception.message", err.Error()))
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
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-failed:
		return err
	case <-cc.Closed():
		return errors.New("usage: llm consumer closed")
	}
}
