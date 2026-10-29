package metrics_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/VelimirMueller/synthwerk-pulse/internal/metrics"
)

// jetStream starts an in-process NATS server with JetStream (same version as compose).
func jetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return js
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for range 200 {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

//nolint:contextcheck // the helpers take their context from t.
func TestCollectorWithBus(t *testing.T) {
	js := jetStream(t)
	ctx := t.Context()
	now := time.Now().UTC()

	c := metrics.New(metrics.Config{JS: js, Interval: time.Hour}, nil)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	t.Run("Run - no llm stream yet - AI null, bus empty", func(t *testing.T) {
		waitFor(t, "first refresh", func() bool { return !c.Platform().GeneratedAt.IsZero() })
		p := c.Platform()
		if p.AI != nil || p.Bus == nil || len(p.Bus.Streams) != 0 {
			t.Fatalf("platform %+v, want AI null and an empty bus", p)
		}
	})

	st, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: metrics.LLMStream, Subjects: []string{"evt.llm.>"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable: "slow", AckPolicy: jetstream.AckExplicitPolicy}); err != nil {
		t.Fatal(err)
	}
	for _, ev := range [][]byte{llmEvent(now, "org-a", 300, 200, 100, "completed"), llmEvent(now, "org-b", 10, 0, 5, "failed")} {
		if _, err := js.Publish(ctx, metrics.LLMSubject, ev); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("RunAI - stream created later - events read after the retry", func(t *testing.T) {
		// The retry waits 15 s; run a second reader with a short retry on a fresh tracker.
		tr := metrics.NewAITracker(5*time.Minute, nil)
		rctx, rcancel := context.WithCancel(ctx)
		defer rcancel()
		go metrics.RunAI(rctx, js, tr, slogDiscard(), 50*time.Millisecond)
		waitFor(t, "two answers", func() bool { s, ok := tr.Stats(""); return ok && s.Answers == 2 })
		a, _ := tr.Stats("org-a")
		if a.Answers != 1 || a.TokensPerMinute != 100 || tr.Window() != 5*time.Minute {
			t.Fatalf("org-a %+v, want 1 answer, 100 tokens/min", a)
		}
	})

	t.Run("Refresh - stream with a slow consumer - pending messages", func(t *testing.T) {
		c.Refresh(ctx)
		b := c.Platform().Bus
		if b == nil || len(b.Streams) != 1 || b.Streams[0].Messages != 2 || b.Streams[0].Pending != 2 || b.PendingMessages != 2 {
			t.Fatalf("bus %+v, want EVT_LLM with 2 messages, 2 pending", b)
		}
	})

	t.Run("RunAI - stream deleted - AI unavailable", func(t *testing.T) {
		tr := metrics.NewAITracker(0, nil)
		rctx, rcancel := context.WithCancel(ctx)
		defer rcancel()
		go metrics.RunAI(rctx, js, tr, slogDiscard(), 50*time.Millisecond)
		waitFor(t, "connect", func() bool { _, ok := tr.Stats(""); return ok })
		if err := js.DeleteStream(ctx, metrics.LLMStream); err != nil {
			t.Fatal(err)
		}
		waitFor(t, "disconnect", func() bool { _, ok := tr.Stats(""); return !ok })
	})
}

func slogDiscard() *slog.Logger { return slog.New(slog.DiscardHandler) }
