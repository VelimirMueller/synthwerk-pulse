// AI usage per day: parsing, cost per day and the durable consumer (synthwerk#57).
// The Postgres part is in tests/integration.
package usage_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/VelimirMueller/synthwerk-pulse/internal/metrics"
	"github.com/VelimirMueller/synthwerk-pulse/internal/usage"
)

var t0 = time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)

func answer(id string, at time.Time, org, provider, model string, in, out int) []byte {
	return fmt.Appendf(nil, `{"specversion":"1.0","id":"e-%s","source":"/synthwerk/llm/ci","type":"dev.synthwerk.llm.message.completed.v1",`+
		`"time":%q,"orgid":%q,"data":{"conversation_id":"c","message_id":%q,"model":%q,"provider":%q,`+
		`"input_tokens":%d,"output_tokens":%d,"latency_ms":10,"status":"completed"}}`,
		id, at.Format(time.RFC3339Nano), org, id, model, provider, in, out)
}

func TestParseAnswer(t *testing.T) {
	t.Run("valid event - day in UTC", func(t *testing.T) {
		at := time.Date(2026, 11, 16, 0, 30, 0, 0, time.FixedZone("CET", 3600)) // 2026-11-15 23:30 UTC
		a, err := usage.ParseAnswer(answer("m-1", at, "org-a", "anthropic", "default", 10, 20))
		if err != nil {
			t.Fatal(err)
		}
		if a.MessageID != "m-1" || a.Day.Format(time.DateOnly) != "2026-11-15" || a.OrgID != "org-a" ||
			a.Provider != "anthropic" || a.Model != "default" || a.InputTokens != 10 || a.OutputTokens != 20 || !a.Completed {
			t.Fatalf("got %+v", a)
		}
	})

	for name, raw := range map[string][]byte{
		"not json":        []byte("{"),
		"no org":          answer("m-1", t0, "", "fake", "default", 1, 1),
		"no message id":   answer("", t0, "org-a", "fake", "default", 1, 1),
		"wildcard model":  answer("m-1", t0, "org-a", "fake", "*", 1, 1),
		"bad provider":    answer("m-1", t0, "org-a", "Fake!", "default", 1, 1),
		"negative tokens": answer("m-1", t0, "org-a", "fake", "default", -1, 1),
		"too many tokens": answer("m-1", t0, "org-a", "fake", "default", 10_000_001, 1),
		"no time":         []byte(`{"orgid":"o","data":{"message_id":"m","model":"default","provider":"fake"}}`),
	} {
		t.Run(name+" - ErrBadEvent", func(t *testing.T) {
			if _, err := usage.ParseAnswer(raw); !errors.Is(err, usage.ErrBadEvent) {
				t.Fatalf("err %v", err)
			}
		})
	}
}

func TestBuildCost(t *testing.T) {
	prices, err := usage.LoadPrices(write(t, `
currency: USD
prices:
  - {provider: anthropic, model: default, input_per_million: 3, output_per_million: 15}
  - {provider: ollama, model: "*", input_per_million: 0, output_per_million: 0}
`))
	if err != nil {
		t.Fatal(err)
	}
	day := func(n int) time.Time { return t0.Truncate(24*time.Hour).AddDate(0, 0, -n) }
	got := usage.BuildCost(prices, t0, []usage.Usage{
		{Day: day(0), Provider: "anthropic", Model: "default", Answers: 2, InputTokens: 100_000, OutputTokens: 20_000},
		{Day: day(0), Provider: "ollama", Model: "default", Answers: 5, InputTokens: 9_000, OutputTokens: 9_000},
		{Day: day(0), Provider: "openai_compat", Model: "default", Answers: 1, InputTokens: 50, OutputTokens: 50},
		{Day: day(6), Provider: "anthropic", Model: "default", Answers: 1, InputTokens: 1, OutputTokens: 0},
		{Day: day(7), Provider: "anthropic", Model: "default", Answers: 9, InputTokens: 9e6, OutputTokens: 9e6}, // outside
	})

	t.Run("seven UTC days - oldest first, today last, zero days kept", func(t *testing.T) {
		if got.Currency != "USD" || len(got.Days) != metrics.CostDays || got.Days[0].Date != "2026-11-10" || got.Days[6].Date != "2026-11-16" {
			t.Fatalf("got %+v", got)
		}
		if got.Days[3] != (metrics.CostDay{Date: "2026-11-13"}) {
			t.Fatalf("day 3 %+v, want zeros", got.Days[3])
		}
	})

	t.Run("today - priced cost, local answers free, unpriced counted", func(t *testing.T) {
		d := got.Days[6]
		if d.Cost != 0.6 || d.Answers != 8 || d.InputTokens != 109_050 || d.OutputTokens != 29_050 || d.UnpricedAnswers != 1 {
			t.Fatalf("today %+v, want cost 0.3 + 0.3, 8 answers, 1 unpriced", d)
		}
	})

	t.Run("oldest day - cost rounded to 6 decimals", func(t *testing.T) {
		if d := got.Days[0]; d.Cost != 0.000003 || d.Answers != 1 {
			t.Fatalf("oldest %+v", d)
		}
	})
}

// recorder is a fake usage store: it fails the first failN records of a message id.
type recorder struct {
	mu     sync.Mutex
	failN  map[string]int
	seen   map[string]int
	pruned int
}

func (r *recorder) Record(_ context.Context, raw []byte) (bool, error) {
	a, err := usage.ParseAnswer(raw)
	if err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failN[a.MessageID] > 0 {
		r.failN[a.MessageID]--
		return false, errors.New("database down")
	}
	r.seen[a.MessageID]++
	return r.seen[a.MessageID] == 1, nil
}

func (r *recorder) Prune(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruned++
	return nil
}

func (r *recorder) count(id string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen[id]
}

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
	for range 400 {
		if ok() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

//nolint:contextcheck // the helpers take their context from t.
func TestConsumer(t *testing.T) {
	js := jetStream(t)
	ctx, cancel := context.WithCancel(t.Context())
	rec := &recorder{failN: map[string]int{"m-retry": 1}, seen: map[string]int{}}
	done := make(chan struct{})
	go func() {
		usage.Run(ctx, js, rec, slog.New(slog.DiscardHandler), 50*time.Millisecond)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	t.Run("stream missing at start - retried, prune runs", func(t *testing.T) {
		time.Sleep(120 * time.Millisecond)
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: metrics.LLMStream, Subjects: []string{"evt.llm.>"}}); err != nil {
			t.Fatal(err)
		}
		waitFor(t, "the durable consumer", func() bool {
			_, err := js.Consumer(ctx, metrics.LLMStream, usage.Consumer)
			return err == nil
		})
		waitFor(t, "prune", func() bool {
			rec.mu.Lock()
			defer rec.mu.Unlock()
			return rec.pruned > 0
		})
	})

	for _, raw := range [][]byte{
		answer("m-1", t0, "org-a", "fake", "default", 1, 1),
		answer("m-retry", t0, "org-a", "fake", "default", 1, 1),
		[]byte(`{"not":"an answer"}`),
	} {
		if _, err := js.Publish(ctx, metrics.LLMSubject, raw); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("answers - each counted once, a failed record is delivered again", func(t *testing.T) {
		waitFor(t, "both answers", func() bool { return rec.count("m-1") == 1 && rec.count("m-retry") == 1 })
	})

	t.Run("all acked - nothing pending, bad event terminated", func(t *testing.T) {
		waitFor(t, "no pending messages", func() bool {
			c, err := js.Consumer(ctx, metrics.LLMStream, usage.Consumer)
			if err != nil {
				return false
			}
			info, err := c.Info(ctx)
			return err == nil && info.NumPending == 0 && info.NumAckPending == 0 && info.AckFloor.Stream == 3
		})
	})

	t.Run("stream deleted - consumer recreated after the stream is back", func(t *testing.T) {
		if err := js.DeleteStream(ctx, metrics.LLMStream); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: metrics.LLMStream, Subjects: []string{"evt.llm.>"}}); err != nil {
			t.Fatal(err)
		}
		if _, err := js.Publish(ctx, metrics.LLMSubject, answer("m-2", t0, "org-a", "fake", "default", 1, 1)); err != nil {
			t.Fatal(err)
		}
		waitFor(t, "the answer on the new stream", func() bool { return rec.count("m-2") == 1 })
	})
}
