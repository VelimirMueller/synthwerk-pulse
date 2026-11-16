// Public numbers of the site (synthwerk#47).
package public_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VelimirMueller/synthwerk-pulse/internal/public"
)

var t0 = time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)

func TestStats(t *testing.T) {
	now := t0
	var calls atomic.Int32
	src := public.Sources{
		Uptime: func(_ context.Context, days int) (float64, bool, error) {
			calls.Add(1)
			if days != 30 {
				t.Errorf("uptime days %d", days)
			}
			return 0.999349, true, nil
		},
		P95: func(_ context.Context, w time.Duration) (float64, bool, error) {
			if w != 24*time.Hour {
				t.Errorf("p95 window %v", w)
			}
			return 412.54, true, nil
		},
		Answered: func(context.Context, int) (int64, error) { return 1234, nil },
		Now:      func() time.Time { return now },
	}
	s := public.New(src, nil)

	t.Run("every source - rounded numbers, 30 days", func(t *testing.T) {
		st := s.Stats(t.Context())
		if st.WindowDays != 30 || *st.UptimeRatio != 0.9993 || *st.P95Ms != 412.5 || *st.AnsweredChats != 1234 || !st.GeneratedAt.Equal(t0) {
			t.Fatalf("stats %+v", st)
		}
	})

	t.Run("many callers inside one minute - sources read once", func(t *testing.T) {
		var wg sync.WaitGroup
		for range 50 {
			wg.Go(func() { s.Stats(t.Context()) })
		}
		wg.Wait()
		if calls.Load() != 1 {
			t.Fatalf("uptime read %d times, want 1", calls.Load())
		}
	})

	t.Run("after one minute - read again", func(t *testing.T) {
		now = t0.Add(61 * time.Second)
		s.Stats(t.Context())
		if calls.Load() != 2 {
			t.Fatalf("uptime read %d times, want 2", calls.Load())
		}
	})

	t.Run("refresh running - other callers get the last answer at once", func(t *testing.T) {
		block := make(chan struct{})
		slow := public.New(public.Sources{
			Answered: func(context.Context, int) (int64, error) { <-block; return 7, nil },
			Now:      func() time.Time { return now },
		}, nil)
		close(block)
		slow.Stats(t.Context()) // first answer: 7
		block = make(chan struct{})
		now = now.Add(2 * time.Minute)
		done := make(chan struct{})
		go func() { slow.Stats(t.Context()); close(done) }() // refresh blocks on the source
		time.Sleep(20 * time.Millisecond)
		st := slow.Stats(t.Context())
		close(block)
		<-done
		if st.AnsweredChats == nil || *st.AnsweredChats != 7 {
			t.Fatalf("stats %+v, want the last answer", st)
		}
	})

	t.Run("GET - public cache header, JSON body", func(t *testing.T) {
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/public-stats", nil))
		var body map[string]any
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if rec.Header().Get("Cache-Control") != "public, max-age=60" || body["answered_chats"] != float64(1234) || len(body) != 5 {
			t.Fatalf("headers %v body %v", rec.Header(), body)
		}
	})
}

func TestStatsNulls(t *testing.T) {
	boom := errors.New("down")
	s := public.New(public.Sources{
		Uptime:   func(context.Context, int) (float64, bool, error) { return 0, false, nil },
		P95:      func(context.Context, time.Duration) (float64, bool, error) { return 0, false, boom },
		Answered: func(context.Context, int) (int64, error) { return 0, boom },
	}, nil)

	t.Run("no sample, failing sources - null numbers, no error", func(t *testing.T) {
		st := s.Stats(t.Context())
		if st.UptimeRatio != nil || st.P95Ms != nil || st.AnsweredChats != nil || st.WindowDays != 30 {
			t.Fatalf("stats %+v", st)
		}
	})

	t.Run("no sources - null numbers", func(t *testing.T) {
		st := public.New(public.Sources{}, nil).Stats(t.Context())
		if st.UptimeRatio != nil || st.P95Ms != nil || st.AnsweredChats != nil {
			t.Fatalf("stats %+v", st)
		}
	})
}
