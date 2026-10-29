// Package sse reads a Server-Sent Events stream in tests.
package sse

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Event is one SSE event. Comment lines (": ping") are kept as Event "comment".
type Event struct {
	ID    string
	Event string
	Data  string
}

// Stream is an open SSE response.
type Stream struct {
	Res    *http.Response
	events chan Event
}

// Open sends GET url and starts reading events. The stream closes when the test ends.
func Open(t *testing.T, url string) *Stream {
	t.Helper()
	return OpenWith(t, url, nil)
}

// OpenWith is Open with request headers, for example Authorization.
func OpenWith(t *testing.T, url string, header http.Header) *Stream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = res.Body.Close() })
	s := &Stream{Res: res, events: make(chan Event, 256)}
	go s.read(res.Body)
	return s
}

func (s *Stream) read(body io.Reader) {
	defer close(s.events)
	sc := bufio.NewScanner(body)
	var ev Event
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if ev.Event != "" || ev.Data != "" {
				s.events <- ev
			}
			ev = Event{}
		case strings.HasPrefix(line, ":"):
			s.events <- Event{Event: "comment", Data: strings.TrimSpace(line[1:])}
		case strings.HasPrefix(line, "id: "):
			ev.ID = line[4:]
		case strings.HasPrefix(line, "event: "):
			ev.Event = line[7:]
		case strings.HasPrefix(line, "data: "):
			if ev.Data != "" {
				ev.Data += "\n"
			}
			ev.Data += line[6:]
		}
	}
}

// Next returns the next event that is not a comment. It fails the test after timeout.
// ok is false when the stream ended.
func (s *Stream) Next(t *testing.T, timeout time.Duration) (Event, bool) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-s.events:
			if !ok {
				return Event{}, false
			}
			if ev.Event == "comment" {
				continue
			}
			return ev, true
		case <-deadline:
			t.Fatalf("no event within %s", timeout)
			return Event{}, false
		}
	}
}

// NextAny returns the next event, comments included.
func (s *Stream) NextAny(t *testing.T, timeout time.Duration) (Event, bool) {
	t.Helper()
	select {
	case ev, ok := <-s.events:
		return ev, ok
	case <-time.After(timeout):
		t.Fatalf("no event within %s", timeout)
		return Event{}, false
	}
}

// Ended reports whether the stream ends within timeout.
func (s *Stream) Ended(timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		select {
		case _, ok := <-s.events:
			if !ok {
				return true
			}
		case <-deadline:
			return false
		}
	}
}
