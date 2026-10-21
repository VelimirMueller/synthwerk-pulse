// Package stream holds the live status of every target and sends it to clients:
// GET /v1/health as one JSON document, GET /v1/health/stream as Server-Sent Events.
//
// Stream events (docs/features/live-status.md):
//
//	event: snapshot   on connect: every target with its status
//	event: status     a target changed its status
//	: ping            comment line every 15 s, keeps proxies from closing the stream
//
// Every event has an id. The ids grow by one per status change, starting at 0 in each
// process. The first event of every stream is a snapshot, also after a reconnect with
// Last-Event-ID: the snapshot has the full state, so no event needs to be sent again.
package stream

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/VelimirMueller/synthwerk-pulse/internal/check"
	"github.com/VelimirMueller/synthwerk-pulse/internal/status"
)

// Target is the live status of one target.
type Target struct {
	Target string        `json:"target"`
	Status status.Status `json:"status"`
	// Since is the time of the last status change (or the start of pulse).
	Since time.Time `json:"since"`
	// CheckedAt is the time of the last check. Null before the first check.
	CheckedAt *time.Time `json:"checked_at"`
	// LatencyMs of the last check.
	LatencyMs int64 `json:"latency_ms"`
	// Checks of the last /readyz body. Empty for other targets.
	Checks []check.Item `json:"checks"`
}

// Snapshot is the body of GET /v1/health and the data of the snapshot event.
type Snapshot struct {
	Targets []Target `json:"targets"`
}

// Change is the data of the status event.
type Change struct {
	Target string        `json:"target"`
	From   status.Status `json:"from"`
	Status status.Status `json:"status"`
	Since  time.Time     `json:"since"`
	Checks []check.Item  `json:"checks"`
}

// Config of the hub. Zero values take the defaults of design §9.
type Config struct {
	// MaxClients is the max number of open streams. Default 50.
	MaxClients int
	// Heartbeat is the time between two ping comments. Default 15 s.
	Heartbeat time.Duration
	// MaxDuration closes a stream after this time; the client reconnects. Default 1 h.
	MaxDuration time.Duration
	// Buffer is the number of events a slow client may fall behind before the hub
	// closes its stream. Default 64.
	Buffer int
}

var (
	errFull   = errors.New("stream: too many clients")
	errClosed = errors.New("stream: hub closed")
)

// Hub is safe for concurrent use.
type Hub struct {
	cfg     Config
	mu      sync.Mutex
	seq     uint64
	order   []string
	targets map[string]Target
	clients map[chan []byte]struct{}
	closed  bool
	done    chan struct{}
}

// New returns a hub with every target in status unknown since start.
func New(names []string, start time.Time, cfg Config) *Hub {
	if cfg.MaxClients <= 0 {
		cfg.MaxClients = 50
	}
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = 15 * time.Second
	}
	if cfg.MaxDuration <= 0 {
		cfg.MaxDuration = time.Hour
	}
	if cfg.Buffer <= 0 {
		cfg.Buffer = 64
	}
	h := &Hub{
		cfg:     cfg,
		order:   append([]string(nil), names...),
		targets: make(map[string]Target, len(names)),
		clients: map[chan []byte]struct{}{},
		done:    make(chan struct{}),
	}
	for _, n := range names {
		h.targets[n] = Target{Target: n, Status: status.Unknown, Since: start.UTC(), Checks: []check.Item{}}
	}
	return h
}

// Update stores the latest state of a target. When changed is true it also sends a
// status event with from as the old status to every open stream.
func (h *Hub) Update(t Target, from status.Status, changed bool) {
	if t.Checks == nil {
		t.Checks = []check.Item{}
	}
	t.Since = t.Since.UTC()
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.targets[t.Target]; !ok {
		return
	}
	h.targets[t.Target] = t
	if !changed {
		return
	}
	h.seq++
	frame := encode(h.seq, "status", Change{Target: t.Target, From: from, Status: t.Status, Since: t.Since, Checks: t.Checks})
	for ch := range h.clients {
		select {
		case ch <- frame:
		default:
			// Too slow: close the stream. The client reconnects and gets a snapshot.
			delete(h.clients, ch)
			close(ch)
		}
	}
}

// Snapshot returns every target in the order of the targets file.
func (h *Hub) Snapshot() Snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.snapshot()
}

func (h *Hub) snapshot() Snapshot {
	s := Snapshot{Targets: make([]Target, 0, len(h.order))}
	for _, n := range h.order {
		s.Targets = append(s.Targets, h.targets[n])
	}
	return s
}

// Clients returns the number of open streams.
func (h *Hub) Clients() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

// Close ends every open stream and refuses new ones. Call it on shutdown.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.closed {
		h.closed = true
		close(h.done)
	}
}

func (h *Hub) subscribe() ([]byte, chan []byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, nil, errClosed
	}
	if len(h.clients) >= h.cfg.MaxClients {
		return nil, nil, errFull
	}
	ch := make(chan []byte, h.cfg.Buffer)
	h.clients[ch] = struct{}{}
	return encode(h.seq, "snapshot", h.snapshot()), ch, nil
}

func (h *Hub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[ch]; ok {
		delete(h.clients, ch)
		close(ch)
	}
}

// ServeSnapshot serves GET /v1/health.
func (h *Hub) ServeSnapshot(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(h.Snapshot())
}

// ServeStream serves GET /v1/health/stream.
func (h *Hub) ServeStream(w http.ResponseWriter, r *http.Request) {
	first, ch, err := h.subscribe()
	if err != nil {
		w.Header().Set("Retry-After", "5")
		http.Error(w, "stream not available", http.StatusServiceUnavailable)
		return
	}
	defer h.unsubscribe(ch)

	rc := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	send := func(b []byte) bool {
		if _, err := w.Write(b); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	if !send(append([]byte("retry: 3000\n\n"), first...)) {
		return
	}

	heartbeat := time.NewTicker(h.cfg.Heartbeat)
	defer heartbeat.Stop()
	limit := time.NewTimer(h.cfg.MaxDuration)
	defer limit.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-h.done:
			return
		case <-limit.C:
			return
		case <-heartbeat.C:
			if !send([]byte(": ping\n\n")) {
				return
			}
		case frame, ok := <-ch:
			if !ok || !send(frame) {
				return
			}
		}
	}
}

func encode(id uint64, event string, data any) []byte {
	raw, err := json.Marshal(data)
	if err != nil {
		// The types above always marshal.
		panic(fmt.Sprintf("stream: marshal %s: %v", event, err))
	}
	return fmt.Appendf(nil, "id: %d\nevent: %s\ndata: %s\n\n", id, event, raw)
}
