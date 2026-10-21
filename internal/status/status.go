// Package status turns single check results into a stable target status (design §3.3).
//
// One failed check does not make a target red, and one good check does not make it
// green again. The thresholds:
//
//   - unknown -> ok or degraded: the first result that is not down.
//   - any -> down: 2 down results in a row.
//   - ok -> degraded: 1 degraded result.
//   - degraded -> ok: 2 ok results in a row.
//   - down -> ok or degraded: 3 results in a row that are not down. The new status is
//     the last result.
package status

import "time"

// Status of a target. The values are the ones of the event
// dev.synthwerk.pulse.target.status_changed.v1.
type Status string

// The statuses. Unknown is only the start value: a check result is never unknown.
const (
	Unknown  Status = "unknown"
	OK       Status = "ok"
	Degraded Status = "degraded"
	Down     Status = "down"
)

// Thresholds of the hysteresis.
const (
	DownAfter        = 2 // down results in a row: any -> down
	OKAfterDegraded  = 2 // ok results in a row: degraded -> ok
	UpAfterDown      = 3 // results in a row that are not down: down -> ok or degraded
	maxStreakCounted = 1 << 20
)

// Valid reports whether s is a status a check can report (ok, degraded or down).
func Valid(s Status) bool { return s == OK || s == Degraded || s == Down }

// Tracker holds the status of one target. The zero value is not ready: use NewTracker.
type Tracker struct {
	status  Status
	since   time.Time
	downRun int // down results in a row
	okRun   int // ok results in a row
	upRun   int // results in a row that are not down
}

// NewTracker returns a tracker in status unknown since start.
func NewTracker(start time.Time) *Tracker {
	return &Tracker{status: Unknown, since: start}
}

// Status returns the current status and the time it started.
func (t *Tracker) Status() (Status, time.Time) { return t.status, t.since }

// Observe adds one check result at time now. It returns the status before the result
// and whether the status changed. A result that is not ok, degraded or down counts as down.
func (t *Tracker) Observe(result Status, now time.Time) (from Status, changed bool) {
	if !Valid(result) {
		result = Down
	}
	switch result {
	case Down:
		t.downRun, t.okRun, t.upRun = inc(t.downRun), 0, 0
	case OK:
		t.downRun, t.okRun, t.upRun = 0, inc(t.okRun), inc(t.upRun)
	case Degraded, Unknown: // Unknown cannot happen: Valid turned it into Down.
		t.downRun, t.okRun, t.upRun = 0, 0, inc(t.upRun)
	}

	next := t.next(result)
	from = t.status
	if next == t.status {
		return from, false
	}
	t.status, t.since = next, now
	return from, true
}

func (t *Tracker) next(result Status) Status {
	switch t.status {
	case Unknown:
		if result == Down {
			if t.downRun >= DownAfter {
				return Down
			}
			return Unknown
		}
		return result
	case OK:
		if result == Down && t.downRun >= DownAfter {
			return Down
		}
		if result == Degraded {
			return Degraded
		}
	case Degraded:
		if result == Down && t.downRun >= DownAfter {
			return Down
		}
		if result == OK && t.okRun >= OKAfterDegraded {
			return OK
		}
	case Down:
		if result != Down && t.upRun >= UpAfterDown {
			return result
		}
	}
	return t.status
}

func inc(n int) int {
	if n >= maxStreakCounted {
		return n
	}
	return n + 1
}
