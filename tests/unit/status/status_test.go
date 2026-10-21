package status_test

import (
	"testing"
	"time"

	"github.com/VelimirMueller/synthwerk-pulse/internal/status"
)

var t0 = time.Date(2026, 10, 21, 8, 0, 0, 0, time.UTC)

// feed observes the results one second apart and returns the status after each one.
func feed(tr *status.Tracker, results ...status.Status) []status.Status {
	out := make([]status.Status, 0, len(results))
	for i, r := range results {
		tr.Observe(r, t0.Add(time.Duration(i+1)*time.Second))
		s, _ := tr.Status()
		out = append(out, s)
	}
	return out
}

func equal(a, b []status.Status) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestTrackerObserve(t *testing.T) {
	const (
		u   = status.Unknown
		ok  = status.OK
		deg = status.Degraded
		dn  = status.Down
	)
	cases := []struct {
		name    string
		results []status.Status
		want    []status.Status
	}{
		{"Observe - first result ok - ok at once", []status.Status{ok}, []status.Status{ok}},
		{"Observe - first result degraded - degraded at once", []status.Status{deg}, []status.Status{deg}},
		{"Observe - unknown and 2 failures - down", []status.Status{dn, dn}, []status.Status{u, dn}},
		{"Observe - ok and 1 failure - still ok", []status.Status{ok, dn, ok}, []status.Status{ok, ok, ok}},
		{"Observe - ok and 2 consecutive failures - down", []status.Status{ok, dn, dn}, []status.Status{ok, ok, dn}},
		{"Observe - down then 2 ok - still down", []status.Status{ok, dn, dn, ok, ok}, []status.Status{ok, ok, dn, dn, dn}},
		{"Observe - down then 3 ok - ok", []status.Status{ok, dn, dn, ok, ok, ok}, []status.Status{ok, ok, dn, dn, dn, ok}},
		{"Observe - down then ok ok degraded - degraded", []status.Status{dn, dn, ok, ok, deg}, []status.Status{u, dn, dn, dn, deg}},
		{"Observe - down then a failure between ok results - streak starts again", []status.Status{dn, dn, ok, ok, dn, ok, ok, ok}, []status.Status{u, dn, dn, dn, dn, dn, dn, ok}},
		{"Observe - ok then degraded - degraded at once", []status.Status{ok, deg}, []status.Status{ok, deg}},
		{"Observe - degraded then 1 ok - still degraded", []status.Status{deg, ok}, []status.Status{deg, deg}},
		{"Observe - degraded then 2 ok - ok", []status.Status{deg, ok, ok}, []status.Status{deg, deg, ok}},
		{"Observe - degraded then 2 failures - down", []status.Status{deg, dn, dn}, []status.Status{deg, deg, dn}},
		{"Observe - invalid result - counts as down", []status.Status{ok, "", "weird"}, []status.Status{ok, ok, dn}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := feed(status.NewTracker(t0), c.results...)
			if !equal(got, c.want) {
				t.Fatalf("statuses %v, want %v", got, c.want)
			}
		})
	}

	t.Run("Observe - status changes - reports from and the new since", func(t *testing.T) {
		tr := status.NewTracker(t0)
		at := t0.Add(5 * time.Second)
		from, changed := tr.Observe(status.OK, at)
		if from != status.Unknown || !changed {
			t.Fatalf("from %s changed %v, want unknown true", from, changed)
		}
		s, since := tr.Status()
		if s != status.OK || !since.Equal(at) {
			t.Fatalf("status %s since %s, want ok since %s", s, since, at)
		}
		from, changed = tr.Observe(status.OK, at.Add(time.Second))
		if from != status.OK || changed {
			t.Fatalf("same status again: from %s changed %v, want ok false", from, changed)
		}
		if _, since := tr.Status(); !since.Equal(at) {
			t.Fatalf("since moved to %s without a change", since)
		}
	})
}
