package core

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeClock lets a test drive physical time so HLC behaviour is deterministic
// rather than dependent on how fast the machine runs.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

func (f *fakeClock) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
}

func newFake(t time.Time) *fakeClock { return &fakeClock{t: t} }

// TestClockNowIsMonotonic covers the case that motivates the counter: a node
// producing several writes inside one nanosecond, and a node whose system clock
// jumps backwards. Neither may produce two writes that compare equal, or LWW
// stops being a total order.
func TestClockNowIsMonotonic(t *testing.T) {
	t.Parallel()

	f := newFake(time.Unix(1_700_000_000, 0))
	c := newClockAt(f.now)

	prev := c.Now()
	for i := range 100 {
		got := c.Now()
		if got.Compare(prev) <= 0 {
			t.Fatalf("tick %d: Now() = %v, not strictly after previous %v", i, got, prev)
		}
		prev = got
	}

	// A backwards jump must not let the clock repeat itself.
	f.advance(-time.Hour)
	got := c.Now()
	if got.Compare(prev) <= 0 {
		t.Fatalf("after backwards jump: Now() = %v, not strictly after %v", got, prev)
	}
}

func TestClockObserveAdoptsRemoteTime(t *testing.T) {
	t.Parallel()

	base := time.Unix(1_700_000_000, 0)
	f := newFake(base)
	c := newClockAt(f.now)

	// A remote write from one second in our future: legal, well inside drift.
	remote := Timestamp{Wall: base.Add(time.Second).UnixNano(), Counter: 7}
	got, err := c.Observe(remote)
	if err != nil {
		t.Fatalf("Observe() = %v, want it accepted", err)
	}
	if got.Compare(remote) <= 0 {
		t.Fatalf("Observe() = %v, want strictly after the remote timestamp %v", got, remote)
	}

	// The point of observing: our next local write is ordered after the remote
	// one we just reacted to, even though our physical clock never moved.
	local := c.Now()
	if local.Compare(remote) <= 0 {
		t.Fatalf("Now() after Observe() = %v, want strictly after remote %v", local, remote)
	}
}

// TestClockObserveRejectsDrift is the guard against one node with a broken RTC
// dragging the whole cluster's clock into the future and then winning every
// conflict forever.
func TestClockObserveRejectsDrift(t *testing.T) {
	t.Parallel()

	base := time.Unix(1_700_000_000, 0)
	f := newFake(base)
	c := newClockAt(f.now)

	before := c.Now()
	insane := Timestamp{Wall: base.Add(MaxClockDrift + time.Minute).UnixNano()}
	if _, err := c.Observe(insane); !errors.Is(err, ErrClockDrift) {
		t.Fatalf("Observe(far future) error = %v, want ErrClockDrift", err)
	}

	// Rejection must leave the clock untouched — a poisoned reading that is
	// refused but still absorbed would defeat the entire guard.
	after := c.Now()
	if after.Wall > base.Add(MaxClockDrift).UnixNano() {
		t.Fatalf("clock adopted the rejected timestamp: %v (was %v)", after, before)
	}
}

func TestClockConcurrentUse(t *testing.T) {
	t.Parallel()

	c := NewClock()
	var wg sync.WaitGroup
	seen := make(chan Timestamp, 400)
	for range 4 {
		wg.Go(func() {
			for range 100 {
				seen <- c.Now()
			}
		})
	}
	wg.Wait()
	close(seen)

	// Every reading must be distinct: two concurrent local writes that share a
	// timestamp would be unorderable against each other.
	uniq := make(map[Timestamp]bool, 400)
	for ts := range seen {
		if uniq[ts] {
			t.Fatalf("Now() returned duplicate timestamp %v under concurrency", ts)
		}
		uniq[ts] = true
	}
}

// TestVersionAfter pins the total order LWW depends on, including the node-ID
// tiebreak that stops two nodes from disagreeing forever on a clock tie.
func TestVersionAfter(t *testing.T) {
	t.Parallel()

	ts := func(wall int64, ctr uint32) Timestamp { return Timestamp{Wall: wall, Counter: ctr} }

	tests := []struct {
		name string
		a, b Version
		want bool
	}{
		{"later wall wins", Version{ts(2, 0), "n1"}, Version{ts(1, 9), "n1"}, true},
		{"earlier wall loses", Version{ts(1, 9), "n1"}, Version{ts(2, 0), "n1"}, false},
		{"counter breaks equal wall", Version{ts(1, 2), "n1"}, Version{ts(1, 1), "n1"}, true},
		{"node id breaks full tie", Version{ts(1, 1), "n2"}, Version{ts(1, 1), "n1"}, true},
		{"node id loses full tie", Version{ts(1, 1), "n1"}, Version{ts(1, 1), "n2"}, false},
		{"identical is not after", Version{ts(1, 1), "n1"}, Version{ts(1, 1), "n1"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.a.After(tc.b); got != tc.want {
				t.Errorf("%v.After(%v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
			// Antisymmetry: exactly one of a>b, b>a, a==b holds. Without this
			// two nodes can both believe they hold the winner.
			if tc.a.Equal(tc.b) {
				if tc.a.After(tc.b) || tc.b.After(tc.a) {
					t.Errorf("equal versions %v and %v also compare as ordered", tc.a, tc.b)
				}
			} else if tc.a.After(tc.b) == tc.b.After(tc.a) {
				t.Errorf("versions %v and %v are not antisymmetric", tc.a, tc.b)
			}
		})
	}
}
