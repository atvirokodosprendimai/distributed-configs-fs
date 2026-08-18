package core

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// MaxClockDrift bounds how far ahead of local physical time a peer's timestamp
// may be before we refuse to adopt it.
//
// This guard is not optional. A hybrid logical clock takes the maximum of the
// local and remote wall components, so a single node with a broken RTC set to
// 2038 would drag every other node's clock forward with it and then win every
// last-writer-wins comparison in the cluster, permanently. Rejecting the write
// loses one node's changes; adopting it corrupts ordering for everyone.
const MaxClockDrift = 5 * time.Minute

// ErrClockDrift is returned when a peer's timestamp is further ahead of local
// physical time than MaxClockDrift allows.
var ErrClockDrift = errors.New("core: peer timestamp exceeds max clock drift")

// Timestamp is a hybrid logical clock reading: physical time, plus a counter
// that breaks ties when several events share the same wall-clock nanosecond or
// when the physical clock fails to advance.
//
// Wall time alone is not enough to order writes across machines whose clocks
// disagree; a purely logical clock orders them but drifts arbitrarily far from
// human time, which makes tombstone TTLs and "which of these is newer?" support
// questions unanswerable. The hybrid keeps both properties.
type Timestamp struct {
	Wall    int64  `json:"wall"` // Unix nanoseconds
	Counter uint32 `json:"ctr"`
}

// Compare returns -1, 0, or +1 as t sorts before, equal to, or after o.
func (t Timestamp) Compare(o Timestamp) int {
	switch {
	case t.Wall < o.Wall:
		return -1
	case t.Wall > o.Wall:
		return 1
	case t.Counter < o.Counter:
		return -1
	case t.Counter > o.Counter:
		return 1
	default:
		return 0
	}
}

// Time returns the physical component as a time.Time, for logging and for
// tombstone expiry arithmetic.
func (t Timestamp) Time() time.Time { return time.Unix(0, t.Wall) }

// String implements fmt.Stringer.
func (t Timestamp) String() string {
	return fmt.Sprintf("%s+%d", t.Time().UTC().Format(time.RFC3339Nano), t.Counter)
}

// Version identifies one write to one path: when it happened, and who made it.
//
// The Origin tiebreak is what makes ordering a total order rather than a
// partial one. Without it two nodes can produce byte-identical timestamps and
// then disagree forever about which write won, each keeping its own — the
// cluster would never converge.
type Version struct {
	HLC    Timestamp `json:"hlc"`
	Origin string    `json:"origin"`
}

// After reports whether v wins a last-writer-wins comparison against o.
// Ties on the clock are broken by node ID, which is arbitrary but identical on
// every node — and identical is the only property that matters.
func (v Version) After(o Version) bool {
	if c := v.HLC.Compare(o.HLC); c != 0 {
		return c > 0
	}
	return v.Origin > o.Origin
}

// Equal reports whether v and o are the same write.
func (v Version) Equal(o Version) bool {
	return v.HLC.Compare(o.HLC) == 0 && v.Origin == o.Origin
}

// String implements fmt.Stringer.
func (v Version) String() string { return v.Origin + "@" + v.HLC.String() }

// Clock is a node's hybrid logical clock. Its zero value is not usable; call
// NewClock.
//
// It is safe for concurrent use: every local write and every received remote
// write ticks it, and those arrive from the fsnotify watcher, the FUSE layer,
// and the anti-entropy puller at once.
type Clock struct {
	mu   sync.Mutex
	now  func() time.Time // injectable so tests can drive physical time
	last Timestamp
}

// NewClock returns a Clock driven by the system wall clock.
func NewClock() *Clock { return &Clock{now: time.Now} }

// newClockAt returns a Clock driven by now, for tests.
func newClockAt(now func() time.Time) *Clock { return &Clock{now: now} }

// Now returns a timestamp for a locally originated event. It is strictly
// greater than every timestamp this Clock has previously returned or observed,
// so a node can never order two of its own writes ambiguously even if the
// system clock jumps backwards.
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	physical := c.now().UnixNano()
	if physical > c.last.Wall {
		c.last = Timestamp{Wall: physical, Counter: 0}
	} else {
		// Physical time did not advance (same nanosecond, or the clock went
		// backwards). Advance logically instead so monotonicity holds.
		c.last.Counter++
	}
	return c.last
}

// Observe folds a peer's timestamp into the local clock and returns the new
// local reading. Call it on every remote write, so that a subsequent local
// write is ordered after the remote one it reacted to.
//
// It returns ErrClockDrift, and leaves the clock untouched, when remote is
// implausibly far in the future — see MaxClockDrift.
func (c *Clock) Observe(remote Timestamp) (Timestamp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	physical := c.now().UnixNano()
	if remote.Wall-physical > int64(MaxClockDrift) {
		return Timestamp{}, fmt.Errorf("%w: peer is %s ahead",
			ErrClockDrift, time.Duration(remote.Wall-physical))
	}

	// Standard HLC merge: take the greatest wall component, then derive the
	// counter from whichever inputs are tied with it.
	wall := max(physical, c.last.Wall, remote.Wall)
	var counter uint32
	switch {
	case wall == c.last.Wall && wall == remote.Wall:
		counter = max(c.last.Counter, remote.Counter) + 1
	case wall == c.last.Wall:
		counter = c.last.Counter + 1
	case wall == remote.Wall:
		counter = remote.Counter + 1
	default:
		// Physical time overtook both; the counter can restart.
		counter = 0
	}

	c.last = Timestamp{Wall: wall, Counter: counter}
	return c.last, nil
}
