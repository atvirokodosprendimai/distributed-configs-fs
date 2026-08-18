package syncer

import "github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"

// decision is what to do with one incoming remote entry.
type decision uint8

const (
	// decisionSkip means the local copy is the same write or a newer one.
	// Skipping is not a loss: the peer will learn our version on its own pull,
	// because replication is symmetric and every node pulls from every other.
	decisionSkip decision = iota
	// decisionAccept means the remote write wins and supersedes ours cleanly.
	decisionAccept
	// decisionConflict means the remote write wins, but our version was made
	// concurrently rather than superseded by it, so the local content is
	// preserved as a conflict copy before the remote write is applied.
	decisionConflict
)

// String implements fmt.Stringer for log lines and test failures.
func (d decision) String() string {
	switch d {
	case decisionSkip:
		return "skip"
	case decisionAccept:
		return "accept"
	case decisionConflict:
		return "conflict"
	default:
		return "decision(?)"
	}
}

// resolve decides the fate of a remote entry against the local one.
//
// It is deliberately pure: this is the one piece of logic where a subtle error
// silently destroys somebody's configuration file, so it is a function of its
// arguments alone and is exercised by a table of cases rather than by a live
// cluster.
//
// The ordering rule is last-writer-wins over the hybrid logical clock, with the
// origin node ID breaking exact ties (core.Version.After). The interesting part
// is telling a *supersession* from a *concurrent edit*, which is what
// Meta.PrevHash exists for — it records the content hash the remote writer
// replaced:
//
//   - remote.PrevHash == local.Hash: the remote node started from exactly what
//     we hold and moved forward. A fast-forward; nothing to preserve.
//   - remote.Hash == local.Hash: both sides ended at the same content, whatever
//     route they took. Nothing to preserve.
//   - local.Hash == "": we hold no content — the entry is new to us, a
//     directory, or a tombstone. Nothing to preserve.
//   - otherwise: the remote write descends from a version we never had, so our
//     content and theirs are siblings rather than sequential. Exactly one of
//     them can occupy the path, and the other is kept as a conflict copy.
//
// This is a heuristic, not a vector clock, and it errs on the side of keeping a
// copy: the cost of a spurious conflict file is that an operator deletes it,
// while the cost of a missed one is a configuration change that vanishes.
func resolve(local core.Meta, haveLocal bool, remote core.Meta) decision {
	if !haveLocal {
		return decisionAccept
	}
	// The same write arriving twice — via gossip and again via anti-entropy —
	// is the common case, not an anomaly.
	if local.Version.Equal(remote.Version) {
		return decisionSkip
	}
	if local.Version.After(remote.Version) {
		return decisionSkip
	}

	// From here the remote write wins. The only question left is whether ours
	// deserves preserving.
	switch {
	case local.Deleted:
		// We hold a tombstone. There is no local content to lose, and a
		// resurrection is the correct outcome when the remote write is newer.
		return decisionAccept
	case local.Hash == "":
		return decisionAccept
	case remote.PrevHash == local.Hash:
		return decisionAccept
	case remote.Hash == local.Hash:
		return decisionAccept
	default:
		return decisionConflict
	}
}
