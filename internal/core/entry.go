// Package core is the kernel of distributed-configs-fs: the domain types and
// ports every other package depends on.
//
// It deliberately imports nothing but the standard library. The store, the
// transport, the cluster layer, and both projections (mirror and FUSE) all
// import core and never each other, which is what keeps them independently
// buildable and testable.
//
// The domain in one paragraph: a cluster of nodes replicates one tree of
// configuration files. Every node holds the whole tree in a local SQLite
// database, which is the source of truth; the mirror directory and the FUSE
// mount are projections of it. Nodes agree on "which write wins" with
// last-writer-wins ordering over a hybrid logical clock (see Timestamp), and
// preserve the loser of a genuinely concurrent edit as a conflict copy rather
// than dropping it (see Meta.PrevHash).
package core

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

// ErrTooLarge reports a file beyond the configured per-file cap.
//
// It lives in the kernel because two unrelated packages need to agree on it:
// the syncer raises it, and the FUSE projection maps it onto EFBIG so the
// program doing the write sees the refusal at the syscall that caused it.
var ErrTooLarge = errors.New("core: file exceeds max size")

// Kind distinguishes the two things the cluster replicates. Symlinks, sockets,
// FIFOs and device nodes are deliberately not replicated: a symlink is a path
// that means something different on every host, and a device node in a config
// tree is a way to hand a peer a write primitive it should not have.
type Kind uint8

// The replicated node kinds.
const (
	KindFile Kind = 1
	KindDir  Kind = 2
)

// String implements fmt.Stringer so Kind reads sensibly in logs and errors.
func (k Kind) String() string {
	switch k {
	case KindFile:
		return "file"
	case KindDir:
		return "dir"
	default:
		return fmt.Sprintf("kind(%d)", uint8(k))
	}
}

// Valid reports whether k is a kind this cluster replicates.
func (k Kind) Valid() bool { return k == KindFile || k == KindDir }

// Meta is the replicated metadata for one path: everything about an entry
// except its bytes. It is what travels in a manifest.
//
// Content travels separately and is addressed by Hash, so a node rejoining
// after a week exchanges metadata for the whole tree but fetches only the blobs
// it is actually missing. That split is the reason a rejoin is cheap.
type Meta struct {
	// Path is cluster-relative and always slash-separated, with no leading
	// slash and no "." or ".." elements. It has passed SanitizePath.
	Path string `json:"path"`
	Kind Kind   `json:"kind"`

	// Hash is the lowercase hex SHA-256 of the file's content. It is empty for
	// directories and for tombstones.
	Hash string `json:"hash,omitempty"`

	// PrevHash is the Hash this write replaced, as observed by the node that
	// made it. It is what lets a receiver tell "this write is based on what I
	// already have" (fast-forward) from "this write is based on something else"
	// (concurrent edit, keep a conflict copy). Empty when the write created the
	// entry or when the writer had no previous version.
	PrevHash string `json:"prev_hash,omitempty"`

	Size int64  `json:"size"`
	Mode uint32 `json:"mode"` // permission bits only; see SanitizeMode
	UID  uint32 `json:"uid"`
	GID  uint32 `json:"gid"`

	// Version orders this write against every other write to the same path.
	Version Version `json:"version"`

	// Deleted marks a tombstone. Tombstones are replicated like any other
	// entry, because a node that was offline during a deletion would otherwise
	// resurrect the file on rejoin. They are garbage-collected once DeletedAt
	// is older than the configured tombstone TTL.
	Deleted   bool  `json:"deleted,omitempty"`
	DeletedAt int64 `json:"deleted_at,omitempty"` // Unix nanoseconds

	// Seq is this entry's position in the *serving node's* changes feed. It is
	// assigned locally on every write and is meaningless outside the node that
	// assigned it — a puller records the last Seq it saw from each peer and
	// asks that peer for "everything after N". It is therefore not part of the
	// replicated state and never participates in conflict resolution.
	Seq int64 `json:"seq"`
}

// Change is one entry plus, when the receiver cannot already reconstruct it,
// its content. Content is nil whenever the receiver's blob store already holds
// Meta.Hash, and always nil for directories and tombstones.
type Change struct {
	Meta    Meta
	Content []byte
}

// HashContent returns the lowercase hex SHA-256 used to address a blob.
// Content addressing is what makes deduplication and "do I need to fetch this?"
// a single string comparison.
func HashContent(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
