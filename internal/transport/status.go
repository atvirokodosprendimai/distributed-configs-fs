package transport

import (
	"strconv"
	"time"
)

// Status is the operator-facing read model: everything `dcfs status` prints and
// everything /v1/status serves, in one shape.
//
// It is assembled fresh on every request from the store, the cluster layer and
// the syncer rather than maintained incrementally. A status document that can
// go stale is worse than none, because it is believed.
type Status struct {
	Node      string    `json:"node"`
	Addr      string    `json:"addr"`
	StartedAt time.Time `json:"started_at"`

	// Digest and Entries are what an operator compares between nodes to answer
	// "are these two actually in sync?" without diffing two directories.
	Digest     string `json:"digest"`
	Entries    int64  `json:"entries"`
	Tombstones int64  `json:"tombstones"`
	Blobs      int64  `json:"blobs"`
	BlobBytes  int64  `json:"blob_bytes"`
	Head       int64  `json:"head"`

	// Conflicts lists conflict-copy paths. A non-empty list means two nodes
	// edited the same file while unable to see each other and a human needs to
	// pick a winner — it is the one field here that is a call to action.
	Conflicts []string `json:"conflicts"`

	Members []Member `json:"members"`

	// MirrorDir and MountPoint are empty when that projection is not enabled,
	// which is how the CLI reports which modes this node is actually running.
	MirrorDir  string `json:"mirror_dir,omitempty"`
	MountPoint string `json:"mount_point,omitempty"`

	LastSync  time.Time `json:"last_sync"`
	LastError string    `json:"last_error,omitempty"`
}

// Member is one node as this node currently sees it.
type Member struct {
	Node  string `json:"node"`
	Addr  string `json:"addr"`
	Alive bool   `json:"alive"`
	Self  bool   `json:"self"`

	// LastSeq is how far through this member's changes feed we have read. It
	// is this node's bookkeeping, not the member's own position, so it says
	// "how current am I with them", not "how busy are they".
	LastSeq int64 `json:"last_seq"`
}

// parseSeq parses a "?since=" query value, treating absent or malformed input
// as 0, meaning "send me everything".
//
// Being permissive is deliberate. Rejecting the request leaves two nodes
// diverged with no way to recover, whereas the worst case here is one
// unnecessary full manifest — a cost paid in bandwidth, once.
func parseSeq(raw string) int64 {
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}
