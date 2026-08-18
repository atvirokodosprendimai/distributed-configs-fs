package syncer

import (
	"testing"

	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"
)

// ver builds a version for tests.
func ver(wall int64, origin string) core.Version {
	return core.Version{HLC: core.Timestamp{Wall: wall}, Origin: origin}
}

// file builds a live file entry.
func file(path, hash, prev string, v core.Version) core.Meta {
	return core.Meta{Path: path, Kind: core.KindFile, Hash: hash, PrevHash: prev, Mode: 0o644, Version: v}
}

// TestResolve is the table that guards the one place where a subtle mistake
// silently destroys a configuration file. Each case names the real situation it
// stands for.
func TestResolve(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		local     core.Meta
		haveLocal bool
		remote    core.Meta
		want      decision
	}{
		{
			name:   "new to us",
			remote: file("a.conf", "h2", "", ver(200, "node2")),
			want:   decisionAccept,
		},
		{
			name:      "same write arriving twice, via gossip and again via anti-entropy",
			local:     file("a.conf", "h1", "", ver(100, "node1")),
			haveLocal: true,
			remote:    file("a.conf", "h1", "", ver(100, "node1")),
			want:      decisionSkip,
		},
		{
			name:      "our version is newer, so the peer will learn ours instead",
			local:     file("a.conf", "h2", "h1", ver(300, "node1")),
			haveLocal: true,
			remote:    file("a.conf", "h1", "", ver(100, "node2")),
			want:      decisionSkip,
		},
		{
			name:      "clean fast-forward: the peer edited exactly what we hold",
			local:     file("a.conf", "h1", "", ver(100, "node1")),
			haveLocal: true,
			remote:    file("a.conf", "h2", "h1", ver(200, "node2")),
			want:      decisionAccept,
		},
		{
			name:      "both sides converged on identical content by different routes",
			local:     file("a.conf", "h9", "h1", ver(100, "node1")),
			haveLocal: true,
			remote:    file("a.conf", "h9", "h5", ver(200, "node2")),
			want:      decisionAccept,
		},
		{
			name:      "concurrent edit: the peer's write descends from a version we never had",
			local:     file("a.conf", "local-edit", "h1", ver(100, "node1")),
			haveLocal: true,
			remote:    file("a.conf", "remote-edit", "h1", ver(200, "node2")),
			want:      decisionConflict,
		},
		{
			name:      "exact clock tie is broken by node id, higher wins",
			local:     file("a.conf", "h1", "", ver(100, "node1")),
			haveLocal: true,
			remote:    file("a.conf", "h2", "h1", ver(100, "node2")),
			want:      decisionAccept,
		},
		{
			name:      "exact clock tie is broken by node id, lower loses",
			local:     file("a.conf", "h1", "", ver(100, "node2")),
			haveLocal: true,
			remote:    file("a.conf", "h2", "h1", ver(100, "node1")),
			want:      decisionSkip,
		},
		{
			name:      "newer deletion supersedes our copy",
			local:     file("a.conf", "h1", "", ver(100, "node1")),
			haveLocal: true,
			remote: core.Meta{Path: "a.conf", Kind: core.KindFile, PrevHash: "h1",
				Version: ver(200, "node2"), Deleted: true, DeletedAt: 200},
			want: decisionAccept,
		},
		{
			name: "our tombstone loses to a newer re-creation, and there is nothing to preserve",
			local: core.Meta{Path: "a.conf", Kind: core.KindFile,
				Version: ver(100, "node1"), Deleted: true, DeletedAt: 100},
			haveLocal: true,
			remote:    file("a.conf", "h5", "", ver(200, "node2")),
			want:      decisionAccept,
		},
		{
			name: "our tombstone is newer than the peer's re-creation, so the deletion holds",
			local: core.Meta{Path: "a.conf", Kind: core.KindFile,
				Version: ver(300, "node1"), Deleted: true, DeletedAt: 300},
			haveLocal: true,
			remote:    file("a.conf", "h5", "", ver(200, "node2")),
			want:      decisionSkip,
		},
		{
			name: "directories carry no content, so a newer one is never a conflict",
			local: core.Meta{Path: "conf.d", Kind: core.KindDir, Mode: 0o755,
				Version: ver(100, "node1")},
			haveLocal: true,
			remote: core.Meta{Path: "conf.d", Kind: core.KindDir, Mode: 0o700,
				Version: ver(200, "node2")},
			want: decisionAccept,
		},
		{
			name:      "a concurrent edit that only changed the mode still conflicts on content",
			local:     file("a.conf", "local-edit", "", ver(100, "node1")),
			haveLocal: true,
			remote:    file("a.conf", "remote-edit", "", ver(200, "node2")),
			want:      decisionConflict,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := resolve(tc.local, tc.haveLocal, tc.remote); got != tc.want {
				t.Errorf("resolve() = %v, want %v\n local:  %+v\n remote: %+v",
					got, tc.want, tc.local, tc.remote)
			}
		})
	}
}

// TestResolveConverges is the property that matters more than any single case:
// two nodes holding different versions of the same path must, after exchanging
// them, agree on which one occupies the path. If both keep their own, the
// cluster never converges and no amount of syncing fixes it.
func TestResolveConverges(t *testing.T) {
	t.Parallel()

	versions := []core.Meta{
		file("a.conf", "h1", "", ver(100, "node1")),
		file("a.conf", "h2", "", ver(100, "node2")), // exact clock tie
		file("a.conf", "h3", "h1", ver(200, "node1")),
		file("a.conf", "h4", "", ver(200, "node3")),
		file("a.conf", "h5", "h9", ver(50, "node2")),
	}

	for _, a := range versions {
		for _, b := range versions {
			if a.Version.Equal(b.Version) {
				continue
			}
			// Node A holds a and receives b; node B holds b and receives a.
			aKeeps := resolve(a, true, b) == decisionSkip
			bKeeps := resolve(b, true, a) == decisionSkip

			// Exactly one side must end up keeping its own version.
			if aKeeps == bKeeps {
				t.Errorf("no convergence between %v and %v: aKeeps=%v bKeeps=%v",
					a.Version, b.Version, aKeeps, bKeeps)
			}
		}
	}
}
