package syncer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/cluster"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/store"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/transport"
)

// node is one simulated cluster member: a real store and a real syncer, with
// the network replaced by direct calls. Everything under test — resolution,
// validation, blob fetching, tombstones — is the production code path.
type node struct {
	name  string
	store *store.Store
	svc   *Service
}

// fakePeers satisfies Peers without gossip. Membership is set by the test.
type fakePeers struct {
	events  chan cluster.Event
	members []cluster.Member
}

func (f *fakePeers) Events() <-chan cluster.Event { return f.events }
func (f *fakePeers) Announce(int64)               {}
func (f *fakePeers) Members() []cluster.Member    { return f.members }

// directClient satisfies PeerClient by reading another node's store, standing
// in for the HTTP transport whose own behaviour is tested in its own package.
type directClient struct {
	nodes map[string]*store.Store // keyed by "address"
	fail  map[string]bool         // addresses that refuse blob fetches
}

func (d *directClient) Digest(ctx context.Context, addr string) (transport.DigestResponse, error) {
	s, ok := d.nodes[addr]
	if !ok {
		return transport.DigestResponse{}, fmt.Errorf("no peer at %s", addr)
	}
	digest, entries, head, err := s.Digest(ctx)
	return transport.DigestResponse{Digest: digest, Entries: entries, Head: head}, err
}

func (d *directClient) Manifest(ctx context.Context, addr string, since int64) (transport.ManifestResponse, error) {
	s, ok := d.nodes[addr]
	if !ok {
		return transport.ManifestResponse{}, fmt.Errorf("no peer at %s", addr)
	}
	entries, err := s.ManifestSince(ctx, since, transport.MaxManifestPage)
	if err != nil {
		return transport.ManifestResponse{}, err
	}
	resp := transport.ManifestResponse{Entries: entries, Head: since}
	if n := len(entries); n > 0 {
		resp.Head = entries[n-1].Seq
	}
	return resp, nil
}

func (d *directClient) Blob(ctx context.Context, addr, hash string) ([]byte, error) {
	if d.fail[addr] {
		return nil, errors.New("peer refused the blob")
	}
	s, ok := d.nodes[addr]
	if !ok {
		return nil, fmt.Errorf("no peer at %s", addr)
	}
	return s.Blob(ctx, hash)
}

// cluster2 builds two nodes that can sync from each other.
func cluster2(t *testing.T) (a, b *node, client *directClient) {
	t.Helper()

	client = &directClient{nodes: map[string]*store.Store{}, fail: map[string]bool{}}
	mk := func(name string) *node {
		st, err := store.Open(t.Context(), filepath.Join(t.TempDir(), name+".db"))
		if err != nil {
			t.Fatalf("open store for %s: %v", name, err)
		}
		t.Cleanup(func() { _ = st.Close() })
		client.nodes[name] = st

		return &node{name: name, store: st, svc: New(Config{
			Node:            name,
			Store:           st,
			Peers:           &fakePeers{events: make(chan cluster.Event)},
			Peer:            client,
			MaxFileSize:     1 << 20,
			TombstoneTTL:    24 * time.Hour,
			SyncInterval:    time.Hour,
			JanitorInterval: time.Hour,
			Log:             slog.New(slog.DiscardHandler),
		})}
	}
	return mk("a"), mk("b"), client
}

// contentAt reads a node's file content, failing the test if it is absent.
func contentAt(t *testing.T, n *node, path string) string {
	t.Helper()
	m, err := n.store.Get(t.Context(), path)
	if err != nil {
		t.Fatalf("%s: Get(%q) = %v", n.name, path, err)
	}
	if m.Deleted {
		t.Fatalf("%s: %q is a tombstone, want live content", n.name, path)
	}
	blob, err := n.store.Blob(t.Context(), m.Hash)
	if err != nil {
		t.Fatalf("%s: Blob for %q = %v", n.name, path, err)
	}
	return string(blob)
}

// TestPutFileIsIdempotent is the echo-loop regression test. After the mirror
// writes a file the cluster sent us, fsnotify fires and hands those exact bytes
// straight back. If that second offer produced a new version, two nodes would
// bounce a file between them forever.
func TestPutFileIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	a, _, _ := cluster2(t)

	content := []byte("listen 80;")
	if err := a.svc.PutFile(ctx, "nginx.conf", content, 0o644, 0, 0); err != nil {
		t.Fatalf("PutFile() = %v", err)
	}
	first, err := a.store.Get(ctx, "nginx.conf")
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}

	// The same bytes and metadata offered again, as the watcher would.
	for range 5 {
		if err := a.svc.PutFile(ctx, "nginx.conf", content, 0o644, 0, 0); err != nil {
			t.Fatalf("PutFile() repeat = %v", err)
		}
	}
	after, err := a.store.Get(ctx, "nginx.conf")
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if after.Seq != first.Seq || !after.Version.Equal(first.Version) {
		t.Errorf("re-offering identical content created a new version: %v seq %d -> %v seq %d",
			first.Version, first.Seq, after.Version, after.Seq)
	}

	// A genuine change must still be recorded, or the short-circuit is too eager.
	if err := a.svc.PutFile(ctx, "nginx.conf", []byte("listen 443;"), 0o644, 0, 0); err != nil {
		t.Fatalf("PutFile() change = %v", err)
	}
	changed, err := a.store.Get(ctx, "nginx.conf")
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if changed.Seq == first.Seq {
		t.Error("a real content change was swallowed by the idempotence check")
	}
	// PrevHash must record what was replaced; conflict detection depends on it.
	if changed.PrevHash != first.Hash {
		t.Errorf("PrevHash = %q, want the replaced hash %q", changed.PrevHash, first.Hash)
	}

	// A mode-only change is a real change too.
	if err := a.svc.PutFile(ctx, "nginx.conf", []byte("listen 443;"), 0o600, 0, 0); err != nil {
		t.Fatalf("PutFile() chmod = %v", err)
	}
	moded, err := a.store.Get(ctx, "nginx.conf")
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if moded.Mode != 0o600 || moded.Seq == changed.Seq {
		t.Errorf("mode-only change was not recorded: mode %#o seq %d", moded.Mode, moded.Seq)
	}
}

// TestReplicationCarriesContentAndMetadata is the basic end-to-end: a write on
// one node reaches another, with permissions and ownership intact.
func TestReplicationCarriesContentAndMetadata(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	a, b, _ := cluster2(t)

	if err := a.svc.PutDir(ctx, "nginx", 0o755, 0, 0); err != nil {
		t.Fatalf("PutDir() = %v", err)
	}
	if err := a.svc.PutFile(ctx, "nginx/nginx.conf", []byte("worker_processes 4;"), 0o640, 33, 33); err != nil {
		t.Fatalf("PutFile() = %v", err)
	}

	if err := b.svc.syncPeer(ctx, "a", "a"); err != nil {
		t.Fatalf("syncPeer() = %v", err)
	}

	if got := contentAt(t, b, "nginx/nginx.conf"); got != "worker_processes 4;" {
		t.Errorf("replicated content = %q", got)
	}
	got, err := b.store.Get(ctx, "nginx/nginx.conf")
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if got.Mode != 0o640 || got.UID != 33 || got.GID != 33 {
		t.Errorf("replicated mode/uid/gid = %#o/%d/%d, want 0640/33/33", got.Mode, got.UID, got.GID)
	}
	// Empty directories are load-bearing in config trees (conf.d), so they
	// replicate as entries rather than being implied by their children.
	dir, err := b.store.Get(ctx, "nginx")
	if err != nil {
		t.Fatalf("directory did not replicate: %v", err)
	}
	if dir.Kind != core.KindDir {
		t.Errorf("replicated kind = %v, want dir", dir.Kind)
	}

	// Syncing again must be a no-op — the digests now agree.
	before, err := b.store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats() = %v", err)
	}
	if err := b.svc.syncPeer(ctx, "a", "a"); err != nil {
		t.Fatalf("second syncPeer() = %v", err)
	}
	after, err := b.store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats() = %v", err)
	}
	if after.Head != before.Head {
		t.Errorf("a redundant sync rewrote entries: head %d -> %d", before.Head, after.Head)
	}
}

// TestConcurrentEditProducesConflictCopy is the partition scenario: two nodes
// edit the same file while unable to see each other. One version wins the path;
// the other must survive under a conflict name rather than vanishing.
func TestConcurrentEditProducesConflictCopy(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	a, b, _ := cluster2(t)

	// Both start from the same base.
	if err := a.svc.PutFile(ctx, "app.conf", []byte("base"), 0o644, 0, 0); err != nil {
		t.Fatalf("PutFile() = %v", err)
	}
	if err := b.svc.syncPeer(ctx, "a", "a"); err != nil {
		t.Fatalf("syncPeer() = %v", err)
	}

	// The partition: each edits its own copy, neither seeing the other.
	if err := a.svc.PutFile(ctx, "app.conf", []byte("edited on A"), 0o644, 0, 0); err != nil {
		t.Fatalf("a.PutFile() = %v", err)
	}
	if err := b.svc.PutFile(ctx, "app.conf", []byte("edited on B"), 0o644, 0, 0); err != nil {
		t.Fatalf("b.PutFile() = %v", err)
	}

	// The partition heals. Both nodes pull from each other, as the anti-entropy
	// loop does — the conflict is necessarily detected on whichever node holds
	// the losing version, and only that node can preserve it, so a one-way sync
	// would not be a fair test of convergence.
	for range 2 {
		if err := b.svc.syncPeer(ctx, "a", "a"); err != nil {
			t.Fatalf("b.syncPeer() = %v", err)
		}
		if err := a.svc.syncPeer(ctx, "b", "b"); err != nil {
			t.Fatalf("a.syncPeer() = %v", err)
		}
	}

	// Both nodes must end up with the same tree...
	da, _, _, err := a.store.Digest(ctx)
	if err != nil {
		t.Fatalf("a.Digest() = %v", err)
	}
	db, _, _, err := b.store.Digest(ctx)
	if err != nil {
		t.Fatalf("b.Digest() = %v", err)
	}
	if da != db {
		t.Fatal("the nodes did not converge after the partition healed")
	}

	// ...and that tree must contain exactly one conflict copy, on both nodes,
	// so an operator sees the problem on whichever machine they log into.
	for _, n := range []*node{a, b} {
		conflicts, err := n.svc.Conflicts(ctx)
		if err != nil {
			t.Fatalf("%s: Conflicts() = %v", n.name, err)
		}
		if len(conflicts) != 1 {
			t.Fatalf("%s: Conflicts() = %v, want exactly one conflict copy", n.name, conflicts)
		}
		if !core.IsConflictName(conflicts[0]) {
			t.Errorf("%s: path %q is not recognisable as a conflict copy", n.name, conflicts[0])
		}

		// Neither edit may be lost: one holds the path, the other the copy.
		winner := contentAt(t, n, "app.conf")
		loser := contentAt(t, n, conflicts[0])
		if winner == loser {
			t.Fatalf("%s: conflict copy duplicates the winner (%q) instead of preserving the loser",
				n.name, winner)
		}
		both := winner + "|" + loser
		if !strings.Contains(both, "edited on A") || !strings.Contains(both, "edited on B") {
			t.Errorf("%s: an edit was lost: winner=%q loser=%q", n.name, winner, loser)
		}
	}
}

// TestFastForwardDoesNotConflict is the counterweight to the test above: an
// ordinary sequential edit must not litter the tree with conflict copies.
func TestFastForwardDoesNotConflict(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	a, b, _ := cluster2(t)

	if err := a.svc.PutFile(ctx, "app.conf", []byte("v1"), 0o644, 0, 0); err != nil {
		t.Fatalf("PutFile() = %v", err)
	}
	if err := b.svc.syncPeer(ctx, "a", "a"); err != nil {
		t.Fatalf("syncPeer() = %v", err)
	}
	// A edits on top of what B already has, and B pulls again.
	if err := a.svc.PutFile(ctx, "app.conf", []byte("v2"), 0o644, 0, 0); err != nil {
		t.Fatalf("PutFile() = %v", err)
	}
	if err := b.svc.syncPeer(ctx, "a", "a"); err != nil {
		t.Fatalf("syncPeer() = %v", err)
	}

	conflicts, err := b.svc.Conflicts(ctx)
	if err != nil {
		t.Fatalf("Conflicts() = %v", err)
	}
	if len(conflicts) != 0 {
		t.Errorf("a plain sequential edit produced conflict copies: %v", conflicts)
	}
	if got := contentAt(t, b, "app.conf"); got != "v2" {
		t.Errorf("content = %q, want v2", got)
	}
}

// TestDeletionReachesAnAbsentNode is the requirement that a node which was down
// during a deletion does not resurrect the file when it comes back.
func TestDeletionReachesAnAbsentNode(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	a, b, _ := cluster2(t)

	if err := a.svc.PutFile(ctx, "old.conf", []byte("obsolete"), 0o644, 0, 0); err != nil {
		t.Fatalf("PutFile() = %v", err)
	}
	if err := b.svc.syncPeer(ctx, "a", "a"); err != nil {
		t.Fatalf("syncPeer() = %v", err)
	}

	// B goes away; A deletes the file.
	if err := a.svc.Delete(ctx, "old.conf"); err != nil {
		t.Fatalf("Delete() = %v", err)
	}

	// B returns and syncs.
	if err := b.svc.syncPeer(ctx, "a", "a"); err != nil {
		t.Fatalf("syncPeer() on rejoin = %v", err)
	}
	got, err := b.store.Get(ctx, "old.conf")
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if !got.Deleted {
		t.Fatal("the deletion did not reach the returning node; it would resurrect the file")
	}

	// And B must not push it back to A as a live entry.
	if err := a.svc.syncPeer(ctx, "b", "b"); err != nil {
		t.Fatalf("syncPeer() back = %v", err)
	}
	back, err := a.store.Get(ctx, "old.conf")
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if !back.Deleted {
		t.Fatal("the returning node resurrected a file the cluster had deleted")
	}
}

// TestNewNodeGetsTheWholeTree is the "mirror to new everything" requirement:
// a node that has never seen the cluster receives all of it.
func TestNewNodeGetsTheWholeTree(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	a, b, _ := cluster2(t)

	for i := range 50 {
		path := fmt.Sprintf("conf.d/%02d.conf", i)
		if err := a.svc.PutFile(ctx, path, fmt.Appendf(nil, "setting %d", i), 0o644, 0, 0); err != nil {
			t.Fatalf("PutFile(%s) = %v", path, err)
		}
	}

	if err := b.svc.syncPeer(ctx, "a", "a"); err != nil {
		t.Fatalf("syncPeer() = %v", err)
	}

	da, _, _, err := a.store.Digest(ctx)
	if err != nil {
		t.Fatalf("a.Digest() = %v", err)
	}
	db, _, _, err := b.store.Digest(ctx)
	if err != nil {
		t.Fatalf("b.Digest() = %v", err)
	}
	if da != db {
		t.Fatal("a brand-new node did not converge onto the existing tree")
	}
	if got := contentAt(t, b, "conf.d/07.conf"); got != "setting 7" {
		t.Errorf("content = %q, want %q", got, "setting 7")
	}
}

// TestRemoteEntriesAreValidated covers hostile input from a cluster member.
// Each entry here is something a compromised peer could put in its manifest.
func TestRemoteEntriesAreValidated(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	_, b, _ := cluster2(t)

	goodHash := core.HashContent([]byte("x"))
	bad := []struct {
		name string
		meta core.Meta
	}{
		{"path traversal", core.Meta{Path: "../../etc/shadow", Kind: core.KindFile,
			Hash: goodHash, Version: ver(100, "evil")}},
		{"absolute path", core.Meta{Path: "/etc/shadow", Kind: core.KindFile,
			Hash: goodHash, Version: ver(100, "evil")}},
		{"unknown kind", core.Meta{Path: "a.conf", Kind: core.Kind(99),
			Version: ver(100, "evil")}},
		{"no origin", core.Meta{Path: "a.conf", Kind: core.KindFile,
			Hash: goodHash, Version: core.Version{HLC: core.Timestamp{Wall: 100}}}},
		{"oversized", core.Meta{Path: "a.conf", Kind: core.KindFile, Hash: goodHash,
			Size: 1 << 30, Version: ver(100, "evil")}},
		{"malformed hash", core.Meta{Path: "a.conf", Kind: core.KindFile,
			Hash: "nothex", Version: ver(100, "evil")}},
		{"directory carrying content", core.Meta{Path: "d", Kind: core.KindDir,
			Hash: goodHash, Version: ver(100, "evil")}},
		{"timestamp far in the future", core.Meta{Path: "a.conf", Kind: core.KindFile,
			Hash: goodHash, Version: core.Version{
				HLC:    core.Timestamp{Wall: time.Now().Add(48 * time.Hour).UnixNano()},
				Origin: "evil"}}},
	}

	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			// applyRemote must not abort the page; it logs and skips.
			if _, err := b.svc.applyRemote(ctx, "a", []core.Meta{tc.meta}); err != nil {
				t.Fatalf("applyRemote() = %v, want the bad entry skipped rather than fatal", err)
			}
			if _, err := b.store.Get(ctx, tc.meta.Path); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("a rejected entry reached the store: Get(%q) = %v", tc.meta.Path, err)
			}
		})
	}

	// Positive control: a well-formed entry alongside bad ones still lands, so
	// one hostile row cannot stall replication of everything else.
	if _, err := b.store.PutBlob(ctx, []byte("x")); err != nil {
		t.Fatalf("PutBlob() = %v", err)
	}
	good := core.Meta{Path: "fine.conf", Kind: core.KindFile, Hash: goodHash,
		Size: 1, Mode: 0o644, Version: ver(time.Now().UnixNano(), "peer")}
	mixed := []core.Meta{bad[0].meta, good, bad[4].meta}
	if _, err := b.svc.applyRemote(ctx, "a", mixed); err != nil {
		t.Fatalf("applyRemote(mixed) = %v", err)
	}
	if _, err := b.store.Get(ctx, "fine.conf"); err != nil {
		t.Errorf("a valid entry was dropped because bad ones shared its page: %v", err)
	}
}

func TestLocalWritesEnforceLimits(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	a, _, _ := cluster2(t)

	if err := a.svc.PutFile(ctx, "big.conf", make([]byte, (1<<20)+1), 0o644, 0, 0); !errors.Is(err, ErrTooLarge) {
		t.Errorf("PutFile(oversized) = %v, want ErrTooLarge", err)
	}
	if err := a.svc.PutFile(ctx, "../escape.conf", []byte("x"), 0o644, 0, 0); !errors.Is(err, core.ErrBadPath) {
		t.Errorf("PutFile(traversal) = %v, want ErrBadPath", err)
	}
	// Type bits offered locally are stripped, so a mirror walk of a strange
	// filesystem cannot smuggle one into the cluster.
	if err := a.svc.PutFile(ctx, "ok.conf", []byte("x"), 0o644|uint32(0x8000000), 0, 0); err != nil {
		t.Fatalf("PutFile() = %v", err)
	}
	m, err := a.store.Get(ctx, "ok.conf")
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if m.Mode != 0o644 {
		t.Errorf("stored mode = %#o, want the type bits stripped to 0644", m.Mode)
	}
}

// TestEntryDeferredWhenContentUnavailable covers the ordinary convergence race
// where a peer advertises an entry whose blob we cannot fetch yet. The entry
// must be deferred, not stored pointing at content we do not have.
func TestEntryDeferredWhenContentUnavailable(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	a, b, client := cluster2(t)

	if err := a.svc.PutFile(ctx, "a.conf", []byte("content"), 0o644, 0, 0); err != nil {
		t.Fatalf("PutFile() = %v", err)
	}
	client.fail["a"] = true

	if err := b.svc.syncPeer(ctx, "a", "a"); err != nil {
		t.Fatalf("syncPeer() = %v", err)
	}
	if _, err := b.store.Get(ctx, "a.conf"); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("stored an entry whose content could not be fetched")
	}

	// Once the peer can serve it, the next pass picks it up. The bookmark was
	// advanced, so this exercises the digest mismatch path rather than a replay.
	client.fail["a"] = false
	if err := b.svc.syncPeer(ctx, "a", "a"); err != nil {
		t.Fatalf("retry syncPeer() = %v", err)
	}
	if got := contentAt(t, b, "a.conf"); got != "content" {
		t.Errorf("content after retry = %q, want %q", got, "content")
	}
}

// TestPeerRebuildRestartsTheFeed covers a peer whose database was wiped and
// which restarted its seq numbering. Our bookmark points into a feed that no
// longer exists, so we must re-read from the beginning rather than sit at a
// bookmark past its head forever.
func TestPeerRebuildRestartsTheFeed(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	a, b, client := cluster2(t)

	for i := range 5 {
		if err := a.svc.PutFile(ctx, fmt.Sprintf("f%d.conf", i), []byte("v1"), 0o644, 0, 0); err != nil {
			t.Fatalf("PutFile() = %v", err)
		}
	}
	if err := b.svc.syncPeer(ctx, "a", "a"); err != nil {
		t.Fatalf("syncPeer() = %v", err)
	}
	if seq, err := b.store.PeerSeq(ctx, "a"); err != nil || seq == 0 {
		t.Fatalf("PeerSeq() = %d, %v; want a recorded bookmark", seq, err)
	}

	// A is rebuilt from nothing, keeping its name, and gets one new file.
	fresh, err := store.Open(ctx, filepath.Join(t.TempDir(), "a-rebuilt.db"))
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	t.Cleanup(func() { _ = fresh.Close() })
	client.nodes["a"] = fresh
	rebuilt := New(Config{
		Node: "a", Store: fresh, Peer: client, MaxFileSize: 1 << 20,
		TombstoneTTL: time.Hour, SyncInterval: time.Hour, JanitorInterval: time.Hour,
		Log: slog.New(slog.DiscardHandler),
	})
	if err := rebuilt.PutFile(ctx, "after-rebuild.conf", []byte("new"), 0o644, 0, 0); err != nil {
		t.Fatalf("PutFile() = %v", err)
	}

	if err := b.svc.syncPeer(ctx, "a", "a"); err != nil {
		t.Fatalf("syncPeer() after rebuild = %v", err)
	}
	if got := contentAt(t, b, "after-rebuild.conf"); got != "new" {
		t.Errorf("content = %q; the stale bookmark hid the rebuilt peer's writes", got)
	}
	_ = a // a's original store stays open for cleanup
}

func TestJanitorCollectsExpiredTombstonesAndOrphanBlobs(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "gc.db"))
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	svc := New(Config{
		Node: "gc", Store: st, MaxFileSize: 1 << 20,
		// A negative TTL puts the horizon in the future, so a tombstone written
		// now is already expired and the collector runs without a sleep.
		TombstoneTTL: -time.Hour,
		SyncInterval: time.Hour, JanitorInterval: time.Hour,
		Log: slog.New(slog.DiscardHandler),
	})

	if err := svc.PutFile(ctx, "doomed.conf", []byte("bytes"), 0o644, 0, 0); err != nil {
		t.Fatalf("PutFile() = %v", err)
	}
	if err := svc.PutFile(ctx, "kept.conf", []byte("keep me"), 0o644, 0, 0); err != nil {
		t.Fatalf("PutFile() = %v", err)
	}
	if err := svc.Delete(ctx, "doomed.conf"); err != nil {
		t.Fatalf("Delete() = %v", err)
	}

	svc.collect(ctx)

	if _, err := st.Get(ctx, "doomed.conf"); !errors.Is(err, store.ErrNotFound) {
		t.Error("expired tombstone survived the janitor")
	}
	if _, err := st.Get(ctx, "kept.conf"); err != nil {
		t.Errorf("the janitor removed a live entry: %v", err)
	}
	if got := string(mustBlob(t, st, "kept.conf")); got != "keep me" {
		t.Errorf("live content was collected: %q", got)
	}
	stats, err := st.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats() = %v", err)
	}
	if stats.Blobs != 1 {
		t.Errorf("Blobs = %d, want 1 — the deleted file's blob was not collected", stats.Blobs)
	}
}

// mustBlob reads the content behind a path.
func mustBlob(t *testing.T, st *store.Store, path string) []byte {
	t.Helper()
	m, err := st.Get(t.Context(), path)
	if err != nil {
		t.Fatalf("Get(%q) = %v", path, err)
	}
	b, err := st.Blob(t.Context(), m.Hash)
	if err != nil {
		t.Fatalf("Blob for %q = %v", path, err)
	}
	return b
}

// TestSubscribeWakesProjections pins that a write nudges the mirror, and that a
// slow subscriber is never able to block the writer.
func TestSubscribeWakesProjections(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	a, _, _ := cluster2(t)

	ch, unsubscribe := a.svc.Subscribe()
	defer unsubscribe()

	if err := a.svc.PutFile(ctx, "a.conf", []byte("x"), 0o644, 0, 0); err != nil {
		t.Fatalf("PutFile() = %v", err)
	}
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("a write did not wake the subscriber; the mirror would never update")
	}

	// Many writes with nobody reading must not block the writer, because the
	// signal is a nudge and the subscriber re-reads the store when it wakes.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 20 {
			_ = a.svc.PutFile(ctx, fmt.Sprintf("f%d.conf", i), []byte("x"), 0o644, 0, 0)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("writer blocked on a subscriber that was not reading")
	}
}
