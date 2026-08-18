package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"
)

// open returns a Store backed by a fresh database in the test's temp dir.
func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.Context(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close() = %v", err)
		}
	})
	return s
}

// meta builds a plausible file entry for tests.
func meta(path, hash string, wall int64, origin string) core.Meta {
	return core.Meta{
		Path:    path,
		Kind:    core.KindFile,
		Hash:    hash,
		Size:    int64(len(hash)),
		Mode:    0o644,
		UID:     0,
		GID:     0,
		Version: core.Version{HLC: core.Timestamp{Wall: wall}, Origin: origin},
	}
}

func TestApplyAndGetRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := open(t)

	want := meta("nginx/nginx.conf", "abc123", 100, "node1")
	want.PrevHash = "old999"
	want.UID, want.GID, want.Mode = 33, 33, 0o600
	if _, err := s.Apply(ctx, want); err != nil {
		t.Fatalf("Apply() = %v", err)
	}

	got, err := s.Get(ctx, want.Path)
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}
	// Seq is assigned by the store, so compare it separately from the rest.
	if got.Seq != 1 {
		t.Errorf("Seq = %d, want 1 for the first write", got.Seq)
	}
	got.Seq = 0
	if got != want {
		t.Errorf("round trip changed the entry:\n got %+v\nwant %+v", got, want)
	}

	if _, err := s.Get(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(missing) error = %v, want ErrNotFound", err)
	}
}

// TestApplyAdvancesFeedOnUpdate is the regression test for the reason seq is a
// dedicated counter rather than a rowid: a peer must learn that a file it
// already knows about has changed.
func TestApplyAdvancesFeedOnUpdate(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := open(t)

	if _, err := s.Apply(ctx, meta("a.conf", "v1", 100, "node1")); err != nil {
		t.Fatalf("Apply() = %v", err)
	}
	first, err := s.Get(ctx, "a.conf")
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}

	if _, err := s.Apply(ctx, meta("a.conf", "v2", 200, "node1")); err != nil {
		t.Fatalf("Apply() update = %v", err)
	}
	second, err := s.Get(ctx, "a.conf")
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}

	if second.Seq <= first.Seq {
		t.Fatalf("updating an existing path did not advance the feed: seq %d -> %d", first.Seq, second.Seq)
	}
	if second.Hash != "v2" {
		t.Errorf("Hash = %q, want the update to have replaced the row", second.Hash)
	}
	// Upsert, not append: one path is one row.
	all, err := s.All(ctx)
	if err != nil {
		t.Fatalf("All() = %v", err)
	}
	if len(all) != 1 {
		t.Errorf("All() returned %d rows, want 1 — the update appended instead of replacing", len(all))
	}
}

func TestManifestSinceIsAFeed(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := open(t)

	batch := []core.Meta{
		meta("a.conf", "h1", 100, "node1"),
		meta("b.conf", "h2", 101, "node1"),
		meta("c.conf", "h3", 102, "node1"),
	}
	if _, err := s.Apply(ctx, batch...); err != nil {
		t.Fatalf("Apply(batch) = %v", err)
	}

	// since=0 is the whole tree, which is what a brand-new node asks for.
	full, err := s.ManifestSince(ctx, 0, 100)
	if err != nil {
		t.Fatalf("ManifestSince(0) = %v", err)
	}
	if len(full) != 3 {
		t.Fatalf("ManifestSince(0) returned %d entries, want 3", len(full))
	}
	for i, m := range full {
		if want := int64(i + 1); m.Seq != want {
			t.Errorf("entry %d has seq %d, want %d — batch seqs are not contiguous", i, m.Seq, want)
		}
	}

	// A delta pull returns only what the peer has not seen.
	delta, err := s.ManifestSince(ctx, 2, 100)
	if err != nil {
		t.Fatalf("ManifestSince(2) = %v", err)
	}
	if len(delta) != 1 || delta[0].Path != "c.conf" {
		t.Errorf("ManifestSince(2) = %+v, want just c.conf", delta)
	}

	// Limit bounds a full sync so one manifest cannot be arbitrarily large.
	page, err := s.ManifestSince(ctx, 0, 2)
	if err != nil {
		t.Fatalf("ManifestSince(0, limit 2) = %v", err)
	}
	if len(page) != 2 {
		t.Errorf("limit ignored: got %d entries, want 2", len(page))
	}
}

// TestDigestIgnoresLocalSeq is the property anti-entropy depends on: two nodes
// holding identical trees must produce identical digests even though their
// local feed positions differ, or they resync forever.
func TestDigestIgnoresLocalSeq(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	a, b := open(t), open(t)
	entries := []core.Meta{
		meta("a.conf", "h1", 100, "node1"),
		meta("b.conf", "h2", 101, "node2"),
	}

	// Node a learns both at once; node b learns them one at a time and in the
	// other order, so its seq numbering differs.
	if _, err := a.Apply(ctx, entries...); err != nil {
		t.Fatalf("a.Apply() = %v", err)
	}
	if _, err := b.Apply(ctx, entries[1]); err != nil {
		t.Fatalf("b.Apply() = %v", err)
	}
	if _, err := b.Apply(ctx, entries[0]); err != nil {
		t.Fatalf("b.Apply() = %v", err)
	}

	da, na, _, err := a.Digest(ctx)
	if err != nil {
		t.Fatalf("a.Digest() = %v", err)
	}
	db, nb, _, err := b.Digest(ctx)
	if err != nil {
		t.Fatalf("b.Digest() = %v", err)
	}
	if da != db {
		t.Errorf("identical trees produced different digests:\n a=%s\n b=%s", da, db)
	}
	if na != 2 || nb != 2 {
		t.Errorf("entry counts = %d and %d, want 2 each", na, nb)
	}

	// A real difference must change the digest, or the check is worthless.
	if _, err := b.Apply(ctx, meta("a.conf", "changed", 200, "node2")); err != nil {
		t.Fatalf("b.Apply() = %v", err)
	}
	db2, _, _, err := b.Digest(ctx)
	if err != nil {
		t.Fatalf("b.Digest() = %v", err)
	}
	if da == db2 {
		t.Error("digest did not change after the tree diverged")
	}
}

func TestBlobsAreContentAddressedAndDeduplicated(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := open(t)

	content := []byte("server { listen 80; }")
	hash, err := s.PutBlob(ctx, content)
	if err != nil {
		t.Fatalf("PutBlob() = %v", err)
	}
	if hash != core.HashContent(content) {
		t.Errorf("PutBlob() = %q, want the content hash %q", hash, core.HashContent(content))
	}

	// Re-storing identical content is a no-op, not a duplicate row or an error.
	if _, err := s.PutBlob(ctx, content); err != nil {
		t.Fatalf("PutBlob() second call = %v", err)
	}
	st, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats() = %v", err)
	}
	if st.Blobs != 1 {
		t.Errorf("Blobs = %d after storing the same content twice, want 1", st.Blobs)
	}

	got, err := s.Blob(ctx, hash)
	if err != nil {
		t.Fatalf("Blob() = %v", err)
	}
	if string(got) != string(content) {
		t.Errorf("Blob() = %q, want %q", got, content)
	}

	has, err := s.HasBlob(ctx, hash)
	if err != nil || !has {
		t.Errorf("HasBlob(present) = %v, %v; want true, nil", has, err)
	}
	has, err = s.HasBlob(ctx, "0000")
	if err != nil || has {
		t.Errorf("HasBlob(absent) = %v, %v; want false, nil", has, err)
	}
	if _, err := s.Blob(ctx, "0000"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Blob(absent) error = %v, want ErrNotFound", err)
	}
}

// TestGCTombstonesRespectsHorizon pins the rule that a tombstone younger than
// the TTL must survive. Collecting one early lets a node that was offline
// resurrect the file it describes.
func TestGCTombstonesRespectsHorizon(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := open(t)

	now := time.Now().UnixNano()
	old := core.Meta{
		Path: "gone.conf", Kind: core.KindFile, Mode: 0o644,
		Version:   core.Version{HLC: core.Timestamp{Wall: 100}, Origin: "node1"},
		Deleted:   true,
		DeletedAt: now - int64(48*time.Hour),
	}
	recent := core.Meta{
		Path: "justwent.conf", Kind: core.KindFile, Mode: 0o644,
		Version:   core.Version{HLC: core.Timestamp{Wall: 101}, Origin: "node1"},
		Deleted:   true,
		DeletedAt: now - int64(time.Minute),
	}
	live := meta("alive.conf", "h1", 102, "node1")
	if _, err := s.Apply(ctx, old, recent, live); err != nil {
		t.Fatalf("Apply() = %v", err)
	}

	horizon := now - int64(24*time.Hour)
	n, err := s.GCTombstones(ctx, horizon)
	if err != nil {
		t.Fatalf("GCTombstones() = %v", err)
	}
	if n != 1 {
		t.Fatalf("GCTombstones() collected %d, want exactly the one past the horizon", n)
	}
	if _, err := s.Get(ctx, "gone.conf"); !errors.Is(err, ErrNotFound) {
		t.Error("expired tombstone survived collection")
	}
	if _, err := s.Get(ctx, "justwent.conf"); err != nil {
		t.Errorf("tombstone inside the horizon was collected early: %v", err)
	}
	if _, err := s.Get(ctx, "alive.conf"); err != nil {
		t.Errorf("GCTombstones() deleted a live entry: %v", err)
	}
}

func TestGCBlobsKeepsReferencedContent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := open(t)

	kept, err := s.PutBlob(ctx, []byte("still referenced"))
	if err != nil {
		t.Fatalf("PutBlob() = %v", err)
	}
	orphan, err := s.PutBlob(ctx, []byte("nothing points here"))
	if err != nil {
		t.Fatalf("PutBlob() = %v", err)
	}
	if _, err := s.Apply(ctx, meta("a.conf", kept, 100, "node1")); err != nil {
		t.Fatalf("Apply() = %v", err)
	}

	n, err := s.GCBlobs(ctx)
	if err != nil {
		t.Fatalf("GCBlobs() = %v", err)
	}
	if n != 1 {
		t.Fatalf("GCBlobs() collected %d blobs, want 1", n)
	}
	if _, err := s.Blob(ctx, kept); err != nil {
		t.Errorf("GCBlobs() collected a referenced blob: %v", err)
	}
	if _, err := s.Blob(ctx, orphan); !errors.Is(err, ErrNotFound) {
		t.Error("orphan blob survived collection")
	}
}

func TestPeerSeqSurvivesRestart(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()
	path := filepath.Join(dir, "peers.db")

	s, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	// A peer we have never spoken to reads as 0, which asks for its whole tree.
	if seq, err := s.PeerSeq(ctx, "node2"); err != nil || seq != 0 {
		t.Fatalf("PeerSeq(unknown) = %d, %v; want 0, nil", seq, err)
	}
	if err := s.SetPeerSeq(ctx, "node2", "10.0.0.2:7947", 42, 1700); err != nil {
		t.Fatalf("SetPeerSeq() = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	// Reopening must resume the delta rather than re-pulling the whole tree.
	s2, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen = %v", err)
	}
	defer s2.Close()

	seq, err := s2.PeerSeq(ctx, "node2")
	if err != nil {
		t.Fatalf("PeerSeq() after restart = %v", err)
	}
	if seq != 42 {
		t.Errorf("PeerSeq() after restart = %d, want 42", seq)
	}

	peers, err := s2.Peers(ctx)
	if err != nil {
		t.Fatalf("Peers() = %v", err)
	}
	if len(peers) != 1 || peers[0].Addr != "10.0.0.2:7947" {
		t.Errorf("Peers() = %+v, want the recorded address preserved", peers)
	}
}

func TestOpenIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "twice.db")

	s, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	if _, err := s.Apply(ctx, meta("a.conf", "h1", 100, "node1")); err != nil {
		t.Fatalf("Apply() = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	// Re-running migrations over an existing database must not wipe it.
	s2, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("second Open() = %v", err)
	}
	defer s2.Close()
	if _, err := s2.Get(ctx, "a.conf"); err != nil {
		t.Errorf("data did not survive reopening: %v", err)
	}
}
