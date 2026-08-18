package mirror

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/store"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/syncer"
)

// harness builds a mirror over a real syncer and store, so the tests exercise
// the production promote/render path rather than a stub of it.
func harness(t *testing.T) (*Mirror, *syncer.Service, *store.Store, string) {
	t.Helper()
	ctx := t.Context()

	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatalf("store.Open() = %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	svc := syncer.New(syncer.Config{
		Node: "node1", Store: st, MaxFileSize: 1 << 20,
		TombstoneTTL: time.Hour, SyncInterval: time.Hour, JanitorInterval: time.Hour,
		Log: slog.New(slog.DiscardHandler),
	})

	dir := filepath.Join(t.TempDir(), "cluster")
	m, err := Open(Config{
		Dir: dir, Tree: svc,
		ScanInterval: time.Hour, Settle: 20 * time.Millisecond,
		Log: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	return m, svc, st, dir
}

func write(t *testing.T, dir, rel, content string, mode os.FileMode) {
	t.Helper()
	full := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("MkdirAll() = %v", err)
	}
	if err := os.WriteFile(full, []byte(content), mode); err != nil {
		t.Fatalf("WriteFile(%s) = %v", rel, err)
	}
}

func read(t *testing.T, dir, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, rel))
	if err != nil {
		t.Fatalf("ReadFile(%s) = %v", rel, err)
	}
	return string(b)
}

// TestScanAdoptsExistingFiles is the "operator populated /etc/cluster before
// starting the daemon" case. Those files must be taken into the cluster, not
// wiped by a render that has never heard of them.
func TestScanAdoptsExistingFiles(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	m, svc, _, dir := harness(t)

	write(t, dir, "nginx.conf", "listen 80;", 0o640)
	write(t, dir, "conf.d/app.conf", "debug = true", 0o644)
	if err := os.MkdirAll(filepath.Join(dir, "empty.d"), 0o755); err != nil {
		t.Fatalf("MkdirAll() = %v", err)
	}

	if err := m.scan(ctx); err != nil {
		t.Fatalf("scan() = %v", err)
	}

	entries, err := svc.Live(ctx)
	if err != nil {
		t.Fatalf("Live() = %v", err)
	}
	got := make(map[string]core.Meta, len(entries))
	for _, e := range entries {
		got[e.Path] = e
	}
	for _, want := range []string{"nginx.conf", "conf.d", "conf.d/app.conf", "empty.d"} {
		if _, ok := got[want]; !ok {
			t.Errorf("%q was not adopted into the cluster", want)
		}
	}
	// An empty directory is often load-bearing in a config tree, so it must
	// replicate in its own right.
	if got["empty.d"].Kind != core.KindDir {
		t.Error("empty directory was not adopted as a directory entry")
	}
	if got["nginx.conf"].Mode != 0o640 {
		t.Errorf("adopted mode = %#o, want 0640", got["nginx.conf"].Mode)
	}

	// The render that follows must leave those files alone, since nothing has
	// tombstoned them.
	if err := m.render(ctx); err != nil {
		t.Fatalf("render() = %v", err)
	}
	if got := read(t, dir, "nginx.conf"); got != "listen 80;" {
		t.Errorf("render destroyed an adopted file: %q", got)
	}
}

// TestRenderWritesTheTreeToDisk is the receiving side: entries that arrived
// from a peer appear on disk with the right content and permissions.
func TestRenderWritesTheTreeToDisk(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	m, svc, _, dir := harness(t)

	if err := svc.PutDir(ctx, "nginx", 0o750, 0, 0); err != nil {
		t.Fatalf("PutDir() = %v", err)
	}
	if err := svc.PutFile(ctx, "nginx/nginx.conf", []byte("worker_processes 4;"), 0o600, 0, 0); err != nil {
		t.Fatalf("PutFile() = %v", err)
	}

	if err := m.render(ctx); err != nil {
		t.Fatalf("render() = %v", err)
	}

	if got := read(t, dir, "nginx/nginx.conf"); got != "worker_processes 4;" {
		t.Errorf("rendered content = %q", got)
	}
	info, err := os.Stat(filepath.Join(dir, "nginx/nginx.conf"))
	if err != nil {
		t.Fatalf("Stat() = %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("rendered mode = %#o, want 0600 — a secret-bearing config must not widen", info.Mode().Perm())
	}
	dirInfo, err := os.Stat(filepath.Join(dir, "nginx"))
	if err != nil {
		t.Fatalf("Stat(dir) = %v", err)
	}
	if dirInfo.Mode().Perm() != 0o750 {
		t.Errorf("rendered dir mode = %#o, want 0750", dirInfo.Mode().Perm())
	}

	// No scratch files may be left behind by the atomic write.
	assertNoTempFiles(t, dir)
}

// assertNoTempFiles fails if any atomic-write scratch file survived.
func assertNoTempFiles(t *testing.T, dir string) {
	t.Helper()
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && len(d.Name()) > len(tmpPrefix) && d.Name()[:len(tmpPrefix)] == tmpPrefix {
			t.Errorf("left a scratch file behind: %s", p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
}

// TestRenderIsIdempotent is the other half of the echo-loop defence. Render
// must not rewrite a file that is already correct, because every rewrite fires
// an inotify event, which promotes, which changes the tree, which renders.
func TestRenderIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	m, svc, st, dir := harness(t)

	if err := svc.PutFile(ctx, "a.conf", []byte("x"), 0o644, 0, 0); err != nil {
		t.Fatalf("PutFile() = %v", err)
	}
	if err := m.render(ctx); err != nil {
		t.Fatalf("render() = %v", err)
	}
	before, err := os.Stat(filepath.Join(dir, "a.conf"))
	if err != nil {
		t.Fatalf("Stat() = %v", err)
	}

	// Render, then scan, then render — the exact loop the run loop performs.
	for range 3 {
		if err := m.render(ctx); err != nil {
			t.Fatalf("render() = %v", err)
		}
		if err := m.scan(ctx); err != nil {
			t.Fatalf("scan() = %v", err)
		}
	}

	after, err := os.Stat(filepath.Join(dir, "a.conf"))
	if err != nil {
		t.Fatalf("Stat() = %v", err)
	}
	if !before.ModTime().Equal(after.ModTime()) {
		t.Error("render rewrote a file that was already correct; this is the echo loop")
	}

	// And the tree must not have gained versions from the round trip.
	entry, err := st.Get(ctx, "a.conf")
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if entry.Seq != 1 {
		t.Errorf("seq = %d after repeated render/scan cycles, want 1", entry.Seq)
	}
}

// TestLaterModificationWins covers the startup ambiguity that has no snapshot
// answer: the disk and the tree disagree about a file, and only their
// timestamps say which change happened last.
func TestLaterModificationWins(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	t.Run("an edit made while the daemon was stopped is adopted", func(t *testing.T) {
		t.Parallel()
		m, svc, _, dir := harness(t)

		if err := svc.PutFile(ctx, "a.conf", []byte("from the cluster"), 0o644, 0, 0); err != nil {
			t.Fatalf("PutFile() = %v", err)
		}
		if err := m.render(ctx); err != nil {
			t.Fatalf("render() = %v", err)
		}

		// The operator edits the file with the daemon down. Its mtime is now,
		// which is after the stored version's clock.
		write(t, dir, "a.conf", "edited offline", 0o644)

		if err := m.scan(ctx); err != nil {
			t.Fatalf("scan() = %v", err)
		}
		if err := m.render(ctx); err != nil {
			t.Fatalf("render() = %v", err)
		}

		if got := read(t, dir, "a.conf"); got != "edited offline" {
			t.Errorf("disk = %q, want the newer offline edit to have won", got)
		}
		e, err := svc.Get(ctx, "a.conf")
		if err != nil {
			t.Fatalf("Get() = %v", err)
		}
		content, err := svc.Content(ctx, e.Hash)
		if err != nil {
			t.Fatalf("Content() = %v", err)
		}
		if string(content) != "edited offline" {
			t.Errorf("tree = %q, want the offline edit promoted into the cluster", content)
		}
	})

	t.Run("a peer version newer than the local file overwrites it", func(t *testing.T) {
		t.Parallel()
		m, svc, _, dir := harness(t)

		// A stale local file, backdated to stand for one written long ago.
		write(t, dir, "a.conf", "stale local copy", 0o644)
		old := time.Now().Add(-time.Hour)
		if err := os.Chtimes(filepath.Join(dir, "a.conf"), old, old); err != nil {
			t.Fatalf("Chtimes() = %v", err)
		}

		// A peer's version, stamped now, arrives before the first scan.
		if err := svc.PutFile(ctx, "a.conf", []byte("newer from peer"), 0o644, 0, 0); err != nil {
			t.Fatalf("PutFile() = %v", err)
		}

		if err := m.scan(ctx); err != nil {
			t.Fatalf("scan() = %v", err)
		}
		if err := m.render(ctx); err != nil {
			t.Fatalf("render() = %v", err)
		}

		if got := read(t, dir, "a.conf"); got != "newer from peer" {
			t.Errorf("disk = %q, want the newer peer version to have won", got)
		}
		e, err := svc.Get(ctx, "a.conf")
		if err != nil {
			t.Fatalf("Get() = %v", err)
		}
		content, err := svc.Content(ctx, e.Hash)
		if err != nil {
			t.Fatalf("Content() = %v", err)
		}
		if string(content) != "newer from peer" {
			t.Errorf("tree = %q; the stale local file was promoted over a newer peer version", content)
		}
	})
}

// TestRenderRemovesTombstonedPathsOnly pins the destructive boundary: the
// cluster's deletions are applied to disk, and nothing else is.
func TestRenderRemovesTombstonedPathsOnly(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	m, svc, _, dir := harness(t)

	if err := svc.PutFile(ctx, "doomed.conf", []byte("bye"), 0o644, 0, 0); err != nil {
		t.Fatalf("PutFile() = %v", err)
	}
	if err := m.render(ctx); err != nil {
		t.Fatalf("render() = %v", err)
	}

	// A file the cluster has never heard of, sitting next to it.
	write(t, dir, "stranger.conf", "not ours", 0o644)

	if err := svc.Delete(ctx, "doomed.conf"); err != nil {
		t.Fatalf("Delete() = %v", err)
	}
	if err := m.render(ctx); err != nil {
		t.Fatalf("render() = %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, "doomed.conf")); !os.IsNotExist(err) {
		t.Error("a cluster deletion did not reach the disk")
	}
	if got := read(t, dir, "stranger.conf"); got != "not ours" {
		t.Error("render deleted a local file the cluster had no row for")
	}
}

// TestWatcherPromotesEditsAndDeletions is the live path: an edit made in the
// mirror directory reaches the cluster, and so does a removal.
func TestWatcherPromotesEditsAndDeletions(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	m, svc, st, dir := harness(t)

	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()

	write(t, dir, "watched.conf", "first version", 0o644)
	waitFor(t, "the new file to reach the cluster", func() bool {
		e, err := st.Get(ctx, "watched.conf")
		return err == nil && !e.Deleted
	})

	write(t, dir, "watched.conf", "second version", 0o644)
	waitFor(t, "the edit to reach the cluster", func() bool {
		e, err := st.Get(ctx, "watched.conf")
		if err != nil || e.Deleted {
			return false
		}
		content, err := svc.Content(ctx, e.Hash)
		return err == nil && string(content) == "second version"
	})

	if err := os.Remove(filepath.Join(dir, "watched.conf")); err != nil {
		t.Fatalf("Remove() = %v", err)
	}
	waitFor(t, "the deletion to reach the cluster", func() bool {
		e, err := st.Get(ctx, "watched.conf")
		return err == nil && e.Deleted
	})

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run() = %v", err)
	}
}

// TestWatcherFollowsNewSubdirectories is the regression test for a bug a live
// two-node smoke test found and the unit tests missed: everything above only
// ever touched a root-level file.
//
// inotify is not recursive, so a directory created after startup needs its own
// watch. Without one, changes inside it are invisible to the watcher — and
// since deletions are detected *only* from events, a file removed from that
// directory was never deleted from the cluster. Worse, the next render put it
// straight back, so the removal silently undid itself.
func TestWatcherFollowsNewSubdirectories(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	m, _, st, dir := harness(t)

	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()

	// A directory that did not exist when the watcher started.
	write(t, dir, "nginx/nginx.conf", "worker_processes 4;", 0o644)
	waitFor(t, "the file in the new subdirectory to reach the cluster", func() bool {
		e, err := st.Get(ctx, "nginx/nginx.conf")
		return err == nil && !e.Deleted
	})

	// The actual regression: removing it must be recorded as a deletion.
	if err := os.Remove(filepath.Join(dir, "nginx", "nginx.conf")); err != nil {
		t.Fatalf("Remove() = %v", err)
	}
	waitFor(t, "the deletion inside the subdirectory to reach the cluster", func() bool {
		e, err := st.Get(ctx, "nginx/nginx.conf")
		return err == nil && e.Deleted
	})

	// And it must stay deleted — a render that resurrects it is the same bug
	// wearing a different hat.
	waitFor(t, "the file to stay gone from disk", func() bool {
		_, err := os.Stat(filepath.Join(dir, "nginx", "nginx.conf"))
		return os.IsNotExist(err)
	})
	time.Sleep(200 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(dir, "nginx", "nginx.conf")); !os.IsNotExist(err) {
		t.Error("the deleted file reappeared on disk")
	}

	// Nesting deeper must work too, since each level needs its own watch.
	write(t, dir, "a/b/c/deep.conf", "deep", 0o644)
	waitFor(t, "a deeply nested file to reach the cluster", func() bool {
		e, err := st.Get(ctx, "a/b/c/deep.conf")
		return err == nil && !e.Deleted
	})
	if err := os.Remove(filepath.Join(dir, "a/b/c/deep.conf")); err != nil {
		t.Fatalf("Remove() = %v", err)
	}
	waitFor(t, "the deeply nested deletion to reach the cluster", func() bool {
		e, err := st.Get(ctx, "a/b/c/deep.conf")
		return err == nil && e.Deleted
	})

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run() = %v", err)
	}
}

// TestDeletionSurvivesConcurrentRender is the regression test for a race a live
// two-node run found and every unit test above missed.
//
// A user removes a file. The watcher holds the event for the settle delay. If a
// render fires inside that window it sees the path live in the tree and missing
// from disk, and writes it back — so when the event is finally processed the
// file exists again and is read as a modification rather than a removal. The
// deletion is then lost permanently, because the rescan deliberately never
// deletes.
//
// The scan interval here is far shorter than the settle delay specifically so
// that a render is virtually guaranteed to land in the window.
func TestDeletionSurvivesConcurrentRender(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	m, svc, st, dir := harness(t)
	// Renders fire constantly; the settle delay is long enough that one is
	// certain to hit while a removal is pending.
	m.cfg.ScanInterval = 5 * time.Millisecond
	m.cfg.Settle = 150 * time.Millisecond

	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()

	// Several files, in a subdirectory, so the case matches what the live run
	// actually did.
	for i := range 5 {
		if err := svc.PutFile(ctx, fmt.Sprintf("nginx/f%d.conf", i), []byte("v1"), 0o644, 0, 0); err != nil {
			t.Fatalf("PutFile() = %v", err)
		}
	}
	waitFor(t, "the files to be rendered to disk", func() bool {
		_, err := os.Stat(filepath.Join(dir, "nginx", "f4.conf"))
		return err == nil
	})

	// Delete them while renders are running flat out.
	for i := range 5 {
		if err := os.Remove(filepath.Join(dir, "nginx", fmt.Sprintf("f%d.conf", i))); err != nil {
			t.Fatalf("Remove() = %v", err)
		}
	}

	waitFor(t, "every deletion to be recorded", func() bool {
		for i := range 5 {
			e, err := st.Get(ctx, fmt.Sprintf("nginx/f%d.conf", i))
			if err != nil || !e.Deleted {
				return false
			}
		}
		return true
	})

	// And they must stay gone rather than being written back by a later render.
	time.Sleep(300 * time.Millisecond)
	for i := range 5 {
		p := filepath.Join(dir, "nginx", fmt.Sprintf("f%d.conf", i))
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s came back after being deleted", p)
		}
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run() = %v", err)
	}
}

// TestRunRendersWhatArrivesFromPeers covers the other direction under the run
// loop: something applied to the tree lands on disk without any local activity.
func TestRunRendersWhatArrivesFromPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	m, svc, _, dir := harness(t)

	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()

	// Stand in for an entry arriving from a peer.
	if err := svc.PutFile(ctx, "from-peer.conf", []byte("remote value"), 0o644, 0, 0); err != nil {
		t.Fatalf("PutFile() = %v", err)
	}
	waitFor(t, "the entry to reach the disk", func() bool {
		b, err := os.ReadFile(filepath.Join(dir, "from-peer.conf"))
		return err == nil && string(b) == "remote value"
	})

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run() = %v", err)
	}
}

// TestSymlinksAreNotReplicated covers the local-attacker case os.Root exists
// for: a symlink planted in the mirror directory must not become a path into
// the rest of the filesystem.
func TestSymlinksAreNotReplicated(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	m, svc, _, dir := harness(t)

	outside := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(outside, []byte("root:x:0:0"), 0o600); err != nil {
		t.Fatalf("WriteFile() = %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "shadow")); err != nil {
		t.Fatalf("Symlink() = %v", err)
	}
	if err := os.Symlink("/etc", filepath.Join(dir, "etc")); err != nil {
		t.Fatalf("Symlink() = %v", err)
	}
	write(t, dir, "real.conf", "ordinary", 0o644)

	if err := m.scan(ctx); err != nil {
		t.Fatalf("scan() = %v", err)
	}

	entries, err := svc.Live(ctx)
	if err != nil {
		t.Fatalf("Live() = %v", err)
	}
	for _, e := range entries {
		if e.Path == "shadow" || e.Path == "etc" || strings.HasPrefix(e.Path, "etc/") {
			t.Errorf("a symlink was replicated: %q", e.Path)
		}
	}
	// The ordinary file beside them must still be adopted, so the guard is not
	// simply refusing everything.
	if _, err := svc.Get(ctx, "real.conf"); err != nil {
		t.Errorf("a regular file next to a symlink was skipped: %v", err)
	}
}

// TestOversizedFilesAreSkipped covers a large file dropped into the mirror
// directory. It must be refused on its size, before being read into memory.
func TestOversizedFilesAreSkipped(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	m, svc, _, dir := harness(t)

	write(t, dir, "huge.bin", string(make([]byte, (1<<20)+1)), 0o644)
	write(t, dir, "small.conf", "fine", 0o644)

	if err := m.scan(ctx); err != nil {
		t.Fatalf("scan() = %v", err)
	}
	if _, err := svc.Get(ctx, "huge.bin"); err == nil {
		t.Error("an oversized file was replicated")
	}
	if _, err := svc.Get(ctx, "small.conf"); err != nil {
		t.Errorf("an ordinary file was skipped alongside the oversized one: %v", err)
	}
}

// waitFor polls until cond holds. Filesystem notification is asynchronous, so
// tests wait on the observable outcome rather than a guessed sleep.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
