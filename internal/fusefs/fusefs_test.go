package fusefs

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/store"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/syncer"
)

// mount brings up a real FUSE mount over a real syncer, or skips the test when
// the platform cannot provide one. Mounting needs /dev/fuse on Linux and
// macFUSE on darwin, neither of which is available in every CI sandbox, so the
// mount-dependent tests are skipped rather than failing the suite for a reason
// that has nothing to do with this code.
func mount(t *testing.T) (dir string, svc *syncer.Service) {
	t.Helper()

	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skipf("FUSE is not supported on %s", runtime.GOOS)
	}
	if runtime.GOOS == "linux" {
		if _, err := os.Stat("/dev/fuse"); err != nil {
			t.Skip("no /dev/fuse; mount tests need a FUSE-capable kernel")
		}
	}

	st, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "fuse.db"))
	if err != nil {
		t.Fatalf("store.Open() = %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	svc = syncer.New(syncer.Config{
		Node: "node1", Store: st, MaxFileSize: 1 << 20,
		TombstoneTTL: time.Hour, SyncInterval: time.Hour, JanitorInterval: time.Hour,
		Log: slog.New(slog.DiscardHandler),
	})

	dir = filepath.Join(t.TempDir(), "mnt")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll() = %v", err)
	}
	server, err := Mount(dir, svc, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Skipf("cannot mount FUSE here: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Unmount(); err != nil {
			// A failed unmount leaves a wedged mountpoint behind, which breaks
			// every later run in the same sandbox, so try the CLI as a backstop.
			_ = exec.Command("fusermount", "-u", dir).Run()
			t.Logf("unmount %s: %v", dir, err)
		}
	})
	return dir, svc
}

// TestMountRoundTrip exercises the write path the mirror cannot offer: the
// kernel asks this process before the data lands.
func TestMountRoundTrip(t *testing.T) {
	dir, svc := mount(t)
	ctx := t.Context()

	// Write through the mount and read it back out of the tree.
	target := filepath.Join(dir, "nginx.conf")
	if err := os.WriteFile(target, []byte("listen 80;"), 0o640); err != nil {
		t.Fatalf("WriteFile() = %v", err)
	}
	entry, err := svc.Get(ctx, "nginx.conf")
	if err != nil {
		t.Fatalf("the write did not reach the tree: %v", err)
	}
	content, err := svc.Content(ctx, entry.Hash)
	if err != nil {
		t.Fatalf("Content() = %v", err)
	}
	if string(content) != "listen 80;" {
		t.Errorf("tree content = %q, want %q", content, "listen 80;")
	}

	// And read it back through the mount.
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("ReadFile() = %v", err)
	}
	if string(got) != "listen 80;" {
		t.Errorf("read back = %q", got)
	}

	// Something arriving from a peer must appear in the mount without any
	// local filesystem activity.
	if err := svc.PutFile(ctx, "from-peer.conf", []byte("remote"), 0o644, 0, 0); err != nil {
		t.Fatalf("PutFile() = %v", err)
	}
	waitFor(t, "the peer entry to appear in the mount", func() bool {
		b, err := os.ReadFile(filepath.Join(dir, "from-peer.conf"))
		return err == nil && string(b) == "remote"
	})

	// Directories, including empty ones.
	if err := os.Mkdir(filepath.Join(dir, "conf.d"), 0o750); err != nil {
		t.Fatalf("Mkdir() = %v", err)
	}
	if _, err := svc.Get(ctx, "conf.d"); err != nil {
		t.Fatalf("mkdir did not reach the tree: %v", err)
	}
	names, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() = %v", err)
	}
	seen := make(map[string]bool, len(names))
	for _, e := range names {
		seen[e.Name()] = true
	}
	for _, want := range []string{"nginx.conf", "from-peer.conf", "conf.d"} {
		if !seen[want] {
			t.Errorf("readdir did not list %q", want)
		}
	}

	// Deletion through the mount becomes a cluster tombstone.
	if err := os.Remove(target); err != nil {
		t.Fatalf("Remove() = %v", err)
	}
	deleted, err := svc.Get(ctx, "nginx.conf")
	if err != nil {
		t.Fatalf("Get() after unlink = %v", err)
	}
	if !deleted.Deleted {
		t.Error("unlink did not tombstone the entry")
	}
}

// TestMountRejectsOversizedWrite is the point of the FUSE mode: the program
// doing the write learns it failed, at the syscall responsible.
func TestMountRejectsOversizedWrite(t *testing.T) {
	dir, _ := mount(t)

	f, err := os.Create(filepath.Join(dir, "huge.bin"))
	if err != nil {
		t.Fatalf("Create() = %v", err)
	}
	defer f.Close()

	_, err = f.Write(make([]byte, (1<<20)+1))
	if err == nil {
		t.Fatal("an oversized write succeeded; the mount is not enforcing the size cap")
	}
	if !errors.Is(err, syscall.EFBIG) {
		t.Errorf("write error = %v, want EFBIG so the caller can tell why", err)
	}
}

// TestMountChmod covers metadata-only changes taking effect without rewriting
// content.
func TestMountChmod(t *testing.T) {
	dir, svc := mount(t)
	ctx := t.Context()

	target := filepath.Join(dir, "secret.conf")
	if err := os.WriteFile(target, []byte("password = hunter2"), 0o644); err != nil {
		t.Fatalf("WriteFile() = %v", err)
	}
	before, err := svc.Get(ctx, "secret.conf")
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}

	if err := os.Chmod(target, 0o600); err != nil {
		t.Fatalf("Chmod() = %v", err)
	}
	after, err := svc.Get(ctx, "secret.conf")
	if err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if after.Mode != 0o600 {
		t.Errorf("stored mode = %#o, want 0600", after.Mode)
	}
	if after.Hash != before.Hash {
		t.Error("a chmod rewrote the file content")
	}

	info, err := os.Stat(target)
	if err != nil {
		t.Fatalf("Stat() = %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("mount reports mode %#o, want 0600", info.Mode().Perm())
	}
}

// waitFor polls until cond holds, since the kernel caches entries briefly.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestErrnoMapping pins the translation that makes the mount usable: a caller
// must be able to tell "too big" from "bad name" from "not there" without
// reading this program's log.
func TestErrnoMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want syscall.Errno
	}{
		{"success", nil, fs_OK},
		{"oversized", fmt.Errorf("wrapped: %w", core.ErrTooLarge), syscall.EFBIG},
		{"unreplicable path", fmt.Errorf("wrapped: %w", core.ErrBadPath), syscall.EINVAL},
		{"missing entry", fmt.Errorf("wrapped: %w", store.ErrNotFound), syscall.ENOENT},
		{"anything else", errors.New("disk on fire"), syscall.EIO},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := errnoOf(tc.err); got != tc.want {
				t.Errorf("errnoOf(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// fs_OK mirrors fs.OK without importing it into the test's expectations table.
const fs_OK = syscall.Errno(0)

func TestInodeNumbersAreStableAndDistinct(t *testing.T) {
	t.Parallel()

	// Stability across calls is what lets a mount survive this process
	// restarting without every open handle going stale.
	if inoOf("a/b.conf") != inoOf("a/b.conf") {
		t.Error("inoOf is not stable for the same path")
	}
	if inoOf("a/b.conf") == inoOf("a/c.conf") {
		t.Error("two different paths collided; readdir would report duplicates")
	}
}

func TestAttrOfMarksKindAndOwnership(t *testing.T) {
	t.Parallel()

	var file, dir fuse.Attr
	attrOf(core.Meta{Path: "a.conf", Kind: core.KindFile, Mode: 0o640, Size: 12,
		UID: 33, GID: 44, Version: core.Version{HLC: core.Timestamp{Wall: 2e9}}}, &file)
	attrOf(core.Meta{Path: "d", Kind: core.KindDir, Mode: 0o750}, &dir)

	if file.Mode&syscall.S_IFREG == 0 {
		t.Error("file entry is not marked as a regular file; the kernel would not open it")
	}
	if dir.Mode&syscall.S_IFDIR == 0 {
		t.Error("directory entry is not marked as a directory")
	}
	if file.Mode&0o777 != 0o640 {
		t.Errorf("permission bits = %#o, want 0640", file.Mode&0o777)
	}
	if file.Owner.Uid != 33 || file.Owner.Gid != 44 {
		t.Errorf("owner = %d:%d, want 33:44 — ownership is replicated", file.Owner.Uid, file.Owner.Gid)
	}
	if file.Size != 12 {
		t.Errorf("size = %d, want 12", file.Size)
	}
	// Some tools treat a directory with fewer than two links as broken.
	if dir.Nlink < 2 {
		t.Errorf("directory Nlink = %d, want at least 2", dir.Nlink)
	}
}
