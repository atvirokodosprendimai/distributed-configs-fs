// Package mirror projects the replicated tree onto a real directory — the
// /etc/cluster mode — and promotes local edits back into it.
//
// # Which direction wins
//
// The hard question in a two-way mirror is what "the store has this file live,
// the disk does not" means. It is either "the store learned it from a peer and
// the disk has not caught up" (write it out) or "somebody deleted it" (tombstone
// it), and the two states are indistinguishable from a snapshot.
//
// This package resolves that by source rather than by state:
//
//   - Deletions come only from an explicit filesystem event. Removing a file
//     while the daemon is running deletes it cluster-wide; that is the delete UX.
//   - The periodic rescan never deletes from the cluster. It exists to catch
//     modifications and additions that inotify dropped, nothing more.
//
// The consequence is deliberate and worth stating plainly: a file deleted while
// the daemon is stopped comes back when it starts. That matches the Proxmox
// behaviour this tool is modelled on — you do not edit a cluster filesystem by
// stopping the thing that replicates it — and it is what makes "a node comes
// back and the tree reappears" work at all.
//
// # Why inotify is not the correctness mechanism
//
// fsnotify is a latency optimisation. inotify drops events when its queue
// overflows, it is not recursive so every directory needs its own watch, and on
// Docker Desktop it does not propagate through the host bind mount at all. The
// periodic full rescan is what makes this package correct; the watcher only
// makes it fast.
//
// # Traversal safety
//
// Every filesystem mutation goes through an *os.Root anchored at the mirror
// directory. A peer sending "../../etc/shadow" is already refused by
// core.SanitizePath, but a *local* user can plant a symlink inside the mirror
// directory pointing anywhere, and no amount of path string checking sees that.
// os.Root refuses to escape its root even through a symlink, which closes the
// hole that string checks cannot.
package mirror

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"
)

// tmpPrefix marks the scratch files used for atomic writes. They are skipped by
// the scanner: a half-written temp file must never be promoted into the cluster.
const tmpPrefix = ".dcfs-tmp-"

// Tree is the syncer as the mirror uses it.
type Tree interface {
	Live(ctx context.Context) ([]core.Meta, error)
	Get(ctx context.Context, path string) (core.Meta, error)
	Content(ctx context.Context, hash string) ([]byte, error)
	PutFile(ctx context.Context, path string, content []byte, mode, uid, gid uint32) error
	PutDir(ctx context.Context, path string, mode, uid, gid uint32) error
	Delete(ctx context.Context, path string) error
	Subscribe() (<-chan struct{}, func())
	MaxFileSize() int64
}

// Config configures a mirror.
type Config struct {
	// Dir is the directory the tree is projected onto, e.g. /etc/cluster.
	Dir string
	// Tree is the replicated tree this directory reflects.
	Tree Tree
	// ScanInterval is how often the full rescan runs. This is the correctness
	// backstop for dropped inotify events, so it is not optional.
	ScanInterval time.Duration
	// Settle is how long to wait after the last event on a path before reading
	// it. Without it the watcher hashes half-written files: most editors save
	// by writing then renaming, and some write in place over several syscalls.
	Settle time.Duration
	Log    *slog.Logger
}

// Mirror keeps a directory and the replicated tree in step.
//
// Its methods are driven by the single Run loop and are not safe for concurrent
// use; that is deliberate, since the whole point of this package is to make one
// directory and one tree agree, and two of them doing it at once would not.
type Mirror struct {
	cfg  Config
	root *os.Root
	log  *slog.Logger

	// canChown records whether this process can actually set file ownership.
	//
	// It gates *reading* ownership as much as writing it, which is the
	// non-obvious half. Ownership is replicated, so a node that cannot chown
	// renders every file owned by its own user — and if it then promoted what
	// it sees on disk, it would overwrite the cluster's intended uid/gid with
	// its own, on every scan, forever. A node that cannot set ownership must
	// therefore not report ownership either.
	canChown bool
	// warnedChown keeps the privilege warning to once per process rather than
	// once per file per scan.
	warnedChown bool
}

// Open prepares the mirror directory, creating it if absent.
func Open(cfg Config) (*Mirror, error) {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("create mirror dir %s: %w", cfg.Dir, err)
	}
	root, err := os.OpenRoot(cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("open mirror root %s: %w", cfg.Dir, err)
	}
	// Setting arbitrary ownership needs root (or CAP_CHOWN, which is rare
	// enough not to assume). The first failed chown downgrades this anyway, so
	// euid is a starting guess rather than the final word.
	return &Mirror{cfg: cfg, root: root, log: cfg.Log, canChown: os.Geteuid() == 0}, nil
}

// Close releases the mirror directory handle.
func (m *Mirror) Close() error { return m.root.Close() }

// Run keeps the directory and the tree in step until ctx is cancelled.
//
// The startup order is load-bearing. The scan runs first so that files already
// sitting in the directory are adopted into the cluster rather than deleted by a
// render that has never heard of them — the case of an operator populating
// /etc/cluster before starting the daemon for the first time. Render runs second
// and writes out whatever the cluster knows that the disk does not.
//
// Scanning first does not mean the disk wins: promote compares the file's mtime
// against the stored version's clock and offers the local file only if it is
// genuinely newer (see diskIsNewer). So an edit made while the daemon was
// stopped is adopted, while a peer's version that arrived while this node was
// down is rendered over the older local copy. Later modification wins, in both
// directions.
func (m *Mirror) Run(ctx context.Context) error {
	changed, unsubscribe := m.cfg.Tree.Subscribe()
	defer unsubscribe()

	if err := m.scan(ctx); err != nil {
		return fmt.Errorf("initial scan of %s: %w", m.cfg.Dir, err)
	}
	if err := m.render(ctx); err != nil {
		return fmt.Errorf("initial render of %s: %w", m.cfg.Dir, err)
	}

	events, stopWatching, err := m.watch(ctx)
	if err != nil {
		// A watcher that will not start is a degradation, not a failure: the
		// rescan alone still converges, just with rescan-interval latency.
		// Refusing to run would be worse than running slowly.
		m.log.Warn("filesystem watch unavailable; falling back to periodic rescan only",
			"dir", m.cfg.Dir, "interval", m.cfg.ScanInterval, "err", err)
		events = nil
	} else {
		defer stopWatching()
	}

	ticker := time.NewTicker(m.cfg.ScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-changed:
			// The tree moved: push it to disk.
			if err := m.render(ctx); err != nil {
				m.log.Error("render tree to disk", "dir", m.cfg.Dir, "err", err)
			}

		case batch, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			m.applyEvents(ctx, batch)

		case <-ticker.C:
			// The backstop. Scan first so a local edit is promoted before the
			// render would otherwise overwrite it with the stored version.
			if err := m.scan(ctx); err != nil {
				m.log.Error("rescan mirror", "dir", m.cfg.Dir, "err", err)
			}
			if err := m.render(ctx); err != nil {
				m.log.Error("render tree to disk", "dir", m.cfg.Dir, "err", err)
			}
		}
	}
}

// render writes the tree out to disk: creates directories, writes files whose
// content or metadata differ, and removes what tombstones say is gone.
//
// It never removes a path the store has no row for. Those are local files the
// cluster has not adopted yet, and deleting them would make starting the daemon
// against a populated directory destructive.
func (m *Mirror) render(ctx context.Context) error {
	entries, err := m.cfg.Tree.Live(ctx)
	if err != nil {
		return err
	}
	// Shortest path first, so a parent directory exists before its children.
	slices.SortFunc(entries, func(a, b core.Meta) int {
		return strings.Compare(a.Path, b.Path)
	})

	for _, e := range entries {
		switch e.Kind {
		case core.KindDir:
			if err := m.renderDir(e); err != nil {
				m.log.Error("render directory", "path", e.Path, "err", err)
			}
		case core.KindFile:
			if err := m.renderFile(ctx, e); err != nil {
				m.log.Error("render file", "path", e.Path, "err", err)
			}
		}
	}
	return m.removeTombstoned(ctx)
}

// renderDir ensures a directory exists with the recorded metadata.
func (m *Mirror) renderDir(e core.Meta) error {
	if err := m.root.MkdirAll(e.Path, fs.FileMode(e.Mode)); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("mkdir %s: %w", e.Path, err)
	}
	return m.applyMeta(e)
}

// renderFile ensures a file's content and metadata match the tree.
func (m *Mirror) renderFile(ctx context.Context, e core.Meta) error {
	onDisk, err := m.readLocal(e.Path)
	switch {
	case err == nil && onDisk.hash == e.Hash:
		// Content already matches; metadata may still need a nudge.
		return m.applyMeta(e)
	case err != nil && !errors.Is(err, fs.ErrNotExist):
		return err
	}

	content, err := m.cfg.Tree.Content(ctx, e.Hash)
	if err != nil {
		return fmt.Errorf("load content for %s: %w", e.Path, err)
	}
	if err := m.writeAtomic(e.Path, content, fs.FileMode(e.Mode)); err != nil {
		return err
	}
	return m.applyMeta(e)
}

// writeAtomic writes content to a scratch file and renames it into place.
//
// The rename is what makes a reader — nginx reloading, a service starting —
// see either the old file or the new one, never a truncated one. A plain
// truncate-and-write leaves a window in which the config on disk is invalid,
// and that window is exactly when a service is most likely to read it.
func (m *Mirror) writeAtomic(p string, content []byte, mode fs.FileMode) error {
	dir := path.Dir(p)
	if dir != "." {
		if err := m.root.MkdirAll(dir, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}

	tmp := path.Join(dir, tmpPrefix+path.Base(p))
	f, err := m.root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("create temp for %s: %w", p, err)
	}
	if _, err := f.Write(content); err != nil {
		f.Close()
		_ = m.root.Remove(tmp)
		return fmt.Errorf("write temp for %s: %w", p, err)
	}
	// Sync before rename: without it a host power loss can leave the rename
	// durable but the data not, which is a zero-length config file.
	if err := f.Sync(); err != nil {
		f.Close()
		_ = m.root.Remove(tmp)
		return fmt.Errorf("sync temp for %s: %w", p, err)
	}
	if err := f.Close(); err != nil {
		_ = m.root.Remove(tmp)
		return fmt.Errorf("close temp for %s: %w", p, err)
	}
	if err := m.root.Rename(tmp, p); err != nil {
		_ = m.root.Remove(tmp)
		return fmt.Errorf("rename temp into %s: %w", p, err)
	}
	return nil
}

// applyMeta sets permissions and ownership on a rendered path.
func (m *Mirror) applyMeta(e core.Meta) error {
	if err := m.root.Chmod(e.Path, fs.FileMode(e.Mode)); err != nil {
		return fmt.Errorf("chmod %s: %w", e.Path, err)
	}
	if !m.canChown {
		return nil
	}
	// Ownership is replicated, so setting it needs root or CAP_CHOWN. Running
	// unprivileged is a legitimate deployment, so a failure here degrades to a
	// warning rather than failing the whole render — the file content, which is
	// the point, is already correct.
	if err := m.root.Lchown(e.Path, int(e.UID), int(e.GID)); err != nil {
		m.canChown = false
		if !m.warnedChown {
			m.warnedChown = true
			m.log.Warn("cannot set ownership; content and permissions still replicate, "+
				"and this node will not overwrite the cluster's uid/gid with its own. "+
				"run as root or grant CAP_CHOWN to replicate ownership",
				"path", e.Path, "uid", e.UID, "gid", e.GID, "err", err)
		}
		return nil
	}
	return nil
}

// removeTombstoned deletes from disk the paths the tree says are gone.
func (m *Mirror) removeTombstoned(ctx context.Context) error {
	live, err := m.cfg.Tree.Live(ctx)
	if err != nil {
		return err
	}
	keep := make(map[string]bool, len(live))
	for _, e := range live {
		keep[e.Path] = true
	}

	var doomed []string
	err = m.walk(func(p string, d fs.DirEntry) error {
		if keep[p] {
			return nil
		}
		// Only paths the tree explicitly knows as deleted are removed. A path
		// with no row at all is a local file awaiting adoption, and deleting it
		// would make a first run against a populated directory destructive.
		entry, err := m.cfg.Tree.Get(ctx, p)
		if err != nil || !entry.Deleted {
			return nil
		}
		doomed = append(doomed, p)
		return nil
	})
	if err != nil {
		return err
	}

	// Deepest first, so a directory is empty by the time it is removed.
	slices.SortFunc(doomed, func(a, b string) int { return strings.Compare(b, a) })
	for _, p := range doomed {
		if err := m.root.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			m.log.Warn("remove deleted path", "path", p, "err", err)
			continue
		}
		m.log.Info("removed path deleted by the cluster", "path", p)
	}
	return nil
}

// scan walks the directory and promotes anything the tree does not already
// know. It never deletes from the cluster; see the package documentation.
func (m *Mirror) scan(ctx context.Context) error {
	return m.walk(func(p string, d fs.DirEntry) error {
		m.promote(ctx, p, d.IsDir())
		return nil
	})
}

// walk visits every replicable path under the mirror directory, skipping what
// the cluster does not carry.
func (m *Mirror) walk(visit func(p string, d fs.DirEntry) error) error {
	return fs.WalkDir(m.root.FS(), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			// A directory that vanished mid-walk is normal, not fatal: the
			// next pass will see a consistent tree.
			m.log.Debug("skipping unreadable path during walk", "path", p, "err", err)
			return nil
		}
		if p == "." {
			return nil
		}
		if strings.HasPrefix(path.Base(p), tmpPrefix) {
			return nil
		}
		// Symlinks, sockets, FIFOs and devices are not replicated, and a
		// directory reached through one must not be descended into either.
		if !d.IsDir() && !d.Type().IsRegular() {
			return nil
		}
		if _, err := core.SanitizePath(p); err != nil {
			m.log.Warn("skipping unreplicable path", "path", p, "err", err)
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		return visit(p, d)
	})
}

// promote offers one local path to the tree. The syncer decides whether it is
// actually a change; offering an unchanged file is a no-op there, which is what
// keeps this safe to call on every event and every rescan.
//
// When the disk and the tree disagree about a path, the more recently modified
// side wins: the file's mtime is compared against the stored version's clock,
// and the local file is offered only if it is newer. That comparison is the
// whole answer to the startup ambiguity — an operator editing a file while the
// daemon was stopped, versus a peer's newer version arriving while this node was
// down look identical from a snapshot, and only their timestamps separate them.
//
// The promotion is stamped with a fresh hybrid logical clock reading rather
// than with the file's mtime. mtime is not trustworthy as a cluster-wide
// ordering source — tar, rsync -a and touch all set it arbitrarily, and a
// timestamp in the future would win every conflict forever — so it decides the
// local question it is qualified to answer and nothing beyond it.
func (m *Mirror) promote(ctx context.Context, p string, isDir bool) {
	info, err := m.root.Lstat(p)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			m.log.Warn("stat local path", "path", p, "err", err)
		}
		return
	}
	stored, known := m.stored(ctx, p)
	if !m.diskIsNewer(stored, known, info.ModTime()) {
		return
	}
	uid, gid := m.ownershipFor(info, stored, known)
	mode := uint32(info.Mode().Perm())

	if isDir {
		if err := m.cfg.Tree.PutDir(ctx, p, mode, uid, gid); err != nil {
			m.log.Error("promote local directory", "path", p, "err", err)
		}
		return
	}

	// Check the size before reading: a 2 GiB file dropped into the mirror
	// directory must be refused, not loaded into memory and then refused.
	if info.Size() > m.cfg.Tree.MaxFileSize() {
		m.log.Warn("skipping oversized file; it will not replicate",
			"path", p, "size", info.Size(), "limit", m.cfg.Tree.MaxFileSize())
		return
	}
	content, err := m.root.ReadFile(p)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			m.log.Warn("read local file", "path", p, "err", err)
		}
		return
	}
	if err := m.cfg.Tree.PutFile(ctx, p, content, mode, uid, gid); err != nil {
		m.log.Error("promote local file", "path", p, "err", err)
	}
}

// stored returns what the tree holds for p, and whether it holds anything.
func (m *Mirror) stored(ctx context.Context, p string) (core.Meta, bool) {
	entry, err := m.cfg.Tree.Get(ctx, p)
	if err != nil {
		return core.Meta{}, false
	}
	return entry, true
}

// diskIsNewer reports whether the local file should win over what the tree
// already holds for that path.
//
// A path the tree has never seen is always promoted — that is adoption, and
// there is no stored timestamp to lose to. A path the tree holds as a tombstone
// is never promoted: the cluster has decided it is gone, and render will remove
// it from disk shortly.
func (m *Mirror) diskIsNewer(stored core.Meta, known bool, modTime time.Time) bool {
	if !known {
		return true // unknown to the tree: adopt it
	}
	if stored.Deleted {
		return false
	}
	// Strictly newer. Equal timestamps mean the tree's version is what produced
	// this file, so there is nothing local to promote — and treating equality as
	// "disk wins" would make every render followed by a rescan look like an
	// edit, which is the echo loop in a different disguise.
	return modTime.After(stored.Version.HLC.Time())
}

// ownershipFor decides which uid/gid a promotion should carry.
//
// When this node cannot set ownership, the uid/gid visible on disk is this
// process's own rather than the cluster's, so the stored values are kept.
// Reporting what we see would let one unprivileged node overwrite everybody
// else's ownership on every scan. A file the tree has never seen has no stored
// value to keep, so its on-disk ownership is the only truth available.
func (m *Mirror) ownershipFor(info fs.FileInfo, stored core.Meta, known bool) (uid, gid uint32) {
	if !m.canChown && known {
		return stored.UID, stored.GID
	}
	return ownerOf(info)
}

// localFile is what a path currently looks like on disk.
type localFile struct {
	hash string
	mode uint32
	uid  uint32
	gid  uint32
}

// readLocal hashes and stats a file, so render can tell whether it needs
// rewriting. Comparing hashes rather than timestamps is what makes render
// idempotent, and idempotent render is what stops the mirror from rewriting —
// and therefore re-notifying on — files that are already correct.
func (m *Mirror) readLocal(p string) (localFile, error) {
	info, err := m.root.Lstat(p)
	if err != nil {
		return localFile{}, err
	}
	if !info.Mode().IsRegular() {
		return localFile{}, fmt.Errorf("%s is not a regular file", p)
	}
	content, err := m.root.ReadFile(p)
	if err != nil {
		return localFile{}, err
	}
	uid, gid := ownerOf(info)
	return localFile{
		hash: core.HashContent(content),
		mode: uint32(info.Mode().Perm()),
		uid:  uid,
		gid:  gid,
	}, nil
}

// ownerOf extracts uid and gid from a FileInfo.
//
// This package is Unix-only by design: it replicates POSIX ownership onto a
// directory such as /etc/cluster, which has no Windows equivalent worth
// emulating.
func ownerOf(info fs.FileInfo) (uid, gid uint32) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0
	}
	return st.Uid, st.Gid
}

// Dir returns the directory this mirror projects onto.
func (m *Mirror) Dir() string { return m.cfg.Dir }

// abs turns a cluster-relative path into a filesystem path, for the watcher,
// which needs real paths and only ever reads.
func (m *Mirror) abs(p string) string { return filepath.Join(m.cfg.Dir, filepath.FromSlash(p)) }
