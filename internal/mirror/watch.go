package mirror

import (
	"context"
	"errors"
	"io/fs"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"
)

// event is one settled filesystem change.
type event struct {
	Path    string // cluster-relative
	Removed bool
}

// watch starts an fsnotify watcher over the mirror directory and returns a
// channel of settled event batches.
//
// Two properties of inotify shape everything here. It is not recursive, so
// every directory needs its own watch and new directories must be watched as
// they appear. And it coalesces and drops events under load, which is why the
// caller still runs a periodic rescan — this watcher is an accelerator, not a
// guarantee.
func (m *Mirror) watch(ctx context.Context) (<-chan []event, func(), error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, nil, err
	}
	if err := m.addWatches(w, "."); err != nil {
		_ = w.Close()
		return nil, nil, err
	}

	out := make(chan []event, 1)
	go m.pump(ctx, w, out)

	return out, func() { _ = w.Close() }, nil
}

// addWatches registers dir and every directory beneath it.
func (m *Mirror) addWatches(w *fsnotify.Watcher, dir string) error {
	return fs.WalkDir(m.root.FS(), dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // vanished mid-walk; the rescan will catch up
		}
		if !d.IsDir() {
			return nil
		}
		if p != "." && strings.HasPrefix(path.Base(p), tmpPrefix) {
			return fs.SkipDir
		}
		if err := w.Add(m.abs(p)); err != nil {
			// The usual cause is exhausting fs.inotify.max_user_watches. Say so
			// explicitly: the symptom otherwise is "changes are slow sometimes",
			// which is unpleasant to diagnose from first principles.
			m.log.Warn("cannot watch directory; changes there are only picked up by the rescan. "+
				"if this repeats, raise fs.inotify.max_user_watches",
				"path", p, "err", err)
		}
		return nil
	})
}

// pump translates raw fsnotify events into settled, deduplicated batches.
//
// The settle delay exists because a single logical save is several syscalls.
// Editors commonly write a temporary file and rename it over the target, and
// others truncate and rewrite in place; reading on the first event yields a
// half-written file, and promoting that into the cluster would replicate a
// truncated config to every node.
func (m *Mirror) pump(ctx context.Context, w *fsnotify.Watcher, out chan<- []event) {
	defer close(out)

	pending := make(map[string]bool) // cluster-relative path -> removed
	var timer <-chan time.Time

	flush := func() []event {
		batch := make([]event, 0, len(pending))
		for p, removed := range pending {
			batch = append(batch, event{Path: p, Removed: removed})
		}
		clear(pending)
		return batch
	}

	for {
		select {
		case <-ctx.Done():
			return

		case err, ok := <-w.Errors:
			if !ok {
				return
			}
			// An inotify queue overflow arrives here. It is exactly the case
			// the rescan exists for, so it is logged rather than escalated.
			m.log.Warn("filesystem watch error; the rescan will reconcile", "err", err)

		case ev, ok := <-w.Events:
			if !ok {
				return
			}
			rel, ok := m.relative(ev.Name)
			if !ok {
				continue
			}
			removed := ev.Has(fsnotify.Remove) || ev.Has(fsnotify.Rename)

			// A new directory needs its own watch, and anything created inside
			// it before the watch lands would otherwise be missed until the
			// next rescan — so it is enqueued for a walk as well.
			if ev.Has(fsnotify.Create) {
				if info, err := m.root.Lstat(rel); err == nil && info.IsDir() {
					if err := m.addWatches(w, rel); err != nil {
						m.log.Warn("watch new directory", "path", rel, "err", err)
					}
					m.enqueueTree(pending, rel)
				}
			}

			// A later event supersedes an earlier one for the same path: a file
			// removed and recreated within the settle window is a modification.
			pending[rel] = removed
			timer = time.After(m.cfg.Settle)

		case <-timer:
			timer = nil
			if len(pending) == 0 {
				continue
			}
			batch := flush()
			select {
			case out <- batch:
			case <-ctx.Done():
				return
			}
		}
	}
}

// enqueueTree adds everything under dir to the pending set, for the case of a
// directory appearing whole — an unpacked archive, a renamed tree — where the
// individual creations happened before there was a watch to see them.
func (m *Mirror) enqueueTree(pending map[string]bool, dir string) {
	_ = fs.WalkDir(m.root.FS(), dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || p == "." {
			return nil
		}
		if strings.HasPrefix(path.Base(p), tmpPrefix) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		pending[p] = false
		return nil
	})
}

// relative converts an absolute filesystem path into a cluster-relative one,
// reporting false for anything the cluster does not carry.
func (m *Mirror) relative(abs string) (string, bool) {
	rel, err := filepath.Rel(m.cfg.Dir, abs)
	if err != nil {
		return "", false
	}
	rel = filepath.ToSlash(rel)
	if rel == "." {
		return "", false
	}
	// Our own atomic-write scratch files must never be promoted; they are
	// half-written by definition.
	if strings.HasPrefix(path.Base(rel), tmpPrefix) {
		return "", false
	}
	if _, err := core.SanitizePath(rel); err != nil {
		return "", false
	}
	return rel, true
}

// applyEvents promotes changed paths and tombstones removed ones.
//
// This is the only place a deletion becomes a cluster-wide deletion, and the
// reason is in the package documentation: an explicit removal event is the one
// signal that unambiguously means "somebody deleted this", as opposed to "the
// disk has not caught up with the tree yet".
func (m *Mirror) applyEvents(ctx context.Context, batch []event) {
	for _, ev := range batch {
		if !ev.Removed {
			// Re-stat rather than trusting the event: within the settle window
			// a file may have been created and then deleted again.
			info, err := m.root.Lstat(ev.Path)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					m.deleteLocal(ctx, ev.Path)
				}
				continue
			}
			if !info.IsDir() && !info.Mode().IsRegular() {
				continue // symlinks and device nodes are not replicated
			}
			m.promote(ctx, ev.Path, info.IsDir())
			continue
		}

		// A rename fires Remove on the old name. If something is there again
		// it was replaced, not deleted — the atomic-save pattern.
		if _, err := m.root.Lstat(ev.Path); err == nil {
			m.promote(ctx, ev.Path, false)
			continue
		}
		m.deleteLocal(ctx, ev.Path)
	}
}

// deleteLocal tombstones a path the user removed from disk.
func (m *Mirror) deleteLocal(ctx context.Context, p string) {
	if err := m.cfg.Tree.Delete(ctx, p); err != nil {
		m.log.Error("record local deletion", "path", p, "err", err)
		return
	}
	m.log.Info("recorded local deletion", "path", p)
}
