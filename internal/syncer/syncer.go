// Package syncer is the single writer.
//
// Everything that changes this node's tree goes through here: the fsnotify
// watcher promoting a local edit, the FUSE layer servicing a write(2), and the
// anti-entropy loop applying what a peer served. Each of those arrives on its
// own goroutine, and each is a read-modify-write — read the current version,
// compare, decide, store — so they are serialised. Without that, two sources
// both read the old version, both conclude they win, and one write disappears.
//
// The syncer is also where policy lives that the store deliberately does not
// have: what counts as too large, how far a peer's clock may drift, when a
// tombstone may be collected, and when a losing edit is preserved as a conflict
// copy instead of dropped.
package syncer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/cluster"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/store"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/transport"
)

// ErrTooLarge is returned when a file exceeds the configured size cap. FUSE
// maps it to EFBIG; the mirror logs it and skips the file.
var ErrTooLarge = errors.New("syncer: file exceeds max size")

// Peers is the membership view the syncer reacts to. It is narrow so tests can
// drive convergence without standing up gossip.
type Peers interface {
	Events() <-chan cluster.Event
	Announce(seq int64)
	Members() []cluster.Member
}

// PeerClient fetches state from another node's HTTP API.
type PeerClient interface {
	Digest(ctx context.Context, addr string) (transport.DigestResponse, error)
	Manifest(ctx context.Context, addr string, since int64) (transport.ManifestResponse, error)
	Blob(ctx context.Context, addr, hash string) ([]byte, error)
}

// Config configures the syncer.
type Config struct {
	Node  string
	Store *store.Store
	Peers Peers
	Peer  PeerClient

	// MaxFileSize caps what this node will accept, locally or from a peer.
	// A config tree is not a file share, and without a cap one oversized file
	// dropped into the mirror directory replicates itself into every node's
	// database.
	MaxFileSize int64
	// TombstoneTTL is how long a deletion is remembered. It must exceed the
	// longest outage a node may have, or a returning node re-announces files
	// the cluster deleted while it was away.
	TombstoneTTL time.Duration
	// SyncInterval is how often anti-entropy runs regardless of gossip. It is
	// the correctness backstop: gossip hints only make convergence faster.
	SyncInterval time.Duration
	// JanitorInterval is how often tombstones and orphaned blobs are collected.
	JanitorInterval time.Duration

	Log *slog.Logger
}

// Service is the single writer and the anti-entropy driver.
type Service struct {
	cfg   Config
	clock *core.Clock
	log   *slog.Logger

	// write serialises the read-modify-write of conflict resolution. It is not
	// about database access — the store handles that — but about the decision
	// made between reading the current version and storing the new one.
	write sync.Mutex

	// syncing tracks peers with a pull already in flight, so a burst of gossip
	// hints from a busy node does not start a dozen overlapping syncs against it.
	syncing sync.Map

	subs struct {
		sync.Mutex
		next int
		m    map[int]chan struct{}
	}

	status struct {
		sync.Mutex
		lastSync time.Time
		lastErr  string
	}
}

// New returns a syncer. It does not start any loops; call Run for that.
func New(cfg Config) *Service {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	s := &Service{cfg: cfg, clock: core.NewClock(), log: cfg.Log}
	s.subs.m = make(map[int]chan struct{})
	return s
}

// Subscribe returns a channel signalled whenever the local tree changes, plus a
// function to unsubscribe.
//
// The channel is buffered with depth one and sends are dropped when it is full,
// which is correct because the signal carries no payload: a subscriber that has
// one pending wake-up does not need a second, since it re-reads the whole store
// when it wakes. This is the "publish that something changed, never the state
// itself" rule — the store is the truth, the signal is only a nudge.
func (s *Service) Subscribe() (<-chan struct{}, func()) {
	s.subs.Lock()
	defer s.subs.Unlock()

	id := s.subs.next
	s.subs.next++
	ch := make(chan struct{}, 1)
	s.subs.m[id] = ch

	return ch, func() {
		s.subs.Lock()
		defer s.subs.Unlock()
		if c, ok := s.subs.m[id]; ok {
			delete(s.subs.m, id)
			close(c)
		}
	}
}

// notify wakes every subscriber without blocking on any of them.
func (s *Service) notify() {
	s.subs.Lock()
	defer s.subs.Unlock()
	for _, ch := range s.subs.m {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// PutFile records a locally originated file write.
//
// It returns without writing anything when the stored entry already matches
// the content and metadata offered. That short-circuit is what breaks the echo
// loop: after the mirror writes a file the cluster sent us, fsnotify fires and
// the watcher offers those exact bytes straight back. Comparing against the
// store makes the second offer a no-op, so no suppression table, no timers and
// no "ignore events for the next N milliseconds" heuristics are needed.
func (s *Service) PutFile(ctx context.Context, path string, content []byte, mode, uid, gid uint32) error {
	clean, err := core.SanitizePath(path)
	if err != nil {
		return err
	}
	if int64(len(content)) > s.cfg.MaxFileSize {
		return fmt.Errorf("%w: %s is %d bytes, limit is %d",
			ErrTooLarge, clean, len(content), s.cfg.MaxFileSize)
	}
	mode = core.SanitizeMode(mode)

	s.write.Lock()
	defer s.write.Unlock()

	cur, have, err := s.lookup(ctx, clean)
	if err != nil {
		return err
	}
	hash := core.HashContent(content)
	if have && !cur.Deleted && cur.Kind == core.KindFile &&
		cur.Hash == hash && cur.Mode == mode && cur.UID == uid && cur.GID == gid {
		return nil
	}

	if _, err := s.cfg.Store.PutBlob(ctx, content); err != nil {
		return err
	}
	next := core.Meta{
		Path:     clean,
		Kind:     core.KindFile,
		Hash:     hash,
		PrevHash: cur.Hash, // empty when the entry is new; that is the correct signal
		Size:     int64(len(content)),
		Mode:     mode,
		UID:      uid,
		GID:      gid,
		Version:  core.Version{HLC: s.clock.Now(), Origin: s.cfg.Node},
	}
	return s.commit(ctx, next)
}

// PutDir records a locally originated directory.
//
// Directories are replicated as entries in their own right rather than implied
// by the paths beneath them, so that an empty directory — which for config
// trees is often load-bearing, think conf.d — survives replication.
func (s *Service) PutDir(ctx context.Context, path string, mode, uid, gid uint32) error {
	clean, err := core.SanitizePath(path)
	if err != nil {
		return err
	}
	mode = core.SanitizeMode(mode)

	s.write.Lock()
	defer s.write.Unlock()

	cur, have, err := s.lookup(ctx, clean)
	if err != nil {
		return err
	}
	if have && !cur.Deleted && cur.Kind == core.KindDir &&
		cur.Mode == mode && cur.UID == uid && cur.GID == gid {
		return nil
	}
	return s.commit(ctx, core.Meta{
		Path:    clean,
		Kind:    core.KindDir,
		Mode:    mode,
		UID:     uid,
		GID:     gid,
		Version: core.Version{HLC: s.clock.Now(), Origin: s.cfg.Node},
	})
}

// Delete records a locally originated deletion as a tombstone.
//
// The entry is kept, marked deleted, rather than removed. A node that was
// offline during the deletion still holds the file; on rejoin it compares
// against a tombstone and deletes its copy. Remove the row instead and that
// node sees a file the cluster has never heard of and re-announces it,
// undoing the deletion everywhere.
func (s *Service) Delete(ctx context.Context, path string) error {
	clean, err := core.SanitizePath(path)
	if err != nil {
		return err
	}

	s.write.Lock()
	defer s.write.Unlock()

	cur, have, err := s.lookup(ctx, clean)
	if err != nil {
		return err
	}
	if !have || cur.Deleted {
		return nil
	}
	return s.commit(ctx, core.Meta{
		Path:      clean,
		Kind:      cur.Kind,
		PrevHash:  cur.Hash,
		Mode:      cur.Mode,
		UID:       cur.UID,
		GID:       cur.GID,
		Version:   core.Version{HLC: s.clock.Now(), Origin: s.cfg.Node},
		Deleted:   true,
		DeletedAt: time.Now().UnixNano(),
	})
}

// lookup reads the current entry, translating "absent" into a boolean rather
// than an error so callers do not each repeat the errors.Is dance.
func (s *Service) lookup(ctx context.Context, path string) (core.Meta, bool, error) {
	cur, err := s.cfg.Store.Get(ctx, path)
	if errors.Is(err, store.ErrNotFound) {
		return core.Meta{}, false, nil
	}
	if err != nil {
		return core.Meta{}, false, err
	}
	return cur, true, nil
}

// commit persists entries, wakes the projections and gossips the new position.
// Callers must hold s.write.
func (s *Service) commit(ctx context.Context, metas ...core.Meta) error {
	head, err := s.cfg.Store.Apply(ctx, metas...)
	if err != nil {
		return err
	}
	s.notify()
	// Announce after the write has landed, never before: a peer that pulls on
	// the strength of a hint for a write that then failed would see nothing,
	// and we would have advertised state we do not have.
	if s.cfg.Peers != nil {
		s.cfg.Peers.Announce(head)
	}
	return nil
}

// Live returns the current non-deleted tree, which is what the projections
// render.
func (s *Service) Live(ctx context.Context) ([]core.Meta, error) {
	return s.cfg.Store.Live(ctx)
}

// Get returns one entry, tombstones included.
func (s *Service) Get(ctx context.Context, path string) (core.Meta, error) {
	return s.cfg.Store.Get(ctx, path)
}

// Content returns a file's bytes.
func (s *Service) Content(ctx context.Context, hash string) ([]byte, error) {
	return s.cfg.Store.Blob(ctx, hash)
}

// MaxFileSize reports the configured per-file cap, so projections can reject an
// oversized write at the point the user makes it rather than after reading it
// all into memory.
func (s *Service) MaxFileSize() int64 { return s.cfg.MaxFileSize }

// Conflicts lists the conflict copies currently in the tree. A non-empty result
// means a human has to choose between two versions of a config file.
func (s *Service) Conflicts(ctx context.Context) ([]string, error) {
	entries, err := s.cfg.Store.Live(ctx)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, m := range entries {
		if core.IsConflictName(m.Path) {
			out = append(out, m.Path)
		}
	}
	return out, nil
}
