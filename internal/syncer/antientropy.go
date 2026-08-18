package syncer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/cluster"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"
)

// Run drives replication until ctx is cancelled: it reacts to cluster events,
// runs anti-entropy on a timer, and collects garbage on another.
//
// The timer is what makes this correct. Gossip hints are best-effort — a
// dropped UDP datagram, a node that was down when the write happened, a full
// event buffer — so nothing may depend on receiving one. The periodic pass
// re-derives the whole answer from the peer's changes feed, which means the
// worst consequence of losing every hint is that convergence takes until the
// next tick instead of milliseconds.
func (s *Service) Run(ctx context.Context) error {
	sync := time.NewTicker(s.cfg.SyncInterval)
	defer sync.Stop()
	janitor := time.NewTicker(s.cfg.JanitorInterval)
	defer janitor.Stop()

	events := make(<-chan cluster.Event)
	if s.cfg.Peers != nil {
		events = s.cfg.Peers.Events()
	}

	for {
		select {
		case <-ctx.Done():
			return nil

		case e, ok := <-events:
			if !ok {
				// The cluster shut down; keep serving locally, since the store
				// is authoritative with or without peers.
				events = nil
				continue
			}
			switch e.Kind {
			case cluster.EventJoin, cluster.EventHint:
				// A join is the rejoin case: a node that has been away, or one
				// that has never seen us, and either way the pull below starts
				// from wherever its feed left off.
				s.triggerSync(ctx, e.Node, e.Addr)
			case cluster.EventLeave:
				s.log.Info("peer left; its entries stay in the tree", "node", e.Node)
			}

		case <-sync.C:
			s.syncAll(ctx)

		case <-janitor.C:
			s.collect(ctx)
		}
	}
}

// syncAll starts a pull against every known peer.
func (s *Service) syncAll(ctx context.Context) {
	if s.cfg.Peers == nil {
		return
	}
	for _, m := range s.cfg.Peers.Members() {
		if m.Self || m.Addr == "" {
			continue
		}
		s.triggerSync(ctx, m.Node, m.Addr)
	}
}

// triggerSync starts a pull unless one against this peer is already running.
//
// The guard matters because a node applying a large batch gossips a hint per
// commit. Without it, a peer catching up would answer each hint with its own
// concurrent pull, and a handful of nodes would generate a burst of duplicate
// full-tree transfers at exactly the moment they can least afford it.
func (s *Service) triggerSync(ctx context.Context, node, addr string) {
	if node == "" || addr == "" || node == s.cfg.Node {
		return
	}
	if _, busy := s.syncing.LoadOrStore(node, struct{}{}); busy {
		return
	}
	go func() {
		defer s.syncing.Delete(node)
		if err := s.syncPeer(ctx, node, addr); err != nil && ctx.Err() == nil {
			s.log.Warn("sync with peer failed", "node", node, "addr", addr, "err", err)
			s.recordSync(err)
			return
		}
		s.recordSync(nil)
	}()
}

// syncPeer brings this node up to date with one peer.
func (s *Service) syncPeer(ctx context.Context, node, addr string) error {
	// Compare digests first. Two nodes that already agree exchange one hash
	// instead of a manifest listing every path in /etc, which is what makes a
	// short sync interval affordable.
	remote, err := s.cfg.Peer.Digest(ctx, addr)
	if err != nil {
		return fmt.Errorf("fetch digest: %w", err)
	}
	local, _, _, err := s.cfg.Store.Digest(ctx)
	if err != nil {
		return err
	}
	if local == remote.Digest {
		return s.cfg.Store.SetPeerSeq(ctx, node, addr, remote.Head, time.Now().UnixNano())
	}

	since, err := s.cfg.Store.PeerSeq(ctx, node)
	if err != nil {
		return err
	}
	// A peer whose feed head is behind where we last read has been rebuilt from
	// scratch — a wiped database, a fresh container with the same node name.
	// Its seq numbering restarted, so our bookmark points into a feed that no
	// longer exists and we must re-read from the beginning.
	if remote.Head < since {
		s.log.Info("peer feed went backwards; restarting from the beginning",
			"node", node, "their_head", remote.Head, "our_bookmark", since)
		since = 0
	}

	// cursor walks the peer's feed; bookmark is what gets persisted. They differ
	// whenever an entry has to be deferred: the walk must move on to finish the
	// page, but the bookmark must stay behind the deferral so the next pass
	// picks it up again.
	cursor := since
	bookmark := since
	for {
		page, err := s.cfg.Peer.Manifest(ctx, addr, cursor)
		if err != nil {
			return fmt.Errorf("fetch manifest since %d: %w", cursor, err)
		}
		if len(page.Entries) == 0 {
			break
		}
		safe, err := s.applyRemote(ctx, addr, page.Entries)
		if err != nil {
			return err
		}

		cursor = page.Head
		bookmark = max(bookmark, safe)
		// Record the bookmark after each page, not at the end. A sync
		// interrupted by shutdown or a network fault then resumes near where it
		// stopped instead of re-fetching everything.
		if err := s.cfg.Store.SetPeerSeq(ctx, node, addr, bookmark, time.Now().UnixNano()); err != nil {
			return err
		}
		if !page.More {
			break
		}
	}
	return nil
}

// applyRemote validates, resolves and stores a page of a peer's entries.
//
// It returns the highest feed position that may safely be bookmarked. Entries
// at or after the first *deferral* — an entry whose content could not be
// fetched — are excluded, so the next pass re-reads them. Without that
// distinction a transient blob-fetch failure would advance the bookmark past
// the entry and this node would stay short of that one file forever, with a
// digest that never matches and no mechanism left to notice.
//
// A *rejected* entry is different and does advance the bookmark: a malformed
// path or an oversized file will never become acceptable, so retrying it every
// cycle would stall the feed permanently on a single bad row.
func (s *Service) applyRemote(ctx context.Context, addr string, entries []core.Meta) (int64, error) {
	s.write.Lock()
	defer s.write.Unlock()

	var (
		batch    []core.Meta
		conflict int
		safe     int64
		deferred bool
	)
	// advance records that everything up to and including this entry is
	// accounted for, unless something earlier was deferred.
	advance := func(m core.Meta) {
		if !deferred {
			safe = max(safe, m.Seq)
		}
	}

	for _, remote := range entries {
		if err := s.validate(remote); err != nil {
			// One bad entry must not abort the page: the rest of the peer's
			// tree is still worth having, and refusing it wholesale would let a
			// single malformed path stall replication indefinitely.
			s.log.Warn("rejected entry from peer",
				"peer_addr", addr, "path", remote.Path, "err", err)
			advance(remote)
			continue
		}
		// Fold the peer's clock into ours so a subsequent local write is
		// ordered after the remote write it reacts to. A reading too far in the
		// future is refused here, which is what stops one node with a broken
		// RTC from winning every future conflict cluster-wide.
		if _, err := s.clock.Observe(remote.Version.HLC); err != nil {
			s.log.Warn("rejected entry with implausible timestamp",
				"peer_addr", addr, "path", remote.Path, "err", err)
			advance(remote)
			continue
		}

		local, have, err := s.lookup(ctx, remote.Path)
		if err != nil {
			return 0, err
		}
		switch resolve(local, have, remote) {
		case decisionSkip:
			advance(remote)
			continue
		case decisionConflict:
			copyOf := s.conflictCopy(local)
			s.log.Warn("concurrent edit; preserving the local version as a conflict copy",
				"path", remote.Path, "copy", copyOf.Path,
				"local_version", local.Version.String(), "remote_version", remote.Version.String())
			batch = append(batch, copyOf)
			conflict++
		case decisionAccept:
		}

		// Fetch content only when it is genuinely absent. A file that moved,
		// or that already exists elsewhere in the tree, costs nothing.
		if remote.Kind == core.KindFile && !remote.Deleted && remote.Hash != "" {
			if err := s.ensureBlob(ctx, addr, remote.Hash); err != nil {
				// Storing the entry now would leave it pointing at a blob we
				// cannot serve, so defer it — and, via the flag, hold the
				// bookmark back so the next pass actually retries it.
				s.log.Warn("could not fetch content; deferring entry",
					"peer_addr", addr, "path", remote.Path, "err", err)
				deferred = true
				continue
			}
		}
		batch = append(batch, remote)
		advance(remote)
	}

	if len(batch) == 0 {
		return safe, nil
	}
	if conflict > 0 {
		s.log.Warn("conflict copies written; a human must pick a winner", "count", conflict)
	}
	if err := s.commit(ctx, batch...); err != nil {
		return 0, err
	}
	return safe, nil
}

// validate rejects a peer's entry before it can reach the store. Everything
// here is attacker-controlled if any cluster member is compromised.
func (s *Service) validate(m core.Meta) error {
	if _, err := core.SanitizePath(m.Path); err != nil {
		return err
	}
	if !m.Kind.Valid() {
		return fmt.Errorf("unknown kind %d", m.Kind)
	}
	if m.Version.Origin == "" {
		return errors.New("entry has no origin node")
	}
	if m.Size > s.cfg.MaxFileSize {
		return fmt.Errorf("%w: %d bytes, limit is %d", ErrTooLarge, m.Size, s.cfg.MaxFileSize)
	}
	if m.Hash != "" {
		if len(m.Hash) != 64 {
			return fmt.Errorf("hash is %d characters, want 64", len(m.Hash))
		}
		if _, err := hex.DecodeString(m.Hash); err != nil {
			return fmt.Errorf("hash is not hex: %w", err)
		}
	}
	if m.Kind == core.KindDir && m.Hash != "" {
		return errors.New("directory entry carries content")
	}
	return nil
}

// ensureBlob fetches content from a peer unless it is already local.
func (s *Service) ensureBlob(ctx context.Context, addr, hash string) error {
	have, err := s.cfg.Store.HasBlob(ctx, hash)
	if err != nil {
		return err
	}
	if have {
		return nil
	}
	// The client verifies the returned bytes against this hash before handing
	// them back, so a peer cannot substitute different content here.
	content, err := s.cfg.Peer.Blob(ctx, addr, hash)
	if err != nil {
		return err
	}
	if int64(len(content)) > s.cfg.MaxFileSize {
		return fmt.Errorf("%w: %d bytes", ErrTooLarge, len(content))
	}
	_, err = s.cfg.Store.PutBlob(ctx, content)
	return err
}

// conflictCopy turns the losing local entry into a new entry at a derived path.
//
// It gets a fresh local version so it replicates to every node like any other
// file — the conflict happened to the cluster, not to one machine, and an
// operator should see it wherever they look.
func (s *Service) conflictCopy(local core.Meta) core.Meta {
	copyOf := local
	copyOf.Path = core.ConflictName(local.Path, local.Version.Origin, local.Version.HLC.Wall)
	copyOf.PrevHash = ""
	copyOf.Seq = 0
	copyOf.Version = core.Version{HLC: s.clock.Now(), Origin: s.cfg.Node}
	return copyOf
}

// collect runs the janitor: expired tombstones first, then the blobs that only
// they were keeping alive.
func (s *Service) collect(ctx context.Context) {
	s.write.Lock()
	defer s.write.Unlock()

	horizon := time.Now().Add(-s.cfg.TombstoneTTL).UnixNano()
	tombstones, err := s.cfg.Store.GCTombstones(ctx, horizon)
	if err != nil {
		s.log.Error("collect tombstones", "err", err)
		return
	}
	// Order matters: a tombstone that has just expired is what releases the
	// blob its file used to reference, so blobs are collected second.
	blobs, err := s.cfg.Store.GCBlobs(ctx)
	if err != nil {
		s.log.Error("collect blobs", "err", err)
		return
	}
	if tombstones > 0 || blobs > 0 {
		s.log.Info("collected garbage", "tombstones", tombstones, "blobs", blobs,
			"ttl", s.cfg.TombstoneTTL)
	}
}

// recordSync notes the outcome of the most recent sync for the status endpoint.
func (s *Service) recordSync(err error) {
	s.status.Lock()
	defer s.status.Unlock()
	s.status.lastSync = time.Now()
	if err != nil {
		s.status.lastErr = err.Error()
		return
	}
	s.status.lastErr = ""
}

// SyncState reports when replication last ran and what went wrong if anything
// did. An operator staring at two nodes that disagree needs to know whether
// syncing is failing or merely slow.
func (s *Service) SyncState() (last time.Time, lastErr string) {
	s.status.Lock()
	defer s.status.Unlock()
	return s.status.lastSync, s.status.lastErr
}
