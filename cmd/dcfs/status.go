package main

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/urfave/cli/v3"

	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/cluster"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/store"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/syncer"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/transport"
)

// statusInputs are the pieces the status read model is assembled from. Status
// spans the store, the cluster and the syncer, so it is built here rather than
// in any one of them.
type statusInputs struct {
	node       string
	addr       string
	started    time.Time
	store      *store.Store
	svc        *syncer.Service
	members    *cluster.Cluster
	mirrorDir  string
	mountPoint string
}

// buildStatus assembles the status document fresh on every request.
//
// Nothing here is cached. A status document that can go stale is worse than no
// status document, because an operator comparing two nodes believes it.
func buildStatus(ctx context.Context, in statusInputs) (transport.Status, error) {
	stats, err := in.store.Stats(ctx)
	if err != nil {
		return transport.Status{}, err
	}
	digest, _, _, err := in.store.Digest(ctx)
	if err != nil {
		return transport.Status{}, err
	}
	conflicts, err := in.svc.Conflicts(ctx)
	if err != nil {
		return transport.Status{}, err
	}
	// How far this node has read each peer's feed, which is the useful sense of
	// "how current am I with them".
	bookmarks, err := in.store.Peers(ctx)
	if err != nil {
		return transport.Status{}, err
	}
	seqOf := make(map[string]int64, len(bookmarks))
	for _, p := range bookmarks {
		seqOf[p.Node] = p.LastSeq
	}

	var members []transport.Member
	for _, m := range in.members.Members() {
		members = append(members, transport.Member{
			Node: m.Node, Addr: m.Addr, Alive: true, Self: m.Self,
			LastSeq: seqOf[m.Node],
		})
	}

	lastSync, lastErr := in.svc.SyncState()
	return transport.Status{
		Node:       in.node,
		Addr:       in.addr,
		StartedAt:  in.started,
		Digest:     digest,
		Entries:    stats.Entries,
		Tombstones: stats.Tombstones,
		Blobs:      stats.Blobs,
		BlobBytes:  stats.BlobBytes,
		Head:       stats.Head,
		Conflicts:  conflicts,
		Members:    members,
		MirrorDir:  in.mirrorDir,
		MountPoint: in.mountPoint,
		LastSync:   lastSync,
		LastError:  lastErr,
	}, nil
}

// fetchStatus asks the local node for its status document.
func fetchStatus(ctx context.Context, cmd *cli.Command) (transport.Status, error) {
	keys, err := core.DeriveKeys(cmd.String("secret"))
	if err != nil {
		return transport.Status{}, fmt.Errorf("%w — set DCFS_CLUSTER_SECRET to the cluster's secret", err)
	}
	addr := cmd.String("api-addr")
	client := transport.NewClient(cmd.String("node"), keys, manifestOverhead, 10*time.Second)

	st, err := client.Status(ctx, addr)
	if err != nil {
		return transport.Status{}, fmt.Errorf("reach the node at %s: %w "+
			"(is dcfs serve running, and is --api-addr right?)", addr, err)
	}
	return st, nil
}

// runStatus prints this node's view of the cluster.
func runStatus(ctx context.Context, cmd *cli.Command) error {
	st, err := fetchStatus(ctx, cmd)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "node\t%s\n", st.Node)
	fmt.Fprintf(w, "uptime\t%s\n", time.Since(st.StartedAt).Truncate(time.Second))
	// The digest is the field that answers "are these two nodes actually in
	// sync?" without diffing two directories, so it leads.
	fmt.Fprintf(w, "digest\t%s\n", short(st.Digest))
	fmt.Fprintf(w, "entries\t%d (%d tombstones)\n", st.Entries, st.Tombstones)
	fmt.Fprintf(w, "content\t%d blobs, %s\n", st.Blobs, humanBytes(st.BlobBytes))
	fmt.Fprintf(w, "feed head\t%d\n", st.Head)
	if st.MirrorDir != "" {
		fmt.Fprintf(w, "mirror\t%s\n", st.MirrorDir)
	}
	if st.MountPoint != "" {
		fmt.Fprintf(w, "mount\t%s\n", st.MountPoint)
	}
	if st.LastSync.IsZero() {
		fmt.Fprintf(w, "last sync\tnever\n")
	} else {
		fmt.Fprintf(w, "last sync\t%s ago\n", time.Since(st.LastSync).Truncate(time.Second))
	}
	if st.LastError != "" {
		fmt.Fprintf(w, "last error\t%s\n", st.LastError)
	}
	fmt.Fprintf(w, "members\t%d\n", len(st.Members))
	fmt.Fprintf(w, "conflicts\t%d\n", len(st.Conflicts))
	if err := w.Flush(); err != nil {
		return err
	}

	// Conflicts are the one line here that is a call to action, so they are
	// spelled out rather than left as a count.
	if len(st.Conflicts) > 0 {
		fmt.Println()
		fmt.Println("conflict copies await a decision:")
		for _, p := range st.Conflicts {
			fmt.Printf("  %s\n", p)
		}
	}
	return nil
}

// runPeers lists cluster members.
func runPeers(ctx context.Context, cmd *cli.Command) error {
	st, err := fetchStatus(ctx, cmd)
	if err != nil {
		return err
	}
	if len(st.Members) == 0 {
		fmt.Println("no members; this node has not reached any peer")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NODE\tAPI ADDRESS\tROLE\tREAD TO")
	for _, m := range st.Members {
		role := "peer"
		if m.Self {
			role = "self"
		}
		addr := m.Addr
		if addr == "" {
			// A member whose metadata has not propagated cannot be pulled from,
			// which is worth showing rather than printing an empty column.
			addr = "(not advertised)"
		}
		readTo := fmt.Sprint(m.LastSeq)
		if m.Self {
			readTo = "-"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", m.Node, addr, role, readTo)
	}
	return w.Flush()
}

// runConflicts lists conflict copies.
func runConflicts(ctx context.Context, cmd *cli.Command) error {
	st, err := fetchStatus(ctx, cmd)
	if err != nil {
		return err
	}
	if len(st.Conflicts) == 0 {
		fmt.Println("no conflicts")
		return nil
	}
	for _, p := range st.Conflicts {
		fmt.Println(p)
	}
	// A non-zero exit so a health check or a cron job can act on it without
	// parsing the output.
	return fmt.Errorf("%d conflict copies await a decision; "+
		"compare each with the file it was derived from, then delete the copy", len(st.Conflicts))
}

// short truncates a digest for display. The full value is available over the
// API; on a terminal the first characters are enough to compare two nodes.
func short(digest string) string {
	if len(digest) <= 16 {
		return digest
	}
	return digest[:16]
}

// humanBytes formats a byte count for an operator rather than for a machine.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
