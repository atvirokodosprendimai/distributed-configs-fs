// Package cluster is membership: who else is out there, are they alive, and
// where do I reach their API.
//
// It wraps hashicorp/memberlist, which is a SWIM gossip implementation. That
// choice bounds what this package can promise, and the boundary matters:
// memberlist provides membership, failure detection and best-effort broadcast.
// It provides no ordering and no quorum. Nothing here decides which write wins
// — that is the syncer's job, using the hybrid logical clock — and nothing here
// can stop a partitioned node from accepting writes. This cluster is AP by
// construction; conflicts are resolved after the fact, not prevented.
//
// The only thing gossiped is a sync hint: "node X is now at feed position N".
// File contents and metadata are pulled over the peer HTTP API. Publishing the
// position rather than the payload keeps datagrams small, avoids fragmenting
// config files across a UDP protocol that was never meant to carry them, and
// means a hint that arrives out of order or twice costs nothing — the puller
// re-reads the truth either way.
package cluster

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"
)

// EventKind classifies a membership or peer-state event.
type EventKind uint8

// The kinds of event the syncer reacts to.
const (
	// EventJoin fires when a node appears, including at startup for every
	// existing member. It is the trigger for a full anti-entropy pass against
	// that peer — which is exactly what makes a rejoining node catch up.
	EventJoin EventKind = iota + 1
	// EventLeave fires when a node departs or is declared failed.
	EventLeave
	// EventHint fires when a peer gossips that its feed has advanced.
	EventHint
)

// String implements fmt.Stringer.
func (k EventKind) String() string {
	switch k {
	case EventJoin:
		return "join"
	case EventLeave:
		return "leave"
	case EventHint:
		return "hint"
	default:
		return "event(" + strconv.Itoa(int(k)) + ")"
	}
}

// Event is something that happened to the cluster's view of a peer.
type Event struct {
	Kind EventKind
	Node string
	Addr string // the peer's API address, from its gossiped metadata
	Seq  int64  // the peer's feed position, for EventHint
}

// hint is the gossip payload: a peer's name and how far its changes feed has
// advanced. Nothing else travels over gossip.
type hint struct {
	Node string `json:"n"`
	Addr string `json:"a"`
	Seq  int64  `json:"s"`
}

// nodeMeta is what a node advertises about itself in memberlist's per-node
// metadata, so peers know where to send HTTP pulls.
type nodeMeta struct {
	API string `json:"api"`
}

// Config configures a cluster member.
type Config struct {
	// Node is this node's unique name. Two nodes sharing a name will fight
	// over identity in memberlist and break LWW's origin tiebreak, so it must
	// be genuinely unique across the cluster.
	Node string
	// BindAddr and BindPort are where gossip listens.
	BindAddr string
	BindPort int
	// AdvertiseAddr and AdvertisePort are what peers are told to reach. They
	// differ from the bind address behind NAT and inside container networks
	// with published ports, which is the common case for this program.
	AdvertiseAddr string
	AdvertisePort int
	// APIAddr is this node's peer HTTP address, gossiped so peers can pull.
	APIAddr string
	// Join lists seed addresses to contact at startup. Any one reachable seed
	// is enough; membership propagates from there.
	Join []string
	// Keys carries the gossip key. Gossip is encrypted because membership
	// metadata alone tells an observer the shape of the cluster.
	Keys core.Keys
	Log  *slog.Logger
}

// Cluster is this node's membership view.
type Cluster struct {
	cfg    Config
	ml     *memberlist.Memberlist
	queue  *memberlist.TransmitLimitedQueue
	events chan Event
	log    *slog.Logger

	// seq is the local feed position last broadcast. Atomic because the syncer
	// updates it from its apply loop while memberlist reads it from its own
	// goroutines.
	seq atomic.Int64

	closeOnce sync.Once
}

// EventBuffer is how many events may queue before the oldest are dropped.
type EventBuffer = int

// eventBuffer sizes the event channel.
//
// Events are dropped rather than blocking when it fills. This is not laziness:
// NotifyMsg and the event delegate run on memberlist's protocol goroutines, and
// blocking there stalls the entire UDP receive loop for every peer. A dropped
// hint costs nothing because the periodic anti-entropy pass re-derives the
// truth regardless — hints only make convergence faster, never correct.
const eventBuffer = 256

// Join starts gossiping and contacts the configured seeds.
//
// Failing to reach a seed is not fatal. A node that boots before its peers must
// still come up and serve its local tree; memberlist keeps retrying, and the
// first peer to reach us pulls us into the cluster from the other side.
func Join(cfg Config) (*Cluster, error) {
	if cfg.Node == "" {
		return nil, fmt.Errorf("cluster: node name is required")
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}

	c := &Cluster{
		cfg:    cfg,
		events: make(chan Event, eventBuffer),
		log:    cfg.Log,
	}

	mlCfg := memberlist.DefaultLANConfig()
	mlCfg.Name = cfg.Node
	mlCfg.BindAddr = cfg.BindAddr
	mlCfg.BindPort = cfg.BindPort
	mlCfg.AdvertiseAddr = cfg.AdvertiseAddr
	mlCfg.AdvertisePort = cfg.AdvertisePort
	mlCfg.SecretKey = cfg.Keys.Gossip
	mlCfg.Delegate = (*delegate)(c)
	mlCfg.Events = (*events)(c)
	// memberlist logs through the standard library; bridge it into slog at
	// debug so its routine chatter does not drown this program's own log.
	mlCfg.Logger = slog.NewLogLogger(cfg.Log.Handler(), slog.LevelDebug)

	c.queue = &memberlist.TransmitLimitedQueue{
		NumNodes:       func() int { return c.ml.NumMembers() },
		RetransmitMult: mlCfg.RetransmitMult,
	}

	ml, err := memberlist.Create(mlCfg)
	if err != nil {
		return nil, fmt.Errorf("start gossip on %s:%d: %w", cfg.BindAddr, cfg.BindPort, err)
	}
	c.ml = ml

	if len(cfg.Join) > 0 {
		joined, err := ml.Join(cfg.Join)
		if err != nil {
			// Warn, do not fail: see the doc comment above.
			cfg.Log.Warn("could not reach any seed at startup; will keep trying",
				"seeds", cfg.Join, "err", err)
		} else {
			cfg.Log.Info("joined cluster", "contacted", joined, "seeds", cfg.Join)
		}
	}
	return c, nil
}

// Events returns the stream of membership and hint events. It is closed when
// the cluster shuts down.
func (c *Cluster) Events() <-chan Event { return c.events }

// Announce gossips that this node's changes feed has reached seq, so peers pull
// promptly instead of waiting for the next anti-entropy tick.
//
// It is advisory. A hint that is dropped, duplicated or delivered out of order
// changes nothing, because the receiver pulls the authoritative state anyway.
func (c *Cluster) Announce(seq int64) {
	c.seq.Store(seq)
	payload, err := json.Marshal(hint{Node: c.cfg.Node, Addr: c.cfg.APIAddr, Seq: seq})
	if err != nil {
		c.log.Error("marshal sync hint", "err", err)
		return
	}
	c.queue.QueueBroadcast(broadcast(payload))
}

// Members returns the current membership, self included.
func (c *Cluster) Members() []Member {
	nodes := c.ml.Members()
	out := make([]Member, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, Member{
			Node: n.Name,
			Addr: apiAddrOf(n),
			Self: n.Name == c.cfg.Node,
		})
	}
	return out
}

// Member is one node in the current membership view.
type Member struct {
	Node string
	Addr string // peer API address
	Self bool
}

// Leave announces departure and shuts gossip down.
//
// The announcement matters: without it peers wait out the full failure
// detector before marking this node gone, and an operator restarting a node
// sees it reported as failed for no reason.
func (c *Cluster) Leave(timeout time.Duration) error {
	var err error
	c.closeOnce.Do(func() {
		if lerr := c.ml.Leave(timeout); lerr != nil {
			err = fmt.Errorf("announce leave: %w", lerr)
		}
		if serr := c.ml.Shutdown(); serr != nil && err == nil {
			err = fmt.Errorf("shutdown gossip: %w", serr)
		}
		close(c.events)
	})
	return err
}

// emit delivers an event without ever blocking memberlist's goroutines.
func (c *Cluster) emit(e Event) {
	select {
	case c.events <- e:
	default:
		// See eventBuffer: dropping is correct, because anti-entropy is what
		// guarantees convergence and hints only accelerate it.
		c.log.Debug("dropped cluster event; consumer is behind",
			"kind", e.Kind, "node", e.Node)
	}
}

// apiAddrOf extracts a node's gossiped API address, falling back to nothing
// when the metadata is absent or unreadable — a peer we cannot address is one
// we simply do not pull from.
func apiAddrOf(n *memberlist.Node) string {
	if len(n.Meta) == 0 {
		return ""
	}
	var meta nodeMeta
	if err := json.Unmarshal(n.Meta, &meta); err != nil {
		return ""
	}
	return meta.API
}

// broadcast is a one-shot memberlist broadcast carrying a sync hint.
type broadcast []byte

// Invalidates reports whether this broadcast supersedes another.
//
// It always returns false, which looks wasteful but is not. Hints are already
// tiny and already idempotent, and a superseding rule would need to compare
// node names inside the payload on every queue operation. Letting an older
// hint ride along costs one datagram; getting the comparison wrong costs a
// missed update.
func (b broadcast) Invalidates(memberlist.Broadcast) bool { return false }

// Message returns the payload.
func (b broadcast) Message() []byte { return b }

// Finished is required by the interface; there is nothing to clean up.
func (b broadcast) Finished() {}

// delegate implements memberlist.Delegate. It is a distinct type over *Cluster
// so the protocol callbacks do not clutter Cluster's own method set.
type delegate Cluster

// NodeMeta advertises this node's API address to peers.
func (d *delegate) NodeMeta(limit int) []byte {
	meta, err := json.Marshal(nodeMeta{API: d.cfg.APIAddr})
	if err != nil || len(meta) > limit {
		// Exceeding the limit would make memberlist truncate the metadata into
		// unparseable JSON, so advertise nothing instead. Peers then skip us
		// for pulls, which is degraded but coherent.
		d.log.Error("cannot advertise api address",
			"addr", d.cfg.APIAddr, "limit", limit, "err", err)
		return nil
	}
	return meta
}

// NotifyMsg handles an inbound sync hint. It must not block: it runs on the UDP
// receive loop.
func (d *delegate) NotifyMsg(msg []byte) {
	var h hint
	if err := json.Unmarshal(msg, &h); err != nil {
		d.log.Debug("discarded malformed gossip payload", "err", err)
		return
	}
	if h.Node == "" || h.Node == d.cfg.Node {
		return // our own hint, echoed back through the mesh
	}
	(*Cluster)(d).emit(Event{Kind: EventHint, Node: h.Node, Addr: h.Addr, Seq: h.Seq})
}

// GetBroadcasts hands memberlist the queued hints.
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.queue.GetBroadcasts(overhead, limit)
}

// LocalState is unused. memberlist's TCP push/pull could carry the tree digest
// and shave a few seconds off detecting divergence, but the syncer's periodic
// anti-entropy pass already guarantees convergence on its own. A second,
// subtly different sync path is a second thing to get right for no change in
// outcome.
func (d *delegate) LocalState(bool) []byte { return nil }

// MergeRemoteState is unused; see LocalState.
func (d *delegate) MergeRemoteState([]byte, bool) {}

// events implements memberlist.EventDelegate.
type events Cluster

// NotifyJoin reports a node appearing. It fires for every existing member when
// this node starts, which is what kicks off the initial full sync.
func (e *events) NotifyJoin(n *memberlist.Node) {
	if n.Name == e.cfg.Node {
		return
	}
	e.log.Info("peer joined", "node", n.Name, "addr", net.JoinHostPort(n.Addr.String(), strconv.Itoa(int(n.Port))))
	(*Cluster)(e).emit(Event{Kind: EventJoin, Node: n.Name, Addr: apiAddrOf(n)})
}

// NotifyLeave reports a node departing or being declared failed.
func (e *events) NotifyLeave(n *memberlist.Node) {
	if n.Name == e.cfg.Node {
		return
	}
	e.log.Info("peer left", "node", n.Name)
	(*Cluster)(e).emit(Event{Kind: EventLeave, Node: n.Name, Addr: apiAddrOf(n)})
}

// NotifyUpdate reports a node's metadata changing — most usefully, a peer that
// restarted on a different API port. Treated as a join so we re-sync against
// the new address.
func (e *events) NotifyUpdate(n *memberlist.Node) {
	if n.Name == e.cfg.Node {
		return
	}
	(*Cluster)(e).emit(Event{Kind: EventJoin, Node: n.Name, Addr: apiAddrOf(n)})
}
