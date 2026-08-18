package cluster

import (
	"fmt"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"
)

// freePort reserves an ephemeral port and returns it. There is an inherent
// race between releasing it and memberlist binding it, but it is the standard
// approach and far less flaky than hardcoding ports across parallel tests.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return port
}

func testKeys(t *testing.T) core.Keys {
	t.Helper()
	keys, err := core.DeriveKeys("a-sufficiently-long-cluster-secret")
	if err != nil {
		t.Fatalf("DeriveKeys() = %v", err)
	}
	return keys
}

// start brings up one member bound to loopback.
func start(t *testing.T, name string, keys core.Keys, seeds []string) (*Cluster, int) {
	t.Helper()
	port := freePort(t)
	c, err := Join(Config{
		Node:          name,
		BindAddr:      "127.0.0.1",
		BindPort:      port,
		AdvertiseAddr: "127.0.0.1",
		AdvertisePort: port,
		APIAddr:       fmt.Sprintf("127.0.0.1:%d", port+10000),
		Join:          seeds,
		Keys:          keys,
		Log:           slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatalf("Join(%s) = %v", name, err)
	}
	t.Cleanup(func() { _ = c.Leave(time.Second) })
	return c, port
}

// waitFor polls until cond holds or the deadline passes. Gossip is
// asynchronous, so tests wait on the observable outcome rather than sleeping a
// guessed interval.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestTwoNodesJoinAndExchangeHints is the end-to-end check on this package:
// two members find each other, learn each other's API address from gossiped
// metadata, and a hint from one reaches the other.
func TestTwoNodesJoinAndExchangeHints(t *testing.T) {
	keys := testKeys(t)

	a, aPort := start(t, "node-a", keys, nil)
	b, _ := start(t, "node-b", keys, []string{fmt.Sprintf("127.0.0.1:%d", aPort)})

	waitFor(t, "both nodes to see two members", func() bool {
		return len(a.Members()) == 2 && len(b.Members()) == 2
	})

	// The API address must survive the trip: without it a peer is discovered
	// but unreachable, and no data ever moves.
	var peer Member
	for _, m := range a.Members() {
		if !m.Self {
			peer = m
		}
	}
	if peer.Node != "node-b" {
		t.Fatalf("node-a sees peer %q, want node-b", peer.Node)
	}
	if peer.Addr == "" {
		t.Fatal("peer API address did not propagate through node metadata")
	}

	// Drain the join events so the hint assertion cannot pass on a stale one.
	drain(a.Events())

	b.Announce(42)
	waitFor(t, "node-a to receive node-b's hint", func() bool {
		select {
		case e := <-a.Events():
			if e.Kind == EventHint && e.Node == "node-b" && e.Seq == 42 {
				return true
			}
		case <-time.After(50 * time.Millisecond):
		}
		return false
	})
}

// drain empties a channel without blocking.
func drain[T any](ch <-chan T) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// TestWrongSecretCannotJoin pins the trust model: possession of the cluster
// secret is membership. A node with the wrong secret must not get in, because
// membership is what grants the right to serve config files.
func TestWrongSecretCannotJoin(t *testing.T) {
	good := testKeys(t)
	bad, err := core.DeriveKeys("an-entirely-different-cluster-secret")
	if err != nil {
		t.Fatalf("DeriveKeys() = %v", err)
	}

	a, aPort := start(t, "node-a", good, nil)
	intruder, _ := start(t, "intruder", bad, nil)

	// Join is expected to fail; the assertion is on the membership that
	// follows, since memberlist can report a contacted node before the
	// encrypted handshake is rejected.
	_, _ = intruder.ml.Join([]string{fmt.Sprintf("127.0.0.1:%d", aPort)})

	// Give gossip time to fail to converge, then assert both are still alone.
	time.Sleep(2 * time.Second)
	if n := len(a.Members()); n != 1 {
		t.Errorf("node-a has %d members, want 1 — a node with the wrong secret got in", n)
	}
	if n := len(intruder.Members()); n != 1 {
		t.Errorf("intruder has %d members, want 1 — it joined a cluster it has no key for", n)
	}
}

// TestEmitDropsRatherThanBlocks pins the rule that memberlist's protocol
// goroutines are never blocked by a slow consumer. Blocking NotifyMsg stalls
// the UDP receive loop for every peer, which is a far worse failure than
// losing an advisory hint.
func TestEmitDropsRatherThanBlocks(t *testing.T) {
	t.Parallel()

	c := &Cluster{
		events: make(chan Event, 2),
		log:    slog.New(slog.DiscardHandler),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Many more events than the buffer holds, with nobody reading.
		for i := range 100 {
			c.emit(Event{Kind: EventHint, Node: "peer", Seq: int64(i)})
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("emit blocked on a full channel; this would stall memberlist's receive loop")
	}
	if len(c.events) != 2 {
		t.Errorf("buffered %d events, want the channel capped at 2", len(c.events))
	}
}

func TestJoinRequiresNodeName(t *testing.T) {
	t.Parallel()

	if _, err := Join(Config{Keys: testKeys(t)}); err == nil {
		t.Fatal("Join() accepted an empty node name; LWW's origin tiebreak needs it unique")
	}
}

// TestJoinSurvivesUnreachableSeeds covers the ordinary startup race in
// docker-compose, where a node boots before its seeds exist. It must come up
// and serve its local tree rather than exiting.
func TestJoinSurvivesUnreachableSeeds(t *testing.T) {
	keys := testKeys(t)
	port := freePort(t)

	c, err := Join(Config{
		Node:          "lonely",
		BindAddr:      "127.0.0.1",
		BindPort:      port,
		AdvertiseAddr: "127.0.0.1",
		AdvertisePort: port,
		APIAddr:       "127.0.0.1:1",
		Join:          []string{"127.0.0.1:1"}, // nothing is listening there
		Keys:          keys,
		Log:           slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatalf("Join() with an unreachable seed = %v, want the node to start anyway", err)
	}
	t.Cleanup(func() { _ = c.Leave(time.Second) })

	if n := len(c.Members()); n != 1 {
		t.Errorf("Members() = %d, want just self", n)
	}
}
