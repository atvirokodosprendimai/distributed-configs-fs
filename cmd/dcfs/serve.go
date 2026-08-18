package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/urfave/cli/v3"
	"golang.org/x/sync/errgroup"

	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/cluster"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/fusefs"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/mirror"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/store"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/syncer"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/transport"
)

// manifestOverhead is how much room a peer response needs beyond one file.
//
// A full manifest page carries metadata for MaxManifestPage entries, so the
// client's body limit has to admit that as well as the largest blob. It is a
// generous constant rather than a computed one because getting it slightly too
// large costs nothing, while getting it too small stalls replication with an
// error that reads like a network fault.
const manifestOverhead = 16 << 20

// runServe starts a node and blocks until the process is signalled.
func runServe(ctx context.Context, cmd *cli.Command) error {
	log := newLogger(cmd.String("log-level"))

	keys, err := core.DeriveKeys(cmd.String("secret"))
	if err != nil {
		// The most common first-run mistake, so it gets a pointed message
		// rather than the raw sentinel.
		return fmt.Errorf("%w — set DCFS_CLUSTER_SECRET to a long random string, "+
			"identical on every node", err)
	}

	node := cmd.String("node")
	dbPath := cmd.String("db")
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		return fmt.Errorf("create database directory: %w", err)
	}
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		return err
	}
	defer func() {
		if err := st.Close(); err != nil {
			log.Error("close store", "err", err)
		}
	}()

	apiAddr := cmd.String("api-addr")
	advertiseAPI := cmd.String("advertise-api")
	if advertiseAPI == "" {
		advertiseAPI = apiAddr
	}
	maxFileSize := int64(cmd.Int("max-file-size"))

	peerClient := transport.NewClient(node, keys, maxFileSize+manifestOverhead, 30*time.Second)

	gossipPort := cmd.Int("gossip-port")
	advertiseAddr := cmd.String("advertise")
	if advertiseAddr == "" && cmd.String("bind") != "0.0.0.0" {
		advertiseAddr = cmd.String("bind")
	}
	advertiseGossipPort := cmd.Int("advertise-gossip-port")
	if advertiseGossipPort == 0 {
		advertiseGossipPort = gossipPort
	}

	members, err := cluster.Join(cluster.Config{
		Node:          node,
		BindAddr:      cmd.String("bind"),
		BindPort:      int(gossipPort),
		AdvertiseAddr: advertiseAddr,
		AdvertisePort: int(advertiseGossipPort),
		APIAddr:       advertiseAPI,
		Join:          cmd.StringSlice("join"),
		Keys:          keys,
		Log:           log,
	})
	if err != nil {
		return err
	}

	svc := syncer.New(syncer.Config{
		Node:            node,
		Store:           st,
		Peers:           members,
		Peer:            peerClient,
		MaxFileSize:     maxFileSize,
		TombstoneTTL:    cmd.Duration("tombstone-ttl"),
		SyncInterval:    cmd.Duration("sync-interval"),
		JanitorInterval: cmd.Duration("janitor-interval"),
		Log:             log,
	})

	mirrorDir := cmd.String("mirror")
	mountPoint := cmd.String("mount")
	if mirrorDir == "" && mountPoint == "" {
		// Legitimate — such a node still replicates and serves peers — but
		// almost always a misconfiguration, so it is called out.
		log.Warn("neither --mirror nor --mount is set; this node replicates but exposes nothing locally")
	}

	started := time.Now()
	api := transport.NewServer(st, func(ctx context.Context) (transport.Status, error) {
		return buildStatus(ctx, statusInputs{
			node: node, addr: advertiseAPI, started: started,
			store: st, svc: svc, members: members,
			mirrorDir: mirrorDir, mountPoint: mountPoint,
		})
	}, keys, log)

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error { return api.Serve(ctx, apiAddr) })
	group.Go(func() error { return svc.Run(ctx) })

	if mirrorDir != "" {
		mir, err := mirror.Open(mirror.Config{
			Dir:          mirrorDir,
			Tree:         svc,
			ScanInterval: cmd.Duration("scan-interval"),
			Settle:       cmd.Duration("settle"),
			Log:          log,
		})
		if err != nil {
			return err
		}
		defer mir.Close()
		group.Go(func() error { return mir.Run(ctx) })
		log.Info("mirroring tree to directory", "dir", mirrorDir)
	}

	if mountPoint != "" {
		server, err := mountTree(mountPoint, svc, log)
		if err != nil {
			return err
		}
		// Unmount before anything else tears down. A FUSE mount whose server
		// has gone leaves a directory that hangs every process touching it,
		// including the next start of this one.
		defer func() {
			if err := server.Unmount(); err != nil {
				log.Error("unmount; the mountpoint may need `fusermount -u`",
					"mount", mountPoint, "err", err)
			}
		}()
		group.Go(func() error {
			<-ctx.Done()
			return nil
		})
		log.Info("mounted tree", "mount", mountPoint)
	}

	log.Info("node started",
		"node", node, "api", apiAddr, "gossip", net.JoinHostPort(cmd.String("bind"), strconv.Itoa(int(gossipPort))),
		"db", dbPath, "max_file_size", maxFileSize, "tombstone_ttl", cmd.Duration("tombstone-ttl"))

	err = group.Wait()

	// Announce departure so peers mark this node gone immediately instead of
	// waiting out the failure detector and reporting a clean restart as a fault.
	if lerr := members.Leave(5 * time.Second); lerr != nil {
		log.Error("leave cluster", "err", lerr)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	log.Info("node stopped", "node", node)
	return nil
}

// mountTree mounts the FUSE projection, creating the mountpoint if needed.
func mountTree(mountPoint string, svc *syncer.Service, log *slog.Logger) (*fuse.Server, error) {
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		return nil, fmt.Errorf("create mountpoint: %w", err)
	}
	server, err := fusefs.Mount(mountPoint, svc, log)
	if err != nil {
		// The three things that actually go wrong here, named, because the
		// underlying error is usually just "operation not permitted".
		return nil, fmt.Errorf("%w — a container needs --device /dev/fuse, "+
			"--cap-add SYS_ADMIN, and a bind mount with rshared propagation "+
			"for the mount to be visible on the host", err)
	}
	return server, nil
}
