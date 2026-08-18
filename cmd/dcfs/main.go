// Command dcfs runs a distributed-configs-fs node and inspects a running one.
//
// A node replicates one tree of configuration files across a cluster and
// projects it onto a real directory, a FUSE mount, or both. See the README for
// the consistency model; the short version is that it is AP — every node keeps
// accepting writes, ordering is last-writer-wins over a hybrid logical clock,
// and a genuinely concurrent edit is preserved as a conflict copy rather than
// dropped.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/urfave/cli/v3"
)

// version is stamped at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	// The context is cancelled on the first signal; a second one kills the
	// process outright. That is deliberate: shutdown unmounts a filesystem and
	// closes a database, and an operator who has asked twice should not be made
	// to wait for a hung peer to time out.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := command().Run(ctx, os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "dcfs:", err)
		os.Exit(1)
	}
}

// command builds the CLI.
func command() *cli.Command {
	return &cli.Command{
		Name:    "dcfs",
		Usage:   "replicate a tree of configuration files across a cluster",
		Version: version,
		Flags:   globalFlags(),
		Commands: []*cli.Command{
			{
				Name:   "serve",
				Usage:  "run a cluster node",
				Flags:  serveFlags(),
				Action: runServe,
			},
			{
				Name:   "status",
				Usage:  "show this node's view of the cluster",
				Action: runStatus,
			},
			{
				Name:   "peers",
				Usage:  "list cluster members and how current this node is with each",
				Action: runPeers,
			},
			{
				Name:   "conflicts",
				Usage:  "list conflict copies awaiting a human decision",
				Action: runConflicts,
			},
		},
	}
}

// globalFlags are shared by the daemon and the inspection commands: both need
// to know who they are talking to and with what secret.
func globalFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:    "secret",
			Usage:   "shared cluster secret; possession of it is membership",
			Sources: cli.EnvVars("DCFS_CLUSTER_SECRET"),
		},
		&cli.StringFlag{
			Name:    "node",
			Usage:   "this node's unique name (defaults to the hostname)",
			Sources: cli.EnvVars("DCFS_NODE"),
			Value:   defaultNodeName(),
		},
		&cli.StringFlag{
			Name:    "api-addr",
			Usage:   "peer API address to bind, and for the CLI to talk to",
			Sources: cli.EnvVars("DCFS_API_ADDR"),
			Value:   "127.0.0.1:7947",
		},
		&cli.StringFlag{
			Name:    "log-level",
			Usage:   "debug, info, warn or error",
			Sources: cli.EnvVars("DCFS_LOG_LEVEL"),
			Value:   "info",
		},
	}
}

// serveFlags configure the daemon itself.
func serveFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:    "db",
			Usage:   "path to this node's SQLite database, the source of truth",
			Sources: cli.EnvVars("DCFS_DB"),
			Value:   "/var/lib/dcfs/dcfs.db",
		},
		&cli.StringFlag{
			Name:    "mirror",
			Usage:   "directory to project the tree onto; empty disables mirror mode",
			Sources: cli.EnvVars("DCFS_MIRROR_DIR"),
			Value:   "/etc/cluster",
		},
		&cli.StringFlag{
			Name:    "mount",
			Usage:   "FUSE mountpoint; empty disables mount mode",
			Sources: cli.EnvVars("DCFS_MOUNT_POINT"),
		},
		&cli.StringFlag{
			Name:    "bind",
			Usage:   "address gossip listens on",
			Sources: cli.EnvVars("DCFS_BIND_ADDR"),
			Value:   "0.0.0.0",
		},
		&cli.IntFlag{
			Name:    "gossip-port",
			Usage:   "port gossip listens on (TCP and UDP)",
			Sources: cli.EnvVars("DCFS_GOSSIP_PORT"),
			Value:   7946,
		},
		&cli.StringFlag{
			Name: "advertise",
			// Behind NAT or a container network with published ports, what
			// peers must dial is not what this process bound to.
			Usage:   "address peers should use to reach this node (defaults to the bind address)",
			Sources: cli.EnvVars("DCFS_ADVERTISE_ADDR"),
		},
		&cli.IntFlag{
			Name:    "advertise-gossip-port",
			Usage:   "gossip port peers should use (defaults to --gossip-port)",
			Sources: cli.EnvVars("DCFS_ADVERTISE_GOSSIP_PORT"),
		},
		&cli.StringFlag{
			Name:    "advertise-api",
			Usage:   "peer API address peers should use (defaults to --api-addr)",
			Sources: cli.EnvVars("DCFS_ADVERTISE_API"),
		},
		&cli.StringSliceFlag{
			Name:    "join",
			Usage:   "seed addresses to contact at startup; any one reachable seed is enough",
			Sources: cli.EnvVars("DCFS_JOIN"),
		},
		&cli.IntFlag{
			Name:    "max-file-size",
			Usage:   "largest file this node will replicate, in bytes",
			Sources: cli.EnvVars("DCFS_MAX_FILE_SIZE"),
			Value:   1 << 20,
		},
		&cli.DurationFlag{
			Name: "tombstone-ttl",
			// Naming the failure in the help text, because the wrong value here
			// does not fail loudly — it silently resurrects deleted files.
			Usage:   "how long deletions are remembered; must exceed the longest node outage or deleted files come back",
			Sources: cli.EnvVars("DCFS_TOMBSTONE_TTL"),
			Value:   7 * 24 * time.Hour,
		},
		&cli.DurationFlag{
			Name:    "sync-interval",
			Usage:   "how often to reconcile with peers regardless of gossip",
			Sources: cli.EnvVars("DCFS_SYNC_INTERVAL"),
			Value:   30 * time.Second,
		},
		&cli.DurationFlag{
			Name:    "scan-interval",
			Usage:   "how often to rescan the mirror directory; the backstop for dropped inotify events",
			Sources: cli.EnvVars("DCFS_SCAN_INTERVAL"),
			Value:   60 * time.Second,
		},
		&cli.DurationFlag{
			Name:    "janitor-interval",
			Usage:   "how often to collect expired tombstones and orphaned content",
			Sources: cli.EnvVars("DCFS_JANITOR_INTERVAL"),
			Value:   time.Hour,
		},
		&cli.DurationFlag{
			Name:    "settle",
			Usage:   "how long to wait after the last change to a file before reading it",
			Sources: cli.EnvVars("DCFS_SETTLE"),
			Value:   300 * time.Millisecond,
		},
	}
}

// defaultNodeName returns the hostname, which is unique per machine and is what
// an operator expects to see in cluster output.
func defaultNodeName() string {
	name, err := os.Hostname()
	if err != nil || name == "" {
		return "dcfs-node"
	}
	return name
}

// newLogger builds the process logger.
func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}
