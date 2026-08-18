# distributed-configs-fs

A replicated filesystem for configuration files, in the spirit of Proxmox's
`/etc/pve` — but standalone, with no corosync and no kernel module.

Every node holds the whole config tree in a local SQLite database and exposes it
two ways: as a real directory (`/etc/cluster`) kept in step by a filesystem
watcher, or as a FUSE mount. Write a file on one node and it appears on the
others. Lose a node, change things while it is away, bring it back with its disk
wiped, and it re-mirrors everything it missed.

```
             ┌──────── node1 ────────┐   ┌──────── node2 ────────┐
   you edit  │  /etc/cluster  ◀────┐ │   │ ┌────▶  /etc/cluster  │  and it
   this ────▶│       │             │ │   │ │             │       │◀─── changes
             │    fsnotify      render│   │render      fsnotify   │     here
             │       ▼             │ │   │ │             ▼       │
             │   ┌─────────────────┴─┴┐ ┌┴─┴─────────────────┐   │
             │   │  syncer (1 writer) │ │  syncer (1 writer) │   │
             │   └─────────┬──────────┘ └──────────┬─────────┘   │
             │        SQLite (truth)          SQLite (truth)     │
             └───────────────┬───────────────────┬───────────────┘
                             │                   │
                     gossip: "I'm at seq 42"  (memberlist, UDP)
                     pull:   manifest + content (HTTP, sealed)
```

## Quick start

```sh
cp .env.example .env
# set DCFS_CLUSTER_SECRET — openssl rand -base64 32
docker compose up --build
```

Three nodes come up. Then:

```sh
echo "worker_processes 4;" > data/node1/cluster/nginx.conf
cat data/node2/cluster/nginx.conf     # it is already there
cat data/node3/cluster/nginx.conf
```

Without Docker:

```sh
go build ./cmd/dcfs
export DCFS_CLUSTER_SECRET=$(openssl rand -base64 32)

./dcfs serve --node n1 --db /tmp/n1.db --mirror /tmp/n1/cluster \
    --bind 127.0.0.1 --gossip-port 7946 --api-addr 127.0.0.1:7947 &

./dcfs serve --node n2 --db /tmp/n2.db --mirror /tmp/n2/cluster \
    --bind 127.0.0.1 --gossip-port 7948 --api-addr 127.0.0.1:7949 \
    --join 127.0.0.1:7946 &

./dcfs status --api-addr 127.0.0.1:7947
```

## The two modes

Both are projections of the same SQLite store. Neither is the source of truth,
and you can run both at once.

|                      | **Mirror** (`--mirror`)                     | **FUSE** (`--mount`)                          |
| -------------------- | ------------------------------------------- | --------------------------------------------- |
| How it works         | Watches a real directory, mirrors both ways | A mounted filesystem backed by the store      |
| Writes are           | Observed after the fact                     | **Mediated** — can be refused                 |
| Oversized file       | Logged and skipped, silently to the writer  | `write(2)` returns `EFBIG`                    |
| Needs                | Nothing                                     | `/dev/fuse`, `SYS_ADMIN`, `rshared` in Docker |
| Runs on              | Anything                                    | Linux, macOS with macFUSE                     |

If you only want one, use the mirror. The FUSE mode exists for the case where
you need a program to *find out* that its write was rejected.

## Consistency model — read this before relying on it

**This is an AP system.** It has no quorum. Every node keeps accepting writes,
including one that can see nobody else. That is a deliberate choice and it is
different from Proxmox, whose `/etc/pve` goes read-only when it loses quorum.

- Ordering is **last-writer-wins** over a hybrid logical clock, with the node ID
  breaking exact ties so the order is total and the cluster always converges.
- A **genuinely concurrent edit** — two nodes editing the same file while unable
  to see each other — does not silently lose one side. The loser is preserved as
  `path.conflict.<node>.<timestamp>`, which replicates like any other file so
  you see it whichever node you log into. `dcfs conflicts` lists them and exits
  non-zero.
- A file that was simply edited twice in sequence produces **no** conflict copy.

If you need "a partitioned node must refuse writes", this tool is the wrong
shape and you want Raft.

### Which side wins in the mirror

When the disk and the tree disagree about a file, **the later modification
wins** — the file's mtime is compared against the stored version's clock. So an
edit made while the daemon was stopped is adopted, and a peer's newer version
that arrived while the node was down is written over the older local file.

mtime decides only that local question. The promotion itself is stamped with a
fresh logical clock reading, because mtime is not trustworthy for cluster-wide
ordering — `tar`, `rsync -a` and `touch` all set it arbitrarily, and a timestamp
in the future would win every conflict forever.

### Deletions

Deleting a file **while the daemon is running** deletes it cluster-wide. That is
the delete UX; there is no separate command.

Deleting one **while the daemon is stopped does not**, and the file comes back
when the daemon starts. This is deliberate: the periodic rescan never deletes
from the cluster, because "the tree has this file and the disk does not" is
indistinguishable from "the disk has not caught up yet". It is also what makes
a returning node get its tree back rather than wiping everyone else's.

### Tombstone TTL — the one setting that can lose data

A deletion is replicated as a tombstone. A node that was offline during the
deletion learns about it by comparing against that tombstone when it returns.
Once the tombstone is collected, a node returning **after** that still has the
file, sees nothing saying it was deleted, and re-announces it — undoing the
deletion across the whole cluster.

**`DCFS_TOMBSTONE_TTL` must exceed the longest outage any node may have.**
Default is 7 days.

## What is replicated

- Regular files and directories, including **empty** directories (a `conf.d`
  with nothing in it is often load-bearing).
- Permission bits, **and uid/gid**.
- Not replicated: symlinks, sockets, FIFOs, device nodes. A symlink means
  something different on every host, and a device node in a config tree is a
  write primitive you did not intend to hand out.

### The uid/gid contract

Ownership is replicated as raw numeric uid/gid. **Every node must share a
uid/gid namespace** — a file owned by uid 1001 must mean the same person
everywhere, or a config lands owned by a stranger. If your hosts do not agree on
`/etc/passwd`, fix that first.

Setting ownership needs root or `CAP_CHOWN`. An unprivileged node still
replicates content and permissions, and deliberately does **not** report the
ownership it sees on disk — otherwise it would overwrite the cluster's intended
uid/gid with its own on every scan.

## Operating notes

**inotify is not the correctness mechanism.** It drops events when its queue
overflows and is not recursive. The periodic rescan (`DCFS_SCAN_INTERVAL`) is
what makes the mirror correct; the watcher only makes it fast.

- **Raise `fs.inotify.max_user_watches`** if you have many directories. One
  watch per directory. The symptom of running out is "changes are sometimes
  slow", which is unpleasant to diagnose; the log says so explicitly instead.
- **Docker Desktop (macOS/Windows):** inotify does not propagate through a host
  bind mount at all, so host-side edits are only picked up by the rescan. Lower
  `DCFS_SCAN_INTERVAL` when developing there. Native Linux is unaffected.
- **macOS natively:** fsnotify uses kqueue, which needs a file descriptor per
  watched file. Fine for a config tree, not for tens of thousands of files.

Scale target is well under 10k files. Nothing here is designed for a file share.

## Security

`DCFS_CLUSTER_SECRET` is expanded with HKDF into three independent keys, and
possession of it is membership. Minimum 16 characters, enforced at startup.

- Gossip is encrypted (memberlist `SecretKey`, AES-GCM).
- Every peer API request is HMAC-SHA256 signed, bound to method, path and a
  timestamp, and compared in constant time.
- Every peer API response body is sealed with AES-256-GCM. Authentication alone
  would leave the contents of `/etc` readable to anything on the wire, and the
  manifest alone leaks the shape of the tree.
- **The peer API is pull-only.** Nothing a peer can reach mutates local state, so
  a compromised member can serve bad data but cannot push anything.
- Fetched content is verified against the hash that was requested.
- Peer-supplied paths are refused if they are absolute, non-canonical, or
  contain `..`; and every mirror filesystem operation additionally goes through
  an `os.Root`, which refuses to escape the mirror directory even through a
  symlink a local user planted.

`/healthz` is unauthenticated and returns only `ok`, because container health
checks run before an orchestrator knows any secret. Everything else needs the
key.

## Commands

```
dcfs serve        run a node
dcfs status       this node's view: digest, counts, members, conflicts
dcfs peers        members and how current this node is with each
dcfs conflicts    conflict copies awaiting a decision (exits non-zero if any)
```

To check whether two nodes actually agree, compare their digests — it is a hash
of the whole replicated tree, so matching digests mean identical trees without
diffing two directories:

```sh
dcfs status --api-addr node1:7947 | grep digest
dcfs status --api-addr node2:7947 | grep digest
```

Every flag has a `DCFS_*` environment fallback; see `.env.example`.

## How it works

`internal/core` is a dependency-free kernel — domain types, the hybrid logical
clock, path and mode sanitization, key derivation. Everything imports it and
nothing imports a sibling.

`internal/store` is the source of truth: SQLite via gorm on the pure-Go
`glebarez` driver, schema owned by goose migrations. Files are content-addressed
by SHA-256, so identical content is stored once and "do I need to fetch this
from a peer?" is a primary-key lookup — which is what makes a rejoin cheap.
Every entry also carries a local `seq`, forming a changes feed peers page
through.

`internal/syncer` is the **single writer**. The watcher, the FUSE layer and the
anti-entropy loop all perform a read-modify-write, so they are serialised; the
mutex guards the decision, not the database. It also owns all the policy the
store deliberately lacks: size limits, clock-drift rejection, tombstone
collection, and conflict-copy creation.

`internal/cluster` wraps hashicorp/memberlist. It provides membership and
failure detection and nothing else — **no ordering, no quorum**. The only thing
gossiped is `{node, seq}`: content is pulled over HTTP, so a hint that arrives
twice or out of order costs nothing.

`internal/transport` is that HTTP API — `digest`, `manifest?since=`, `blob/{hash}`,
`status`. Anti-entropy compares digests first, so two nodes that already agree
exchange one hash instead of a manifest of every path in `/etc`.

`internal/mirror` and `internal/fusefs` are the two projections.

## Development

```sh
go test ./... -race
go vet ./...
```

The mirror and FUSE tests use real filesystems and a real mount; the FUSE ones
skip themselves where the platform cannot provide one. The syncer tests build
multi-node clusters in-process with the network replaced by direct calls, so
replication, conflict resolution and rejoin are exercised as production code
paths rather than stubs.

## License

See [LICENSE](LICENSE).
