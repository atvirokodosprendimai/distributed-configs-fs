-- +goose Up

-- entries is the replicated tree: one row per path, tombstones included.
-- State-based, not event-sourced — the current row IS the state, and history of
-- a config file is git's job, not this program's.
CREATE TABLE entries (
    path        TEXT    NOT NULL PRIMARY KEY,
    kind        INTEGER NOT NULL,                 -- core.Kind: 1=file, 2=dir
    hash        TEXT    NOT NULL DEFAULT '',      -- sha256 hex; '' for dirs and tombstones
    prev_hash   TEXT    NOT NULL DEFAULT '',      -- the hash this write replaced
    size        INTEGER NOT NULL DEFAULT 0,
    mode        INTEGER NOT NULL,                 -- permission bits only
    uid         INTEGER NOT NULL DEFAULT 0,
    gid         INTEGER NOT NULL DEFAULT 0,
    hlc_wall    INTEGER NOT NULL,                 -- hybrid logical clock, physical part
    hlc_counter INTEGER NOT NULL,                 -- hybrid logical clock, logical part
    origin      TEXT    NOT NULL,                 -- node that made this write; LWW tiebreak
    deleted     INTEGER NOT NULL DEFAULT 0,
    deleted_at  INTEGER NOT NULL DEFAULT 0,       -- unix nanos, drives tombstone GC
    seq         INTEGER NOT NULL                  -- LOCAL changes-feed position
) STRICT;

-- seq drives the "?since=N" changes feed peers pull. It is assigned locally on
-- every write and is meaningless on any other node.
CREATE UNIQUE INDEX idx_entries_seq ON entries (seq);

-- Partial indexes: the janitor only ever scans tombstones, and blob GC only
-- ever looks at rows that actually reference a blob.
CREATE INDEX idx_entries_deleted_at ON entries (deleted_at) WHERE deleted = 1;
CREATE INDEX idx_entries_hash ON entries (hash) WHERE hash != '';

-- blobs is content-addressed, which buys two things at once: identical files
-- across the tree are stored once, and "do I need to fetch this from a peer?"
-- is a primary-key lookup rather than a comparison.
CREATE TABLE blobs (
    hash TEXT    NOT NULL PRIMARY KEY,            -- sha256 hex of data
    size INTEGER NOT NULL,
    data BLOB    NOT NULL
) STRICT;

-- peers records how far through each peer's changes feed we have read, so a
-- restart resumes with a delta pull instead of re-fetching the whole tree.
CREATE TABLE peers (
    node     TEXT    NOT NULL PRIMARY KEY,
    addr     TEXT    NOT NULL DEFAULT '',
    last_seq INTEGER NOT NULL DEFAULT 0,
    seen_at  INTEGER NOT NULL DEFAULT 0
) STRICT;

-- local is a single-row table holding this node's monotonic seq counter.
-- A dedicated counter rather than AUTOINCREMENT because an UPDATE to an
-- existing path must also advance the feed — peers would otherwise never learn
-- that a file they already know about changed.
CREATE TABLE local (
    id  INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),
    seq INTEGER NOT NULL DEFAULT 0
) STRICT;

INSERT INTO local (id, seq) VALUES (1, 0);

-- +goose Down
DROP TABLE local;
DROP TABLE peers;
DROP TABLE blobs;
DROP TABLE entries;
