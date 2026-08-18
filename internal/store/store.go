// Package store is this node's source of truth: a local SQLite database
// holding the whole replicated config tree plus the blobs its files point at.
//
// The mirror directory and the FUSE mount are projections of what is in here,
// never the other way round. That is what lets a node lose its mirror
// directory, or mount somewhere else entirely, without losing cluster state.
//
// The package is deliberately dumb persistence: it stores what it is told and
// answers questions. Conflict resolution, size policy and clock handling live
// in the syncer, which is the only writer.
package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"

	"github.com/glebarez/sqlite"
	"github.com/pressly/goose/v3"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"

	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// ErrNotFound is returned when a path or blob is absent. Callers match on it
// rather than on gorm.ErrRecordNotFound so the gorm dependency stops here.
var ErrNotFound = errors.New("store: not found")

// Store is the SQLite-backed source of truth. It is safe for concurrent use:
// reads may run freely, and writes are serialised both by the syncer above it
// and by the single connection below it.
type Store struct {
	db  *gorm.DB
	sql *sql.DB
}

// Open opens (creating if needed) the database at path and migrates it to the
// current schema.
func Open(ctx context.Context, path string) (*Store, error) {
	// WAL so readers never block the writer; busy_timeout so the rare
	// contention with the janitor waits instead of failing; synchronous=NORMAL
	// because with WAL that is durable across process crashes (only a host
	// power loss can lose the last commits, and a peer will re-send them).
	dsn := path + "?_pragma=journal_mode(WAL)" +
		"&_pragma=busy_timeout(5000)" +
		"&_pragma=synchronous(NORMAL)" +
		"&_pragma=foreign_keys(1)"

	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger:                 logger.Discard,
		SkipDefaultTransaction: true, // we open transactions explicitly where they matter
	})
	if err != nil {
		return nil, fmt.Errorf("open sqlite at %s: %w", path, err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("unwrap sql.DB: %w", err)
	}
	// One connection: SQLite has a single writer anyway, and capping it here
	// turns would-be lock contention into ordinary queueing.
	sqlDB.SetMaxOpenConns(1)

	if err := migrate(ctx, sqlDB); err != nil {
		return nil, err
	}
	return &Store{db: db, sql: sqlDB}, nil
}

// migrate brings the schema up to date from the embedded goose migrations.
//
// It uses goose's Provider API rather than the package-level goose.Up. The
// package-level functions configure the dialect and base filesystem through
// mutable globals, which races the moment two Stores are opened concurrently —
// as happens in parallel tests, and as would happen to anyone embedding this
// package alongside another goose user.
func migrate(ctx context.Context, db *sql.DB) error {
	sub, err := fs.Sub(migrationFS, "migrations")
	if err != nil {
		return fmt.Errorf("open embedded migrations: %w", err)
	}
	provider, err := goose.NewProvider(goose.DialectSQLite3, db, sub)
	if err != nil {
		return fmt.Errorf("build migration provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}
	return nil
}

// Close releases the database handle.
func (s *Store) Close() error { return s.sql.Close() }

// entryRow is the gorm model behind the entries table. It exists so that
// core.Meta stays a clean wire/domain type with no persistence tags on it.
type entryRow struct {
	Path       string `gorm:"column:path;primaryKey"`
	Kind       uint8  `gorm:"column:kind"`
	Hash       string `gorm:"column:hash"`
	PrevHash   string `gorm:"column:prev_hash"`
	Size       int64  `gorm:"column:size"`
	Mode       uint32 `gorm:"column:mode"`
	UID        uint32 `gorm:"column:uid"`
	GID        uint32 `gorm:"column:gid"`
	HLCWall    int64  `gorm:"column:hlc_wall"`
	HLCCounter uint32 `gorm:"column:hlc_counter"`
	Origin     string `gorm:"column:origin"`
	Deleted    bool   `gorm:"column:deleted"`
	DeletedAt  int64  `gorm:"column:deleted_at"`
	Seq        int64  `gorm:"column:seq"`
}

// TableName pins the table name so gorm's pluralisation cannot drift from the
// goose migration that actually owns the schema.
func (entryRow) TableName() string { return "entries" }

// meta converts a stored row into the domain type.
func (r entryRow) meta() core.Meta {
	return core.Meta{
		Path:      r.Path,
		Kind:      core.Kind(r.Kind),
		Hash:      r.Hash,
		PrevHash:  r.PrevHash,
		Size:      r.Size,
		Mode:      r.Mode,
		UID:       r.UID,
		GID:       r.GID,
		Version:   core.Version{HLC: core.Timestamp{Wall: r.HLCWall, Counter: r.HLCCounter}, Origin: r.Origin},
		Deleted:   r.Deleted,
		DeletedAt: r.DeletedAt,
		Seq:       r.Seq,
	}
}

// rowOf converts a domain entry into a storable row. Seq is assigned by Apply,
// not by the caller.
func rowOf(m core.Meta) entryRow {
	return entryRow{
		Path:       m.Path,
		Kind:       uint8(m.Kind),
		Hash:       m.Hash,
		PrevHash:   m.PrevHash,
		Size:       m.Size,
		Mode:       m.Mode,
		UID:        m.UID,
		GID:        m.GID,
		HLCWall:    m.Version.HLC.Wall,
		HLCCounter: m.Version.HLC.Counter,
		Origin:     m.Version.Origin,
		Deleted:    m.Deleted,
		DeletedAt:  m.DeletedAt,
	}
}

// blobRow is the gorm model behind the blobs table.
type blobRow struct {
	Hash string `gorm:"column:hash;primaryKey"`
	Size int64  `gorm:"column:size"`
	Data []byte `gorm:"column:data"`
}

// TableName pins the blobs table name.
func (blobRow) TableName() string { return "blobs" }

// peerRow is the gorm model behind the peers table.
type peerRow struct {
	Node    string `gorm:"column:node;primaryKey"`
	Addr    string `gorm:"column:addr"`
	LastSeq int64  `gorm:"column:last_seq"`
	SeenAt  int64  `gorm:"column:seen_at"`
}

// TableName pins the peers table name.
func (peerRow) TableName() string { return "peers" }

// Get returns the entry at path, tombstones included — a caller reconciling
// against disk needs to see a deletion, not a missing row.
func (s *Store) Get(ctx context.Context, path string) (core.Meta, error) {
	var row entryRow
	err := s.db.WithContext(ctx).Where("path = ?", path).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return core.Meta{}, fmt.Errorf("%w: path %q", ErrNotFound, path)
	}
	if err != nil {
		return core.Meta{}, fmt.Errorf("get entry %q: %w", path, err)
	}
	return row.meta(), nil
}

// All returns every entry including tombstones, ordered by path.
func (s *Store) All(ctx context.Context) ([]core.Meta, error) {
	return query(s.db.WithContext(ctx).Order("path"))
}

// Live returns every non-deleted entry, ordered by path. This is what the
// mirror and FUSE projections render.
func (s *Store) Live(ctx context.Context) ([]core.Meta, error) {
	return query(s.db.WithContext(ctx).Where("deleted = 0").Order("path"))
}

// ManifestSince returns up to limit entries written locally after seq, in feed
// order. This is the changes feed peers pull; seq 0 yields the whole tree,
// which is exactly what a new or long-absent node needs.
func (s *Store) ManifestSince(ctx context.Context, seq int64, limit int) ([]core.Meta, error) {
	return query(s.db.WithContext(ctx).Where("seq > ?", seq).Order("seq").Limit(limit))
}

// query runs a prepared entries query and maps the rows to domain types. The
// context already rides on tx, which is why it is not a separate parameter.
func query(tx *gorm.DB) ([]core.Meta, error) {
	var rows []entryRow
	if err := tx.Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("query entries: %w", err)
	}
	out := make([]core.Meta, len(rows))
	for i, r := range rows {
		out[i] = r.meta()
	}
	return out, nil
}

// Digest summarises the whole replicated tree in one hash, alongside the entry
// count and the local feed head.
//
// Anti-entropy compares digests before pulling anything: two nodes that already
// agree exchange 32 bytes instead of a manifest of every path in /etc. Only the
// replicated fields are folded in — seq is local, so including it would make
// two identical trees look different forever.
func (s *Store) Digest(ctx context.Context) (digest string, entries int64, head int64, err error) {
	rows, err := s.All(ctx)
	if err != nil {
		return "", 0, 0, err
	}
	h := sha256.New()
	for _, m := range rows {
		fmt.Fprintf(h, "%s\x00%d\x00%s\x00%d\x00%d\x00%d\x00%d\x00%d\x00%s\x00%t\n",
			m.Path, m.Kind, m.Hash, m.Mode, m.UID, m.GID,
			m.Version.HLC.Wall, m.Version.HLC.Counter, m.Version.Origin, m.Deleted)
		head = max(head, m.Seq)
	}
	return hex.EncodeToString(h.Sum(nil)), int64(len(rows)), head, nil
}

// Apply persists entries, stamps each with the next local feed position, and
// returns the resulting feed head.
//
// It performs no conflict resolution: the syncer decides what wins before
// calling this. Batching matters because a rejoining node applies the whole
// tree at once, and a transaction per path would turn a rejoin into ten
// thousand fsyncs. The returned head is what the caller gossips, so peers know
// there is something new to pull.
func (s *Store) Apply(ctx context.Context, metas ...core.Meta) (int64, error) {
	if len(metas) == 0 {
		return s.Head(ctx)
	}
	var head int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// RETURNING keeps the read and the increment in one statement, so the
		// counter cannot be read by one caller and written by another.
		if err := tx.Raw("UPDATE local SET seq = seq + ? WHERE id = 1 RETURNING seq", len(metas)).
			Scan(&head).Error; err != nil {
			return fmt.Errorf("advance feed counter: %w", err)
		}

		rows := make([]entryRow, len(metas))
		base := head - int64(len(metas))
		for i, m := range metas {
			rows[i] = rowOf(m)
			rows[i].Seq = base + int64(i) + 1
		}
		// Upsert: an entry's path is its identity, and a newer version simply
		// replaces the row rather than accumulating history.
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "path"}},
			UpdateAll: true,
		}).Create(&rows).Error; err != nil {
			return fmt.Errorf("upsert %d entries: %w", len(rows), err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return head, nil
}

// Head returns this node's current changes-feed position.
func (s *Store) Head(ctx context.Context) (int64, error) {
	var head int64
	if err := s.db.WithContext(ctx).Raw("SELECT seq FROM local WHERE id = 1").Scan(&head).Error; err != nil {
		return 0, fmt.Errorf("read feed head: %w", err)
	}
	return head, nil
}

// PutBlob stores content and returns its hash. Storing a blob that is already
// present is a no-op, which is what makes content addressing pay: the same
// config file replicated to fifty paths costs one row.
func (s *Store) PutBlob(ctx context.Context, content []byte) (string, error) {
	hash := core.HashContent(content)
	row := blobRow{Hash: hash, Size: int64(len(content)), Data: content}
	err := s.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&row).Error
	if err != nil {
		return "", fmt.Errorf("put blob %s: %w", hash[:8], err)
	}
	return hash, nil
}

// Blob returns the content addressed by hash.
func (s *Store) Blob(ctx context.Context, hash string) ([]byte, error) {
	var row blobRow
	err := s.db.WithContext(ctx).Where("hash = ?", hash).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("%w: blob %s", ErrNotFound, hash)
	}
	if err != nil {
		return nil, fmt.Errorf("get blob %s: %w", hash, err)
	}
	return row.Data, nil
}

// HasBlob reports whether the content addressed by hash is already local.
// This is the check that keeps a rejoin cheap: the puller asks it for every
// entry in the peer's manifest and only fetches the misses.
func (s *Store) HasBlob(ctx context.Context, hash string) (bool, error) {
	var n int64
	if err := s.db.WithContext(ctx).Model(&blobRow{}).Where("hash = ?", hash).Count(&n).Error; err != nil {
		return false, fmt.Errorf("check blob %s: %w", hash, err)
	}
	return n > 0, nil
}

// PeerSeq returns how far through node's changes feed this node has read.
// A peer we have never spoken to returns 0, which asks for its whole tree.
func (s *Store) PeerSeq(ctx context.Context, node string) (int64, error) {
	var row peerRow
	err := s.db.WithContext(ctx).Where("node = ?", node).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get peer %q: %w", node, err)
	}
	return row.LastSeq, nil
}

// SetPeerSeq records how far through node's feed this node has read, so a
// restart resumes with a delta pull rather than the whole tree.
func (s *Store) SetPeerSeq(ctx context.Context, node, addr string, seq, seenAt int64) error {
	row := peerRow{Node: node, Addr: addr, LastSeq: seq, SeenAt: seenAt}
	err := s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "node"}},
		DoUpdates: clause.AssignmentColumns([]string{"addr", "last_seq", "seen_at"}),
	}).Create(&row).Error
	if err != nil {
		return fmt.Errorf("set peer %q seq: %w", node, err)
	}
	return nil
}

// Peers returns every peer this node has ever pulled from.
func (s *Store) Peers(ctx context.Context) ([]Peer, error) {
	var rows []peerRow
	if err := s.db.WithContext(ctx).Order("node").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("list peers: %w", err)
	}
	out := make([]Peer, len(rows))
	for i, r := range rows {
		out[i] = Peer{Node: r.Node, Addr: r.Addr, LastSeq: r.LastSeq, SeenAt: r.SeenAt}
	}
	return out, nil
}

// Peer is what this node remembers about another node between restarts.
type Peer struct {
	Node    string `json:"node"`
	Addr    string `json:"addr"`
	LastSeq int64  `json:"last_seq"`
	SeenAt  int64  `json:"seen_at"`
}

// GCTombstones deletes tombstones whose DeletedAt is older than before,
// returning how many went.
//
// The horizon is load-bearing: a node offline for longer than the TTL rejoins,
// finds no tombstone for a file it still has, and re-announces it as a live
// entry — the deletion is undone cluster-wide. The TTL must therefore exceed
// the longest outage the operator is willing to tolerate.
func (s *Store) GCTombstones(ctx context.Context, before int64) (int64, error) {
	res := s.db.WithContext(ctx).
		Where("deleted = 1 AND deleted_at > 0 AND deleted_at < ?", before).
		Delete(&entryRow{})
	if res.Error != nil {
		return 0, fmt.Errorf("gc tombstones: %w", res.Error)
	}
	return res.RowsAffected, nil
}

// GCBlobs deletes blobs no live entry references, returning how many went.
// Run it after GCTombstones — a tombstone that has just expired is what frees
// the blob its file used to point at.
func (s *Store) GCBlobs(ctx context.Context) (int64, error) {
	res := s.db.WithContext(ctx).
		Where("hash NOT IN (SELECT hash FROM entries WHERE hash != '')").
		Delete(&blobRow{})
	if res.Error != nil {
		return 0, fmt.Errorf("gc blobs: %w", res.Error)
	}
	return res.RowsAffected, nil
}

// Stats is a point-in-time summary of what this node holds, for the status
// endpoint and the CLI.
type Stats struct {
	Entries    int64 `json:"entries"`
	Tombstones int64 `json:"tombstones"`
	Blobs      int64 `json:"blobs"`
	BlobBytes  int64 `json:"blob_bytes"`
	Head       int64 `json:"head"`
}

// Stats summarises the local database.
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	var st Stats
	db := s.db.WithContext(ctx)

	if err := db.Model(&entryRow{}).Count(&st.Entries).Error; err != nil {
		return Stats{}, fmt.Errorf("count entries: %w", err)
	}
	if err := db.Model(&entryRow{}).Where("deleted = 1").Count(&st.Tombstones).Error; err != nil {
		return Stats{}, fmt.Errorf("count tombstones: %w", err)
	}
	if err := db.Model(&blobRow{}).Count(&st.Blobs).Error; err != nil {
		return Stats{}, fmt.Errorf("count blobs: %w", err)
	}
	// COALESCE because SUM over an empty table is NULL, which will not scan
	// into an int64.
	if err := db.Model(&blobRow{}).Select("COALESCE(SUM(size), 0)").Scan(&st.BlobBytes).Error; err != nil {
		return Stats{}, fmt.Errorf("sum blob sizes: %w", err)
	}
	head, err := s.Head(ctx)
	if err != nil {
		return Stats{}, err
	}
	st.Head = head
	return st, nil
}
