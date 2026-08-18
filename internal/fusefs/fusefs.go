// Package fusefs projects the replicated tree as a mounted filesystem — the
// mode closest to Proxmox's /etc/pve.
//
// It is the sibling of the mirror package and differs from it in one way that
// matters: a FUSE write is *mediated*. The kernel asks this process before the
// data lands, so an oversized file or an unreplicable path can be refused with
// an errno the calling program actually sees. The mirror, watching a real
// directory, only ever learns about a write after it has already happened.
//
// That is why both modes exist rather than one. FUSE gives correct write
// semantics; the mirror runs anywhere, needs no privileges, and survives a
// kernel without FUSE support. They are two projections of the same store and
// neither is the source of truth.
//
// # Whole-file buffering
//
// A file being written is held in memory and committed to the tree on flush,
// rather than being streamed. Config files are small, the store is
// content-addressed so a hash needs the whole file anyway, and partial writes
// to a config file are not a thing anyone does deliberately. The configured
// size cap bounds what this can cost.
package fusefs

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"
	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/store"
)

// cacheTimeout is how long the kernel may cache an entry or its attributes.
//
// It is short on purpose. The tree changes underneath this mount whenever a
// peer writes, and a config file that is stale for a minute because the kernel
// cached it is precisely the failure this program exists to prevent. One second
// is enough to keep `ls -l` of a large tree from becoming a query storm, and
// short enough that a change from a peer shows up while an operator is still
// looking at the terminal.
const cacheTimeout = time.Second

// Tree is the syncer as the mount uses it.
type Tree interface {
	Live(ctx context.Context) ([]core.Meta, error)
	Get(ctx context.Context, path string) (core.Meta, error)
	Content(ctx context.Context, hash string) ([]byte, error)
	PutFile(ctx context.Context, path string, content []byte, mode, uid, gid uint32) error
	PutDir(ctx context.Context, path string, mode, uid, gid uint32) error
	SetAttr(ctx context.Context, path string, mode, uid, gid uint32) error
	Delete(ctx context.Context, path string) error
	MaxFileSize() int64
}

// FS is the mounted projection.
type FS struct {
	tree Tree
	log  *slog.Logger
}

// node is one path in the mount. It carries the cluster-relative path rather
// than an inode number, because the store is keyed by path and translating in
// one place is cheaper than maintaining a second index that could disagree.
type node struct {
	fs.Inode
	fs   *FS
	path string // cluster-relative; "" is the mount root
}

// Compile-time proof that node implements everything the mount needs. Without
// these, a typo in a method signature silently degrades to the default
// behaviour — a read-only filesystem that returns ENOTSUP — rather than
// failing to build.
var (
	_ fs.NodeLookuper  = (*node)(nil)
	_ fs.NodeReaddirer = (*node)(nil)
	_ fs.NodeGetattrer = (*node)(nil)
	_ fs.NodeSetattrer = (*node)(nil)
	_ fs.NodeOpener    = (*node)(nil)
	_ fs.NodeCreater   = (*node)(nil)
	_ fs.NodeMkdirer   = (*node)(nil)
	_ fs.NodeUnlinker  = (*node)(nil)
	_ fs.NodeRmdirer   = (*node)(nil)
	_ fs.NodeRenamer   = (*node)(nil)
)

// Mount mounts the tree at mountpoint and returns the running server. Call
// Unmount on the result, or the mount outlives the process.
func Mount(mountpoint string, tree Tree, log *slog.Logger) (*fuse.Server, error) {
	if log == nil {
		log = slog.Default()
	}
	root := &node{fs: &FS{tree: tree, log: log}}

	server, err := fs.Mount(mountpoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: "dcfs",
			Name:   "dcfs",
			// The tree is replicated configuration, so it must survive a
			// restart of this process being visible as an empty directory:
			// allowing the mount to be interrupted rather than hang is kinder
			// than a wedged /etc.
			AllowOther: false,
		},
		EntryTimeout: ptr(cacheTimeout),
		AttrTimeout:  ptr(cacheTimeout),
	})
	if err != nil {
		return nil, fmt.Errorf("mount %s: %w", mountpoint, err)
	}
	return server, nil
}

// ptr returns a pointer to v, for the option fields that take one.
func ptr[T any](v T) *T { return &v }

// child returns the cluster path of a named child of this node.
func (n *node) child(name string) string {
	if n.path == "" {
		return name
	}
	return n.path + "/" + name
}

// inoOf derives a stable inode number from a path.
//
// A hash rather than a counter, so the same path keeps the same inode across
// restarts and across nodes — which is what makes a mount survive this process
// restarting without every open file handle going stale. Collisions are
// possible in principle; at the scale this tool targets they are not worth a
// second index to prevent.
func inoOf(p string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(p))
	return h.Sum64()
}

// attrOf fills a fuse.Attr from a tree entry.
func attrOf(m core.Meta, out *fuse.Attr) {
	out.Ino = inoOf(m.Path)
	out.Size = uint64(m.Size)
	out.Mode = m.Mode
	if m.Kind == core.KindDir {
		out.Mode |= syscall.S_IFDIR
	} else {
		out.Mode |= syscall.S_IFREG
	}
	out.Owner = fuse.Owner{Uid: m.UID, Gid: m.GID}
	secs := uint64(m.Version.HLC.Wall / int64(time.Second))
	out.Mtime, out.Ctime, out.Atime = secs, secs, secs
	// One link for files, two for directories (itself and "."). Some tools
	// treat a directory with fewer than two links as broken.
	out.Nlink = 1
	if m.Kind == core.KindDir {
		out.Nlink = 2
	}
	out.Blocks = uint64((m.Size + 511) / 512)
}

// stableOf builds the inode identity for an entry.
func stableOf(m core.Meta) fs.StableAttr {
	mode := uint32(syscall.S_IFREG)
	if m.Kind == core.KindDir {
		mode = syscall.S_IFDIR
	}
	return fs.StableAttr{Mode: mode, Ino: inoOf(m.Path)}
}

// Lookup resolves one name inside this directory.
func (n *node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	p := n.child(name)
	if _, err := core.SanitizePath(p); err != nil {
		// A name the cluster cannot replicate does not exist as far as this
		// mount is concerned; reporting ENOENT is more useful than EINVAL
		// because callers already handle it.
		return nil, syscall.ENOENT
	}
	m, err := n.fs.tree.Get(ctx, p)
	if err != nil || m.Deleted {
		return nil, syscall.ENOENT
	}
	attrOf(m, &out.Attr)
	child := n.NewInode(ctx, &node{fs: n.fs, path: p}, stableOf(m))
	return child, fs.OK
}

// Readdir lists the direct children of this directory.
//
// The store holds a flat list of paths, so children are found by prefix rather
// than by walking a tree. At the scale this tool targets — thousands of config
// files, not millions — a filtered scan is simpler and more obviously correct
// than maintaining a parallel tree index that could drift out of step.
func (n *node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := n.fs.tree.Live(ctx)
	if err != nil {
		n.fs.log.Error("readdir", "path", n.path, "err", err)
		return nil, syscall.EIO
	}

	prefix := ""
	if n.path != "" {
		prefix = n.path + "/"
	}
	var out []fuse.DirEntry
	for _, m := range entries {
		if !strings.HasPrefix(m.Path, prefix) {
			continue
		}
		rest := m.Path[len(prefix):]
		if rest == "" || strings.Contains(rest, "/") {
			continue // not a direct child
		}
		mode := uint32(syscall.S_IFREG)
		if m.Kind == core.KindDir {
			mode = syscall.S_IFDIR
		}
		out = append(out, fuse.DirEntry{Name: rest, Mode: mode, Ino: inoOf(m.Path)})
	}
	return fs.NewListDirStream(out), fs.OK
}

// Getattr reports this node's attributes.
func (n *node) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// A file being written reports the size of what is buffered, not what is
	// committed. Without this, a program that writes and then fstats its own
	// output — which cp and install both do — sees the old size and concludes
	// the write failed.
	if h, ok := f.(*writeHandle); ok {
		h.mu.Lock()
		defer h.mu.Unlock()
		out.Ino = inoOf(h.path)
		out.Size = uint64(len(h.buf))
		out.Mode = h.mode | syscall.S_IFREG
		out.Owner = fuse.Owner{Uid: h.uid, Gid: h.gid}
		out.Nlink = 1
		return fs.OK
	}

	if n.path == "" {
		// The mount root is not an entry in the tree; it is the tree, so its
		// attributes are synthesised rather than looked up.
		//
		// It is owned by whoever is running this process. Reporting the zero
		// value would make it root-owned, and an unprivileged mount would then
		// be a directory its own owner cannot write into — which is how this
		// was found.
		out.Mode = syscall.S_IFDIR | 0o755
		out.Ino = inoOf("")
		out.Nlink = 2
		out.Owner = fuse.Owner{Uid: uint32(os.Geteuid()), Gid: uint32(os.Getegid())}
		return fs.OK
	}
	m, err := n.fs.tree.Get(ctx, n.path)
	if err != nil || m.Deleted {
		return syscall.ENOENT
	}
	attrOf(m, &out.Attr)
	return fs.OK
}

// Setattr handles chmod, chown and truncate.
func (n *node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if n.path == "" {
		return syscall.EPERM
	}
	m, err := n.fs.tree.Get(ctx, n.path)
	if err != nil || m.Deleted {
		return syscall.ENOENT
	}

	// Truncate is handled first because it changes content, and the metadata
	// write below would otherwise be superseded by it.
	if size, ok := in.GetSize(); ok && m.Kind == core.KindFile {
		if int64(size) > n.fs.tree.MaxFileSize() {
			return syscall.EFBIG
		}
		content, err := n.fs.tree.Content(ctx, m.Hash)
		if err != nil {
			return syscall.EIO
		}
		switch {
		case uint64(len(content)) > size:
			content = content[:size]
		case uint64(len(content)) < size:
			// Growing by truncate produces a hole, which for a config file is
			// a run of NULs. Materialising them keeps the stored content and
			// the reported size consistent.
			grown := make([]byte, size)
			copy(grown, content)
			content = grown
		}
		if err := n.fs.tree.PutFile(ctx, n.path, content, m.Mode, m.UID, m.GID); err != nil {
			return errnoOf(err)
		}
		m.Size = int64(len(content))
	}

	mode, uid, gid := m.Mode, m.UID, m.GID
	if v, ok := in.GetMode(); ok {
		mode = core.SanitizeMode(v)
	}
	if v, ok := in.GetUID(); ok {
		uid = v
	}
	if v, ok := in.GetGID(); ok {
		gid = v
	}
	if mode != m.Mode || uid != m.UID || gid != m.GID {
		if err := n.fs.tree.SetAttr(ctx, n.path, mode, uid, gid); err != nil {
			return errnoOf(err)
		}
		m.Mode, m.UID, m.GID = mode, uid, gid
	}

	attrOf(m, &out.Attr)
	return fs.OK
}

// Open opens an existing file.
func (n *node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	m, err := n.fs.tree.Get(ctx, n.path)
	if err != nil || m.Deleted {
		return nil, 0, syscall.ENOENT
	}
	if m.Kind != core.KindFile {
		return nil, 0, syscall.EISDIR
	}

	writing := flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0
	if !writing {
		content, err := n.fs.tree.Content(ctx, m.Hash)
		if err != nil {
			n.fs.log.Error("read content", "path", n.path, "err", err)
			return nil, 0, syscall.EIO
		}
		// A snapshot taken at open time. A reader that opened the file before a
		// peer's update sees a coherent old version rather than a mixture of
		// two, which is the same guarantee the mirror's atomic rename gives.
		return &readHandle{data: content}, 0, fs.OK
	}

	buf := []byte(nil)
	if flags&syscall.O_TRUNC == 0 {
		if buf, err = n.fs.tree.Content(ctx, m.Hash); err != nil {
			return nil, 0, syscall.EIO
		}
		buf = append([]byte(nil), buf...)
	}
	return &writeHandle{
		fs: n.fs, path: n.path, buf: buf,
		mode: m.Mode, uid: m.UID, gid: m.GID,
	}, 0, fs.OK
}

// Create makes a new file and returns a handle for writing it.
func (n *node) Create(ctx context.Context, name string, flags, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	p := n.child(name)
	if _, err := core.SanitizePath(p); err != nil {
		return nil, nil, 0, syscall.EINVAL
	}
	caller, _ := fuse.FromContext(ctx)
	perm := core.SanitizeMode(mode)

	// Create the entry immediately, so the path exists for anything that
	// stats it before the first write lands, and so a program that creates a
	// file and never writes to it still produces an empty file.
	if err := n.fs.tree.PutFile(ctx, p, nil, perm, caller.Uid, caller.Gid); err != nil {
		return nil, nil, 0, errnoOf(err)
	}
	m, err := n.fs.tree.Get(ctx, p)
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}

	attrOf(m, &out.Attr)
	child := n.NewInode(ctx, &node{fs: n.fs, path: p}, stableOf(m))
	h := &writeHandle{fs: n.fs, path: p, mode: perm, uid: caller.Uid, gid: caller.Gid}
	return child, h, 0, fs.OK
}

// Mkdir creates a directory.
func (n *node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	p := n.child(name)
	if _, err := core.SanitizePath(p); err != nil {
		return nil, syscall.EINVAL
	}
	caller, _ := fuse.FromContext(ctx)
	if err := n.fs.tree.PutDir(ctx, p, core.SanitizeMode(mode), caller.Uid, caller.Gid); err != nil {
		return nil, errnoOf(err)
	}
	m, err := n.fs.tree.Get(ctx, p)
	if err != nil {
		return nil, syscall.EIO
	}
	attrOf(m, &out.Attr)
	return n.NewInode(ctx, &node{fs: n.fs, path: p}, stableOf(m)), fs.OK
}

// Unlink removes a file.
func (n *node) Unlink(ctx context.Context, name string) syscall.Errno {
	return n.remove(ctx, name, core.KindFile)
}

// Rmdir removes a directory, refusing while it still has children.
func (n *node) Rmdir(ctx context.Context, name string) syscall.Errno {
	p := n.child(name)
	entries, err := n.fs.tree.Live(ctx)
	if err != nil {
		return syscall.EIO
	}
	prefix := p + "/"
	for _, m := range entries {
		if strings.HasPrefix(m.Path, prefix) {
			return syscall.ENOTEMPTY
		}
	}
	return n.remove(ctx, name, core.KindDir)
}

// remove tombstones a child of the expected kind.
func (n *node) remove(ctx context.Context, name string, kind core.Kind) syscall.Errno {
	p := n.child(name)
	m, err := n.fs.tree.Get(ctx, p)
	if err != nil || m.Deleted {
		return syscall.ENOENT
	}
	if m.Kind != kind {
		if kind == core.KindFile {
			return syscall.EISDIR
		}
		return syscall.ENOTDIR
	}
	if err := n.fs.tree.Delete(ctx, p); err != nil {
		return errnoOf(err)
	}
	return fs.OK
}

// Rename moves a file or an empty directory.
//
// It is implemented as copy-then-delete rather than as an atomic operation,
// because the store has no rename primitive and a config tree does not need
// one. The window is small and the content is deduplicated by hash, so the copy
// costs a row rather than the bytes. Directories with children are refused
// instead of moved recursively — a half-moved subtree would be worse than a
// clear error.
func (n *node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	target, ok := newParent.(*node)
	if !ok {
		return syscall.EXDEV
	}
	from, to := n.child(name), target.child(newName)
	if _, err := core.SanitizePath(to); err != nil {
		return syscall.EINVAL
	}

	m, err := n.fs.tree.Get(ctx, from)
	if err != nil || m.Deleted {
		return syscall.ENOENT
	}
	switch m.Kind {
	case core.KindDir:
		entries, err := n.fs.tree.Live(ctx)
		if err != nil {
			return syscall.EIO
		}
		for _, e := range entries {
			if strings.HasPrefix(e.Path, from+"/") {
				return syscall.ENOTEMPTY
			}
		}
		if err := n.fs.tree.PutDir(ctx, to, m.Mode, m.UID, m.GID); err != nil {
			return errnoOf(err)
		}
	case core.KindFile:
		content, err := n.fs.tree.Content(ctx, m.Hash)
		if err != nil {
			return syscall.EIO
		}
		if err := n.fs.tree.PutFile(ctx, to, content, m.Mode, m.UID, m.GID); err != nil {
			return errnoOf(err)
		}
	}
	if err := n.fs.tree.Delete(ctx, from); err != nil {
		return errnoOf(err)
	}
	return fs.OK
}

// errnoOf maps a tree error onto the errno the calling program should see.
//
// This mapping is the whole reason the FUSE mode is worth having: a program
// writing an oversized file gets EFBIG at the write(2) that caused it, instead
// of the mirror's after-the-fact log line that nothing reads.
func errnoOf(err error) syscall.Errno {
	switch {
	case err == nil:
		return fs.OK
	case errors.Is(err, core.ErrTooLarge):
		return syscall.EFBIG
	case errors.Is(err, core.ErrBadPath):
		return syscall.EINVAL
	case errors.Is(err, store.ErrNotFound):
		return syscall.ENOENT
	default:
		return syscall.EIO
	}
}

// readHandle serves reads from a snapshot taken when the file was opened.
type readHandle struct{ data []byte }

var _ fs.FileReader = (*readHandle)(nil)

// Read returns the requested slice of the snapshot.
func (h *readHandle) Read(_ context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off < 0 || off > int64(len(h.data)) {
		return fuse.ReadResultData(nil), fs.OK
	}
	end := min(off+int64(len(dest)), int64(len(h.data)))
	return fuse.ReadResultData(h.data[off:end]), fs.OK
}

// writeHandle buffers a file in memory and commits it to the tree on flush.
type writeHandle struct {
	mu   sync.Mutex
	fs   *FS
	path string
	buf  []byte

	mode, uid, gid uint32
	dirty          bool
}

var (
	_ fs.FileWriter  = (*writeHandle)(nil)
	_ fs.FileReader  = (*writeHandle)(nil)
	_ fs.FileFlusher = (*writeHandle)(nil)
)

// Write buffers data at off, growing the buffer as needed.
func (h *writeHandle) Write(_ context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()

	end := off + int64(len(data))
	// Refuse at the write that crosses the limit rather than at flush, so the
	// program sees the failure on the syscall responsible for it.
	if end > h.fs.tree.MaxFileSize() {
		return 0, syscall.EFBIG
	}
	if end > int64(len(h.buf)) {
		grown := make([]byte, end)
		copy(grown, h.buf)
		h.buf = grown
	}
	copy(h.buf[off:], data)
	h.dirty = true
	return uint32(len(data)), fs.OK
}

// Read serves reads from the in-progress buffer, so a file opened O_RDWR reads
// back what it has just written rather than the last committed version.
func (h *writeHandle) Read(_ context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if off < 0 || off > int64(len(h.buf)) {
		return fuse.ReadResultData(nil), fs.OK
	}
	end := min(off+int64(len(dest)), int64(len(h.buf)))
	return fuse.ReadResultData(append([]byte(nil), h.buf[off:end]...)), fs.OK
}

// Flush commits the buffer to the tree.
//
// Flush runs on every close(2), including on a descriptor duplicated by fork,
// so it can fire several times for one logical write. Committing identical
// content is a no-op in the syncer, which is what makes that harmless.
func (h *writeHandle) Flush(ctx context.Context) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.dirty {
		return fs.OK
	}
	if err := h.fs.tree.PutFile(ctx, h.path, h.buf, h.mode, h.uid, h.gid); err != nil {
		h.fs.log.Error("commit write", "path", h.path, "err", err)
		return errnoOf(err)
	}
	h.dirty = false
	return fs.OK
}
