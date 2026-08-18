package core

import (
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"
)

// Path and name limits. These mirror the usual Linux limits rather than trying
// to be clever: a config tree that needs a 300-byte filename has a problem this
// program should not be solving.
const (
	MaxPathLen = 4096
	MaxNameLen = 255
)

// ErrBadPath is the sentinel behind every path rejection, so callers can
// classify a rejection without matching on message text.
var ErrBadPath = errors.New("core: invalid path")

// SanitizePath validates a cluster-relative path and returns it in canonical
// form.
//
// Every path that arrives from outside this process goes through here: from a
// peer's manifest, from a FUSE request, from a directory walk of the mirror.
// The threat is a peer — or a compromised peer — sending "../../etc/shadow" and
// having us write it. Note that this function is only the first of two
// defences: the mirror writer additionally performs all filesystem access
// through an *os.Root, which refuses to escape the mirror directory even via a
// symlink planted by a local user. Neither defence is sufficient alone, because
// this one cannot see symlinks and that one cannot see a path that is merely
// absurd.
func SanitizePath(p string) (string, error) {
	if p == "" {
		return "", fmt.Errorf("%w: empty", ErrBadPath)
	}
	if len(p) > MaxPathLen {
		return "", fmt.Errorf("%w: longer than %d bytes", ErrBadPath, MaxPathLen)
	}
	if strings.ContainsRune(p, 0) {
		return "", fmt.Errorf("%w: contains NUL", ErrBadPath)
	}
	if strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("%w: %q is absolute", ErrBadPath, p)
	}
	// Rejecting rather than silently cleaning: if a peer's idea of a path and
	// ours differ, we want that visible as an error, not quietly reconciled
	// into a path neither node asked for.
	if c := path.Clean(p); c != p {
		return "", fmt.Errorf("%w: %q is not canonical (want %q)", ErrBadPath, p, c)
	}
	for el := range strings.SplitSeq(p, "/") {
		switch el {
		case "":
			return "", fmt.Errorf("%w: %q has an empty element", ErrBadPath, p)
		case ".", "..":
			// path.Clean above already removes interior "." and "..", so this
			// catches the leading forms it legitimately preserves ("../x").
			return "", fmt.Errorf("%w: %q escapes the tree", ErrBadPath, p)
		}
		if len(el) > MaxNameLen {
			return "", fmt.Errorf("%w: element %q longer than %d bytes", ErrBadPath, el, MaxNameLen)
		}
	}
	return p, nil
}

// SanitizeMode strips everything but the permission and setuid/setgid/sticky
// bits from a mode arriving from a peer.
//
// The type bits are never taken from the wire — Kind decides whether an entry
// is a file or a directory. Letting a peer supply raw mode bits would let it
// ask us to create a device node or a socket, which is why the type bits are
// masked off here rather than validated downstream.
func SanitizeMode(mode uint32) uint32 {
	return mode & uint32(fs.ModePerm|fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky)
}

// ConflictName derives the name a conflict copy is stored under when a remote
// write lands on top of a concurrent local edit.
//
// The suffix goes before the extension so that "nginx.conf" becomes
// "nginx.conf.conflict.node2.1755...": tools that dispatch on extension stop
// seeing the copy as a config file, which is the point — a conflict copy is
// evidence for a human, not something a daemon should load.
func ConflictName(p, node string, unixNano int64) string {
	return fmt.Sprintf("%s.conflict.%s.%d", p, node, unixNano)
}

// IsConflictName reports whether p is a conflict copy this program generated.
// Conflict copies replicate like any other file, but the mirror must never
// promote one back into the main entry it was derived from.
func IsConflictName(p string) bool {
	return strings.Contains(path.Base(p), ".conflict.")
}
