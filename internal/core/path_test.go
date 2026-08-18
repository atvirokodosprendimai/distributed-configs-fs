package core

import (
	"errors"
	"io/fs"
	"strings"
	"testing"
)

// TestSanitizePathRejectsEscapes is the security test for this package: every
// case here is a path a hostile or buggy peer could put in a manifest, and any
// one of them getting through means writing outside the mirror directory.
func TestSanitizePathRejectsEscapes(t *testing.T) {
	t.Parallel()

	bad := []struct {
		name string
		in   string
	}{
		{"empty", ""},
		{"absolute", "/etc/shadow"},
		{"parent traversal", "../../etc/shadow"},
		{"leading parent", "../x"},
		{"interior traversal", "a/../../etc/shadow"},
		{"interior dot", "a/./b"},
		{"bare dot", "."},
		{"bare parent", ".."},
		{"double slash", "a//b"},
		{"trailing slash", "a/b/"},
		{"NUL byte", "a\x00b"},
		{"overlong element", strings.Repeat("x", MaxNameLen+1)},
		{"overlong path", strings.Repeat("a/", MaxPathLen)},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := SanitizePath(tc.in)
			if err == nil {
				t.Fatalf("SanitizePath(%q) = %q, want error", tc.in, got)
			}
			if !errors.Is(err, ErrBadPath) {
				t.Errorf("SanitizePath(%q) error = %v, want it to wrap ErrBadPath", tc.in, err)
			}
		})
	}
}

func TestSanitizePathAcceptsValid(t *testing.T) {
	t.Parallel()

	good := []string{
		"nginx.conf",
		"nginx/sites-enabled/default",
		"a/b/c/d/e",
		"weird name with spaces.conf",
		"dot.files/.bashrc",
		"..hidden", // leading dots are fine, ".." as a whole element is not
		"a..b/c",   // dots inside an element are ordinary characters
		strings.Repeat("x", MaxNameLen),
	}
	for _, in := range good {
		t.Run(in, func(t *testing.T) {
			t.Parallel()
			got, err := SanitizePath(in)
			if err != nil {
				t.Fatalf("SanitizePath(%q) = error %v, want it accepted", in, err)
			}
			if got != in {
				t.Errorf("SanitizePath(%q) = %q, want the input back unchanged", in, got)
			}
		})
	}
}

// TestSanitizeModeStripsTypeBits guards the rule that Kind, not the wire,
// decides what an entry is. A peer must not be able to ask us to create a
// device node, a socket, or a symlink by setting mode bits.
func TestSanitizeModeStripsTypeBits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   uint32
		want uint32
	}{
		{"plain file mode survives", 0o644, 0o644},
		{"restrictive mode survives", 0o600, 0o600},
		{"setuid survives", uint32(fs.ModeSetuid) | 0o755, uint32(fs.ModeSetuid) | 0o755},
		{"sticky survives", uint32(fs.ModeSticky) | 0o777, uint32(fs.ModeSticky) | 0o777},
		{"device bit stripped", uint32(fs.ModeDevice) | 0o644, 0o644},
		{"symlink bit stripped", uint32(fs.ModeSymlink) | 0o777, 0o777},
		{"socket bit stripped", uint32(fs.ModeSocket) | 0o644, 0o644},
		{"dir bit stripped", uint32(fs.ModeDir) | 0o755, 0o755},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := SanitizeMode(tc.in); got != tc.want {
				t.Errorf("SanitizeMode(%#o) = %#o, want %#o", tc.in, got, tc.want)
			}
		})
	}
}

func TestConflictName(t *testing.T) {
	t.Parallel()

	got := ConflictName("nginx/nginx.conf", "node2", 1755000000000000000)
	want := "nginx/nginx.conf.conflict.node2.1755000000000000000"
	if got != want {
		t.Errorf("ConflictName() = %q, want %q", got, want)
	}
	if _, err := SanitizePath(got); err != nil {
		t.Errorf("conflict name %q does not survive SanitizePath: %v", got, err)
	}
	if !IsConflictName(got) {
		t.Errorf("IsConflictName(%q) = false, want true", got)
	}
	if IsConflictName("nginx/nginx.conf") {
		t.Error("IsConflictName() reported a plain path as a conflict copy")
	}
	// A directory named ".conflict." somewhere up the tree must not make every
	// file below it look like a conflict copy.
	if IsConflictName("a.conflict.node1.1/real.conf") {
		t.Error("IsConflictName() matched on a parent directory rather than the base name")
	}
}
