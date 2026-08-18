package core

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestDeriveKeysRejectsWeakSecret(t *testing.T) {
	t.Parallel()

	for _, secret := range []string{"", "short", strings.Repeat("x", MinSecretLen-1)} {
		if _, err := DeriveKeys(secret); !errors.Is(err, ErrWeakSecret) {
			t.Errorf("DeriveKeys(%q) error = %v, want ErrWeakSecret", secret, err)
		}
	}
}

// TestDeriveKeysIsDeterministicAndSeparated pins the two properties the trust
// model rests on: every node with the same secret derives the same keys (or
// they cannot talk), and the three keys differ (or a nonce reused in one
// construction weakens another).
func TestDeriveKeysIsDeterministicAndSeparated(t *testing.T) {
	t.Parallel()

	const secret = "correct horse battery staple"
	a, err := DeriveKeys(secret)
	if err != nil {
		t.Fatalf("DeriveKeys() = %v", err)
	}
	b, err := DeriveKeys(secret)
	if err != nil {
		t.Fatalf("DeriveKeys() second call = %v", err)
	}

	if !bytes.Equal(a.Gossip, b.Gossip) || !bytes.Equal(a.API, b.API) || !bytes.Equal(a.Blob, b.Blob) {
		t.Fatal("DeriveKeys() is not deterministic; nodes sharing a secret would not agree on keys")
	}

	for name, k := range map[string][]byte{"gossip": a.Gossip, "api": a.API, "blob": a.Blob} {
		// 32 bytes: memberlist requires AES-256 key material and the blob seal
		// is AES-256-GCM.
		if len(k) != 32 {
			t.Errorf("%s key is %d bytes, want 32", name, len(k))
		}
	}
	if bytes.Equal(a.Gossip, a.API) || bytes.Equal(a.API, a.Blob) || bytes.Equal(a.Gossip, a.Blob) {
		t.Error("derived keys are not domain-separated; one key is being reused across constructions")
	}

	// A different secret must produce entirely different keys, otherwise
	// rotating the secret would not actually evict anybody.
	c, err := DeriveKeys(secret + "!")
	if err != nil {
		t.Fatalf("DeriveKeys() = %v", err)
	}
	if bytes.Equal(a.Gossip, c.Gossip) {
		t.Error("changing the secret did not change the derived gossip key")
	}
}
