package core

import (
	"crypto/hkdf"
	"crypto/sha256"
	"errors"
	"fmt"
)

// MinSecretLen is the shortest cluster secret we accept.
//
// This tool replicates /etc — the files it carries are exactly the ones worth
// stealing. A short secret is not a usability trade-off here, it is a hole, so
// it is refused at startup rather than warned about.
const MinSecretLen = 16

// ErrWeakSecret is returned when the operator's cluster secret is too short.
var ErrWeakSecret = errors.New("core: cluster secret too short")

// Keys are the three independent 32-byte keys the cluster runs on, expanded
// from the single secret the operator sets in the environment.
//
// They are separated rather than reused because the three uses have different
// shapes — memberlist encrypts small UDP datagrams, the API authenticates
// requests, the blob channel seals response bodies — and reusing one key across
// different constructions is how nonce reuse and cross-protocol confusion get
// in. HKDF makes separation free, so there is no reason not to.
type Keys struct {
	// Gossip is memberlist's SecretKey: it encrypts and authenticates the
	// membership protocol itself.
	Gossip []byte
	// API keys the HMAC-SHA256 signature on every peer HTTP request.
	API []byte
	// Blob keys the AES-256-GCM seal over peer HTTP response bodies, so config
	// contents and the tree's shape are not readable by a passive observer.
	Blob []byte
}

// DeriveKeys expands secret into the cluster's three keys. Every node given the
// same secret derives the same keys, which is the whole of the trust model:
// possession of the secret is membership.
func DeriveKeys(secret string) (Keys, error) {
	if len(secret) < MinSecretLen {
		return Keys{}, fmt.Errorf("%w: %d bytes, need at least %d",
			ErrWeakSecret, len(secret), MinSecretLen)
	}

	// A fixed salt rather than a random one: every node must derive identical
	// keys from the shared secret without any coordination, so there is nowhere
	// to put a per-cluster random salt. The domain-separating info strings do
	// the work the salt would otherwise do.
	const salt = "distributed-configs-fs/v1"
	derive := func(info string) ([]byte, error) {
		return hkdf.Key(sha256.New, []byte(secret), []byte(salt), info, 32)
	}

	var (
		k   Keys
		err error
	)
	if k.Gossip, err = derive("gossip"); err != nil {
		return Keys{}, fmt.Errorf("derive gossip key: %w", err)
	}
	if k.API, err = derive("api-hmac"); err != nil {
		return Keys{}, fmt.Errorf("derive api key: %w", err)
	}
	if k.Blob, err = derive("blob-seal"); err != nil {
		return Keys{}, fmt.Errorf("derive blob key: %w", err)
	}
	return k, nil
}
