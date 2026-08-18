package transport

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
)

// ErrSeal is the sentinel behind every failure to open a sealed body: wrong
// key, truncated payload, or tampered ciphertext. They are deliberately not
// distinguished — telling an attacker *why* their forgery failed is free
// information.
var ErrSeal = errors.New("transport: cannot open sealed payload")

// seal encrypts plaintext with AES-256-GCM and returns nonce||ciphertext.
//
// Every peer response body goes through here. Authentication alone would leave
// the contents of /etc readable to anything on the path between two nodes, and
// the manifest alone leaks the shape of the tree, which is most of what an
// attacker wants to know before deciding where to look.
func seal(key, plaintext []byte) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	// A fresh random nonce per message. GCM fails catastrophically on nonce
	// reuse, and there is no shared counter between nodes to draw from, so
	// random-per-message is the only safe source: at 96 bits the collision risk
	// is negligible for this traffic volume.
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// open reverses seal. It returns ErrSeal for every failure mode.
func open(key, payload []byte) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	if len(payload) < gcm.NonceSize() {
		return nil, fmt.Errorf("%w: payload shorter than a nonce", ErrSeal)
	}
	nonce, ciphertext := payload[:gcm.NonceSize()], payload[gcm.NonceSize():]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, ErrSeal
	}
	return plaintext, nil
}

// newGCM builds the AEAD both directions use.
func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("build cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("build gcm: %w", err)
	}
	return gcm, nil
}
