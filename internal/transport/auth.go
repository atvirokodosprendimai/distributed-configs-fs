package transport

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// AuthScheme labels the Authorization header so the format can be changed later
// without a node silently misreading an old one.
const AuthScheme = "DCFS1"

// MaxSkew bounds how far a request's timestamp may be from the verifier's
// clock. It caps how long a captured signature stays usable.
//
// There is no nonce cache behind it, and that is deliberate rather than an
// omission: every peer endpoint is a read. Replaying a GET yields the attacker
// bytes they already captured. Nothing a peer can reach mutates state, so the
// only thing replay protection would buy is bookkeeping.
const MaxSkew = 5 * time.Minute

// ErrUnauthorized is the sentinel behind every rejected request. As with
// ErrSeal, the specific reason is not reported to the caller.
var ErrUnauthorized = errors.New("transport: unauthorized")

// sign computes the request signature. The signed string binds the method, the
// full path including query, the timestamp and the nonce, so a valid signature
// for /v1/blob/aaa cannot be replayed against /v1/blob/bbb, and a signature
// captured today is not valid tomorrow.
func sign(key []byte, method, pathWithQuery, node string, unixNano int64, nonce string) string {
	mac := hmac.New(sha256.New, key)
	fmt.Fprintf(mac, "%s\n%s\n%s\n%d\n%s", method, pathWithQuery, node, unixNano, nonce)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// authorize builds the Authorization header value for a request.
func authorize(key []byte, method, pathWithQuery, node string, now time.Time) (string, error) {
	nonceBytes := make([]byte, 12)
	if _, err := rand.Read(nonceBytes); err != nil {
		return "", fmt.Errorf("generate nonce: %w", err)
	}
	nonce := hex.EncodeToString(nonceBytes)
	ts := now.UnixNano()
	sig := sign(key, method, pathWithQuery, node, ts, nonce)
	return fmt.Sprintf("%s %s:%d:%s:%s", AuthScheme, node, ts, nonce, sig), nil
}

// verify checks an Authorization header against the request and returns the
// claimed node name.
//
// It is the front door for everything a peer sends, so it validates shape
// before it validates cryptography: a malformed header must not reach the MAC
// comparison with a half-parsed timestamp.
func verify(key []byte, r *http.Request) (string, error) {
	header := r.Header.Get("Authorization")
	scheme, rest, ok := strings.Cut(header, " ")
	if !ok || scheme != AuthScheme {
		return "", fmt.Errorf("%w: bad scheme", ErrUnauthorized)
	}

	// node:timestamp:nonce:signature — split from the right so a node name
	// containing a colon cannot shift the other fields.
	parts := strings.Split(rest, ":")
	if len(parts) != 4 {
		return "", fmt.Errorf("%w: malformed credential", ErrUnauthorized)
	}
	node, tsRaw, nonce, gotSig := parts[0], parts[1], parts[2], parts[3]
	if node == "" || nonce == "" {
		return "", fmt.Errorf("%w: empty node or nonce", ErrUnauthorized)
	}

	ts, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		return "", fmt.Errorf("%w: bad timestamp", ErrUnauthorized)
	}
	// Absolute value: a request from the future is as suspect as a stale one,
	// and unsigned subtraction here would wrap.
	if skew := time.Since(time.Unix(0, ts)); skew > MaxSkew || skew < -MaxSkew {
		return "", fmt.Errorf("%w: timestamp %s outside skew window", ErrUnauthorized, skew)
	}

	want := sign(key, r.Method, r.URL.RequestURI(), node, ts, nonce)
	// Constant time: a byte-by-byte comparison leaks the correct signature one
	// byte at a time to anyone willing to time the responses.
	if !hmac.Equal([]byte(want), []byte(gotSig)) {
		return "", fmt.Errorf("%w: bad signature", ErrUnauthorized)
	}
	return node, nil
}
