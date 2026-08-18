package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"
)

// fakeReader is a Reader backed by maps, so transport tests do not drag in a
// database.
type fakeReader struct {
	entries []core.Meta
	blobs   map[string][]byte
	failAll bool
}

func (f *fakeReader) Digest(context.Context) (string, int64, int64, error) {
	if f.failAll {
		return "", 0, 0, errors.New("boom")
	}
	return "digest-abc", int64(len(f.entries)), 99, nil
}

func (f *fakeReader) ManifestSince(_ context.Context, seq int64, limit int) ([]core.Meta, error) {
	if f.failAll {
		return nil, errors.New("boom")
	}
	var out []core.Meta
	for _, m := range f.entries {
		if m.Seq > seq {
			out = append(out, m)
		}
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (f *fakeReader) Blob(_ context.Context, hash string) ([]byte, error) {
	b, ok := f.blobs[hash]
	if !ok {
		return nil, errors.New("no such blob")
	}
	return b, nil
}

// harness spins up a real HTTP server with a real client pointed at it.
func harness(t *testing.T, r *fakeReader) (*Client, string, core.Keys) {
	t.Helper()

	keys, err := core.DeriveKeys("a-sufficiently-long-test-secret")
	if err != nil {
		t.Fatalf("DeriveKeys() = %v", err)
	}
	status := func(context.Context) (Status, error) {
		return Status{Node: "server", Entries: int64(len(r.entries))}, nil
	}
	srv := httptest.NewServer(NewServer(r, status, keys, slog.New(slog.DiscardHandler)).Handler())
	t.Cleanup(srv.Close)

	addr := strings.TrimPrefix(srv.URL, "http://")
	return NewClient("client", keys, 1<<20, 5*time.Second), addr, keys
}

func TestRoundTripDigestManifestBlob(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	content := []byte("server { listen 80; }")
	hash := core.HashContent(content)
	r := &fakeReader{
		entries: []core.Meta{
			{Path: "a.conf", Kind: core.KindFile, Hash: hash, Mode: 0o644, Seq: 1,
				Version: core.Version{HLC: core.Timestamp{Wall: 100}, Origin: "server"}},
			{Path: "b.conf", Kind: core.KindFile, Hash: hash, Mode: 0o600, Seq: 2,
				Version: core.Version{HLC: core.Timestamp{Wall: 101}, Origin: "server"}},
		},
		blobs: map[string][]byte{hash: content},
	}
	c, addr, _ := harness(t, r)

	dig, err := c.Digest(ctx, addr)
	if err != nil {
		t.Fatalf("Digest() = %v", err)
	}
	if dig.Digest != "digest-abc" || dig.Entries != 2 || dig.Head != 99 {
		t.Errorf("Digest() = %+v, want the reader's values through unchanged", dig)
	}

	man, err := c.Manifest(ctx, addr, 0)
	if err != nil {
		t.Fatalf("Manifest() = %v", err)
	}
	if len(man.Entries) != 2 {
		t.Fatalf("Manifest(0) returned %d entries, want 2", len(man.Entries))
	}
	// Head must advance to the last entry returned, or the puller loops on the
	// same page forever.
	if man.Head != 2 {
		t.Errorf("Manifest().Head = %d, want 2", man.Head)
	}
	if man.Entries[0].Mode != 0o644 || man.Entries[1].Mode != 0o600 {
		t.Error("entry metadata did not survive the JSON round trip")
	}

	delta, err := c.Manifest(ctx, addr, 1)
	if err != nil {
		t.Fatalf("Manifest(1) = %v", err)
	}
	if len(delta.Entries) != 1 || delta.Entries[0].Path != "b.conf" {
		t.Errorf("Manifest(1) = %+v, want just b.conf", delta.Entries)
	}

	got, err := c.Blob(ctx, addr, hash)
	if err != nil {
		t.Fatalf("Blob() = %v", err)
	}
	if string(got) != string(content) {
		t.Errorf("Blob() = %q, want %q", got, content)
	}

	st, err := c.Status(ctx, addr)
	if err != nil {
		t.Fatalf("Status() = %v", err)
	}
	if st.Node != "server" {
		t.Errorf("Status().Node = %q, want %q", st.Node, "server")
	}
}

// TestUnauthenticatedRequestsAreRejected is the core access-control test. Each
// case is something an attacker on the network can actually attempt.
func TestUnauthenticatedRequestsAreRejected(t *testing.T) {
	t.Parallel()

	r := &fakeReader{blobs: map[string][]byte{}}
	_, addr, keys := harness(t, r)
	const path = "/v1/digest"
	url := "http://" + addr + path

	goodAuth, err := authorize(keys.API, http.MethodGet, path, "client", time.Now())
	if err != nil {
		t.Fatalf("authorize() = %v", err)
	}

	wrongKeys, err := core.DeriveKeys("an-entirely-different-secret-value")
	if err != nil {
		t.Fatalf("DeriveKeys() = %v", err)
	}
	forged, err := authorize(wrongKeys.API, http.MethodGet, path, "client", time.Now())
	if err != nil {
		t.Fatalf("authorize() = %v", err)
	}
	stale, err := authorize(keys.API, http.MethodGet, path, "client", time.Now().Add(-MaxSkew-time.Minute))
	if err != nil {
		t.Fatalf("authorize() = %v", err)
	}
	future, err := authorize(keys.API, http.MethodGet, path, "client", time.Now().Add(MaxSkew+time.Minute))
	if err != nil {
		t.Fatalf("authorize() = %v", err)
	}
	// A signature that is valid for a different path must not be reusable here.
	otherPath, err := authorize(keys.API, http.MethodGet, "/v1/status", "client", time.Now())
	if err != nil {
		t.Fatalf("authorize() = %v", err)
	}

	tests := []struct {
		name string
		auth string
	}{
		{"no header", ""},
		{"wrong scheme", "Bearer " + strings.TrimPrefix(goodAuth, AuthScheme+" ")},
		{"forged with another secret", forged},
		{"expired timestamp", stale},
		{"timestamp from the future", future},
		{"signature bound to another path", otherPath},
		{"truncated credential", AuthScheme + " client:123"},
		{"empty node", AuthScheme + " :123:nonce:sig"},
		{"garbage timestamp", AuthScheme + " client:notanumber:nonce:sig"},
		{"flipped signature byte", goodAuth[:len(goodAuth)-1] + flip(goodAuth[len(goodAuth)-1:])},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
			if err != nil {
				t.Fatalf("NewRequest() = %v", err)
			}
			if tc.auth != "" {
				req.Header.Set("Authorization", tc.auth)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Do() = %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Errorf("status = %s, want 401", resp.Status)
			}
		})
	}

	// The positive control: the same request with a good signature succeeds,
	// proving the rejections above are the signature and not the setup.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest() = %v", err)
	}
	req.Header.Set("Authorization", goodAuth)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("correctly signed request got %s, want 200", resp.Status)
	}
}

// flip returns s with the last character changed, for corrupting a signature.
func flip(s string) string {
	if s == "A" {
		return "B"
	}
	return "A"
}

// TestBodiesAreEncryptedOnTheWire is the confidentiality test: an observer who
// can read the response must not be able to read /etc out of it.
func TestBodiesAreEncryptedOnTheWire(t *testing.T) {
	t.Parallel()

	secret := []byte("PermitRootLogin yes # a very recognisable string")
	hash := core.HashContent(secret)
	r := &fakeReader{blobs: map[string][]byte{hash: secret}}
	_, addr, keys := harness(t, r)

	path := "/v1/blob/" + hash
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		t.Fatalf("NewRequest() = %v", err)
	}
	auth, err := authorize(keys.API, http.MethodGet, path, "client", time.Now())
	if err != nil {
		t.Fatalf("authorize() = %v", err)
	}
	req.Header.Set("Authorization", auth)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() = %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() = %v", err)
	}

	if strings.Contains(string(raw), "PermitRootLogin") {
		t.Fatal("config content appeared in plaintext on the wire")
	}
	// It must nonetheless decrypt to exactly the original.
	plain, err := open(keys.Blob, raw)
	if err != nil {
		t.Fatalf("open() = %v", err)
	}
	if string(plain) != string(secret) {
		t.Errorf("unsealed body = %q, want %q", plain, secret)
	}
}

func TestSealRejectsTamperingAndWrongKeys(t *testing.T) {
	t.Parallel()

	keys, err := core.DeriveKeys("a-sufficiently-long-test-secret")
	if err != nil {
		t.Fatalf("DeriveKeys() = %v", err)
	}
	other, err := core.DeriveKeys("a-completely-different-secret-x")
	if err != nil {
		t.Fatalf("DeriveKeys() = %v", err)
	}

	plaintext := []byte("listen 443 ssl;")
	sealedBody, err := seal(keys.Blob, plaintext)
	if err != nil {
		t.Fatalf("seal() = %v", err)
	}

	got, err := open(keys.Blob, sealedBody)
	if err != nil {
		t.Fatalf("open() = %v", err)
	}
	if string(got) != string(plaintext) {
		t.Fatalf("round trip = %q, want %q", got, plaintext)
	}

	// Sealing twice must not produce the same bytes; identical ciphertexts
	// would mean a reused nonce, which breaks GCM outright.
	again, err := seal(keys.Blob, plaintext)
	if err != nil {
		t.Fatalf("seal() = %v", err)
	}
	if string(again) == string(sealedBody) {
		t.Error("two seals of the same plaintext produced identical bytes — the nonce is not random")
	}

	if _, err := open(other.Blob, sealedBody); !errors.Is(err, ErrSeal) {
		t.Errorf("open() with the wrong key = %v, want ErrSeal", err)
	}

	tampered := append([]byte(nil), sealedBody...)
	tampered[len(tampered)-1] ^= 0xff
	if _, err := open(keys.Blob, tampered); !errors.Is(err, ErrSeal) {
		t.Errorf("open() of tampered ciphertext = %v, want ErrSeal", err)
	}

	if _, err := open(keys.Blob, []byte("short")); !errors.Is(err, ErrSeal) {
		t.Errorf("open() of a truncated payload = %v, want ErrSeal", err)
	}
}

// TestClientRejectsMismatchedBlob covers a peer that serves content that is not
// what was asked for. Content addressing makes this detectable, and a peer's
// bytes must never reach the store unverified.
func TestClientRejectsMismatchedBlob(t *testing.T) {
	t.Parallel()

	wanted := core.HashContent([]byte("the real config"))
	r := &fakeReader{blobs: map[string][]byte{wanted: []byte("SOMETHING ELSE ENTIRELY")}}
	c, addr, _ := harness(t, r)

	if _, err := c.Blob(t.Context(), addr, wanted); err == nil {
		t.Fatal("client accepted a blob whose content did not match the requested hash")
	}
}

func TestBlobHashParameterIsValidated(t *testing.T) {
	t.Parallel()

	r := &fakeReader{blobs: map[string][]byte{}}
	_, addr, keys := harness(t, r)

	// Path traversal in the hash parameter, a short hash, and a non-hex hash
	// must all be refused before the store is consulted.
	for _, hash := range []string{
		strings.Repeat("z", 64),
		"abc",
		strings.Repeat("a", 63),
		strings.Repeat("a", 65),
	} {
		path := "/v1/blob/" + hash
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+path, nil)
		if err != nil {
			t.Fatalf("NewRequest() = %v", err)
		}
		auth, err := authorize(keys.API, http.MethodGet, path, "client", time.Now())
		if err != nil {
			t.Fatalf("authorize() = %v", err)
		}
		req.Header.Set("Authorization", auth)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Do() = %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("hash %q got %s, want 400", hash, resp.Status)
		}
	}
}

func TestManifestPageIsCappedServerSide(t *testing.T) {
	t.Parallel()

	// More entries than one page can hold, so More must come back true.
	entries := make([]core.Meta, MaxManifestPage+10)
	for i := range entries {
		entries[i] = core.Meta{
			Path: fmt.Sprintf("f%05d.conf", i), Kind: core.KindFile, Mode: 0o644,
			Seq:     int64(i + 1),
			Version: core.Version{HLC: core.Timestamp{Wall: int64(i)}, Origin: "server"},
		}
	}
	c, addr, _ := harness(t, &fakeReader{entries: entries, blobs: map[string][]byte{}})

	page, err := c.Manifest(t.Context(), addr, 0)
	if err != nil {
		t.Fatalf("Manifest() = %v", err)
	}
	if len(page.Entries) != MaxManifestPage {
		t.Errorf("page held %d entries, want the server-side cap of %d", len(page.Entries), MaxManifestPage)
	}
	if !page.More {
		t.Error("More = false, but the tree is larger than one page — the puller would stop early")
	}
	if page.Head != int64(MaxManifestPage) {
		t.Errorf("Head = %d, want %d so the next page resumes correctly", page.Head, MaxManifestPage)
	}
}

// TestClientEnforcesBodyLimit covers a peer that answers with more bytes than
// this node is willing to hold.
func TestClientEnforcesBodyLimit(t *testing.T) {
	t.Parallel()

	keys, err := core.DeriveKeys("a-sufficiently-long-test-secret")
	if err != nil {
		t.Fatalf("DeriveKeys() = %v", err)
	}
	// A server that ignores every limit and floods the response.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		flood := make([]byte, 1<<20)
		for range 4 {
			_, _ = w.Write(flood)
		}
	}))
	t.Cleanup(srv.Close)

	c := NewClient("client", keys, 64<<10, 5*time.Second)
	_, err = c.Digest(t.Context(), strings.TrimPrefix(srv.URL, "http://"))
	if err == nil {
		t.Fatal("client accepted a response far larger than its configured limit")
	}
	if !strings.Contains(err.Error(), "limit") {
		t.Errorf("error = %v, want it to name the limit", err)
	}
}

func TestHealthzNeedsNoCredentials(t *testing.T) {
	t.Parallel()

	_, addr, _ := harness(t, &fakeReader{blobs: map[string][]byte{}})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/healthz", nil)
	if err != nil {
		t.Fatalf("NewRequest() = %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() = %v", err)
	}
	defer resp.Body.Close()

	// Container health checks run before the orchestrator knows any secret.
	if resp.StatusCode != http.StatusOK {
		t.Errorf("healthz = %s, want 200 without credentials", resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() = %v", err)
	}
	if strings.TrimSpace(string(body)) != "ok" {
		t.Errorf("healthz body = %q, want it to reveal nothing beyond liveness", body)
	}
}

func TestServerErrorsDoNotLeakDetail(t *testing.T) {
	t.Parallel()

	c, addr, _ := harness(t, &fakeReader{blobs: map[string][]byte{}, failAll: true})
	_, err := c.Digest(t.Context(), addr)
	if err == nil {
		t.Fatal("Digest() succeeded against a failing reader")
	}
	if strings.Contains(err.Error(), "boom") {
		t.Errorf("internal error text reached the client: %v", err)
	}
}

func TestParseSeqIsPermissive(t *testing.T) {
	t.Parallel()

	tests := map[string]int64{
		"": 0, "0": 0, "42": 42, "-1": 0, "abc": 0, "9e9": 0, "999999": 999999,
	}
	for in, want := range tests {
		if got := parseSeq(in); got != want {
			t.Errorf("parseSeq(%q) = %d, want %d", in, got, want)
		}
	}
}
