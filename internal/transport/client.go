package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"
)

// ErrPeerNotFound is returned when a peer does not hold what was asked for.
// It is an ordinary outcome during convergence — a node can learn of an entry
// from one peer before another peer has the blob — so callers treat it as
// "try elsewhere", not as a failure.
var ErrPeerNotFound = fmt.Errorf("transport: peer does not have it")

// Client talks to another node's peer API.
type Client struct {
	http    *http.Client
	keys    core.Keys
	node    string // this node's name, sent so the server can log who called
	maxBody int64
}

// NewClient returns a Client identifying itself as node.
//
// maxBody caps how many bytes any single response may occupy. It is not
// optional: without it a peer — hostile, or merely misconfigured with a huge
// file — can make this node allocate until it dies, and a config replicator
// that can be OOM-killed by a peer is a liability rather than a safeguard.
func NewClient(node string, keys core.Keys, maxBody int64, timeout time.Duration) *Client {
	return &Client{
		http:    &http.Client{Timeout: timeout},
		keys:    keys,
		node:    node,
		maxBody: maxBody,
	}
}

// Digest fetches a peer's whole-tree digest. Anti-entropy calls this first: two
// nodes that already agree exchange a hash instead of a manifest of every path.
func (c *Client) Digest(ctx context.Context, addr string) (DigestResponse, error) {
	var out DigestResponse
	err := c.getJSON(ctx, addr, "/v1/digest", &out)
	return out, err
}

// Manifest fetches one page of a peer's changes feed, starting after since.
func (c *Client) Manifest(ctx context.Context, addr string, since int64) (ManifestResponse, error) {
	var out ManifestResponse
	err := c.getJSON(ctx, addr, fmt.Sprintf("/v1/manifest?since=%d", since), &out)
	return out, err
}

// Status fetches a peer's status document.
func (c *Client) Status(ctx context.Context, addr string) (Status, error) {
	var out Status
	err := c.getJSON(ctx, addr, "/v1/status", &out)
	return out, err
}

// Blob fetches file content by hash and verifies it before returning.
//
// The verification is the point. Content addressing means we can check that the
// peer sent what we asked for, and a peer that returns different bytes is
// either broken or hostile — either way its answer must not reach the store.
func (c *Client) Blob(ctx context.Context, addr, hash string) ([]byte, error) {
	body, err := c.get(ctx, addr, "/v1/blob/"+hash)
	if err != nil {
		return nil, err
	}
	if got := core.HashContent(body); got != hash {
		return nil, fmt.Errorf("peer %s served blob %s but content hashes to %s", addr, hash, got)
	}
	return body, nil
}

// getJSON fetches path from a peer and decodes the sealed JSON body into out.
func (c *Client) getJSON(ctx context.Context, addr, path string, out any) error {
	body, err := c.get(ctx, addr, path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode %s from %s: %w", path, addr, err)
	}
	return nil
}

// get performs one signed request and returns the unsealed body.
func (c *Client) get(ctx context.Context, addr, path string) ([]byte, error) {
	url := "http://" + addr + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request for %s: %w", url, err)
	}

	auth, err := authorize(c.keys.API, http.MethodGet, path, c.node, time.Now())
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", auth)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("%w: %s", ErrPeerNotFound, path)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer %s returned %s for %s", addr, resp.Status, path)
	}

	// LimitReader before anything else touches the body, so a peer cannot make
	// us allocate more than maxBody no matter what it claims in Content-Length.
	sealedBody, err := io.ReadAll(io.LimitReader(resp.Body, c.maxBody+1))
	if err != nil {
		return nil, fmt.Errorf("read %s from %s: %w", path, addr, err)
	}
	if int64(len(sealedBody)) > c.maxBody {
		return nil, fmt.Errorf("peer %s sent more than the %d byte limit for %s", addr, c.maxBody, path)
	}

	plain, err := open(c.keys.Blob, sealedBody)
	if err != nil {
		return nil, fmt.Errorf("open sealed body from %s: %w", addr, err)
	}
	return plain, nil
}
