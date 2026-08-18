// Package transport is the peer-to-peer HTTP API: how one node fetches tree
// state and file content from another.
//
// The API is deliberately pull-only. Nothing a peer can reach mutates local
// state — a peer serves, and this node decides what to do with the answer.
// That bounds what a compromised member can do to "serve bad data", which the
// syncer then has to survive on its own merits, rather than "write whatever it
// likes into /etc".
//
// Every request is HMAC-signed and every response body is AES-256-GCM sealed,
// both keyed from the operator's cluster secret. Authentication without
// encryption would leave the contents of /etc readable to anything on the wire.
package transport

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/atvirokodosprendimai/distributed-configs-fs/internal/core"
)

// MaxManifestPage caps how many entries one manifest request may return.
//
// The cap is enforced server-side rather than trusted from the query string:
// a peer asking for the whole tree in one response is a memory amplification
// primitive, since the server must hold the JSON and its sealed copy at once.
// A puller that wants more simply asks again with a higher since.
const MaxManifestPage = 2000

// Reader is the read-only view of the local store this API serves.
//
// It is narrow on purpose. A peer-facing handler that is handed something with
// a Write method eventually grows a write endpoint; one that is handed this
// cannot. The store satisfies it without knowing this interface exists.
type Reader interface {
	Digest(ctx context.Context) (digest string, entries int64, head int64, err error)
	ManifestSince(ctx context.Context, seq int64, limit int) ([]core.Meta, error)
	Blob(ctx context.Context, hash string) ([]byte, error)
}

// StatusFunc assembles the operator-facing status document. It is supplied by
// main rather than built here, because status spans the store, the cluster and
// the syncer, and none of those should have to know about HTTP.
type StatusFunc func(ctx context.Context) (Status, error)

// DigestResponse answers "do we already agree?" in one round trip.
type DigestResponse struct {
	Digest  string `json:"digest"`
	Entries int64  `json:"entries"`
	Head    int64  `json:"head"` // the serving node's feed position
}

// ManifestResponse is one page of the serving node's changes feed.
type ManifestResponse struct {
	Entries []core.Meta `json:"entries"`
	Head    int64       `json:"head"` // feed position after the last entry returned
	More    bool        `json:"more"` // true when the page hit MaxManifestPage
}

// Server is the peer HTTP API.
type Server struct {
	reader Reader
	status StatusFunc
	keys   core.Keys
	log    *slog.Logger
}

// NewServer returns a Server reading from r and reporting status via status.
func NewServer(r Reader, status StatusFunc, keys core.Keys, log *slog.Logger) *Server {
	return &Server{reader: r, status: status, keys: keys, log: log}
}

// Handler returns the routed HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := chi.NewRouter()

	// Unauthenticated, and intentionally information-free: container health
	// checks run before the orchestrator knows any secret, so this endpoint
	// must reveal nothing beyond "the process is up".
	mux.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok\n"))
	})

	mux.Group(func(r chi.Router) {
		r.Use(s.authenticate)
		r.Get("/v1/digest", s.getDigest)
		r.Get("/v1/manifest", s.getManifest)
		r.Get("/v1/blob/{hash}", s.getBlob)
		r.Get("/v1/status", s.getStatus)
	})
	return mux
}

// authenticate rejects any request that is not signed with the cluster key.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		node, err := verify(s.keys.API, r)
		if err != nil {
			// Logged at debug: on a shared network this fires for every stray
			// scanner, and at info it would drown the log that matters.
			s.log.Debug("rejected unsigned peer request",
				"path", r.URL.Path, "remote", r.RemoteAddr, "err", err)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), peerKey{}, node)))
	})
}

// peerKey is the context key carrying the verified peer name.
type peerKey struct{}

// PeerFrom returns the authenticated peer name carried on ctx, if any.
func PeerFrom(ctx context.Context) string {
	node, _ := ctx.Value(peerKey{}).(string)
	return node
}

// getDigest serves the whole-tree hash used to short-circuit anti-entropy.
func (s *Server) getDigest(w http.ResponseWriter, r *http.Request) {
	digest, entries, head, err := s.reader.Digest(r.Context())
	if err != nil {
		s.fail(w, r, "digest", err)
		return
	}
	s.sealed(w, r, DigestResponse{Digest: digest, Entries: entries, Head: head})
}

// getManifest serves one page of this node's changes feed.
func (s *Server) getManifest(w http.ResponseWriter, r *http.Request) {
	since := parseSeq(r.URL.Query().Get("since"))

	// Ask for one extra row: if it comes back, there is another page, and the
	// puller can keep going without a separate count query.
	entries, err := s.reader.ManifestSince(r.Context(), since, MaxManifestPage+1)
	if err != nil {
		s.fail(w, r, "manifest", err)
		return
	}
	resp := ManifestResponse{Head: since}
	if len(entries) > MaxManifestPage {
		entries, resp.More = entries[:MaxManifestPage], true
	}
	resp.Entries = entries
	if n := len(entries); n > 0 {
		resp.Head = entries[n-1].Seq
	}
	s.sealed(w, r, resp)
}

// getBlob serves file content by hash.
func (s *Server) getBlob(w http.ResponseWriter, r *http.Request) {
	hash := chi.URLParam(r, "hash")
	// Validate the shape before the database sees it. The parameter is
	// attacker-controlled, and a hash is exactly 64 lowercase hex characters —
	// anything else is not a lookup worth performing.
	if len(hash) != 64 {
		http.Error(w, "bad hash", http.StatusBadRequest)
		return
	}
	if _, err := hex.DecodeString(hash); err != nil {
		http.Error(w, "bad hash", http.StatusBadRequest)
		return
	}

	content, err := s.reader.Blob(r.Context(), hash)
	if err != nil {
		// A blob we do not have is an ordinary outcome during convergence, not
		// a server fault: the peer learned of an entry from a third node.
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	s.sealedBytes(w, r, content, "application/octet-stream")
}

// getStatus serves the operator-facing status document.
func (s *Server) getStatus(w http.ResponseWriter, r *http.Request) {
	st, err := s.status(r.Context())
	if err != nil {
		s.fail(w, r, "status", err)
		return
	}
	s.sealed(w, r, st)
}

// sealed marshals v to JSON and writes it sealed.
func (s *Server) sealed(w http.ResponseWriter, r *http.Request, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		s.fail(w, r, "marshal", err)
		return
	}
	s.sealedBytes(w, r, body, "application/octet-stream")
}

// sealedBytes encrypts body and writes it.
func (s *Server) sealedBytes(w http.ResponseWriter, r *http.Request, body []byte, contentType string) {
	payload, err := seal(s.keys.Blob, body)
	if err != nil {
		s.fail(w, r, "seal", err)
		return
	}
	w.Header().Set("Content-Type", contentType)
	if _, err := w.Write(payload); err != nil {
		// The client hung up mid-response. Nothing to do but note it; the
		// puller will retry on its next cycle.
		s.log.Debug("peer response write failed", "path", r.URL.Path, "err", err)
	}
}

// fail logs a server-side error and returns a body that says nothing about it.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, op string, err error) {
	s.log.Error("peer request failed", "op", op, "path", r.URL.Path,
		"peer", PeerFrom(r.Context()), "err", err)
	http.Error(w, "internal error", http.StatusInternalServerError)
}

// Serve runs the API on addr until ctx is cancelled.
func (s *Server) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:    addr,
		Handler: s.Handler(),
		// Blob responses are bounded by the configured max file size, so a
		// generous but finite write timeout is safe here — unlike the SSE case,
		// nothing on this API is long-lived.
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	errc := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errc <- err
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil {
			return fmt.Errorf("shutdown peer api: %w", err)
		}
		return nil
	}
}
