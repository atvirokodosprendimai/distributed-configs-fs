# Build stage.
#
# CGO_ENABLED=0 is not an optimisation here, it is the point: SQLite is pure Go
# via glebarez/modernc, so the binary is static and cross-compiles to every
# target from one runner without a C toolchain.
FROM golang:1.26-alpine AS build

WORKDIR /src

# Dependencies first, so editing source does not re-download the module cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
RUN CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/dcfs ./cmd/dcfs

# Runtime stage.
#
# Alpine rather than distroless because FUSE mode needs fusermount3 to unmount
# cleanly on shutdown. A mount whose server has gone hangs every process that
# touches the directory, so having the tool available matters more than the few
# megabytes distroless would save.
FROM alpine:3.21

RUN apk add --no-cache ca-certificates fuse3 tini

COPY --from=build /out/dcfs /usr/local/bin/dcfs

# The database lives here and must be on a volume: it is this node's source of
# truth, and losing it means re-fetching the whole tree from a peer.
VOLUME ["/var/lib/dcfs"]

# Gossip needs both, on the same port number: memberlist uses UDP for the
# failure detector and TCP for state exchange and joins.
EXPOSE 7946/tcp 7946/udp
# The peer HTTP API: manifests, content, status.
EXPOSE 7947/tcp

# The healthcheck deliberately hits the one unauthenticated endpoint. An
# orchestrator starts containers before it necessarily knows any secret, so a
# check that needed credentials would fail for the wrong reason.
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -q -O- http://127.0.0.1:7947/healthz || exit 1

# tini reaps zombies and, more importantly here, forwards SIGTERM properly, so
# `docker stop` reaches the process and it can announce its departure and
# unmount rather than being killed outright.
ENTRYPOINT ["/sbin/tini", "--", "dcfs"]
CMD ["serve"]
