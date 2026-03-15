# snapshot-sidecar - Project Guide

## What is this

A generic Kubernetes sidecar for snapshotting project directories to
S3-compatible storage. Two subcommands: `restore` (init container, downloads
latest snapshot) and `serve` (sidecar, ConnectRPC server with on-demand +
periodic + SIGTERM snapshots). See [README.md](README.md) for full
documentation.

## Build & Test

```bash
# Build
go build -o snapshot-sidecar .

# Run tests
go test -race -count=1 ./...

# Lint
golangci-lint run
```

## Architecture

Single-file Go binary (`main.go`) with two subcommands:

- **`serve`** — Starts an h2c HTTP server with ConnectRPC (`SnapshotService`)
  and gRPC health check. On SIGTERM, creates a final snapshot before shutting
  down. Optional periodic auto-save via `AUTOSAVE_INTERVAL`.
- **`restore`** — Downloads and extracts the latest snapshot from S3. Exits 0 if
  no snapshot exists (first boot).

Key types:

- `snapshotter` — Core logic: tar/gzip creation, SHA-256 deduplication, S3
  upload, latest.json marker management
- `envConfig` — All config from environment variables
- `latestMarker` — JSON pointer (`latest.json`) stored in S3 tracking current
  snapshot

## Proto

Service defined in [`proto/snapshot.proto`](proto/snapshot.proto). Generated Go
stubs in `proto/snapshot/v1/`. Regenerate with:

```bash
buf generate
```

Requires: [buf](https://buf.build/), `protoc-gen-go`, `protoc-gen-connect-go`

## Configuration

All via environment variables — see [README.md](README.md) for the full table.
S3 credentials use standard AWS SDK env vars (`AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, `AWS_REGION`).

## CI

- `.github/workflows/ci.yml` — lint + test on push/PR
- `.github/workflows/release.yml` — Docker build to GHCR on `v*` tags

## Dependencies

| Package                        | Purpose                |
|--------------------------------|------------------------|
| `connectrpc.com/connect`       | ConnectRPC framework   |
| `connectrpc.com/grpchealth`    | gRPC health checking   |
| `github.com/aws/aws-sdk-go-v2` | S3 client              |
| `golang.org/x/net`             | h2c (HTTP/2 cleartext) |
| `google.golang.org/protobuf`   | Protobuf runtime       |
