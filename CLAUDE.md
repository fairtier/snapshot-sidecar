# snapshot-sidecar - Project Guide

## What is this

A generic Kubernetes sidecar for persisting project directories with two
backends (`SNAPSHOT_BACKEND=s3|git`): S3 tar.gz snapshots, or a git working
clone (save = commit + plain push, sync = fast-forward pull; never force,
never auto-merge). Two subcommands: `restore` (init container, downloads the
latest snapshot / adopts the clone) and `serve` (sidecar, ConnectRPC server
with on-demand + periodic + SIGTERM snapshots). See [README.md](README.md)
for full documentation.

## Build & Test

```bash
# Build
go build -o snapshot-sidecar .

# Run tests (git backend tests use in-process file remotes — no git binary,
# no network)
go test -race -count=1 ./...

# Lint
golangci-lint run
```

## Architecture

Small Go binary, one file per concern:

- `main.go` — subcommand dispatch, `envConfig`, backend selection, the shared
  serve loop (ConnectRPC + gRPC health + autosave + SIGTERM final save). The
  `backend` interface = `SnapshotServiceHandler` + `save(ctx)`.
- `s3.go` — `snapshotter`: tar/gzip creation, SHA-256 deduplication, S3
  upload, `latestMarker` (latest.json) management, tar.gz restore.
- `git.go` — `gitBackend`: adopt/restore, commit+push save (non-fast-forward
  → status `remote_changed`), clean-only fast-forward sync loop
  (`SYNC_INTERVAL`), `GET /debug/sync-status`. Pure-Go git via go-git v6;
  the token travels in-process (`ClientOptions`/`WithHTTPAuth`), never
  touching `.git/config`.
- `otel.go` — OpenTelemetry: SDK setup (OTLP/HTTP, off unless
  `OTEL_EXPORTER_OTLP_ENDPOINT` is set), the shared instrument set (`tel`),
  attribute keys, and the `withTrigger`/`triggerFrom` context tag that tells
  an RPC save apart from an autosave/SIGTERM one. Instruments hang off the
  OTel *global* providers, which delegate once the SDK is installed — so
  `tel` is safe to build at package init and in tests that never call
  `setupOTel`. Backends add spans/metrics inline; see README for the
  span tree and metric list.

Subcommands:

- **`serve`** — Starts an h2c HTTP server (native `http.Protocols`, no
  x/net/h2c) with ConnectRPC (`SnapshotService`) and gRPC health check. On
  SIGTERM, creates a final snapshot before shutting down. Optional periodic
  auto-save via `AUTOSAVE_INTERVAL`; git backend also runs the sync loop.
- **`restore`** — Populates the project dir: downloads + extracts the latest
  S3 snapshot, or opens/adopts the git clone (existing local files are never
  overwritten). Exits 0 if there is nothing to restore (first boot).

## Proto

Service defined in [`proto/snapshot.proto`](proto/snapshot.proto). Generated Go
stubs in `proto/snapshot/v1/`. Regenerate with:

```bash
buf generate
```

Requires: [buf](https://buf.build/), `protoc-gen-go`, `protoc-gen-connect-go`

NOTE: `TriggerSnapshotResponse.status` is a free-form string — new statuses
(like `remote_changed`) do NOT require a proto change. fairtier.com's
platform_api vendors a superset of this proto; if you DO change the proto,
that copy must be updated in lockstep.

## Configuration

All via environment variables — see [README.md](README.md) for the full table.
S3 credentials use standard AWS SDK env vars (`AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, `AWS_REGION`); the git backend
uses `GIT_REMOTE_URL`/`GIT_TOKEN` (+ optional `GIT_BRANCH`, `GIT_USERNAME`,
`GIT_AUTHOR_*`, `SYNC_INTERVAL`). Telemetry uses the standard `OTEL_*` env
vars and stays off until an OTLP endpoint is set.

## CI

- `.github/workflows/ci.yml` — lint + test on push/PR
- `.github/workflows/release.yml` — Docker build to GHCR on `v*` tags

## Dependencies

| Package                        | Purpose                     |
|--------------------------------|-----------------------------|
| `connectrpc.com/connect`       | ConnectRPC framework        |
| `connectrpc.com/grpchealth`    | gRPC health checking        |
| `github.com/aws/aws-sdk-go-v2` | S3 client                   |
| `github.com/go-git/go-git/v6`  | Pure-Go git (git backend)   |
| `google.golang.org/protobuf`   | Protobuf runtime            |
| `go.opentelemetry.io/otel`     | Traces + metrics (OTLP/HTTP)|
| `connectrpc.com/otelconnect`   | RPC spans + `rpc.server.*`  |
