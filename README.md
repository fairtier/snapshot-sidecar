# snapshot-sidecar

A generic Kubernetes sidecar for persisting project directories, with two
interchangeable backends selected via `SNAPSHOT_BACKEND`:

- **`s3`** (default) — periodically (or on-demand) creates deduplicated tar.gz
  snapshots and uploads them to any S3-compatible backend (AWS S3, Cloudflare
  R2, MinIO, etc.). On startup, `restore` downloads the latest snapshot —
  giving stateless pods persistent project state without persistent volumes.
- **`git`** — the project directory is a working clone of a git remote (e.g. an
  in-cluster Gitea repo). Save = commit + plain push, sync = fast-forward pull.
  See [Git backend](#git-backend).

## How it works

```
┌─────────────────────────────────────────┐
│ Kubernetes Pod                          │
│                                         │
│  ┌──────────────┐  ┌─────────────────┐  │
│  │ Your App     │  │ snapshot-sidecar│  │
│  │              │◄─┤ (ConnectRPC)    │  │
│  │ /project     │  │ :8484           │  │
│  └──────┬───────┘  └────────┬────────┘  │
│         │ shared volume     │           │
│         └───────────────────┘           │
└─────────────────────────────────────────┘
                    │
                    │ S3 API
                    ▼
         S3-compatible storage
         (AWS, R2, MinIO, ...)
```

### Init container → sidecar pattern

1. **Init container** runs `snapshot-sidecar restore` — downloads and extracts
   the latest snapshot into the shared volume. Exits 0 even if no snapshot
   exists (first boot).
2. **Sidecar** runs `snapshot-sidecar serve` — exposes
   a [ConnectRPC](https://connectrpc.com/) service for on-demand snapshots, plus
   optional periodic auto-save and SIGTERM-triggered final save.

### Deduplication

Each snapshot is SHA-256 hashed. If the content hasn't changed since the last
upload, the upload is skipped. This makes frequent auto-save intervals cheap.

## Getting started

### Prerequisites

- Go 1.26+
- S3-compatible storage with a bucket

### Build

```bash
go build -o snapshot-sidecar .
```

### Run

```bash
# Restore latest snapshot (or start fresh if none exists)
S3_BUCKET=my-bucket snapshot-sidecar restore

# Run the sidecar server
S3_BUCKET=my-bucket snapshot-sidecar serve
```

### Docker

```bash
docker build -t snapshot-sidecar .
```

Pre-built images are available at `ghcr.io/fairtier/snapshot-sidecar`.

## Configuration

All configuration is via environment variables:

| Variable               | Default        | Description                          |
|------------------------|----------------|--------------------------------------|
| `SNAPSHOT_BACKEND`     | `s3`           | Persistence backend: `s3` or `git`   |
| `SNAPSHOT_PROJECT_DIR` | `/project`     | Directory to snapshot                |
| `AUTOSAVE_INTERVAL`    | `0` (disabled) | Auto-save interval (e.g. `5m`, `1h`) |
| `LISTEN_ADDR`          | `:8484`        | ConnectRPC server listen address     |

**s3 backend:**

| Variable                | Default      | Description                                               |
|-------------------------|--------------|-----------------------------------------------------------|
| `S3_BUCKET`             | *(required)* | S3 bucket name                                            |
| `SNAPSHOT_PREFIX`       | `snapshots`  | S3 key prefix for snapshots                               |
| `SNAPSHOT_EXCLUDE_DIRS` | *(empty)*    | Comma-separated directories to exclude (e.g. `tmp,stage`) |

S3 credentials are read via the standard AWS SDK environment variables (
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, `AWS_REGION`).

**git backend:**

| Variable           | Default                         | Description                                        |
|--------------------|---------------------------------|----------------------------------------------------|
| `GIT_REMOTE_URL`   | *(required)*                    | HTTP(S) remote URL                                 |
| `GIT_TOKEN`        | *(required)*                    | Access token (HTTP basic auth password)            |
| `GIT_USERNAME`     | `git`                           | HTTP basic auth username                           |
| `GIT_BRANCH`       | `main`                          | Branch to track                                    |
| `GIT_AUTHOR_NAME`  | `FairTier Autosave`             | Commit author name                                 |
| `GIT_AUTHOR_EMAIL` | `snapshot-sidecar@fairtier.com` | Commit author email                                |
| `SYNC_INTERVAL`    | `0` (disabled)                  | Fast-forward pull interval (e.g. `1m`)             |

The token is passed in-process per request (pure-Go git via
[go-git](https://github.com/go-git/go-git)) — it is never written to
`.git/config` or disk. Ignore rules come from the repo's own `.gitignore`.

## API

The sidecar exposes a ConnectRPC service (compatible with gRPC, gRPC-Web, and
Connect protocols):

| RPC               | Description                                                                                 |
|-------------------|---------------------------------------------------------------------------------------------|
| `TriggerSnapshot` | Create a snapshot immediately. Returns `created`, `unchanged`, `busy`, or `remote_changed`. |
| `ListSnapshots`   | List snapshots: S3 objects under the prefix, or git commits (newest first, capped at 50).   |

gRPC health checking is available via the standard `grpc.health.v1.Health`
service.

## Git backend

The project directory is a working clone; "snapshot" maps onto git verbs.
The policy is **optimistic concurrency: never force, never auto-merge** — a
plain push being rejected *is* the "somebody else changed the data" signal.

- **restore** (init container) — open the existing clone, or *adopt* a plain
  directory: init, fetch, point the branch at the remote head, and materialize
  only files missing on disk. Existing local files are never overwritten —
  they become uncommitted modifications for the next save. On an empty remote,
  the directory content is committed and pushed as the initial import.
- **save** (`TriggerSnapshot` / `AUTOSAVE_INTERVAL` / SIGTERM) — commit the
  dirty working tree (`.gitignore`-aware), then plain push. A non-fast-forward
  rejection returns status `remote_changed`; the commit stays local and a later
  save retries the push once the remote is fast-forwardable again.
- **sync** (`SYNC_INTERVAL` loop) — fetch, then fast-forward pull, but ONLY
  when the tree is clean and no local commits are unpushed. Diverged or dirty
  states are left alone and reported.

`GET /debug/sync-status` (same port) returns the current relation to the
remote as JSON:

```json
{"state": "in-sync", "head": "…", "remoteHead": "…", "lastFetch": "…"}
```

`state` is one of `in-sync`, `dirty` (uncommitted local changes), `ahead`
(unpushed local commits), `behind` (remote commits not yet pulled), or
`diverged` (both — needs a human: make the remote fast-forwardable, or
rebase/merge the clone manually, then save again).

### Example with `buf curl`

```bash
buf curl --protocol connect \
  http://localhost:8484/snapshot.v1.SnapshotService/TriggerSnapshot
```

## Kubernetes example

```yaml
initContainers:
  - name: restore
    image: ghcr.io/fairtier/snapshot-sidecar:latest
    args: [ "restore" ]
    env:
      - name: S3_BUCKET
        value: my-bucket
      - name: SNAPSHOT_PROJECT_DIR
        value: /project
    envFrom:
      - secretRef:
          name: s3-credentials
    volumeMounts:
      - name: project
        mountPath: /project

containers:
  - name: app
    image: my-app:latest
    volumeMounts:
      - name: project
        mountPath: /project

  - name: snapshot
    image: ghcr.io/fairtier/snapshot-sidecar:latest
    args: [ "serve" ]
    env:
      - name: S3_BUCKET
        value: my-bucket
      - name: SNAPSHOT_PROJECT_DIR
        value: /project
      - name: AUTOSAVE_INTERVAL
        value: "5m"
    envFrom:
      - secretRef:
          name: s3-credentials
    volumeMounts:
      - name: project
        mountPath: /project
    ports:
      - containerPort: 8484
        name: grpc

volumes:
  - name: project
    emptyDir: { }
```

## Snapshot format

- Snapshots are stored as `{prefix}/{timestamp}-{hash}.tar.gz`
- A `{prefix}/latest.json` marker tracks the current snapshot key and hash
- Snapshots are buffered entirely in memory before upload. Ensure the sidecar
  container has sufficient memory for your project directory size.
- Symlinks are not preserved; only regular files and directories are included.
- Directories listed in `SNAPSHOT_EXCLUDE_DIRS` are excluded from snapshots

## Proto

The service is defined in [`proto/snapshot.proto`](proto/snapshot.proto). To
regenerate Go stubs:

```bash
buf generate
```

Requires: [buf](https://buf.build/), `protoc-gen-go`, `protoc-gen-connect-go`

## License

Apache 2.0 — see [LICENSE](LICENSE)
