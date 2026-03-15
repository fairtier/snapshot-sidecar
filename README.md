# snapshot-sidecar

A generic Kubernetes sidecar for snapshotting project directories to
S3-compatible storage. It runs alongside your application pod, periodically (or
on-demand) creates deduplicated tar.gz snapshots, and uploads them to any
S3-compatible backend (AWS S3, Cloudflare R2, MinIO, etc.).

On startup, the `restore` command downloads the latest snapshot — giving
stateless pods persistent project state without requiring persistent volumes.

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

| Variable                | Default        | Description                                               |
|-------------------------|----------------|-----------------------------------------------------------|
| `S3_BUCKET`             | *(required)*   | S3 bucket name                                            |
| `SNAPSHOT_PROJECT_DIR`  | `/project`     | Directory to snapshot                                     |
| `SNAPSHOT_PREFIX`       | `snapshots`    | S3 key prefix for snapshots                               |
| `AUTOSAVE_INTERVAL`     | `0` (disabled) | Auto-save interval (e.g. `5m`, `1h`)                      |
| `LISTEN_ADDR`           | `:8484`        | ConnectRPC server listen address                          |
| `SNAPSHOT_EXCLUDE_DIRS` | *(empty)*      | Comma-separated directories to exclude (e.g. `tmp,stage`) |

S3 credentials are read via the standard AWS SDK environment variables (
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, `AWS_REGION`).

## API

The sidecar exposes a ConnectRPC service (compatible with gRPC, gRPC-Web, and
Connect protocols):

| RPC               | Description                                                               |
|-------------------|---------------------------------------------------------------------------|
| `TriggerSnapshot` | Create a snapshot immediately. Returns `created`, `unchanged`, or `busy`. |
| `ListSnapshots`   | List all snapshots in the S3 prefix.                                      |

gRPC health checking is available via the standard `grpc.health.v1.Health`
service.

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
