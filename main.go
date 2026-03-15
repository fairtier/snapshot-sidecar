// snapshot-sidecar is a generic sidecar binary for snapshotting project directories to S3.
//
// Subcommands:
//   - serve: Run ConnectRPC sidecar (SnapshotService + gRPC health)
//     with SIGTERM-triggered save and optional periodic auto-save.
//   - restore: Download the latest snapshot from S3 and extract it.
//     Exits 0 even if no snapshot exists (first boot).
package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpchealth"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	snapshotv1 "github.com/fairtier/snapshot-sidecar/proto/snapshot/v1"
	"github.com/fairtier/snapshot-sidecar/proto/snapshot/v1/snapshotv1connect"
)

func shortHash(h string) string {
	if len(h) < 12 {
		return h
	}
	return h[:12]
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: snapshot-sidecar <serve|restore>\n")
		os.Exit(1)
	}

	logger := slog.Default()

	var err error
	switch os.Args[1] {
	case "serve":
		err = runServe(logger)
	case "restore":
		err = runRestore(logger)
	case "version":
		printVersion()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n", os.Args[1])
		os.Exit(1)
	}
	if err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func printVersion() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		fmt.Println("snapshot-sidecar (unknown)")
		return
	}

	version := info.Main.Version
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			version += " " + shortHash(s.Value)
		case "vcs.time":
			version += " " + s.Value
		case "vcs.modified":
			if s.Value == "true" {
				version += " (dirty)"
			}
		}
	}

	fmt.Println("snapshot-sidecar", version)
}

// envConfig reads configuration from environment variables.
type envConfig struct {
	ProjectDir       string        // SNAPSHOT_PROJECT_DIR (default: /project)
	S3Bucket         string        // S3_BUCKET (required)
	SnapshotPrefix   string        // SNAPSHOT_PREFIX (default: snapshots)
	AutosaveInterval time.Duration // AUTOSAVE_INTERVAL (default: 0 = disabled)
	ListenAddr       string        // LISTEN_ADDR (default: :8484)
	ExcludeDirs      []string      // SNAPSHOT_EXCLUDE_DIRS (comma-separated, default: empty)
}

func loadConfig() (envConfig, error) {
	c := envConfig{
		ProjectDir:     envOr("SNAPSHOT_PROJECT_DIR", "/project"),
		S3Bucket:       os.Getenv("S3_BUCKET"),
		SnapshotPrefix: envOr("SNAPSHOT_PREFIX", "snapshots"),
		ListenAddr:     envOr("LISTEN_ADDR", ":8484"),
	}

	if c.S3Bucket == "" {
		return c, fmt.Errorf("S3_BUCKET environment variable is required")
	}

	if v := os.Getenv("SNAPSHOT_EXCLUDE_DIRS"); v != "" {
		for _, d := range strings.Split(v, ",") {
			d = strings.TrimSpace(d)
			if d != "" {
				c.ExcludeDirs = append(c.ExcludeDirs, d)
			}
		}
	}

	if v := os.Getenv("AUTOSAVE_INTERVAL"); v != "" && v != "0" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("invalid AUTOSAVE_INTERVAL %q: %w", v, err)
		}
		c.AutosaveInterval = d
	}

	return c, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// latestMarker is stored as latest.json in S3 to track the current snapshot.
type latestMarker struct {
	Key  string `json:"key"`
	Hash string `json:"hash"`
}

// ── serve ────────────────────────────────────────────────────────────────────

func runServe(logger *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	s3Client, err := newS3Client(context.Background())
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}

	snap := &snapshotter{
		logger: logger,
		s3:     s3Client,
		cfg:    cfg,
	}

	// Load the current latest hash so we can deduplicate.
	if latest, err := snap.getLatestMarker(context.Background()); err != nil {
		logger.Warn("no existing snapshot marker", "err", err)
	} else {
		snap.lastHash = latest.Hash
		logger.Info("loaded latest snapshot hash", "hash", shortHash(latest.Hash))
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	mux := http.NewServeMux()
	mux.Handle(snapshotv1connect.NewSnapshotServiceHandler(snap))
	mux.Handle(grpchealth.NewHandler(grpchealth.NewStaticChecker(
		snapshotv1connect.SnapshotServiceName,
	)))

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           h2c.NewHandler(mux, &http2.Server{}),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("sidecar listening", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	// Optional periodic auto-save.
	var wg sync.WaitGroup
	if cfg.AutosaveInterval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			snap.autosaveLoop(ctx, cfg.AutosaveInterval)
		}()
	}

	// Wait for signal or error.
	select {
	case <-ctx.Done():
		logger.Info("received shutdown signal, creating final snapshot...")
	case err := <-errCh:
		return fmt.Errorf("server error: %w", err)
	}

	// Wait for autosave loop to finish before creating the final snapshot.
	wg.Wait()

	// SIGTERM snapshot — use a fresh context with a deadline.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer shutdownCancel()

	if _, err := snap.createSnapshot(shutdownCtx); err != nil {
		logger.Error("shutdown snapshot failed", "err", err)
	} else {
		logger.Info("shutdown snapshot complete")
	}

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("server shutdown: %w", err)
	}

	logger.Info("sidecar stopped")
	return nil
}

var _ snapshotv1connect.SnapshotServiceHandler = (*snapshotter)(nil)

type snapshotter struct {
	logger   *slog.Logger
	s3       *s3.Client
	cfg      envConfig
	lastHash string
	mu       sync.Mutex // protects lastHash
	busyMu   sync.Mutex // prevents concurrent snapshots
}

func (s *snapshotter) isUnchanged(hash string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastHash == hash
}

func (s *snapshotter) setLastHash(hash string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastHash = hash
}

// TriggerSnapshot implements snapshotv1connect.SnapshotServiceHandler.
func (s *snapshotter) TriggerSnapshot(
	ctx context.Context,
	_ *connect.Request[snapshotv1.TriggerSnapshotRequest],
) (*connect.Response[snapshotv1.TriggerSnapshotResponse], error) {
	result, err := s.createSnapshot(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(result), nil
}

// ListSnapshots implements snapshotv1connect.SnapshotServiceHandler.
func (s *snapshotter) ListSnapshots(
	ctx context.Context,
	_ *connect.Request[snapshotv1.ListSnapshotsRequest],
) (*connect.Response[snapshotv1.ListSnapshotsResponse], error) {
	paginator := s3.NewListObjectsV2Paginator(s.s3, &s3.ListObjectsV2Input{
		Bucket: &s.cfg.S3Bucket,
		Prefix: new(s.cfg.SnapshotPrefix + "/"),
	})

	var snapshots []*snapshotv1.Snapshot
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("list objects: %w", err))
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			if !strings.HasSuffix(key, ".tar.gz") {
				continue
			}
			// Parse hash from key: {prefix}/{timestamp}-{hash}.tar.gz
			base := filepath.Base(key)
			base = strings.TrimSuffix(base, ".tar.gz")
			parts := strings.SplitN(base, "-", 2)
			var hash string
			if len(parts) == 2 {
				hash = parts[1]
			}
			snapshots = append(snapshots, &snapshotv1.Snapshot{
				Key:       key,
				Timestamp: aws.ToTime(obj.LastModified).Format(time.RFC3339),
				Hash:      hash,
			})
		}
	}

	return connect.NewResponse(&snapshotv1.ListSnapshotsResponse{
		Snapshots: snapshots,
	}), nil
}

func (s *snapshotter) createSnapshot(ctx context.Context) (*snapshotv1.TriggerSnapshotResponse, error) {
	// Prevent concurrent snapshots.
	if !s.busyMu.TryLock() {
		return &snapshotv1.TriggerSnapshotResponse{Status: "busy"}, nil
	}
	defer s.busyMu.Unlock()

	// Create tar.gz in memory and compute hash.
	buf, hash, err := s.tarProjectDir()
	if err != nil {
		return nil, fmt.Errorf("create archive: %w", err)
	}

	// Deduplicate.
	if s.isUnchanged(hash) {
		s.logger.Info("snapshot unchanged, skipping upload", "hash", shortHash(hash))
		return &snapshotv1.TriggerSnapshotResponse{Status: "unchanged", Hash: hash}, nil
	}

	// Upload snapshot.
	now := time.Now().UTC()
	key := fmt.Sprintf("%s/%s-%s.tar.gz",
		s.cfg.SnapshotPrefix,
		now.Format("20060102T150405Z"),
		shortHash(hash),
	)

	if err := s.uploadBytes(ctx, key, buf); err != nil {
		return nil, fmt.Errorf("upload snapshot: %w", err)
	}

	// Update latest.json marker.
	marker := latestMarker{Key: key, Hash: hash}
	markerJSON, err := json.Marshal(marker)
	if err != nil {
		return nil, fmt.Errorf("marshal latest marker: %w", err)
	}
	markerKey := s.cfg.SnapshotPrefix + "/latest.json"
	if err := s.uploadBytes(ctx, markerKey, markerJSON); err != nil {
		return nil, fmt.Errorf("upload latest marker: %w", err)
	}

	s.setLastHash(hash)

	s.logger.Info("snapshot created", "key", key, "hash", shortHash(hash))
	return &snapshotv1.TriggerSnapshotResponse{Status: "created", Key: key, Hash: hash}, nil
}

func (s *snapshotter) tarProjectDir() ([]byte, string, error) {
	var buf bytes.Buffer
	h := sha256.New()

	// Write tar.gz to both buf and hasher.
	mw := io.MultiWriter(&buf, h)
	gw := gzip.NewWriter(mw)
	tw := tar.NewWriter(gw)

	err := filepath.WalkDir(s.cfg.ProjectDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(s.cfg.ProjectDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}

		if d.IsDir() {
			for _, excl := range s.cfg.ExcludeDirs {
				if rel == excl {
					return filepath.SkipDir
				}
			}
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = rel

		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()

		_, err = io.Copy(tw, f)
		return err
	})
	if err != nil {
		return nil, "", err
	}

	if err := tw.Close(); err != nil {
		return nil, "", err
	}
	if err := gw.Close(); err != nil {
		return nil, "", err
	}

	return buf.Bytes(), hex.EncodeToString(h.Sum(nil)), nil
}

func (s *snapshotter) uploadBytes(ctx context.Context, key string, data []byte) error {
	_, err := s.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.cfg.S3Bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	return err
}

func (s *snapshotter) getLatestMarker(ctx context.Context) (*latestMarker, error) {
	resp, err := s.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.cfg.S3Bucket,
		Key:    new(s.cfg.SnapshotPrefix + "/latest.json"),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	var m latestMarker
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (s *snapshotter) autosaveLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := s.createSnapshot(ctx); err != nil {
				s.logger.Error("autosave failed", "err", err)
			}
		}
	}
}

// ── restore ──────────────────────────────────────────────────────────────────

func runRestore(logger *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	s3Client, err := newS3Client(ctx)
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}

	// Read latest.json marker.
	markerKey := cfg.SnapshotPrefix + "/latest.json"
	markerResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &cfg.S3Bucket,
		Key:    &markerKey,
	})
	if err != nil {
		if errors.As(err, new(*types.NoSuchKey)) || errors.As(err, new(*types.NotFound)) {
			logger.Info("no snapshot found, starting fresh")
			return nil
		}
		return fmt.Errorf("fetch latest.json: %w", err)
	}
	defer func() { _ = markerResp.Body.Close() }()

	var marker latestMarker
	if err := json.NewDecoder(markerResp.Body).Decode(&marker); err != nil {
		return fmt.Errorf("decode latest.json: %w", err)
	}

	logger.Info("restoring snapshot", "key", marker.Key)

	// Download the snapshot tar.gz.
	snapResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &cfg.S3Bucket,
		Key:    &marker.Key,
	})
	if err != nil {
		return fmt.Errorf("download snapshot: %w", err)
	}
	defer func() { _ = snapResp.Body.Close() }()

	// Extract to project directory.
	if err := extractTarGz(snapResp.Body, cfg.ProjectDir); err != nil {
		return fmt.Errorf("extract snapshot: %w", err)
	}

	logger.Info("snapshot restored", "key", marker.Key)
	return nil
}

func extractTarGz(r io.Reader, destDir string) error {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer func() { _ = gr.Close() }()

	tr := tar.NewReader(gr)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		target := filepath.Join(destDir, header.Name)

		// Prevent path traversal.
		cleanDest := filepath.Clean(destDir) + string(os.PathSeparator)
		if !strings.HasPrefix(filepath.Clean(target), cleanDest) {
			return fmt.Errorf("invalid path in archive: %s", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if header.Size > 1<<32 {
				return fmt.Errorf("file too large: %s (%d bytes)", header.Name, header.Size)
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode)&0o777)
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, io.LimitReader(tr, 1<<32)); err != nil {
				_ = f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

// ── S3 client ────────────────────────────────────────────────────────────────

func newS3Client(ctx context.Context) (*s3.Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		// R2 and other S3-compatible storage requires path-style access.
		o.UsePathStyle = true
	}), nil
}
