// snapshot-sidecar is a generic sidecar binary for persisting project
// directories, with two interchangeable backends:
//
//   - s3 (default): tar.gz snapshots to S3-compatible storage with SHA-256
//     deduplication and a latest.json marker.
//   - git: the project directory is a working clone of a remote; save =
//     commit + plain push, sync = fast-forward pull. Never force, never
//     auto-merge — see git.go.
//
// Subcommands:
//   - serve: Run ConnectRPC sidecar (SnapshotService + gRPC health)
//     with SIGTERM-triggered save and optional periodic auto-save.
//   - restore: Populate the project directory (download latest S3 snapshot,
//     or clone/adopt the git remote). Exits 0 on first boot with nothing to
//     restore.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"connectrpc.com/grpchealth"

	snapshotv1 "github.com/fairtier/snapshot-sidecar/proto/snapshot/v1"
	"github.com/fairtier/snapshot-sidecar/proto/snapshot/v1/snapshotv1connect"
)

const (
	backendS3  = "s3"
	backendGit = "git"
)

// backend is what serve/restore run against — the RPC surface plus the save
// primitive shared by the autosave loop and the SIGTERM handler.
type backend interface {
	snapshotv1connect.SnapshotServiceHandler
	save(ctx context.Context) (*snapshotv1.TriggerSnapshotResponse, error)
}

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
	Backend          string        // SNAPSHOT_BACKEND (s3|git, default: s3)
	ProjectDir       string        // SNAPSHOT_PROJECT_DIR (default: /project)
	AutosaveInterval time.Duration // AUTOSAVE_INTERVAL (default: 0 = disabled)
	ListenAddr       string        // LISTEN_ADDR (default: :8484)

	// s3 backend
	S3Bucket       string   // S3_BUCKET (required for s3)
	SnapshotPrefix string   // SNAPSHOT_PREFIX (default: snapshots)
	ExcludeDirs    []string // SNAPSHOT_EXCLUDE_DIRS (comma-separated, default: empty)

	// git backend
	GitRemoteURL   string        // GIT_REMOTE_URL (required for git)
	GitBranch      string        // GIT_BRANCH (default: main)
	GitUsername    string        // GIT_USERNAME (default: git — token is what matters)
	GitToken       string        // GIT_TOKEN (required for git)
	GitAuthorName  string        // GIT_AUTHOR_NAME (default: FairTier Autosave)
	GitAuthorEmail string        // GIT_AUTHOR_EMAIL (default: snapshot-sidecar@fairtier.com)
	SyncInterval   time.Duration // SYNC_INTERVAL (git only, default: 0 = disabled)
}

func loadConfig() (envConfig, error) {
	c := envConfig{
		Backend:        envOr("SNAPSHOT_BACKEND", backendS3),
		ProjectDir:     envOr("SNAPSHOT_PROJECT_DIR", "/project"),
		S3Bucket:       os.Getenv("S3_BUCKET"),
		SnapshotPrefix: envOr("SNAPSHOT_PREFIX", "snapshots"),
		ListenAddr:     envOr("LISTEN_ADDR", ":8484"),
		GitRemoteURL:   os.Getenv("GIT_REMOTE_URL"),
		GitBranch:      envOr("GIT_BRANCH", "main"),
		GitUsername:    os.Getenv("GIT_USERNAME"),
		GitToken:       os.Getenv("GIT_TOKEN"),
		GitAuthorName:  envOr("GIT_AUTHOR_NAME", "FairTier Autosave"),
		GitAuthorEmail: envOr("GIT_AUTHOR_EMAIL", "snapshot-sidecar@fairtier.com"),
	}

	switch c.Backend {
	case backendS3:
		if c.S3Bucket == "" {
			return c, fmt.Errorf("S3_BUCKET environment variable is required")
		}
	case backendGit:
		if c.GitRemoteURL == "" {
			return c, fmt.Errorf("GIT_REMOTE_URL environment variable is required")
		}
		if c.GitToken == "" {
			return c, fmt.Errorf("GIT_TOKEN environment variable is required")
		}
	default:
		return c, fmt.Errorf("invalid SNAPSHOT_BACKEND %q (want s3 or git)", c.Backend)
	}

	if v := os.Getenv("SNAPSHOT_EXCLUDE_DIRS"); v != "" {
		for _, d := range strings.Split(v, ",") {
			d = strings.TrimSpace(d)
			if d != "" {
				c.ExcludeDirs = append(c.ExcludeDirs, d)
			}
		}
	}

	var err error
	if c.AutosaveInterval, err = durationEnv("AUTOSAVE_INTERVAL"); err != nil {
		return c, err
	}
	if c.SyncInterval, err = durationEnv("SYNC_INTERVAL"); err != nil {
		return c, err
	}

	return c, nil
}

func durationEnv(key string) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" || v == "0" {
		return 0, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", key, v, err)
	}
	return d, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func newBackend(ctx context.Context, logger *slog.Logger, cfg envConfig) (backend, error) {
	switch cfg.Backend {
	case backendGit:
		return newGitBackend(ctx, logger, cfg)
	default:
		return newSnapshotter(ctx, logger, cfg)
	}
}

// ── serve ────────────────────────────────────────────────────────────────────

func runServe(logger *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	b, err := newBackend(ctx, logger, cfg)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle(snapshotv1connect.NewSnapshotServiceHandler(b))
	mux.Handle(grpchealth.NewHandler(grpchealth.NewStaticChecker(
		snapshotv1connect.SnapshotServiceName,
	)))
	if d, ok := b.(interface{ registerDebug(*http.ServeMux) }); ok {
		d.registerDebug(mux)
	}

	// h2c: gRPC/Connect clients speak HTTP/2 without TLS in-cluster.
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		Protocols:         &protocols,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("sidecar listening", "addr", cfg.ListenAddr, "backend", cfg.Backend)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	var wg sync.WaitGroup

	// Optional periodic auto-save.
	if cfg.AutosaveInterval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			autosaveLoop(ctx, logger, b, cfg.AutosaveInterval)
		}()
	}

	// Optional periodic remote sync (git backend only).
	if s, ok := b.(interface {
		syncLoop(context.Context, time.Duration)
	}); ok && cfg.SyncInterval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.syncLoop(ctx, cfg.SyncInterval)
		}()
	}

	// Wait for signal or error.
	select {
	case <-ctx.Done():
		logger.Info("received shutdown signal, creating final snapshot...")
	case err := <-errCh:
		return fmt.Errorf("server error: %w", err)
	}

	// Wait for background loops to finish before creating the final snapshot.
	wg.Wait()

	// SIGTERM snapshot — use a fresh context with a deadline.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer shutdownCancel()

	if resp, err := b.save(shutdownCtx); err != nil {
		logger.Error("shutdown snapshot failed", "err", err)
	} else {
		logger.Info("shutdown snapshot complete", "status", resp.GetStatus())
	}

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("server shutdown: %w", err)
	}

	logger.Info("sidecar stopped")
	return nil
}

func autosaveLoop(ctx context.Context, logger *slog.Logger, b backend, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if resp, err := b.save(ctx); err != nil {
				logger.Error("autosave failed", "err", err)
			} else if resp.GetStatus() == "created" {
				logger.Info("autosave", "key", resp.GetKey())
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

	switch cfg.Backend {
	case backendGit:
		return restoreGit(ctx, logger, cfg)
	default:
		return restoreS3(ctx, logger, cfg)
	}
}
