package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	git "github.com/go-git/go-git/v6"
	gitconfig "github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	gitclient "github.com/go-git/go-git/v6/plumbing/client"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/transport"
	githttp "github.com/go-git/go-git/v6/plumbing/transport/http"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	snapshotv1 "github.com/fairtier/snapshot-sidecar/proto/snapshot/v1"
)

// The git backend treats the project directory as a working clone of a
// remote (e.g. a per-box Gitea repo). "Snapshot" maps onto git verbs:
//
//   - save (TriggerSnapshot / autosave / SIGTERM): commit the dirty working
//     tree, then a PLAIN push. A non-fast-forward rejection is surfaced as
//     status "remote_changed" — the commit stays local, nothing is lost.
//   - sync (SYNC_INTERVAL loop): fetch, then fast-forward pull — but ONLY
//     when the tree is clean and no local commits are unpushed.
//
// Never force, never auto-merge: divergence is reported (TriggerSnapshot
// status + GET /debug/sync-status), resolution is an explicit human action
// (make the remote fast-forwardable, then save again).
//
// defaultRemote is the only remote the backend ever talks to.
const defaultRemote = "origin"

// defaultGitignore seeds the ignore list when the backend has to create the
// very first commit itself (empty remote — normally the repo seeder has
// already pushed a .gitignore, see the fairtier.com rill seed Job).
const defaultGitignore = ".env\ntmp/\n*.db\n*.db.*\n..*\n"

// listSnapshotsLimit caps how many commits ListSnapshots walks.
const listSnapshotsLimit = 50

var _ backend = (*gitBackend)(nil)

type gitBackend struct {
	logger *slog.Logger
	cfg    envConfig
	repo   *git.Repository
	wt     *git.Worktree
	// clientOpts carry HTTP basic auth (token) in-process for every remote
	// operation — the credential never lands in .git/config on the volume.
	clientOpts []gitclient.Option

	busyMu sync.Mutex // prevents concurrent saves/syncs

	stateMu sync.Mutex
	state   gitSyncState
}

// gitSyncState is what GET /debug/sync-status returns.
type gitSyncState struct {
	// State: in-sync | dirty | ahead | behind | diverged
	State      string `json:"state"`
	Head       string `json:"head,omitempty"`
	RemoteHead string `json:"remoteHead,omitempty"`
	LastError  string `json:"lastError,omitempty"`
	LastFetch  string `json:"lastFetch,omitempty"`
	LastSave   string `json:"lastSave,omitempty"`
	LastPull   string `json:"lastPull,omitempty"`
}

func newGitBackend(ctx context.Context, logger *slog.Logger, cfg envConfig) (*gitBackend, error) {
	username := cfg.GitUsername
	if username == "" {
		// Gitea/GitHub accept any non-empty username with a token password.
		username = "git"
	}
	b := &gitBackend{
		logger: logger,
		cfg:    cfg,
		clientOpts: []gitclient.Option{
			gitclient.WithHTTPAuth(&githttp.BasicAuth{
				Username: username,
				Password: cfg.GitToken,
			}),
		},
	}

	repo, err := git.PlainOpen(cfg.ProjectDir)
	switch {
	case err == nil:
		if err := b.ensureRemote(repo); err != nil {
			return nil, err
		}
	case errors.Is(err, git.ErrRepositoryNotExists):
		repo, err = b.adopt(ctx)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("open repository: %w", err)
	}

	wt, err := repo.Worktree()
	if err != nil {
		return nil, fmt.Errorf("worktree: %w", err)
	}

	b.repo, b.wt = repo, wt

	// snapshot.git.state is sampled from the live classifier. classify() is
	// local-only (refs + worktree status), so collection stays cheap; skip it
	// while a save/sync holds the lock rather than blocking the collector.
	if err := tel.observeGitState(func() string {
		if !b.busyMu.TryLock() {
			b.stateMu.Lock()
			defer b.stateMu.Unlock()
			return b.state.State
		}
		defer b.busyMu.Unlock()
		return b.refreshState()
	}); err != nil {
		return nil, fmt.Errorf("register git state gauge: %w", err)
	}

	return b, nil
}

// ensureRemote forces origin's URL to GIT_REMOTE_URL so day-2 URL changes
// converge on the next boot.
func (b *gitBackend) ensureRemote(repo *git.Repository) error {
	cfg, err := repo.Config()
	if err != nil {
		return fmt.Errorf("read repo config: %w", err)
	}
	remote, ok := cfg.Remotes[defaultRemote]
	if ok && len(remote.URLs) == 1 && remote.URLs[0] == b.cfg.GitRemoteURL {
		return nil
	}
	if !ok {
		cfg.Remotes[defaultRemote] = &gitconfig.RemoteConfig{Name: defaultRemote}
	}
	cfg.Remotes[defaultRemote].URLs = []string{b.cfg.GitRemoteURL}
	if err := repo.SetConfig(cfg); err != nil {
		return fmt.Errorf("set remote url: %w", err)
	}
	b.logger.Info("remote url updated", "url", b.cfg.GitRemoteURL)
	return nil
}

// adopt turns a plain (possibly non-empty) project directory into a clone of
// the remote WITHOUT ever discarding a local file:
//
//   - remote branch exists: point HEAD/branch/index at the remote commit and
//     materialize only the tree files MISSING on disk. Files that already
//     exist locally with different content become uncommitted modifications
//     for the next save — adoption itself never creates divergence.
//   - remote empty: commit whatever is present (plus a default .gitignore)
//     and push the initial import.
func (b *gitBackend) adopt(ctx context.Context) (_ *git.Repository, err error) {
	ctx, span := tel.tracer.Start(ctx, "git.adopt", trace.WithAttributes(
		attribute.String("git.branch", b.cfg.GitBranch),
	))
	defer func() {
		recordErr(span, err)
		span.End()
	}()

	branchRef := plumbing.NewBranchReferenceName(b.cfg.GitBranch)

	repo, err := git.PlainInit(b.cfg.ProjectDir, false, git.WithDefaultBranch(branchRef))
	if err != nil {
		return nil, fmt.Errorf("init repository: %w", err)
	}
	if _, err := repo.CreateRemote(&gitconfig.RemoteConfig{
		Name: defaultRemote,
		URLs: []string{b.cfg.GitRemoteURL},
	}); err != nil {
		return nil, fmt.Errorf("create remote: %w", err)
	}
	// Branch tracking config so plain pulls/pushes resolve.
	if err := repo.CreateBranch(&gitconfig.Branch{
		Name:   b.cfg.GitBranch,
		Remote: defaultRemote,
		Merge:  branchRef,
	}); err != nil && !errors.Is(err, git.ErrBranchExists) {
		return nil, fmt.Errorf("create branch config: %w", err)
	}

	err = b.remoteOp(ctx, "git.fetch", func(ctx context.Context) error {
		return repo.FetchContext(ctx, &git.FetchOptions{RemoteName: defaultRemote, ClientOptions: b.clientOpts})
	})
	switch {
	case errors.Is(err, transport.ErrEmptyRemoteRepository):
		span.SetAttributes(attribute.String("git.adopt.mode", "empty-remote"))
		return repo, b.adoptEmptyRemote(ctx, repo)
	case err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate):
		return nil, fmt.Errorf("fetch: %w", err)
	}

	remoteRef, err := repo.Reference(plumbing.NewRemoteReferenceName(defaultRemote, b.cfg.GitBranch), true)
	if errors.Is(err, plumbing.ErrReferenceNotFound) {
		// Remote has refs but not our branch — same as an empty remote from
		// this branch's point of view.
		span.SetAttributes(attribute.String("git.adopt.mode", "branch-missing"))
		return repo, b.adoptEmptyRemote(ctx, repo)
	}
	if err != nil {
		return nil, fmt.Errorf("resolve remote branch: %w", err)
	}

	commit, err := repo.CommitObject(remoteRef.Hash())
	if err != nil {
		return nil, fmt.Errorf("read remote commit: %w", err)
	}

	// Point the local branch (and thereby HEAD, set at init) at the remote
	// commit, and load its tree into the index. MixedReset leaves the
	// working tree alone — local files are never overwritten.
	if err := repo.Storer.SetReference(plumbing.NewHashReference(branchRef, remoteRef.Hash())); err != nil {
		return nil, fmt.Errorf("set branch ref: %w", err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		return nil, fmt.Errorf("worktree: %w", err)
	}
	if err := wt.Reset(&git.ResetOptions{Mode: git.MixedReset, Commit: remoteRef.Hash()}); err != nil {
		return nil, fmt.Errorf("reset index: %w", err)
	}

	written, err := b.materializeMissing(commit)
	if err != nil {
		return nil, err
	}

	span.SetAttributes(
		attribute.String("git.adopt.mode", "existing-remote"),
		attribute.Int("git.adopt.files_written", written),
		attrHash.String(remoteRef.Hash().String()),
	)
	b.logger.Info("adopted existing directory",
		"remote", remoteRef.Hash().String(),
		"filesWritten", written,
	)
	return repo, nil
}

// materializeMissing writes tree files that do not exist on disk yet.
// Existing files are left untouched (they show up as uncommitted
// modifications instead — see adopt).
func (b *gitBackend) materializeMissing(commit *object.Commit) (int, error) {
	tree, err := commit.Tree()
	if err != nil {
		return 0, fmt.Errorf("read tree: %w", err)
	}

	written := 0
	err = tree.Files().ForEach(func(f *object.File) error {
		target := filepath.Join(b.cfg.ProjectDir, filepath.FromSlash(f.Name))
		if _, err := os.Lstat(target); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}

		mode, err := f.Mode.ToOSFileMode()
		if err != nil {
			mode = 0o644
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		r, err := f.Reader()
		if err != nil {
			return err
		}
		defer func() { _ = r.Close() }()

		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode.Perm())
		if err != nil {
			return err
		}
		if _, err := out.ReadFrom(r); err != nil {
			_ = out.Close()
			return err
		}
		if err := out.Close(); err != nil {
			return err
		}
		written++
		return nil
	})
	if err != nil {
		return written, fmt.Errorf("materialize tree files: %w", err)
	}
	return written, nil
}

// adoptEmptyRemote creates the initial import commit from whatever is in the
// project directory and pushes it. Defensive path — the repo seeder normally
// pushes README/.gitignore before the sidecar ever runs.
func (b *gitBackend) adoptEmptyRemote(ctx context.Context, repo *git.Repository) error {
	wt, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("worktree: %w", err)
	}

	ignorePath := filepath.Join(b.cfg.ProjectDir, ".gitignore")
	if _, err := os.Lstat(ignorePath); errors.Is(err, os.ErrNotExist) {
		if err := os.WriteFile(ignorePath, []byte(defaultGitignore), 0o644); err != nil {
			return fmt.Errorf("write default .gitignore: %w", err)
		}
	}

	if err := wt.AddWithOptions(&git.AddOptions{All: true}); err != nil {
		return fmt.Errorf("add: %w", err)
	}
	_, err = wt.Commit("snapshot-sidecar: initial import", &git.CommitOptions{
		Author:            b.signature(),
		AllowEmptyCommits: true, // a bare .gitignore-only dir is a valid import
	})
	if err != nil {
		return fmt.Errorf("commit initial import: %w", err)
	}

	err = b.remoteOp(ctx, "git.push", func(ctx context.Context) error {
		return repo.PushContext(ctx, &git.PushOptions{RemoteName: defaultRemote, ClientOptions: b.clientOpts})
	})
	if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		// Lost a race against another writer — serve's save loop retries.
		trace.SpanFromContext(ctx).AddEvent("initial import push failed, save loop will retry")
		b.logger.Warn("initial import push failed", "err", err)
		return nil
	}
	b.logger.Info("initial import pushed")
	return nil
}

func (b *gitBackend) signature() *object.Signature {
	return &object.Signature{
		Name:  b.cfg.GitAuthorName,
		Email: b.cfg.GitAuthorEmail,
		When:  time.Now(),
	}
}

// ── serve: RPCs ──────────────────────────────────────────────────────────────

// TriggerSnapshot implements snapshotv1connect.SnapshotServiceHandler.
func (b *gitBackend) TriggerSnapshot(
	ctx context.Context,
	_ *connect.Request[snapshotv1.TriggerSnapshotRequest],
) (*connect.Response[snapshotv1.TriggerSnapshotResponse], error) {
	result, err := b.save(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(result), nil
}

// ListSnapshots implements snapshotv1connect.SnapshotServiceHandler. Commits
// map into the snapshot shape: key = "<branch>@<short sha>", hash = full
// sha, timestamp = committer time. Commits not on the remote yet are marked
// "(unpushed)".
func (b *gitBackend) ListSnapshots(
	_ context.Context,
	_ *connect.Request[snapshotv1.ListSnapshotsRequest],
) (*connect.Response[snapshotv1.ListSnapshotsResponse], error) {
	head, err := b.repo.Head()
	if errors.Is(err, plumbing.ErrReferenceNotFound) {
		return connect.NewResponse(&snapshotv1.ListSnapshotsResponse{}), nil
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var remoteHash plumbing.Hash
	if remoteRef, err := b.repo.Reference(
		plumbing.NewRemoteReferenceName(defaultRemote, b.cfg.GitBranch), true,
	); err == nil {
		remoteHash = remoteRef.Hash()
	}

	iter, err := b.repo.Log(&git.LogOptions{From: head.Hash()})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	defer iter.Close()

	var snapshots []*snapshotv1.Snapshot
	pushed := false
	err = iter.ForEach(func(c *object.Commit) error {
		if len(snapshots) >= listSnapshotsLimit {
			return errIterDone
		}
		if c.Hash == remoteHash {
			pushed = true
		}
		key := fmt.Sprintf("%s@%s", b.cfg.GitBranch, shortHash(c.Hash.String()))
		if !pushed {
			key += " (unpushed)"
		}
		snapshots = append(snapshots, &snapshotv1.Snapshot{
			Key:       key,
			Hash:      c.Hash.String(),
			Timestamp: c.Committer.When.UTC().Format(time.RFC3339),
		})
		return nil
	})
	if err != nil && !errors.Is(err, errIterDone) {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&snapshotv1.ListSnapshotsResponse{
		Snapshots: snapshots,
	}), nil
}

var errIterDone = errors.New("iteration done")

// ── serve: save / sync ───────────────────────────────────────────────────────

// save commits the dirty working tree and plain-pushes. Non-fast-forward
// rejection → status "remote_changed" (the commit stays local). A repeated
// save after the remote was made fast-forwardable retries the push — that
// is the resolution path.
func (b *gitBackend) save(ctx context.Context) (resp *snapshotv1.TriggerSnapshotResponse, err error) {
	ctx, span := tel.tracer.Start(ctx, "snapshot.save", trace.WithAttributes(
		attrBackend.String(backendGit),
		attrTrigger.String(triggerFrom(ctx)),
		attribute.String("git.branch", b.cfg.GitBranch),
	))
	defer span.End()

	start := time.Now()
	defer func() {
		status := "error"
		if resp != nil {
			status = resp.GetStatus()
		}
		span.SetAttributes(attrStatus.String(status))
		recordErr(span, err)
		tel.saveDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(
			withErrType([]attribute.KeyValue{
				attrBackend.String(backendGit),
				attrStatus.String(status),
			}, err)...,
		))
	}()

	if !b.busyMu.TryLock() {
		span.AddEvent("save skipped: a save or sync is already running")
		return &snapshotv1.TriggerSnapshotResponse{Status: "busy"}, nil
	}
	defer b.busyMu.Unlock()
	defer b.refreshState()

	status, err := b.wt.Status()
	if err != nil {
		return nil, fmt.Errorf("status: %w", err)
	}

	span.SetAttributes(attribute.Bool("git.worktree.dirty", !status.IsClean()))
	if !status.IsClean() {
		span.SetAttributes(attribute.Int("git.worktree.changed_files", len(status)))
		if err := b.wt.AddWithOptions(&git.AddOptions{All: true}); err != nil {
			return nil, fmt.Errorf("add: %w", err)
		}
		msg := "snapshot-sidecar: save " + time.Now().UTC().Format(time.RFC3339)
		if _, err := b.wt.Commit(msg, &git.CommitOptions{Author: b.signature()}); err != nil &&
			!errors.Is(err, git.ErrEmptyCommit) {
			return nil, fmt.Errorf("commit: %w", err)
		}
		span.AddEvent("commit created")
	}

	head, err := b.repo.Head()
	if err != nil {
		return nil, fmt.Errorf("head: %w", err)
	}
	hash := head.Hash().String()
	key := fmt.Sprintf("%s@%s", b.cfg.GitBranch, shortHash(hash))
	span.SetAttributes(attrKey.String(key), attrHash.String(hash))

	if remoteRef, err := b.repo.Reference(
		plumbing.NewRemoteReferenceName(defaultRemote, b.cfg.GitBranch), true,
	); err == nil && remoteRef.Hash() == head.Hash() {
		return &snapshotv1.TriggerSnapshotResponse{Status: "unchanged", Hash: hash}, nil
	}

	err = b.remoteOp(ctx, "git.push", func(ctx context.Context) error {
		return b.repo.PushContext(ctx, &git.PushOptions{RemoteName: defaultRemote, ClientOptions: b.clientOpts})
	})
	switch {
	case err == nil, errors.Is(err, git.NoErrAlreadyUpToDate):
		b.stateMu.Lock()
		b.state.LastSave = time.Now().UTC().Format(time.RFC3339)
		b.stateMu.Unlock()
		b.logger.Info("saved", "key", key)
		return &snapshotv1.TriggerSnapshotResponse{Status: "created", Key: key, Hash: hash}, nil
	case isNonFastForward(err):
		// Not an error for the caller — the commit is safe locally and a
		// human resolves the divergence — but worth an event on the trace.
		span.AddEvent("push rejected: non-fast-forward, commit kept local")
		b.logger.Warn("push rejected: remote changed", "key", key)
		return &snapshotv1.TriggerSnapshotResponse{Status: "remote_changed", Key: key, Hash: hash}, nil
	default:
		return nil, fmt.Errorf("push: %w", err)
	}
}

// remoteOp wraps a fetch/push/pull in a span. Up-to-date, empty-remote and
// non-fast-forward outcomes are routine here (first boot, idle box, human
// pushed to the remote), so they land as attributes rather than span errors;
// the caller decides what is actually fatal.
func (b *gitBackend) remoteOp(ctx context.Context, name string, fn func(context.Context) error) error {
	ctx, span := tel.tracer.Start(ctx, name, trace.WithAttributes(
		attribute.String("git.branch", b.cfg.GitBranch),
		attribute.String("git.remote", defaultRemote),
	))
	defer span.End()

	err := fn(ctx)
	switch {
	case err == nil:
	case errors.Is(err, git.NoErrAlreadyUpToDate):
		span.SetAttributes(attribute.Bool("git.up_to_date", true))
	case errors.Is(err, transport.ErrEmptyRemoteRepository):
		span.SetAttributes(attribute.Bool("git.remote_empty", true))
	case isNonFastForward(err):
		span.SetAttributes(attribute.Bool("git.non_fast_forward", true))
	default:
		recordErr(span, err)
	}
	return err
}

func isNonFastForward(err error) bool {
	// Pull surfaces the sentinel; push rejections are wrapped strings
	// (remote.go: "non-fast-forward update: <ref>").
	return errors.Is(err, git.ErrNonFastForwardUpdate) ||
		strings.Contains(err.Error(), "non-fast-forward")
}

// syncLoop periodically fast-forwards the working tree from the remote.
// It never commits and never pushes — saves are explicit (RPC/autosave).
func (b *gitBackend) syncLoop(ctx context.Context, interval time.Duration) {
	// One immediate iteration so a fresh pod converges without waiting.
	if err := b.syncOnce(ctx); err != nil {
		b.logger.Error("sync failed", "err", err)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := b.syncOnce(ctx); err != nil {
				b.logger.Error("sync failed", "err", err)
			}
		}
	}
}

func (b *gitBackend) syncOnce(ctx context.Context) (err error) {
	ctx, span := tel.tracer.Start(ctx, "snapshot.sync", trace.WithAttributes(
		attribute.String("git.branch", b.cfg.GitBranch),
	))
	defer span.End()

	start := time.Now()
	result := "skipped"
	defer func() {
		span.SetAttributes(attrSyncResult.String(result))
		recordErr(span, err)
		tel.syncDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(
			withErrType([]attribute.KeyValue{attrSyncResult.String(result)}, err)...,
		))
	}()

	if !b.busyMu.TryLock() {
		span.AddEvent("sync skipped: a save is running, next tick catches up")
		return nil
	}
	defer b.busyMu.Unlock()

	// Local err vars: the deferred recorder reads the *returned* error, so an
	// up-to-date fetch/pull must not leak into it.
	if ferr := b.remoteOp(ctx, "git.fetch", func(ctx context.Context) error {
		return b.repo.FetchContext(ctx, &git.FetchOptions{RemoteName: defaultRemote, ClientOptions: b.clientOpts})
	}); ferr != nil && !errors.Is(ferr, git.NoErrAlreadyUpToDate) {
		b.setLastError(fmt.Errorf("fetch: %w", ferr))
		return fmt.Errorf("fetch: %w", ferr)
	}
	b.stateMu.Lock()
	b.state.LastFetch = time.Now().UTC().Format(time.RFC3339)
	b.stateMu.Unlock()

	state := b.refreshState()
	span.SetAttributes(attrGitState.String(state))
	if state != "behind" {
		// in-sync: nothing to do; dirty/ahead/diverged: hands off — pulling
		// would need a merge, and merges are a human decision here.
		if state == "in-sync" {
			result = "up-to-date"
		}
		return nil
	}

	if perr := b.remoteOp(ctx, "git.pull", func(ctx context.Context) error {
		return b.wt.PullContext(ctx, &git.PullOptions{
			RemoteName:    defaultRemote,
			ReferenceName: plumbing.NewBranchReferenceName(b.cfg.GitBranch),
			ClientOptions: b.clientOpts,
		})
	}); perr != nil && !errors.Is(perr, git.NoErrAlreadyUpToDate) {
		b.setLastError(fmt.Errorf("pull: %w", perr))
		return fmt.Errorf("pull: %w", perr)
	}

	b.stateMu.Lock()
	b.state.LastPull = time.Now().UTC().Format(time.RFC3339)
	b.stateMu.Unlock()
	b.refreshState()
	result = "pulled"
	b.logger.Info("pulled remote changes")
	return nil
}

// refreshState reclassifies local vs remote and records it for
// /debug/sync-status. Returns the new state.
func (b *gitBackend) refreshState() string {
	state, head, remote := b.classify()
	b.stateMu.Lock()
	b.state.State = state
	b.state.Head = head
	b.state.RemoteHead = remote
	b.stateMu.Unlock()
	return state
}

func (b *gitBackend) classify() (state, head, remote string) {
	headRef, err := b.repo.Head()
	if err != nil {
		return "in-sync", "", "" // unborn branch: nothing to compare yet
	}
	head = headRef.Hash().String()

	status, err := b.wt.Status()
	if err == nil && !status.IsClean() {
		state = "dirty"
	}

	remoteRef, err := b.repo.Reference(
		plumbing.NewRemoteReferenceName(defaultRemote, b.cfg.GitBranch), true,
	)
	if err != nil {
		if state == "" {
			state = "ahead" // no remote branch: everything local is unpushed
		}
		return state, head, ""
	}
	remote = remoteRef.Hash().String()

	if state != "" { // dirty trumps ahead/behind for the pull decision
		return state, head, remote
	}
	if head == remote {
		return "in-sync", head, remote
	}

	headC, errH := b.repo.CommitObject(headRef.Hash())
	remoteC, errR := b.repo.CommitObject(remoteRef.Hash())
	if errH != nil || errR != nil {
		return "diverged", head, remote // can't prove ancestry: be conservative
	}
	if ok, err := remoteC.IsAncestor(headC); err == nil && ok {
		return "ahead", head, remote
	}
	if ok, err := headC.IsAncestor(remoteC); err == nil && ok {
		return "behind", head, remote
	}
	return "diverged", head, remote
}

func (b *gitBackend) setLastError(err error) {
	b.stateMu.Lock()
	b.state.LastError = err.Error()
	b.stateMu.Unlock()
}

// registerDebug exposes GET /debug/sync-status on the sidecar mux.
func (b *gitBackend) registerDebug(mux *http.ServeMux) {
	mux.HandleFunc("GET /debug/sync-status", func(w http.ResponseWriter, _ *http.Request) {
		// Reclassify on demand (cheap, local-only) unless a save/sync holds
		// the lock — then report the last recorded state.
		if b.busyMu.TryLock() {
			b.refreshState()
			b.busyMu.Unlock()
		}
		b.stateMu.Lock()
		state := b.state
		b.stateMu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(state)
	})
}

// ── restore ──────────────────────────────────────────────────────────────────

// restoreGit is the init-container entrypoint: open-or-adopt the project
// directory, then try one fast-forward sync. Content-level states (dirty,
// diverged) exit 0 — serve owns them; only config/transport errors fail the
// init container.
func restoreGit(ctx context.Context, logger *slog.Logger, cfg envConfig) error {
	b, err := newGitBackend(ctx, logger, cfg)
	if err != nil {
		return err
	}
	if err := b.syncOnce(ctx); err != nil {
		return err
	}
	state := b.refreshState()
	trace.SpanFromContext(ctx).SetAttributes(attrGitState.String(state))
	logger.Info("restore complete", "state", state)
	return nil
}
