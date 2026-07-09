package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	git "github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
)

// The tests run against local bare repositories — go-git v6's file transport
// is pure Go (in-process upload/receive-pack), so no git binary and no Gitea
// are needed in CI.

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testCfg(projectDir, remote string) envConfig {
	return envConfig{
		Backend:        backendGit,
		ProjectDir:     projectDir,
		GitRemoteURL:   remote,
		GitBranch:      "main",
		GitToken:       "test-token",
		GitAuthorName:  "Test",
		GitAuthorEmail: "test@example.com",
	}
}

func bareRemote(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	_, err := git.PlainInit(dir, true,
		git.WithDefaultBranch(plumbing.NewBranchReferenceName("main")))
	if err != nil {
		t.Fatalf("init bare remote: %v", err)
	}
	return dir
}

// writer is an independent clone used to seed/mutate the remote, standing in
// for the seed Job or a human editing in the Gitea UI.
type writer struct {
	t    *testing.T
	dir  string
	repo *git.Repository
}

func newWriter(t *testing.T, remote string) *writer {
	t.Helper()
	dir := t.TempDir()
	repo, err := git.PlainClone(dir, &git.CloneOptions{
		URL:            remote,
		ReferenceName:  plumbing.NewBranchReferenceName("main"),
		AllowEmptyRepo: true,
	})
	if err != nil {
		t.Fatalf("clone remote: %v", err)
	}
	// Empty-repo clones ignore ReferenceName and leave HEAD at the default
	// branch (master) — pin it to main so pushes create refs/heads/main.
	err = repo.Storer.SetReference(plumbing.NewSymbolicReference(
		plumbing.HEAD, plumbing.NewBranchReferenceName("main")))
	if err != nil {
		t.Fatalf("set HEAD: %v", err)
	}
	return &writer{t: t, dir: dir, repo: repo}
}

func (w *writer) commitFiles(files map[string]string, msg string) plumbing.Hash {
	w.t.Helper()
	wt, err := w.repo.Worktree()
	if err != nil {
		w.t.Fatalf("worktree: %v", err)
	}
	for name, content := range files {
		path := filepath.Join(w.dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			w.t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			w.t.Fatalf("write %s: %v", name, err)
		}
	}
	if err := wt.AddWithOptions(&git.AddOptions{All: true}); err != nil {
		w.t.Fatalf("add: %v", err)
	}
	hash, err := wt.Commit(msg, &git.CommitOptions{
		Author: &object.Signature{Name: "Writer", Email: "writer@example.com"},
	})
	if err != nil {
		w.t.Fatalf("commit: %v", err)
	}
	return hash
}

func (w *writer) push() {
	w.t.Helper()
	if err := w.repo.Push(&git.PushOptions{}); err != nil {
		w.t.Fatalf("push: %v", err)
	}
}

// seededRemote is a remote in the state the fairtier.com rill seed Job leaves
// it: README + .gitignore covering the platform-managed and scratch files.
func seededRemote(t *testing.T) (remote string, head plumbing.Hash) {
	t.Helper()
	remote = bareRemote(t)
	w := newWriter(t, remote)
	head = w.commitFiles(map[string]string{
		"README.md":  "# project\n",
		".gitignore": "/rill.yaml\n/duckdb.yaml\n/.env\ntmp/\n.rill*\n*.db\n*.db.*\n..*\n",
	}, "seed: README + .gitignore")
	w.push()
	return remote, head
}

// scatterLocalJunk populates a project dir the way the box PVC looks before
// adoption: ConfigMap-copied platform files + Rill scratch, all gitignored.
func scatterLocalJunk(t *testing.T, dir string) {
	t.Helper()
	files := map[string]string{
		"rill.yaml":   "title: x\n",
		"duckdb.yaml": "type: connector\n",
		".env":        "SECRET=1\n",
		"tmp/scratch": "junk\n",
		"..data/x":    "configmap mount metadata\n",
	}
	for name, content := range files {
		path := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
}

func remoteHead(t *testing.T, remote string) plumbing.Hash {
	t.Helper()
	repo, err := git.PlainOpen(remote)
	if err != nil {
		t.Fatalf("open remote: %v", err)
	}
	ref, err := repo.Reference(plumbing.NewBranchReferenceName("main"), true)
	if err != nil {
		t.Fatalf("remote main: %v", err)
	}
	return ref.Hash()
}

// setRemoteHead force-moves the bare remote's main — simulating an operator
// resetting the branch to make the box's push fast-forwardable again.
func setRemoteHead(t *testing.T, remote string, hash plumbing.Hash) {
	t.Helper()
	repo, err := git.PlainOpen(remote)
	if err != nil {
		t.Fatalf("open remote: %v", err)
	}
	err = repo.Storer.SetReference(
		plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), hash))
	if err != nil {
		t.Fatalf("set remote main: %v", err)
	}
}

func mustBackend(t *testing.T, cfg envConfig) *gitBackend {
	t.Helper()
	b, err := newGitBackend(t.Context(), testLogger(), cfg)
	if err != nil {
		t.Fatalf("newGitBackend: %v", err)
	}
	return b
}

func trigger(t *testing.T, b *gitBackend) string {
	t.Helper()
	resp, err := b.save(t.Context())
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	return resp.GetStatus()
}

// ── adopt ────────────────────────────────────────────────────────────────────

func TestAdoptEmptyRemote(t *testing.T) {
	remote := bareRemote(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "d1.yaml"), []byte("a: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte("SECRET=1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	b := mustBackend(t, testCfg(dir, remote))

	// Initial import must be pushed…
	head := remoteHead(t, remote)
	commit, err := b.repo.CommitObject(head)
	if err != nil {
		t.Fatalf("remote commit: %v", err)
	}
	tree, err := commit.Tree()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tree.File("d1.yaml"); err != nil {
		t.Errorf("d1.yaml missing from initial import: %v", err)
	}
	if _, err := tree.File(".gitignore"); err != nil {
		t.Errorf(".gitignore missing from initial import: %v", err)
	}
	// …and the default .gitignore must have kept .env out.
	if _, err := tree.File(".env"); err == nil {
		t.Error(".env was committed — default .gitignore not applied")
	}

	if state := b.refreshState(); state != "in-sync" {
		t.Errorf("state = %q, want in-sync", state)
	}
}

func TestAdoptSeededRemoteOverExistingFiles(t *testing.T) {
	// The fokume migration case: PVC holds ConfigMap-copied platform files
	// and scratch — all matched by the seeded .gitignore.
	remote, seedHead := seededRemote(t)
	dir := t.TempDir()
	scatterLocalJunk(t, dir)

	b := mustBackend(t, testCfg(dir, remote))

	// Remote files materialized on disk.
	for _, name := range []string{"README.md", ".gitignore"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("%s not materialized: %v", name, err)
		}
	}
	// Local files untouched.
	if _, err := os.Stat(filepath.Join(dir, "rill.yaml")); err != nil {
		t.Errorf("rill.yaml lost during adoption: %v", err)
	}

	// No commit was created; tree is clean because the junk is ignored.
	headRef, err := b.repo.Head()
	if err != nil {
		t.Fatal(err)
	}
	if headRef.Hash() != seedHead {
		t.Errorf("adoption created a commit: head %s != seed %s", headRef.Hash(), seedHead)
	}
	if state := b.refreshState(); state != "in-sync" {
		t.Errorf("state = %q, want in-sync", state)
	}
}

func TestAdoptDoesNotOverwriteLocalContent(t *testing.T) {
	remote, _ := seededRemote(t)
	dir := t.TempDir()
	// A local README differing from the remote one must survive adoption…
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("local edit\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	b := mustBackend(t, testCfg(dir, remote))

	data, err := os.ReadFile(filepath.Join(dir, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "local edit\n" {
		t.Errorf("local README overwritten: %q", data)
	}
	// …showing up as an uncommitted modification (dirty), not divergence.
	if state := b.refreshState(); state != "dirty" {
		t.Errorf("state = %q, want dirty", state)
	}

	// The next save commits it and fast-forwards the remote.
	if status := trigger(t, b); status != "created" {
		t.Fatalf("save status = %q, want created", status)
	}
	if state := b.refreshState(); state != "in-sync" {
		t.Errorf("state after save = %q, want in-sync", state)
	}
}

// ── save ─────────────────────────────────────────────────────────────────────

func TestSavePushesAndDeduplicates(t *testing.T) {
	remote, _ := seededRemote(t)
	dir := t.TempDir()
	b := mustBackend(t, testCfg(dir, remote))

	if status := trigger(t, b); status != "unchanged" {
		t.Fatalf("clean-tree save = %q, want unchanged", status)
	}

	if err := os.MkdirAll(filepath.Join(dir, "models"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "models/m.sql"), []byte("select 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if status := trigger(t, b); status != "created" {
		t.Fatalf("dirty save = %q, want created", status)
	}

	// The commit reached the remote.
	commit, err := b.repo.CommitObject(remoteHead(t, remote))
	if err != nil {
		t.Fatal(err)
	}
	tree, err := commit.Tree()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tree.File("models/m.sql"); err != nil {
		t.Errorf("models/m.sql not on remote: %v", err)
	}

	if status := trigger(t, b); status != "unchanged" {
		t.Fatalf("repeat save = %q, want unchanged", status)
	}
}

func TestSaveIgnoredOnlyChangesAreUnchanged(t *testing.T) {
	remote, _ := seededRemote(t)
	dir := t.TempDir()
	b := mustBackend(t, testCfg(dir, remote))

	scatterLocalJunk(t, dir) // all gitignored

	if status := trigger(t, b); status != "unchanged" {
		t.Fatalf("ignored-only save = %q, want unchanged", status)
	}
}

func TestSaveRemoteChangedAndRetry(t *testing.T) {
	remote, seedHead := seededRemote(t)
	dir := t.TempDir()
	b := mustBackend(t, testCfg(dir, remote))

	// Someone pushes while we edit locally.
	w := newWriter(t, remote)
	w.commitFiles(map[string]string{"remote.txt": "r\n"}, "remote edit")
	w.push()
	if err := os.WriteFile(filepath.Join(dir, "local.txt"), []byte("l\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if status := trigger(t, b); status != "remote_changed" {
		t.Fatalf("save = %q, want remote_changed", status)
	}
	// The local commit exists and is durable…
	headRef, err := b.repo.Head()
	if err != nil {
		t.Fatal(err)
	}
	if headRef.Hash() == seedHead {
		t.Error("local commit missing after remote_changed")
	}

	// …sync refuses to touch a diverged tree…
	if err := b.syncOnce(t.Context()); err != nil {
		t.Fatalf("syncOnce: %v", err)
	}
	if state := b.refreshState(); state != "diverged" {
		t.Errorf("state = %q, want diverged", state)
	}
	if _, err := os.Stat(filepath.Join(dir, "remote.txt")); err == nil {
		t.Error("diverged sync pulled remote.txt — it must not")
	}

	// …until an operator makes the push fast-forwardable again; a repeated
	// save is the retry path.
	setRemoteHead(t, remote, seedHead)
	if status := trigger(t, b); status != "created" {
		t.Fatalf("retry save = %q, want created", status)
	}
	if got := remoteHead(t, remote); got != headRef.Hash() {
		t.Errorf("remote head = %s, want %s", got, headRef.Hash())
	}
}

// ── sync ─────────────────────────────────────────────────────────────────────

func TestSyncFastForwards(t *testing.T) {
	remote, _ := seededRemote(t)
	dir := t.TempDir()
	b := mustBackend(t, testCfg(dir, remote))

	w := newWriter(t, remote)
	w.commitFiles(map[string]string{"dashboards/d.yaml": "d: 1\n"}, "remote edit")
	w.push()

	if err := b.syncOnce(t.Context()); err != nil {
		t.Fatalf("syncOnce: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "dashboards/d.yaml"))
	if err != nil {
		t.Fatalf("pulled file missing: %v", err)
	}
	if string(data) != "d: 1\n" {
		t.Errorf("pulled content = %q", data)
	}
	if state := b.refreshState(); state != "in-sync" {
		t.Errorf("state = %q, want in-sync", state)
	}
}

func TestSyncSkipsDirtyTree(t *testing.T) {
	remote, _ := seededRemote(t)
	dir := t.TempDir()
	b := mustBackend(t, testCfg(dir, remote))

	if err := os.WriteFile(filepath.Join(dir, "wip.txt"), []byte("wip\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	w := newWriter(t, remote)
	w.commitFiles(map[string]string{"remote.txt": "r\n"}, "remote edit")
	w.push()

	if err := b.syncOnce(t.Context()); err != nil {
		t.Fatalf("syncOnce: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "remote.txt")); err == nil {
		t.Error("dirty sync pulled remote.txt — it must not")
	}
	if state := b.refreshState(); state != "dirty" {
		t.Errorf("state = %q, want dirty", state)
	}
}

// ── list ─────────────────────────────────────────────────────────────────────

func TestListSnapshotsMarksUnpushed(t *testing.T) {
	remote, _ := seededRemote(t)
	dir := t.TempDir()
	b := mustBackend(t, testCfg(dir, remote))

	// One unpushed local commit on top of the seed.
	if err := os.WriteFile(filepath.Join(dir, "x.txt"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	w := newWriter(t, remote)
	w.commitFiles(map[string]string{"remote.txt": "r\n"}, "remote edit")
	w.push()
	if status := trigger(t, b); status != "remote_changed" {
		t.Fatalf("save = %q, want remote_changed", status)
	}

	resp, err := b.ListSnapshots(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	snaps := resp.Msg.GetSnapshots()
	if len(snaps) != 2 {
		t.Fatalf("got %d snapshots, want 2 (local save + seed)", len(snaps))
	}
	// Newest first: the local, unpushed commit.
	if got := snaps[0].GetKey(); got != "main@"+shortHash(snaps[0].GetHash())+" (unpushed)" {
		t.Errorf("snapshot[0].key = %q, want unpushed marker", got)
	}
	// The seed commit exists on the remote (remote's history diverged, but
	// the seed is still its ancestor — from this clone's view it is pushed).
	if got := snaps[1].GetKey(); got != "main@"+shortHash(snaps[1].GetHash()) {
		t.Errorf("snapshot[1].key = %q, want no unpushed marker", got)
	}
}
