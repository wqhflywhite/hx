package kb_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"hx/kb"
	"hx/kb/embed"
	"hx/kb/index"
)

func TestConcurrentWritesWithSameETagSerialize(t *testing.T) {
	_, svc, _, vaultDir := mount(t, nil)
	writeNote(t, vaultDir, "note.md", "# H\noriginal\n")
	if err := svc.(*kb.Service).Sync(); err != nil {
		t.Fatal(err)
	}
	_, etag, err := svc.Read(context.Background(), "note.md", nil)
	if err != nil {
		t.Fatal(err)
	}
	const writers = 12
	start := make(chan struct{})
	errs := make(chan error, writers)
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := svc.Write(context.Background(), "note.md", []byte(fmt.Sprintf("# H\nwriter-%02d\n", i)), etag)
			if err == nil {
				successes.Add(1)
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil && !strings.Contains(err.Error(), "重新 kb_read") {
			t.Fatalf("unexpected write error: %v", err)
		}
	}
	if successes.Load() != 1 {
		t.Fatalf("successful writes = %d; want exactly 1", successes.Load())
	}
	content, fresh, err := svc.Read(context.Background(), "note.md", nil)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.SHA1 == etag.SHA1 || !strings.HasPrefix(content, "# H\nwriter-") {
		t.Fatalf("final content = %q, etag = %+v", content, fresh)
	}
	hits, err := svc.Search(context.Background(), strings.TrimSpace(strings.TrimPrefix(content, "# H\n")), index.SearchOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 {
		t.Fatalf("final atomic content is not indexed: %+v", hits)
	}
	leftovers, err := filepath.Glob(filepath.Join(vaultDir, ".hx-write-*"))
	if err != nil || len(leftovers) != 0 {
		t.Fatalf("temporary write files remain: %v, %v", leftovers, err)
	}
}

func TestEmbeddingDimensionChangeForcesRebuild(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "note.md"), []byte("# H\nalpha body\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, ".hx"), 0o755); err != nil {
		t.Fatal(err)
	}
	idx, err := index.Open(filepath.Join(dir, ".hx", "kb.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	one := 1.0
	first := kb.NewService(dir, idx, embed.Fake{Dim: 8}, &one, &one, 10, 50)
	if err := first.Sync(); err != nil {
		t.Fatal(err)
	}
	second := kb.NewService(dir, idx, embed.Fake{Dim: 16}, &one, &one, 10, 50)
	if err := second.Sync(); err != nil {
		t.Fatal(err)
	}
	changed, err := idx.EmbeddingChanged("fake", 16)
	if err != nil || changed {
		t.Fatalf("dimension was not rebuilt to 16: changed=%t err=%v", changed, err)
	}
	if _, err := second.Search(context.Background(), "alpha", index.SearchOpts{}); err != nil {
		t.Fatal(err)
	}

	fts := kb.NewService(dir, idx, nil, nil, nil, 10, 50)
	if err := fts.Sync(); err != nil {
		t.Fatal(err)
	}
	changed, err = idx.EmbeddingChanged("fake", 0)
	if err != nil || !changed {
		t.Fatalf("FTS transition retained old vector space: changed=%t err=%v", changed, err)
	}
	hits, err := fts.Search(context.Background(), "alpha", index.SearchOpts{})
	if err != nil || len(hits) != 1 {
		t.Fatalf("FTS search after vector cleanup = %+v, %v", hits, err)
	}
}
