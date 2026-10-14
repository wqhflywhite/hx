package index_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"hx/kb/index"
	"hx/kb/vault"
)

var fixedTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func open(t *testing.T) *index.Index {
	t.Helper()
	x, err := index.Open(filepath.Join(t.TempDir(), "kb.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = x.Close() })
	return x
}

func note(path, sha1 string, chunks []vault.Chunk, links []string) index.File {
	return index.File{Path: path, SHA1: sha1, MTime: fixedTime, Chunks: chunks, Links: links}
}

func f64ptr(v float64) *float64 { return &v }

// basis 返回 dim 维的第 i 个单位向量。
func basis(dim, i int) []float32 {
	v := make([]float32, dim)
	v[i] = 1
	return v
}

func TestNeedsReindexAndIdempotence(t *testing.T) {
	x := open(t)
	f := note("a.md", "sha-v1", []vault.Chunk{{Path: []string{"H"}, Start: 1, End: 2, Text: "# H\nalpha body"}}, nil)

	if need, err := x.NeedsReindex("a.md", "sha-v1"); err != nil || !need {
		t.Fatalf("unindexed file: need=%v err=%v; want true", need, err)
	}
	if err := x.ReindexFile(f); err != nil {
		t.Fatal(err)
	}
	if need, err := x.NeedsReindex("a.md", "sha-v1"); err != nil || need {
		t.Fatalf("same sha1: need=%v err=%v; want false", need, err)
	}
	if need, err := x.NeedsReindex("a.md", "sha-v2"); err != nil || !need {
		t.Fatalf("changed sha1: need=%v err=%v; want true", need, err)
	}

	// 对相同内容再次重建索引，对搜索面是 no-op。
	if err := x.ReindexFile(f); err != nil {
		t.Fatal(err)
	}
	hits, err := x.Search(context.Background(), "alpha", index.SearchOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 {
		t.Fatalf("repeat reindex produced %d hits; want exactly 1", len(hits))
	}
}

func TestReindexReplacesOldContent(t *testing.T) {
	x := open(t)
	old := note("a.md", "v1", []vault.Chunk{{Path: []string{"H"}, Start: 1, End: 1, Text: "stale term"}}, []string{"old-target"})
	if err := x.ReindexFile(old); err != nil {
		t.Fatal(err)
	}
	fresh := note("a.md", "v2", []vault.Chunk{{Path: []string{"H"}, Start: 1, End: 1, Text: "fresh term"}}, []string{"new-target"})
	if err := x.ReindexFile(fresh); err != nil {
		t.Fatal(err)
	}

	hits, err := x.Search(context.Background(), "stale", index.SearchOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Fatalf("stale chunk survived reindex: %+v", hits)
	}
	hits, err = x.Search(context.Background(), "fresh", index.SearchOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Path != "a.md" {
		t.Fatalf("fresh hits = %+v", hits)
	}
	if bl, err := x.Backlinks("old-target"); err != nil || len(bl) != 0 {
		t.Fatalf("old link survived reindex: %v %v", bl, err)
	}
	if bl, err := x.Backlinks("new-target"); err != nil || len(bl) != 1 || bl[0] != "a.md" {
		t.Fatalf("backlinks = %v %v; want [a.md]", bl, err)
	}
}

func TestSearchRankingAndNormalization(t *testing.T) {
	x := open(t)
	if err := x.ReindexFile(note("strong.md", "1", []vault.Chunk{
		{Path: []string{"方法", "数据集"}, Start: 1, End: 3, Text: "检索算法 检索算法 检索算法 检索算法"},
	}, nil)); err != nil {
		t.Fatal(err)
	}
	if err := x.ReindexFile(note("weak.md", "1", []vault.Chunk{
		{Path: nil, Start: 1, End: 1, Text: "检索算法"},
	}, nil)); err != nil {
		t.Fatal(err)
	}

	hits, err := x.Search(context.Background(), "检索算法", index.SearchOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 {
		t.Fatalf("hits = %+v; want 2", hits)
	}
	if hits[0].Path != "strong.md" {
		t.Fatalf("best hit = %q; want strong.md", hits[0].Path)
	}
	if hits[0].Score != 1 {
		t.Fatalf("best score = %v; want normalized 1", hits[0].Score)
	}
	// Min-max 归一化把最优候选映射为 1、最差映射为 0。
	if hits[1].Score != 0 {
		t.Fatalf("runner-up score = %v; want min-max normalized 0", hits[1].Score)
	}
	if len(hits[0].HeadingPath) != 2 || hits[0].HeadingPath[1] != "数据集" {
		t.Fatalf("heading path = %v", hits[0].HeadingPath)
	}

	// 权重缩放每个分数；limit 限制结果数量。
	hits, err = x.Search(context.Background(), "检索算法", index.SearchOpts{Limit: 1, FTSWeight: f64ptr(0.5)})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Score != 0.5 {
		t.Fatalf("weighted limited hits = %+v; want one hit at 0.5", hits)
	}

	// 查询里的 FTS 特殊字符无法破坏 MATCH。
	if _, err := x.Search(context.Background(), `"检索算法" OR (broken`, index.SearchOpts{}); err != nil {
		t.Fatalf("hostile query errored: %v", err)
	}
	// 空白查询不匹配任何东西，也不触碰数据库。
	hits, err = x.Search(context.Background(), "   ", index.SearchOpts{})
	if err != nil || hits != nil {
		t.Fatalf("blank query = %+v, %v", hits, err)
	}
}

func TestRebuildIsDisposable(t *testing.T) {
	x := open(t)
	if err := x.ReindexFile(note("gone.md", "1", []vault.Chunk{{Start: 1, End: 1, Text: "old vault"}}, nil)); err != nil {
		t.Fatal(err)
	}
	err := x.Rebuild([]index.File{
		note("b.md", "1", []vault.Chunk{{Start: 1, End: 1, Text: "new vault"}}, []string{"c.md"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	files, err := x.Files()
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0] != "b.md" {
		t.Fatalf("files = %v; want [b.md]", files)
	}
	hits, err := x.Search(context.Background(), "old", index.SearchOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Fatalf("pre-rebuild content survived: %+v", hits)
	}
	if bl, err := x.Backlinks("c.md"); err != nil || len(bl) != 1 || bl[0] != "b.md" {
		t.Fatalf("backlinks = %v %v; want [b.md]", bl, err)
	}
}

func TestRemoveFile(t *testing.T) {
	x := open(t)
	if err := x.ReindexFile(note("a.md", "1", []vault.Chunk{{Start: 1, End: 1, Text: "doomed"}}, []string{"b.md"})); err != nil {
		t.Fatal(err)
	}
	if err := x.RemoveFile("a.md"); err != nil {
		t.Fatal(err)
	}
	hits, err := x.Search(context.Background(), "doomed", index.SearchOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Fatalf("removed file still searchable: %+v", hits)
	}
	if bl, err := x.Backlinks("b.md"); err != nil || len(bl) != 0 {
		t.Fatalf("removed file's links survived: %v %v", bl, err)
	}
	// 删除未知文件不算错误。
	if err := x.RemoveFile("never-indexed.md"); err != nil {
		t.Fatal(err)
	}
}

// TestHybridBlend 钉住双信号混合：一篇纯 FTS 命中的笔记和一篇
// 纯向量命中的笔记，随着权重从 FTS 默认走向向量偏重再到纯向量，
// 排名互换位置。
func TestHybridBlend(t *testing.T) {
	x := open(t)
	kw := note("kw.md", "1", []vault.Chunk{{Start: 1, End: 1, Text: "检索算法 检索算法"}}, nil)
	kw.Vectors = [][]float32{basis(4, 1)} // 与查询向量正交
	if err := x.ReindexFile(kw); err != nil {
		t.Fatal(err)
	}
	sem := note("sem.md", "1", []vault.Chunk{{Start: 1, End: 1, Text: "unrelated prose without the query terms"}}, nil)
	sem.Vectors = [][]float32{basis(4, 0)} // 与查询向量相同
	if err := x.ReindexFile(sem); err != nil {
		t.Fatal(err)
	}
	queryVec := basis(4, 0)

	// 两个权重都未设置：纯 FTS，纯向量笔记缺席。
	hits, err := x.Search(context.Background(), "检索算法", index.SearchOpts{Vector: queryVec})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Path != "kw.md" {
		t.Fatalf("default-weight hits = %+v; want only kw.md", hits)
	}

	// 向量权重更重时排名翻转。
	hits, err = x.Search(context.Background(), "检索算法", index.SearchOpts{
		Vector: queryVec, FTSWeight: f64ptr(1), VectorWeight: f64ptr(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 || hits[0].Path != "sem.md" {
		t.Fatalf("vector-heavy hits = %+v; want sem.md first", hits)
	}
	// kw.md 混合 1×1（FTS 最优）+ 2×0（向量最差）= 1；sem.md 0 + 2×1 = 2。
	if hits[0].Score != 2 || hits[1].Score != 1 {
		t.Fatalf("blended scores = %v, %v; want 2 and 1", hits[0].Score, hits[1].Score)
	}

	// 显式的 FTS 权重 0 不等于「未设置」：搜索是纯向量的，正交
	// 笔记（余弦 0）不是候选。
	hits, err = x.Search(context.Background(), "检索算法", index.SearchOpts{
		Vector: queryVec, FTSWeight: f64ptr(0), VectorWeight: f64ptr(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Path != "sem.md" || hits[0].Score != 1 {
		t.Fatalf("vector-only hits = %+v; want only sem.md at 1", hits)
	}

	// 只有查询向量、没有查询文本时仍然搜索（纯向量）。
	hits, err = x.Search(context.Background(), "", index.SearchOpts{
		Vector: queryVec, FTSWeight: f64ptr(1), VectorWeight: f64ptr(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Path != "sem.md" {
		t.Fatalf("textless vector hits = %+v; want only sem.md", hits)
	}
}

// TestVectorsReplacedOnReindex 验证重建索引会重写 embeddings
// 表，而不是向其追加。
func TestVectorsReplacedOnReindex(t *testing.T) {
	x := open(t)
	f := note("a.md", "v1", []vault.Chunk{
		{Start: 1, End: 1, Text: "first chunk body"},
		{Start: 2, End: 2, Text: "second chunk body"},
	}, nil)
	// 两个 chunk 的向量都与查询保持正相关，因此都能通过候选过滤；
	// 重建索引翻转的正是它们的顺序。
	near := []float32{1, 1, 0, 0} // 与 basis(4, 0) 的余弦为 1/√2
	f.Vectors = [][]float32{basis(4, 0), near}
	if err := x.ReindexFile(f); err != nil {
		t.Fatal(err)
	}
	vecOnly := index.SearchOpts{Vector: basis(4, 0), FTSWeight: f64ptr(0), VectorWeight: f64ptr(1)}
	hits, err := x.Search(context.Background(), "", vecOnly)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 || hits[0].Idx != 0 {
		t.Fatalf("initial vector hits = %+v; want chunk 0 first", hits)
	}

	f.SHA1 = "v2"
	f.Vectors = [][]float32{near, basis(4, 0)}
	if err := x.ReindexFile(f); err != nil {
		t.Fatal(err)
	}
	hits, err = x.Search(context.Background(), "", vecOnly)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 || hits[0].Idx != 1 {
		t.Fatalf("post-reindex vector hits = %+v; want chunk 1 first", hits)
	}
}

// TestVectorCandidatesKeepHighestK 索引的正相关 chunk 多于
// 候选上限，让裁剪生效：分数最高的 chunk 必须存活并排第一，
// 而不是被上限丢掉。
func TestVectorCandidatesKeepHighestK(t *testing.T) {
	x := open(t)
	chunks := make([]vault.Chunk, 0, 25)
	vectors := make([][]float32, 0, 25)
	for i := 0; i < 24; i++ {
		chunks = append(chunks, vault.Chunk{Start: i + 1, End: i + 1, Text: fmt.Sprintf("filler %d", i)})
		v := make([]float32, 8)
		v[0], v[1+(i%7)] = 1, 100 // 与 basis(8, 0) 的余弦 ≈ 0.01
		vectors = append(vectors, v)
	}
	chunks = append(chunks, vault.Chunk{Start: 25, End: 25, Text: "the answer"})
	vectors = append(vectors, basis(8, 0)) // 余弦为 1，远超其他
	f := note("many.md", "1", chunks, nil)
	f.Vectors = vectors
	if err := x.ReindexFile(f); err != nil {
		t.Fatal(err)
	}
	vecOnly := index.SearchOpts{Vector: basis(8, 0), FTSWeight: f64ptr(0), VectorWeight: f64ptr(1), Limit: 5}
	hits, err := x.Search(context.Background(), "", vecOnly)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) == 0 || hits[0].Text != "the answer" {
		t.Fatalf("best vector chunk lost to candidate trimming: %+v", hits)
	}
}

// TestVectorCountMismatch 拒绝 Vectors 与 Chunks 数量不对齐的
// File。
func TestVectorCountMismatch(t *testing.T) {
	x := open(t)
	f := note("a.md", "1", []vault.Chunk{{Start: 1, End: 1, Text: "body"}}, nil)
	f.Vectors = [][]float32{basis(4, 0), basis(4, 1)}
	if err := x.ReindexFile(f); err == nil {
		t.Fatal("ReindexFile accepted 1 chunk with 2 vectors")
	}
}

// BenchmarkVectorSearch 在预期 vault 规模的上界（1 万 chunk、
// 256 维）下测量暴力余弦扫描。
func BenchmarkVectorSearch(b *testing.B) {
	dir := b.TempDir()
	x, err := index.Open(filepath.Join(dir, "kb.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = x.Close() })

	const chunks, dim = 10_000, 256
	f := note("bench.md", "1", make([]vault.Chunk, chunks), nil)
	f.Vectors = make([][]float32, chunks)
	for i := range f.Chunks {
		f.Chunks[i] = vault.Chunk{Start: i + 1, End: i + 1, Text: fmt.Sprintf("benchmark chunk number %d", i)}
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = float32((i*131+j*17)%97) / 97
		}
		f.Vectors[i] = vec
	}
	if err := x.ReindexFile(f); err != nil {
		b.Fatal(err)
	}

	query := make([]float32, dim)
	for j := range query {
		query[j] = float32(j%89) / 89
	}
	opts := index.SearchOpts{Vector: query, FTSWeight: f64ptr(0), VectorWeight: f64ptr(1)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := x.Search(context.Background(), "", opts); err != nil {
			b.Fatal(err)
		}
	}
}
