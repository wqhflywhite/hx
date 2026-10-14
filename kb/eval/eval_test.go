package eval_test

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"hx/kb"
	"hx/kb/embed"
	"hx/kb/eval"
	"hx/kb/index"
	"hx/kb/vault"
)

func f64ptr(v float64) *float64 { return &v }

// copyVault 把签入仓库的 fixture vault 复制到临时目录（服务会把索引
// 数据库写进 vault，绝不能弄脏 testdata）。
func copyVault(t testing.TB) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "vault")
	src := filepath.Join("testdata", "vault")
	err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

// fixtureService 把 fixture vault 的副本同步进一个全新的服务。
func fixtureService(t testing.TB, emb embed.Embedder) *kb.Service {
	t.Helper()
	dir := copyVault(t)
	idx, err := index.Open(filepath.Join(t.TempDir(), "kb.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	svc := kb.NewService(dir, idx, emb, f64ptr(1), f64ptr(1), 10, 50)
	if err := svc.Sync(); err != nil {
		t.Fatal(err)
	}
	return svc
}

func loadGold(t testing.TB) *eval.GoldSet {
	t.Helper()
	g, err := eval.LoadGold(filepath.Join("testdata", "gold.json"))
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// TestFixtureVaultIsLintClean 钉死 fixture 的完整性：每个 wikilink 都能
// 解析、每个 wiki 页都有入链、没有超长内容。
func TestFixtureVaultIsLintClean(t *testing.T) {
	svc := fixtureService(t, nil)
	report, err := svc.Lint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !report.Empty() {
		t.Fatalf("fixture vault is not lint-clean: %+v", report)
	}
}

var (
	ftsOnly    = index.SearchOpts{FTSWeight: f64ptr(1), VectorWeight: f64ptr(0)}
	hybrid     = index.SearchOpts{FTSWeight: f64ptr(1), VectorWeight: f64ptr(1)}
	hybridSoft = index.SearchOpts{FTSWeight: f64ptr(1), VectorWeight: f64ptr(0.5)}
)

// TestGoldSet 运行供 README 引用的确定性对比：同一个假嵌入向量
// 器上纯 FTS 对混合检索。多跳题与其期望笔记没有任何可检索的
// 公共关键词，纯 FTS 必须全部 miss；混合一侧由假嵌入器的
// Synonyms 完成桥接。Dim 取 1024 让哈希投影噪声
// （σ ≈ 1/√Dim）远低于同义词桥接信号。
func TestGoldSet(t *testing.T) {
	gold := loadGold(t)
	fake := embed.Fake{Dim: 1024, Synonyms: gold.Synonyms}
	svc := fixtureService(t, fake)
	ctx := context.Background()

	ftsReport, err := eval.Run(ctx, "fts-only", gold.Questions, ftsOnly, svc.Search)
	if err != nil {
		t.Fatal(err)
	}
	hybReport, err := eval.Run(ctx, "hybrid(1:1)", gold.Questions, hybrid, svc.Search)
	if err != nil {
		t.Fatal(err)
	}
	softReport, err := eval.Run(ctx, "hybrid(1:0.5)", gold.Questions, hybridSoft, svc.Search)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("\n%s", ftsReport)
	t.Logf("\n%s", hybReport)
	t.Logf("\n%s", softReport)
	for _, res := range hybReport.Misses() {
		t.Logf("hybrid miss: %q want %v got %v", res.Question.Query, res.Question.Expect, res.Hits)
	}

	// 确定性：第二次混合检索必须复现完全相同的命中。
	again, err := eval.Run(ctx, "hybrid", gold.Questions, hybrid, svc.Search)
	if err != nil {
		t.Fatal(err)
	}
	for i := range hybReport.Results {
		if strings.Join(hybReport.Results[i].Hits, ",") != strings.Join(again.Results[i].Hits, ",") {
			t.Fatalf("non-deterministic hits for %q: %v vs %v",
				gold.Questions[i].Query, hybReport.Results[i].Hits, again.Results[i].Hits)
		}
	}

	// 设计好的性质：多跳题与期望笔记零关键词重叠，纯 FTS 不可能把任何
	// 一道排进 top-5。
	if got := ftsReport.Groups["multi"].Top5; got != 0 {
		t.Fatalf("fts-only multi top-5 = %d; want 0 (fixture lost its zero-overlap property)", got)
	}
	if hybReport.Groups["multi"].Top5 <= ftsReport.Groups["multi"].Top5 {
		t.Fatalf("hybrid multi top-5 = %d, fts-only = %d; want hybrid strictly better",
			hybReport.Groups["multi"].Top5, ftsReport.Groups["multi"].Top5)
	}
	if hybReport.Total.Top5 < ftsReport.Total.Top5 {
		t.Fatalf("hybrid total top-5 = %d < fts-only %d", hybReport.Total.Top5, ftsReport.Total.Top5)
	}
	// 混合检索不得拖累可关键词作答的单跳组；向量权重减半应救回
	// 等权混合丢掉的单跳 top-1。
	if hybReport.Groups["single"].Top5 < ftsReport.Groups["single"].Top5-1 {
		t.Fatalf("hybrid single top-5 = %d, fts-only = %d; hybrid degraded single-hop by >1",
			hybReport.Groups["single"].Top5, ftsReport.Groups["single"].Top5)
	}
	if softReport.Groups["single"].Top1 < ftsReport.Groups["single"].Top1-1 {
		t.Fatalf("hybrid(1:0.5) single top-1 = %d, fts-only = %d; soft blend did not recover",
			softReport.Groups["single"].Top1, ftsReport.Groups["single"].Top1)
	}
	if softReport.Groups["multi"].Top5 < hybReport.Groups["multi"].Top5-2 {
		t.Fatalf("hybrid(1:0.5) multi top-5 = %d, hybrid(1:1) = %d; soft blend lost too much multi-hop",
			softReport.Groups["multi"].Top5, hybReport.Groups["multi"].Top5)
	}
}

// BenchmarkSearchLatency 测量 fixture vault 上纯 FTS（nil 嵌入
// 向量器）与混合检索（假嵌入向量器）的单查询延迟，并报告各轮
// 迭代观测到的 p95。
func BenchmarkSearchLatency(b *testing.B) {
	gold, err := eval.LoadGold(filepath.Join("testdata", "gold.json"))
	if err != nil {
		b.Fatal(err)
	}
	for _, mode := range []struct {
		name string
		emb  embed.Embedder
		opts index.SearchOpts
	}{
		{"fts-only", nil, ftsOnly},
		{"hybrid", embed.Fake{Dim: 1024, Synonyms: gold.Synonyms}, hybrid},
	} {
		b.Run(mode.name, func(b *testing.B) {
			svc := fixtureService(b, mode.emb)
			samples := make([]time.Duration, 0, b.N)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				q := gold.Questions[i%len(gold.Questions)]
				start := time.Now()
				if _, err := svc.Search(context.Background(), q.Query, mode.opts); err != nil {
					b.Fatal(err)
				}
				samples = append(samples, time.Since(start))
			}
			b.StopTimer()
			b.ReportMetric(float64(eval.Percentile(samples, 50).Microseconds()), "p50-µs/op")
			b.ReportMetric(float64(eval.Percentile(samples, 95).Microseconds()), "p95-µs/op")
		})
	}
}

// BenchmarkIngestLatency 测量同步的写入到可检索路径：文件写入 + 解析 +
// 嵌入向量批处理 + 索引事务，以 p50/p95 报告。
func BenchmarkIngestLatency(b *testing.B) {
	dir := b.TempDir()
	idx, err := index.Open(filepath.Join(b.TempDir(), "kb.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = idx.Close() })
	svc := kb.NewService(dir, idx, embed.Fake{Dim: 64}, f64ptr(1), f64ptr(1), 10, 50)

	samples := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rel := fmt.Sprintf("raw/ingest-%d.md", i)
		abs := filepath.Join(dir, "raw", fmt.Sprintf("ingest-%d.md", i))
		var body strings.Builder
		body.WriteString("# Ingest benchmark note\n\n")
		for s := 0; s < 5; s++ {
			fmt.Fprintf(&body, "## Section %d\n\nbenchmark ingest body %d %d with some prose.\n\n", s, i, s)
		}
		start := time.Now()
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			b.Fatal(err)
		}
		if err := os.WriteFile(abs, []byte(body.String()), 0o644); err != nil {
			b.Fatal(err)
		}
		if err := svc.SyncFile(abs); err != nil {
			b.Fatal(err)
		}
		samples = append(samples, time.Since(start))
		_ = rel
	}
	b.StopTimer()
	b.ReportMetric(float64(eval.Percentile(samples, 50).Microseconds()), "p50-µs/op")
	b.ReportMetric(float64(eval.Percentile(samples, 95).Microseconds()), "p95-µs/op")
}

// BenchmarkRebuild 测量对 fixture vault 的一次性全量索引重建（全部笔记
// 重新解析、重新嵌入、在一组事务内重写）。
func BenchmarkRebuild(b *testing.B) {
	dir := copyVault(b)
	idx, err := index.Open(filepath.Join(b.TempDir(), "kb.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = idx.Close() })
	fake := embed.Fake{Dim: 64}

	var files []index.File
	err = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(d.Name(), ".md") {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		doc, err := vault.Parse(data)
		if err != nil {
			return err
		}
		texts := make([]string, len(doc.Chunks))
		for i, c := range doc.Chunks {
			texts[i] = c.Text
		}
		vecs, err := fake.Embed(context.Background(), texts)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		files = append(files, index.File{
			Path:    filepath.ToSlash(rel),
			SHA1:    fmt.Sprintf("%x", len(data)),
			MTime:   time.Now(),
			Chunks:  doc.Chunks,
			Links:   doc.Links,
			Vectors: vecs,
		})
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := idx.Rebuild(files); err != nil {
			b.Fatal(err)
		}
	}
}

// TestRealEmbedder 用真实的 OpenAI 兼容嵌入向量服务跑 gold
// 题集；缺少 HX_EMBED_* 环境时自动跳过。不提供
// Synonyms——真实模型必须自行跨越假嵌入器靠查表桥接的
// 中英词汇鸿沟。
func TestRealEmbedder(t *testing.T) {
	if os.Getenv("HX_EMBED_BASE_URL") == "" || os.Getenv("HX_EMBED_MODEL") == "" {
		t.Skip("HX_EMBED_BASE_URL / HX_EMBED_MODEL not set")
	}
	p, err := embed.New(embed.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	gold := loadGold(t)
	svc := fixtureService(t, p)
	report, err := eval.Run(context.Background(), "hybrid-real-api", gold.Questions, hybrid, svc.Search)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("\n%s", report)
	for _, res := range report.Misses() {
		t.Logf("miss: %q want %v got %v", res.Question.Query, res.Question.Expect, res.Hits)
	}
	if report.Total.Top5 == 0 {
		t.Fatal("real embedder found nothing at all; endpoint or wiring broken")
	}
}
