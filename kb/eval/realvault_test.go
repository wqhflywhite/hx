package eval_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"hx/kb"
	"hx/kb/embed"
	"hx/kb/eval"
	"hx/kb/index"
)

// 真实语料评测（testdata/realvault，14 篇真实中文技术文档）：与合成
// fixture（vault/，简历定稿数字的来源）完全独立——不动它的题目与断言，
// 用真实文档交叉验证混合检索在真实文本形态下的表现。多跳题沿用对抗性
// 设计口径：查询与目标文档零关键词重叠（无 ≥3 字 CJK 公共子串、无公共
// latin 词元），由 testdata/realgold.json 的题目评审保证。

// requireRealVault 在真实语料缺失时跳过测试：testdata/realvault 与
// testdata/realgold.json 不随公开仓库分发（语料含第三方内容），
// 本地复现数字时自行放回 testdata 即可，其余测试不受影响。
func requireRealVault(t testing.TB) {
	t.Helper()
	for _, p := range []string{
		filepath.Join("testdata", "realvault"),
		filepath.Join("testdata", "realgold.json"),
	} {
		if _, err := os.Stat(p); err != nil {
			t.Skipf("real corpus %s not present (not distributed with the repo)", p)
		}
	}
}

// copyRealVault 把签入仓库的真实语料复制到临时目录（服务会把
// 索引数据库写进 vault，绝不能弄脏
// testdata）。
func copyRealVault(t testing.TB) string {
	t.Helper()
	requireRealVault(t)
	dst := filepath.Join(t.TempDir(), "realvault")
	src := filepath.Join("testdata", "realvault")
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

// realService 把真实语料的副本同步进一个全新的服务，并把摄入延迟
// （写入到可检索：解析 + 可选嵌入向量 + 索引事务）连同 chunk 计数
// 一起打日志。
func realService(t testing.TB, emb embed.Embedder) (*kb.Service, map[string]int) {
	t.Helper()
	dir := copyRealVault(t)
	idx, err := index.Open(filepath.Join(t.TempDir(), "kb.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	svc := kb.NewService(dir, idx, emb, f64ptr(1), f64ptr(1), 10, 50)
	start := time.Now()
	if err := svc.Sync(); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	counts, err := idx.ChunkCounts()
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, n := range counts {
		total += n
	}
	t.Logf("ingest: %d files, %d chunks, sync(write→searchable) %v", len(counts), total, elapsed)
	return svc, counts
}

// loadRealGold 读取真实语料的 gold 题集。
func loadRealGold(t testing.TB) *eval.GoldSet {
	t.Helper()
	requireRealVault(t)
	g, err := eval.LoadGold(filepath.Join("testdata", "realgold.json"))
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// TestRealVaultFTSOnly 是确定性腿（无嵌入向量器）：对真实
// 语料的纯 FTS 检索。多跳题与目标文档零关键词重叠，FTS
// 最多只能碰巧排中它们；单跳题可关键词作答，必须被
// 找到。
func TestRealVaultFTSOnly(t *testing.T) {
	gold := loadRealGold(t)
	svc, _ := realService(t, nil)
	report, err := eval.Run(context.Background(), "real-fts-only", gold.Questions, ftsOnly, svc.Search)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("\n%s", report)
	for _, res := range report.Misses() {
		t.Logf("fts miss: %q want %v got %v", res.Question.Query, res.Question.Expect, res.Hits)
	}
	single := report.Groups["single"]
	if single == nil || single.Top5 < 10 {
		t.Fatalf("fts-only single top-5 = %+v; want ≥10/%d (keyword questions must be findable)",
			single, len(gold.Questions)/2)
	}
	if multi := report.Groups["multi"]; multi != nil && multi.Top5 > 2 {
		t.Fatalf("fts-only multi top-5 = %d; want ≤2 (zero-overlap design)", multi.Top5)
	}
}

// TestRealVaultRealEmbedder 用真实的 OpenAI 兼容嵌入向量服务跑真实语料
// gold 题集；缺少 HX_EMBED_* 环境时自动跳过。纯 FTS 与混合检索一并
// 报告，作为对照组。
func TestRealVaultRealEmbedder(t *testing.T) {
	if os.Getenv("HX_EMBED_BASE_URL") == "" || os.Getenv("HX_EMBED_MODEL") == "" {
		t.Skip("HX_EMBED_BASE_URL / HX_EMBED_MODEL not set")
	}
	p, err := embed.New(embed.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	gold := loadRealGold(t)
	svc, _ := realService(t, p)
	ctx := context.Background()

	ftsReport, err := eval.Run(ctx, "real-fts-only", gold.Questions, ftsOnly, svc.Search)
	if err != nil {
		t.Fatal(err)
	}
	hybReport, err := eval.Run(ctx, "real-hybrid", gold.Questions, hybrid, svc.Search)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("\n%s", ftsReport)
	t.Logf("\n%s", hybReport)
	for _, res := range hybReport.Misses() {
		t.Logf("hybrid miss: %q want %v got %v", res.Question.Query, res.Question.Expect, res.Hits)
	}
	if hybReport.Total.Top5 == 0 {
		t.Fatal("real embedder found nothing at all; endpoint or wiring broken")
	}
	if hybReport.Groups["multi"].Top5 <= ftsReport.Groups["multi"].Top5 {
		t.Logf("note: hybrid multi top-5 %d did not exceed fts-only %d",
			hybReport.Groups["multi"].Top5, ftsReport.Groups["multi"].Top5)
	}
}
