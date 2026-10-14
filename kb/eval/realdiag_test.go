package eval_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"hx/kb/embed"
	"hx/kb/eval"
	"hx/kb/index"
)

// TestRealVaultMissDiagnosis 为混合检索真实嵌入向量腿的每道
// miss 题打印 top-5 命中明细（path#heading、按信号归一化的
// 分数、片段开头），以便在内容层面判定 miss：top-5 里是否有
// 某个相邻 chunk 真的携带了答案要点，还是仅仅语义相近？
// 纯诊断用途：与真实嵌入向量腿一样有环境门槛，且不对命中数
// 做任何断言。
func TestRealVaultMissDiagnosis(t *testing.T) {
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

	hybReport, err := eval.Run(ctx, "real-hybrid", gold.Questions, hybrid, svc.Search)
	if err != nil {
		t.Fatal(err)
	}
	misses := hybReport.Misses()
	if len(misses) == 0 {
		t.Skip("no hybrid misses this run; nothing to diagnose")
	}

	vecOnly := index.SearchOpts{FTSWeight: f64ptr(0), VectorWeight: f64ptr(1), Limit: 50}
	for _, res := range misses {
		q := res.Question
		t.Logf("\n=== MISS %q\n    want %v (%s)", q.Query, q.Expect, q.Note)

		ftsHits, err := svc.Search(ctx, q.Query, ftsOnly)
		if err != nil {
			t.Fatal(err)
		}
		vecHits, err := svc.Search(ctx, q.Query, vecOnly)
		if err != nil {
			t.Fatal(err)
		}
		ftsScore := scoreMap(ftsHits)
		vecScore := scoreMap(vecHits)

		hybOpts := hybrid
		hybOpts.Limit = 5
		top5, err := svc.Search(ctx, q.Query, hybOpts)
		if err != nil {
			t.Fatal(err)
		}
		for i, h := range top5 {
			key := hitKey(h)
			head := strings.Join(h.HeadingPath, " > ")
			if head == "" {
				head = "(no heading)"
			}
			snippet := []rune(h.Text)
			if len(snippet) > 100 {
				snippet = snippet[:100]
			}
			want := ""
			for _, e := range q.Expect {
				if e == h.Path {
					want = "  <== EXPECTED"
				}
			}
			t.Logf("  [%d] %s#%s\n      fts=%.3f vec=%.3f blend=%.3f%s\n      %q",
				i+1, h.Path, head, ftsScore[key], vecScore[key], h.Score, want, string(snippet))
		}
	}
}

func scoreMap(hits []index.Hit) map[string]float64 {
	m := make(map[string]float64, len(hits))
	for _, h := range hits {
		m[hitKey(h)] = h.Score
	}
	return m
}

func hitKey(h index.Hit) string {
	return fmt.Sprintf("%s#%d", h.Path, h.Idx)
}
