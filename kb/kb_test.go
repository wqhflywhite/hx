package kb_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"hx/core/fiber"
	"hx/core/plugin"
	"hx/kb"
	"hx/kb/embed"
	"hx/kb/index"
	"hx/llm"
	"hx/prompt"
	"hx/tools"
)

// mount 在全新的 root 上装配 tools + kb，vault 放在临时目录；
// 返回 context、KB 服务、工具注册表和 vault 根目录。
func mount(t *testing.T, kbCfg map[string]any) (*fiber.Context, kb.KB, *tools.Registry, string) {
	t.Helper()
	vaultDir := t.TempDir()
	root := fiber.New()
	ctx := root.Context()
	t.Cleanup(func() { _ = root.Dispose() })
	cfg := map[string]any{"vault": vaultDir, "debounce_ms": 50}
	for k, v := range kbCfg {
		cfg[k] = v
	}
	if err := plugin.Mount(ctx, "tools", nil); err != nil {
		t.Fatal(err)
	}
	if err := plugin.Mount(ctx, "kb", cfg); err != nil {
		t.Fatal(err)
	}
	svc, ok := fiber.Get[kb.KB](ctx, "kb")
	if !ok {
		t.Fatal("kb service unavailable")
	}
	reg, ok := fiber.Get[*tools.Registry](ctx, "tools")
	if !ok {
		t.Fatal("tools service unavailable")
	}
	return ctx, svc, reg, vaultDir
}

func writeNote(t *testing.T, vaultDir, rel, content string) {
	t.Helper()
	abs := filepath.Join(vaultDir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestSyncIndexesVaultAtMount(t *testing.T) {
	_, svc, _, vaultDir := mount(t, nil)
	writeNote(t, vaultDir, "papers/attention.md", "# Attention\n\nTransformers use self-attention mechanisms.\n\n## 变体\n\n稀疏注意力降低复杂度。\n")
	// 笔记是在挂载时的 Sync 之后才落盘的；再跑一遍 Sync（顺便
	// 做幂等校验：对未变化的文件跑第二次 Sync 不得报错，
	// 也不得重复）。
	if err := svc.(*kb.Service).Sync(); err != nil {
		t.Fatal(err)
	}
	if err := svc.(*kb.Service).Sync(); err != nil {
		t.Fatal(err)
	}

	hits, err := svc.Search(context.Background(), "self-attention", index.SearchOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Path != "papers/attention.md" {
		t.Fatalf("hits = %+v; want the attention note once", hits)
	}
	hits, err = svc.Search(context.Background(), "稀疏注意力", index.SearchOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || len(hits[0].HeadingPath) != 2 || hits[0].HeadingPath[1] != "变体" {
		t.Fatalf("CJK hits = %+v", hits)
	}
}

func TestReadWholeAndSection(t *testing.T) {
	_, svc, _, vaultDir := mount(t, nil)
	writeNote(t, vaultDir, "note.md", "preamble\n\n# 实验方法\n\nmethod body\n\n## 数据集\n\ndataset body\n\n# 结论\n\nconclusion body\n")
	if err := svc.(*kb.Service).Sync(); err != nil {
		t.Fatal(err)
	}

	whole, etag, err := svc.Read(context.Background(), "note.md", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(whole, "preamble") || !strings.Contains(whole, "conclusion body") {
		t.Fatalf("whole read = %q", whole)
	}
	if etag.SHA1 == "" {
		t.Fatal("read returned an empty etag")
	}

	section, _, err := svc.Read(context.Background(), "note.md", []string{"实验方法", "数据集"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(section, "dataset body") || strings.Contains(section, "method body") || strings.Contains(section, "conclusion body") {
		t.Fatalf("section read = %q", section)
	}

	if _, _, err := svc.Read(context.Background(), "note.md", []string{"不存在的标题"}); err == nil {
		t.Fatal("unknown heading accepted")
	}
	if _, _, err := svc.Read(context.Background(), "../escape.md", nil); err == nil {
		t.Fatal("vault escape accepted")
	}
}

func TestWriteOptimisticConcurrency(t *testing.T) {
	_, svc, _, vaultDir := mount(t, nil)
	writeNote(t, vaultDir, "note.md", "# H\noriginal\n")
	if err := svc.(*kb.Service).Sync(); err != nil {
		t.Fatal(err)
	}
	_, etag, err := svc.Read(context.Background(), "note.md", nil)
	if err != nil {
		t.Fatal(err)
	}

	// 并发写入者先落盘；过期的 etag 必须被拒绝并提示重新读取，
	// 且文件必须保留并发写入的内容。
	writeNote(t, vaultDir, "note.md", "# H\nconcurrent edit\n")
	if _, err := svc.Write(context.Background(), "note.md", []byte("# H\nstale edit\n"), etag); err == nil {
		t.Fatal("stale etag accepted")
	} else if !strings.Contains(err.Error(), "文件已被修改，请重新 kb_read 后重试") {
		t.Fatalf("conflict error = %q; want the re-read hint", err)
	}
	data, err := os.ReadFile(filepath.Join(vaultDir, "note.md"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "# H\nconcurrent edit\n" {
		t.Fatalf("conflicted write clobbered the file: %q", data)
	}

	// 重新读取后重试：写入落盘、返回新的 etag，
	// 且立即可以被检索到。
	_, etag, err = svc.Read(context.Background(), "note.md", nil)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := svc.Write(context.Background(), "note.md", []byte("# H\nretried edit\n"), etag)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.SHA1 == etag.SHA1 {
		t.Fatal("write returned the pre-write etag")
	}
	hits, err := svc.Search(context.Background(), "retried", index.SearchOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 {
		t.Fatalf("written content not searchable immediately: %+v", hits)
	}

	// 创建新笔记必须使用零值 etag。
	if _, err := svc.Write(context.Background(), "new.md", []byte("brand new\n"), etag); err == nil {
		t.Fatal("create with a non-zero etag accepted")
	}
	if _, err := svc.Write(context.Background(), "new.md", []byte("brand new\n"), kb.ETag{}); err != nil {
		t.Fatal(err)
	}
}

func TestBacklinks(t *testing.T) {
	_, svc, _, vaultDir := mount(t, nil)
	writeNote(t, vaultDir, "a.md", "see [[b|别名]] and [[c#锚点]]\n")
	writeNote(t, vaultDir, "d.md", "also see [[b]]\n")
	if err := svc.(*kb.Service).Sync(); err != nil {
		t.Fatal(err)
	}
	bl, err := svc.Backlinks(context.Background(), "b")
	if err != nil {
		t.Fatal(err)
	}
	if len(bl) != 2 || bl[0] != "a.md" || bl[1] != "d.md" {
		t.Fatalf("backlinks = %v; want [a.md d.md]", bl)
	}
}

// TestWatcherIndexesAfterDebounce 在挂载后写入一篇笔记，断言
// 索引会在一个宽松的上限内追上（防抖为 50ms）。
func TestWatcherIndexesAfterDebounce(t *testing.T) {
	_, svc, _, vaultDir := mount(t, nil)
	writeNote(t, vaultDir, "fresh.md", "# Fresh\n\ndebounced watcher content\n")

	deadline := time.Now().Add(5 * time.Second)
	for {
		hits, err := svc.Search(context.Background(), "debounced", index.SearchOpts{})
		if err != nil {
			t.Fatal(err)
		}
		if len(hits) == 1 && hits[0].Path == "fresh.md" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("watcher did not index fresh.md within 5s (hits = %+v)", hits)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestWatcherPrunesDeletedNote(t *testing.T) {
	_, svc, _, vaultDir := mount(t, nil)
	writeNote(t, vaultDir, "doomed.md", "# Doomed\n\ndeletable content\n")
	if err := svc.(*kb.Service).Sync(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(vaultDir, "doomed.md")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		hits, err := svc.Search(context.Background(), "deletable", index.SearchOpts{})
		if err != nil {
			t.Fatal(err)
		}
		if len(hits) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("watcher did not prune doomed.md within 5s (hits = %+v)", hits)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestTools(t *testing.T) {
	ctx, _, reg, vaultDir := mount(t, nil)
	writeNote(t, vaultDir, "note.md", "# Go\n\nGo 是静态类型语言。\n")
	svc, _ := fiber.Get[kb.KB](ctx, "kb")
	if err := svc.(*kb.Service).Sync(); err != nil {
		t.Fatal(err)
	}

	res := reg.Execute(context.Background(), ctx, llm.ToolCall{Name: "kb_search", Arguments: `{"query":"静态类型"}`})
	if res.Error != "" {
		t.Fatalf("kb_search failed: %s", res.Error)
	}
	if !strings.Contains(res.Content, "note.md#Go") || !strings.Contains(res.Content, "score 1.00") {
		t.Fatalf("kb_search = %q", res.Content)
	}

	res = reg.Execute(context.Background(), ctx, llm.ToolCall{Name: "kb_read", Arguments: `{"path":"note.md","heading":["Go"]}`})
	if res.Error != "" {
		t.Fatalf("kb_read failed: %s", res.Error)
	}
	if !strings.Contains(res.Content, "静态类型语言") || !strings.Contains(res.Content, "[etag] sha1=") {
		t.Fatalf("kb_read = %q", res.Content)
	}

	// 用 kb_read 输出里的 etag 驱动 kb_write。
	marker := res.Content[strings.LastIndex(res.Content, "[etag] "):]
	var sha1 string
	var mtime int64
	if n, err := fmt.Sscanf(marker, "[etag] sha1=%s mtime=%d", &sha1, &mtime); err != nil || n != 2 {
		t.Fatalf("etag parse failed: %d %v", n, err)
	}
	args, _ := json.Marshal(map[string]any{"path": "note.md", "content": "# Go\n\nGo 是静态类型语言，并发是头等公民。\n", "sha1": sha1, "mtime": mtime})
	res = reg.Execute(context.Background(), ctx, llm.ToolCall{Name: "kb_write", Arguments: string(args)})
	if res.Error != "" {
		t.Fatalf("kb_write failed: %s", res.Error)
	}
	if !strings.Contains(res.Content, "new etag sha1=") {
		t.Fatalf("kb_write = %q", res.Content)
	}

	res = reg.Execute(context.Background(), ctx, llm.ToolCall{Name: "kb_search", Arguments: `{"query":"头等公民"}`})
	if res.Error != "" || !strings.Contains(res.Content, "note.md") {
		t.Fatalf("written content not searchable: %q / %s", res.Content, res.Error)
	}
}

func TestConfigFailLoud(t *testing.T) {
	for name, cfg := range map[string]map[string]any{
		"missing vault":     {},
		"non-string vault":  {"vault": 42},
		"non-int debounce":  {"vault": t.TempDir(), "debounce_ms": "fast"},
		"missing vault dir": {"vault": filepath.Join(t.TempDir(), "nope")},
		"non-number weight": {"vault": t.TempDir(), "fts_weight": "high"},
		"zero search_limit": {"vault": t.TempDir(), "search_limit": 0},
	} {
		t.Run(name, func(t *testing.T) {
			root := fiber.New()
			ctx := root.Context()
			t.Cleanup(func() { _ = root.Dispose() })
			if err := plugin.Mount(ctx, "tools", nil); err != nil {
				t.Fatal(err)
			}
			if err := plugin.Mount(ctx, "kb", cfg); err == nil {
				t.Fatalf("config %v accepted; want fail loud", cfg)
			}
		})
	}
}

// TestNilConfigIsNoop 覆盖 flatWire 挂载：完全不带配置时
// 插件被禁用，而不是启动失败。
func TestNilConfigIsNoop(t *testing.T) {
	root := fiber.New()
	ctx := root.Context()
	t.Cleanup(func() { _ = root.Dispose() })
	if err := plugin.Mount(ctx, "tools", nil); err != nil {
		t.Fatal(err)
	}
	if err := plugin.Mount(ctx, "kb", nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := fiber.Get[kb.KB](ctx, "kb"); ok {
		t.Fatal("kb service provided without a vault config")
	}
}

func TestDisposeReclaimsToolsAndWatcher(t *testing.T) {
	root := fiber.New()
	ctx := root.Context()
	t.Cleanup(func() { _ = root.Dispose() })
	if err := plugin.Mount(ctx, "tools", nil); err != nil {
		t.Fatal(err)
	}
	reg, _ := fiber.Get[*tools.Registry](ctx, "tools")

	before := runtime.NumGoroutine()
	fb, err := root.New("plugin:kb")
	if err != nil {
		t.Fatal(err)
	}
	if err := kb.Plugin().Apply(fb.Context(), map[string]any{"vault": t.TempDir(), "debounce_ms": 50}); err != nil {
		t.Fatal(err)
	}
	fb.Activate()
	for _, name := range []string{"kb_search", "kb_read", "kb_write"} {
		if _, ok := reg.Get(name); !ok {
			t.Fatalf("%s not registered", name)
		}
	}

	if err := fb.Dispose(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"kb_search", "kb_read", "kb_write"} {
		if _, ok := reg.Get(name); ok {
			t.Fatalf("%s survived dispose", name)
		}
	}
	// watcher goroutine 随 fiber 结束；给它留一点退出时间。
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > before {
		t.Fatalf("goroutines after dispose = %d; want <= %d (watcher leaked)", got, before)
	}
}

func f64ptr(v float64) *float64 { return &v }

// TestHybridSearchWithFakeEmbedder 对着确定性的假 embedder 跑
// 完整服务链路（sync 给 chunk 转嵌入向量，search 给查询转
// 嵌入向量）：向量权重占优时，零关键词重叠的笔记应当胜出。
func TestHybridSearchWithFakeEmbedder(t *testing.T) {
	vaultDir := t.TempDir()
	idx, err := index.Open(filepath.Join(t.TempDir(), "kb.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	fake := embed.Fake{Dim: 64, Synonyms: map[string]string{"电动车": "car", "车辆": "car"}}

	writeNote(t, vaultDir, "kw.md", "电动车 续航 实测 数据 分析\n")
	writeNote(t, vaultDir, "sem.md", "车辆 保养\n")

	// 配了 embedder 但权重为 nil：索引默认值让检索保持纯 FTS。
	svc := kb.NewService(vaultDir, idx, fake, nil, nil, 10, 50)
	if err := svc.Sync(); err != nil {
		t.Fatal(err)
	}
	hits, err := svc.Search(context.Background(), "电动车", index.SearchOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Path != "kw.md" {
		t.Fatalf("nil-weight hits = %+v; want only kw.md", hits)
	}

	// 向量权重占优时，零重叠的同义词笔记应当胜出：假 embedder
	// 把 电动车 和 车辆 都归一化为 "car"，而 sem.md 的词项
	// 组合更短，余弦相似度更高。
	svc = kb.NewService(vaultDir, idx, fake, f64ptr(1), f64ptr(2), 10, 50)
	hits, err = svc.Search(context.Background(), "电动车", index.SearchOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 || hits[0].Path != "sem.md" || hits[1].Path != "kw.md" {
		t.Fatalf("hybrid hits = %+v; want sem.md then kw.md", hits)
	}
}

// TestEmbedderConfig 挂载带 embedder 配置节的 kb，embedder
// 指向一个假 OpenAI 兼容服务：sync 把笔记 chunk 一批转入
// 嵌入向量，search 把查询转入嵌入向量。
func TestEmbedderConfig(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var body struct {
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("bad request body: %v", err)
		}
		var rows []string
		for i := range body.Input {
			rows = append(rows, fmt.Sprintf(`{"index":%d,"embedding":[1,1,1,1]}`, i))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[` + strings.Join(rows, ",") + `]}`))
	}))
	defer srv.Close()

	_, svc, _, vaultDir := mount(t, map[string]any{
		"embedder": map[string]any{"base_url": srv.URL, "model": "m"},
	})
	writeNote(t, vaultDir, "note.md", "电动车 续航 实测\n")
	if err := svc.(*kb.Service).Sync(); err != nil {
		t.Fatal(err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("embed calls after sync = %d; want 1 batch", n)
	}
	hits, err := svc.Search(context.Background(), "续航", index.SearchOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Path != "note.md" {
		t.Fatalf("hits = %+v; want note.md", hits)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("embed calls after search = %d; want sync batch + query", n)
	}
}

// TestEmbedderConfigFailLoud 覆盖格式非法的 embedder 配置节。
func TestEmbedderConfigFailLoud(t *testing.T) {
	t.Setenv("HX_EMBED_BASE_URL", "")
	t.Setenv("HX_EMBED_API_KEY", "")
	t.Setenv("HX_EMBED_MODEL", "")
	for name, embedder := range map[string]any{
		"non-map embedder": 42,
		"missing model":    map[string]any{"base_url": "http://127.0.0.1:1"},
		"non-string model": map[string]any{"base_url": "http://127.0.0.1:1", "model": 7},
	} {
		t.Run(name, func(t *testing.T) {
			root := fiber.New()
			ctx := root.Context()
			t.Cleanup(func() { _ = root.Dispose() })
			if err := plugin.Mount(ctx, "tools", nil); err != nil {
				t.Fatal(err)
			}
			cfg := map[string]any{"vault": t.TempDir(), "embedder": embedder}
			if err := plugin.Mount(ctx, "kb", cfg); err == nil {
				t.Fatalf("embedder %v accepted; want fail loud", embedder)
			}
		})
	}
}

// TestLint 检验确定性的健康检查：一条断链 wikilink、一篇孤儿
// wiki 页（raw/ 豁免）、一个未索引文件和一篇超长笔记。
func TestLint(t *testing.T) {
	vaultDir := t.TempDir()
	idx, err := index.Open(filepath.Join(t.TempDir(), "kb.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	svc := kb.NewService(vaultDir, idx, nil, nil, nil, 10, 3)

	writeNote(t, vaultDir, "wiki/a.md", "# A\n\nsee [[wiki/b]] and [[wiki/missing]]\n")
	writeNote(t, vaultDir, "wiki/b.md", "# B\n\nlinked from a, links back to [[wiki/a]]\n")
	writeNote(t, vaultDir, "wiki/orphan.md", "# Orphan\n\nnothing links here\n")
	writeNote(t, vaultDir, "raw/source.md", "source material, exempt from the orphan check\n")
	writeNote(t, vaultDir, "raw/long.md", "# H1\n\nx\n\n# H2\n\nx\n\n# H3\n\nx\n\n# H4\n\nx\n")
	if err := svc.Sync(); err != nil {
		t.Fatal(err)
	}
	// 在 sync 之后才落盘；本测试不跑 watcher。
	writeNote(t, vaultDir, "raw/new.md", "not yet indexed\n")

	report, err := svc.Lint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.BrokenLinks) != 1 || report.BrokenLinks[0].Src != "wiki/a.md" || report.BrokenLinks[0].Dst != "wiki/missing" {
		t.Fatalf("broken links = %+v; want wiki/a.md -> wiki/missing", report.BrokenLinks)
	}
	if len(report.Orphans) != 1 || report.Orphans[0] != "wiki/orphan.md" {
		t.Fatalf("orphans = %v; want [wiki/orphan.md]", report.Orphans)
	}
	if len(report.Unindexed) != 1 || report.Unindexed[0] != "raw/new.md" {
		t.Fatalf("unindexed = %v; want [raw/new.md]", report.Unindexed)
	}
	if len(report.LongNotes) != 1 || report.LongNotes[0].Path != "raw/long.md" || report.LongNotes[0].Chunks != 4 {
		t.Fatalf("long notes = %+v; want raw/long.md with 4 chunks", report.LongNotes)
	}

	// 修好链接目标并对新文件重新 sync 后，除了孤儿和超长笔记
	// 之外 vault 应当健康。
	writeNote(t, vaultDir, "wiki/a.md", "# A\n\nsee [[wiki/b]]\n")
	if err := svc.Sync(); err != nil {
		t.Fatal(err)
	}
	report, err = svc.Lint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.BrokenLinks) != 0 || len(report.Unindexed) != 0 {
		t.Fatalf("after fix: broken = %+v unindexed = %v; want both empty", report.BrokenLinks, report.Unindexed)
	}
}

// TestLintTool 通过工具注册表驱动 kb_lint。
func TestLintTool(t *testing.T) {
	_, svc, reg, vaultDir := mount(t, nil)

	res := reg.Execute(context.Background(), fiber.New().Context(), llm.ToolCall{Name: "kb_lint", Arguments: `{}`})
	if res.Error != "" || !strings.Contains(res.Content, "vault healthy") {
		t.Fatalf("empty vault: %q / %s", res.Content, res.Error)
	}
	res = reg.Execute(context.Background(), fiber.New().Context(), llm.ToolCall{Name: "kb_lint", Arguments: `{"bogus": 1}`})
	if res.Error == "" {
		t.Fatal("kb_lint accepted arguments")
	}

	writeNote(t, vaultDir, "wiki/a.md", "# A\n\nsee [[wiki/missing]]\n")
	if err := svc.(*kb.Service).Sync(); err != nil {
		t.Fatal(err)
	}
	ctx := fiber.New().Context()
	defer func() { _ = ctx.Fiber().Dispose() }()
	res = reg.Execute(context.Background(), ctx, llm.ToolCall{Name: "kb_lint", Arguments: `{}`})
	if res.Error != "" {
		t.Fatalf("kb_lint failed: %s", res.Error)
	}
	if !strings.Contains(res.Content, "broken links (1)") || !strings.Contains(res.Content, "wiki/a.md -> [[wiki/missing]]") ||
		!strings.Contains(res.Content, "orphan notes (1)") {
		t.Fatalf("kb_lint = %q", res.Content)
	}
}

// TestLibrarianPromptSection 先于 prompt 插件挂载 kb：librarian
// section 应当在 prompt 服务出现后落地，并在 kb fiber 被
// dispose（撤销）时移除。
func TestLibrarianPromptSection(t *testing.T) {
	vaultDir := t.TempDir()
	root := fiber.New()
	ctx := root.Context()
	t.Cleanup(func() { _ = root.Dispose() })
	if err := plugin.Mount(ctx, "tools", nil); err != nil {
		t.Fatal(err)
	}
	fb, err := root.New("plugin:kb")
	if err != nil {
		t.Fatal(err)
	}
	if err := kb.Plugin().Apply(fb.Context(), map[string]any{"vault": vaultDir, "debounce_ms": 50}); err != nil {
		t.Fatal(err)
	}
	fb.Activate()
	// prompt 插件在 kb 之后才挂载；section 仍必须落地。
	if err := plugin.Mount(ctx, "prompt", nil); err != nil {
		t.Fatal(err)
	}
	p, ok := fiber.Get[*prompt.Prompt](ctx, "prompt")
	if !ok {
		t.Fatal("prompt service unavailable")
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		text, _, err := p.Assemble(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(text, "知识库写作规程") && strings.Contains(text, "kb_write") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("librarian section did not land within 5s; prompt = %q", text)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err := fb.Dispose(); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		text, _, err := p.Assemble(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(text, "知识库写作规程") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("librarian section survived the kb dispose")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
