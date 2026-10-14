package toolgate_test

import (
	"encoding/json"
	"runtime"
	"strings"
	"testing"
	"time"

	"hx/core/event"
	"hx/core/fiber"
	"hx/core/plugin"
	"hx/llm"
	"hx/prompt"
	"hx/toolgate"
	"hx/tools"
)

// fakeTool 是注册表用的确定性替身。
type fakeTool struct {
	name string
	desc string
}

func (t fakeTool) Name() string { return t.name }
func (t fakeTool) Schema() map[string]any {
	return map[string]any{"description": t.desc, "type": "object"}
}
func (fakeTool) Execute(_ *fiber.Context, _ json.RawMessage) (any, error) { return "ok", nil }

// wire 搭一棵 root → tools/prompt/plugin:toolgate 的 fiber 树，
// 返回 root 上下文、toolgate 的 fiber（供 Dispose）与两个服务。
func wire(t *testing.T, cfg map[string]any) (*fiber.Context, *fiber.Fiber, *tools.Registry, *prompt.Prompt) {
	t.Helper()
	root := fiber.New()
	ctx := root.Context()
	t.Cleanup(func() { _ = root.Dispose() })
	for _, name := range []string{"tools", "prompt"} {
		if err := plugin.Mount(ctx, name, nil); err != nil {
			t.Fatalf("mount %s: %v", name, err)
		}
	}
	fb, err := root.New("plugin:toolgate")
	if err != nil {
		t.Fatal(err)
	}
	if err := toolgate.Plugin().Apply(fb.Context(), cfg); err != nil {
		t.Fatalf("apply toolgate: %v", err)
	}
	fb.Activate()
	reg, ok := fiber.Get[*tools.Registry](ctx, "tools")
	if !ok {
		t.Fatal("tools service unavailable")
	}
	p, ok := fiber.Get[*prompt.Prompt](ctx, "prompt")
	if !ok {
		t.Fatal("prompt service unavailable")
	}
	return ctx, fb, reg, p
}

// addTools 把替身工具挂进注册表，随测试结束摘除。
func addTools(t *testing.T, reg *tools.Registry, ts ...tools.Tool) {
	t.Helper()
	for _, tool := range ts {
		d := reg.Register(tool)
		t.Cleanup(func() { _ = d() })
	}
}

// schemaNames 返回 schema 列表的名字序列。
func schemaNames(schemas []llm.ToolSchema) []string {
	out := make([]string, 0, len(schemas))
	for _, s := range schemas {
		out = append(out, s.Name)
	}
	return out
}

// assembleNames 组装一次并返回工具名序列。
func assembleNames(t *testing.T, p *prompt.Prompt, c *fiber.Context) []string {
	t.Helper()
	_, schemas, err := p.Assemble(c)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	return schemaNames(schemas)
}

func equalNames(got []string, want ...string) bool {
	return strings.Join(got, ",") == strings.Join(want, ",")
}

func TestBelowThresholdPassThrough(t *testing.T) {
	ctx, _, reg, p := wire(t, map[string]any{"max_inline": 10})
	addTools(t, reg, fakeTool{"alpha", "the alpha tool"}, fakeTool{"beta", "the beta tool"})

	if got := assembleNames(t, p, ctx); !equalNames(got, "alpha", "beta", "tool_search") {
		t.Fatalf("schemas = %v; want all tools passed through", got)
	}
}

func TestOverThresholdFilters(t *testing.T) {
	ctx, _, reg, p := wire(t, map[string]any{"max_inline": 2, "always": []any{"alpha"}})
	addTools(t, reg,
		fakeTool{"alpha", "the alpha tool"},
		fakeTool{"beta", "the beta tool"},
		fakeTool{"gamma", "the gamma tool"},
	)

	if got := assembleNames(t, p, ctx); !equalNames(got, "alpha", "tool_search") {
		t.Fatalf("schemas = %v; want always + tool_search only", got)
	}
}

func TestEmptyMountZeroChange(t *testing.T) {
	ctx, _, reg, p := wire(t, map[string]any{"max_inline": 0})
	addTools(t, reg,
		fakeTool{"alpha", "the alpha tool"},
		fakeTool{"beta", "the beta tool"},
		fakeTool{"gamma", "the gamma tool"},
	)

	if _, ok := reg.Get("tool_search"); ok {
		t.Fatal("tool_search must not be registered on an empty mount")
	}
	if got := assembleNames(t, p, ctx); !equalNames(got, "alpha", "beta", "gamma") {
		t.Fatalf("schemas = %v; want zero behavior change", got)
	}
}

func TestActivationFlow(t *testing.T) {
	ctx, _, reg, p := wire(t, map[string]any{"max_inline": 2, "always": []any{"alpha"}})
	addTools(t, reg,
		fakeTool{"alpha", "the alpha tool"},
		fakeTool{"beta", "the beta tool"},
		fakeTool{"gamma", "the gamma tool"},
	)

	// 会话键是最近的 "agent:" 前缀祖先（agentloop 的命名约定）。
	s1, err := ctx.Fiber().New("agent:s1")
	if err != nil {
		t.Fatal(err)
	}
	s1.Activate()
	s1ctx := s1.Context()

	if got := assembleNames(t, p, s1ctx); !equalNames(got, "alpha", "tool_search") {
		t.Fatalf("before search: schemas = %v; want beta filtered out", got)
	}

	res := reg.Execute(s1ctx, llm.ToolCall{Name: "tool_search", Arguments: `{"query":"beta"}`})
	if res.Error != "" {
		t.Fatalf("tool_search error: %s", res.Error)
	}
	if !strings.Contains(res.Content, `"name":"beta"`) {
		t.Fatalf("tool_search result = %s; want beta's full schema", res.Content)
	}

	if got := assembleNames(t, p, s1ctx); !equalNames(got, "alpha", "tool_search", "beta") {
		t.Fatalf("after search: schemas = %v; want beta appended at the tail", got)
	}

	// 顺序稳定性：无新激活时两次组装的 schema 列表逐字节相等。
	_, first, err := p.Assemble(s1ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, second, err := p.Assemble(s1ctx)
	if err != nil {
		t.Fatal(err)
	}
	raw1, _ := json.Marshal(first)
	raw2, _ := json.Marshal(second)
	if string(raw1) != string(raw2) {
		t.Fatalf("assemble not stable:\nfirst  %s\nsecond %s", raw1, raw2)
	}

	// 另一个会话看不到 s1 的激活集合。
	s2, err := ctx.Fiber().New("agent:s2")
	if err != nil {
		t.Fatal(err)
	}
	s2.Activate()
	if got := assembleNames(t, p, s2.Context()); !equalNames(got, "alpha", "tool_search") {
		t.Fatalf("s2 schemas = %v; want s1's activation isolated", got)
	}
}

func TestWaterfallChainNext(t *testing.T) {
	ctx, _, reg, p := wire(t, map[string]any{"max_inline": 2, "always": []any{"alpha"}})
	addTools(t, reg,
		fakeTool{"alpha", "the alpha tool"},
		fakeTool{"beta", "the beta tool"},
		fakeTool{"gamma", "the gamma tool"},
	)

	// 下游再挂一个监听器：它必须被调到，且看到的是闸门过滤后的
	// 列表（过滤发生在调 next 之前）。
	called := false
	var seen []string
	defer event.OnWaterfall[[]llm.ToolSchema, []llm.ToolSchema](ctx, "prompt/tools",
		func(c *fiber.Context, in []llm.ToolSchema, next event.Next[[]llm.ToolSchema, []llm.ToolSchema]) ([]llm.ToolSchema, error) {
			called = true
			seen = schemaNames(in)
			return next(c, in)
		})()

	if got := assembleNames(t, p, ctx); !equalNames(got, "alpha", "tool_search") {
		t.Fatalf("schemas = %v; want filtered list through the chain", got)
	}
	if !called {
		t.Fatal("downstream listener was not called; next() chain broken")
	}
	if !equalNames(seen, "alpha", "tool_search") {
		t.Fatalf("downstream listener saw %v; want the filtered list", seen)
	}
}

func TestConfigFailLoud(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cfg     map[string]any
		wantErr string
	}{
		{"max_inline wrong type", map[string]any{"max_inline": "x"}, `"max_inline" must be an int`},
		{"search_limit wrong type", map[string]any{"search_limit": "x"}, `"search_limit" must be an int`},
		{"search_limit zero", map[string]any{"max_inline": 5, "search_limit": 0}, `"search_limit" must be positive`},
		{"search_limit negative", map[string]any{"max_inline": 5, "search_limit": -1}, `"search_limit" must be positive`},
		{"always not a list", map[string]any{"always": "alpha"}, `"always" must be a list of strings`},
		{"always non-string item", map[string]any{"always": []any{1}}, `"always"[0] must be a string`},
		{"empty mount skips range checks", map[string]any{"max_inline": 0, "search_limit": 0}, ""},
		{"valid", map[string]any{"max_inline": 3, "search_limit": 2, "always": []any{"alpha"}}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := fiber.New()
			defer root.Dispose()
			ctx := root.Context()
			if err := plugin.Mount(ctx, "tools", nil); err != nil {
				t.Fatal(err)
			}
			fb, err := root.New("plugin:toolgate")
			if err != nil {
				t.Fatal(err)
			}
			err = toolgate.Plugin().Apply(fb.Context(), tc.cfg)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("apply: %v; want success", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("apply error = %v; want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestDispose(t *testing.T) {
	settle := func() int {
		time.Sleep(50 * time.Millisecond)
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
		return runtime.NumGoroutine()
	}
	before := settle()

	ctx, fb, reg, p := wire(t, map[string]any{"max_inline": 2, "always": []any{"alpha"}})
	addTools(t, reg,
		fakeTool{"alpha", "the alpha tool"},
		fakeTool{"beta", "the beta tool"},
		fakeTool{"gamma", "the gamma tool"},
	)
	if got := assembleNames(t, p, ctx); !equalNames(got, "alpha", "tool_search") {
		t.Fatalf("schemas = %v; want filtered list before dispose", got)
	}

	if err := fb.Dispose(); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	if _, ok := reg.Get("tool_search"); ok {
		t.Fatal("tool_search must be deregistered after dispose")
	}
	if got := assembleNames(t, p, ctx); !equalNames(got, "alpha", "beta", "gamma") {
		t.Fatalf("schemas = %v; want the listener removed after dispose", got)
	}

	if after := settle(); after > before {
		t.Fatalf("goroutines: before %d, after %d; toolgate leaked", before, after)
	}
}
