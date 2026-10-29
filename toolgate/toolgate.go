// Package toolgate 提供检索式工具加载：注册表工具总数超过阈值时
// 只内联核心工具，其余由模型调 tool_search 元工具检索后按需激活。
package toolgate

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"unicode"

	"hx/core/event"
	"hx/core/fiber"
	"hx/core/plugin"
	"hx/llm"
	"hx/tools"
)

const (
	// defaultMaxInline 是配置未设 max_inline 时的内联阈值。
	defaultMaxInline = 20
	// defaultSearchLimit 是配置未设 search_limit 时单次检索的命中上限。
	defaultSearchLimit = 5
	// searchToolName 是元工具的名字；它永远在放行集合里。
	searchToolName = "tool_search"
)

// gate 持有插件状态：过滤配置与各会话的激活集合。
type gate struct {
	reg       *tools.Registry
	maxInline int
	limit     int
	always    map[string]bool

	mu     sync.Mutex
	active map[*fiber.Fiber][]string // 会话键 → 按激活先后排序的工具名，只增不减
}

// sessionKey 取调用方最近的 "agent:" 前缀祖先（含自身）作为会话键
// （subagent 的 delegationDepth 先例）；链上没有 agent fiber——比如
// 测试把工具直接挂在 root 下——时退化为树根，整棵树共享一份
// 激活集合。
func sessionKey(f *fiber.Fiber) *fiber.Fiber {
	root := f
	for cur := f; cur != nil; cur = cur.Parent() {
		if strings.HasPrefix(cur.Name(), "agent:") {
			return cur
		}
		root = cur
	}
	return root
}

// activated 返回 key 的激活列表副本。首次见到 key 时在锁外登记
// 清理，避免 fiber Dispose 同步执行清理器时与 g.mu 自死锁；
// 已撤销的会话不再发布新条目。
func (g *gate) activated(key *fiber.Fiber) ([]string, bool) {
	g.mu.Lock()
	names, ok := g.active[key]
	g.mu.Unlock()
	if ok {
		return append([]string(nil), names...), true
	}
	if err := key.Context().Effect(func(*fiber.Context) (fiber.Disposer, error) {
		return func() error {
			g.mu.Lock()
			defer g.mu.Unlock()
			delete(g.active, key)
			return nil
		}, nil
	}); err != nil {
		return nil, false
	}
	g.mu.Lock()
	names, ok = g.active[key]
	if !ok {
		g.active[key] = nil
	}
	g.mu.Unlock()
	return append([]string(nil), names...), true
}

// activate 把 names 按序并入 key 的激活集合（已激活的不重复）。
func (g *gate) activate(key *fiber.Fiber, names []string) {
	if _, ok := g.activated(key); !ok {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	cur := g.active[key]
	for _, n := range names {
		if !slices.Contains(cur, n) {
			cur = append(cur, n)
		}
	}
	g.active[key] = cur
}

// filter 是 prompt/tools 瀑布（waterfall）监听器：工具总数不超过
// maxInline 时原样透传，否则只放行 always ∩ 现有、tool_search
// 自身与该会话已激活的工具。基础集合保持 Schemas() 的名字序，
// 激活工具按激活先后追加在尾部，且会话内集合只增不减，保证同一
// 会话相邻两次组装间前缀稳定（KV cache 友好）。
func (g *gate) filter(ctx context.Context, c *fiber.Context, in []llm.ToolSchema, next event.Next[[]llm.ToolSchema, []llm.ToolSchema]) ([]llm.ToolSchema, error) {
	if len(in) <= g.maxInline {
		return next(ctx, c, in)
	}
	byName := make(map[string]llm.ToolSchema, len(in))
	out := make([]llm.ToolSchema, 0, len(in))
	for _, s := range in {
		byName[s.Name] = s
		if s.Name == searchToolName || g.always[s.Name] {
			out = append(out, s)
		}
	}
	activated, _ := g.activated(sessionKey(c.Fiber()))
	for _, name := range activated {
		if s, ok := byName[name]; ok && name != searchToolName && !g.always[name] {
			out = append(out, s)
		}
	}
	return next(ctx, c, out)
}

// search 对注册表全部工具按 query 打分并取前 limit 个：大小写
// 不敏感的词元重叠，名字命中权重高于描述，同分按名字序；
// tool_search 自身不参与结果。
func (g *gate) search(query string) []llm.ToolSchema {
	tokens := strings.FieldsFunc(strings.ToLower(query), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	type scored struct {
		schema llm.ToolSchema
		score  int
	}
	var hits []scored
	for _, s := range g.reg.Schemas() {
		if s.Name == searchToolName {
			continue
		}
		name := strings.ToLower(s.Name)
		desc := strings.ToLower(s.Description)
		score := 0
		for _, tok := range tokens {
			if strings.Contains(name, tok) {
				score += 2
			}
			if strings.Contains(desc, tok) {
				score++
			}
		}
		if score > 0 {
			hits = append(hits, scored{s, score})
		}
	}
	sort.SliceStable(hits, func(i, j int) bool {
		if hits[i].score != hits[j].score {
			return hits[i].score > hits[j].score
		}
		return hits[i].schema.Name < hits[j].schema.Name
	})
	if len(hits) > g.limit {
		hits = hits[:g.limit]
	}
	out := make([]llm.ToolSchema, 0, len(hits))
	for _, h := range hits {
		out = append(out, h.schema)
	}
	return out
}

// searchTool 是 tool_search 元工具：搜到即激活，并返回命中工具的
// 完整 schema JSON，模型下一 step 即可直接按名调用。
type searchTool struct{ gate *gate }

// Name 实现 tools.Tool。
func (searchTool) Name() string { return searchToolName }

// Schema 实现 tools.Tool。
func (searchTool) Schema() map[string]any {
	return map[string]any{
		"description": "Search the full tool catalog by keyword and activate the matched tools for the rest of this session. Returns the complete schemas of the matches; call them directly by name afterwards. Use when the current tool list lacks a capability the task needs.",
		"type":        "object",
		"properties": map[string]any{
			"query": map[string]any{"type": "string", "description": "Keywords to match against tool names and descriptions."},
		},
		"required": []string{"query"},
	}
}

type searchArgs struct {
	Query string `json:"query"`
}

// Execute 实现 tools.Tool。
func (t searchTool) Execute(_ context.Context, c *fiber.Context, args json.RawMessage) (any, error) {
	var a searchArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, fmt.Errorf("tool_search: bad arguments: %w", err)
	}
	if strings.TrimSpace(a.Query) == "" {
		return nil, fmt.Errorf("tool_search: empty query")
	}
	matches := t.gate.search(a.Query)
	names := make([]string, 0, len(matches))
	for _, s := range matches {
		names = append(names, s.Name)
	}
	g := t.gate
	g.activate(sessionKey(c.Fiber()), names)
	raw, err := json.Marshal(matches)
	if err != nil {
		return nil, err
	}
	return string(raw), nil
}

// configInt 解析可选整数键；类型不对即响亮报错。
func configInt(cfg map[string]any, key string, def int) (int, error) {
	v, ok := cfg[key]
	if !ok || v == nil {
		return def, nil
	}
	i, ok := v.(int)
	if !ok {
		return 0, fmt.Errorf("toolgate: %q must be an int, got %T", key, v)
	}
	return i, nil
}

// configStrings 解析可选字符串列表键（YAML 解出 []any，程序化
// 装配可能给 []string）；类型不对即响亮报错。
func configStrings(cfg map[string]any, key string) ([]string, error) {
	v, ok := cfg[key]
	if !ok || v == nil {
		return nil, nil
	}
	if ss, ok := v.([]string); ok {
		return ss, nil
	}
	raw, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("toolgate: %q must be a list of strings, got %T", key, v)
	}
	out := make([]string, 0, len(raw))
	for i, item := range raw {
		s, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("toolgate: %q[%d] must be a string, got %T", key, i, item)
		}
		out = append(out, s)
	}
	return out, nil
}

// Plugin 挂载工具闸门：注册 tool_search 元工具并监听 prompt/tools
// 瀑布（waterfall）。配置键："max_inline"（int，默认 20；≤0 时
// 插件空挂载——不注册工具也不挂监听器，行为与未挂载完全一致）、
// "always"（字符串列表，始终内联的工具名；挂载期注册表可能还没
// 挂全工具，故不校验存在性，过滤时按名匹配）、"search_limit"
// （int，默认 5，必须为正——tool_search 单次最多返回并激活的
// 工具数）。
func Plugin() plugin.Plugin { return gatePlugin{} }

type gatePlugin struct{}

func (gatePlugin) Name() string     { return "toolgate" }
func (gatePlugin) Inject() []string { return []string{"tools"} }
func (gatePlugin) Apply(c *fiber.Context, cfg map[string]any) error {
	maxInline, err := configInt(cfg, "max_inline", defaultMaxInline)
	if err != nil {
		return err
	}
	limit, err := configInt(cfg, "search_limit", defaultSearchLimit)
	if err != nil {
		return err
	}
	always, err := configStrings(cfg, "always")
	if err != nil {
		return err
	}
	if maxInline <= 0 {
		// 空挂载：不注册任何副作用，其余配置键的取值不再校验。
		return nil
	}
	if limit <= 0 {
		return fmt.Errorf("toolgate: %q must be positive, got %d", "search_limit", limit)
	}
	// 瀑布在 agent fiber 上触发，监听器必须挂在它的祖先链上：
	// 即挂载点 fiber（approval 先例），agent 就创建在它旁边。
	parent := c.Fiber().Parent()
	if parent == nil {
		return fmt.Errorf("toolgate: plugin fiber has no parent to listen on")
	}
	g := &gate{
		maxInline: maxInline,
		limit:     limit,
		always:    map[string]bool{},
		active:    map[*fiber.Fiber][]string{},
	}
	for _, n := range always {
		g.always[n] = true
	}
	return c.Effect(func(c *fiber.Context) (fiber.Disposer, error) {
		reg, ok := fiber.Get[*tools.Registry](c, "tools")
		if !ok {
			return nil, fmt.Errorf("toolgate: tools service unavailable")
		}
		g.reg = reg
		d1 := reg.Register(searchTool{g})
		d2 := event.OnWaterfall[[]llm.ToolSchema, []llm.ToolSchema](parent.Context(), "prompt/tools", g.filter)
		return func() error {
			g.mu.Lock()
			g.active = map[*fiber.Fiber][]string{}
			g.mu.Unlock()
			if err := d2(); err != nil {
				return err
			}
			return d1()
		}, nil
	})
}

func init() { plugin.Register(Plugin()) }
