// Package prompt 按有序分区组装系统提示词。
package prompt

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"hx/core/event"
	"hx/core/fiber"
	"hx/core/plugin"
	"hx/llm"
)

// Section 是系统提示词的一个命名分区，按 Order 渲染。
type Section struct {
	Name   string
	Order  int
	Render func(*fiber.Context) (string, error)
}

// ToolSchemer 是组装所需的 tools 能力切片，
// 从上下文解析而来，使 prompt 不依赖 tools 包。
type ToolSchemer interface {
	Schemas() []llm.ToolSchema
}

// Prompt 持有已注册的分区。
type Prompt struct {
	mu       sync.RWMutex
	sections map[string]Section
}

// New 创建一个空的提示词组装器。
func New() *Prompt { return &Prompt{sections: map[string]Section{}} }

// Register 添加一个分区并返回卸载器（disposer）；重名会 panic。
func (p *Prompt) Register(s Section) fiber.Disposer {
	if s.Render == nil {
		panic(fmt.Errorf("prompt: section %q has no Render", s.Name))
	}
	p.mu.Lock()
	if _, ok := p.sections[s.Name]; ok {
		p.mu.Unlock()
		panic(fmt.Errorf("prompt: duplicate section %q", s.Name))
	}
	p.sections[s.Name] = s
	p.mu.Unlock()
	return func() error {
		p.mu.Lock()
		defer p.mu.Unlock()
		delete(p.sections, s.Name)
		return nil
	}
}

// Assemble 按 (Order, Name) 的确定顺序渲染各分区并拼接：Register
// 拒绝重名使该二元组构成全序，拼接结果与 map 遍历、注册顺序无关，
// 跨进程逐字节一致。拼接文本送入 prompt/assemble 瀑布（waterfall）
// 处理，并在挂载了 "tools" 服务时从中收集工具 schema；schema 列表
// 再过一道 prompt/tools 瀑布，检索式加载等插件在这里按需裁剪。
func (p *Prompt) Assemble(c *fiber.Context) (string, []llm.ToolSchema, error) {
	p.mu.RLock()
	sections := make([]Section, 0, len(p.sections))
	for _, s := range p.sections {
		sections = append(sections, s)
	}
	p.mu.RUnlock()
	sort.Slice(sections, func(i, j int) bool {
		if sections[i].Order != sections[j].Order {
			return sections[i].Order < sections[j].Order
		}
		return sections[i].Name < sections[j].Name
	})

	var parts []string
	for _, s := range sections {
		text, err := s.Render(c)
		if err != nil {
			return "", nil, fmt.Errorf("prompt: section %q: %w", s.Name, err)
		}
		if text != "" {
			parts = append(parts, text)
		}
	}
	text, err := event.Waterfall[string, string](c, "prompt/assemble", strings.Join(parts, "\n\n"),
		func(_ *fiber.Context, in string) (string, error) { return in, nil })
	if err != nil {
		return "", nil, err
	}
	var schemas []llm.ToolSchema
	if reg, ok := fiber.Get[ToolSchemer](c, "tools"); ok {
		schemas = reg.Schemas()
	}
	schemas, err = event.Waterfall[[]llm.ToolSchema, []llm.ToolSchema](c, "prompt/tools", schemas,
		func(_ *fiber.Context, in []llm.ToolSchema) ([]llm.ToolSchema, error) { return in, nil })
	if err != nil {
		return "", nil, err
	}
	return text, schemas, nil
}

// Plugin 将 Prompt 以 "prompt" 服务暴露，并带一个默认的 identity
// 分区，使空组装也能产出可用的系统提示词。
func Plugin() plugin.Plugin { return promptPlugin{} }

type promptPlugin struct{}

func (promptPlugin) Name() string     { return "prompt" }
func (promptPlugin) Inject() []string { return nil }
func (promptPlugin) Apply(c *fiber.Context, _ map[string]any) error {
	return c.Effect(func(c *fiber.Context) (fiber.Disposer, error) {
		p := New()
		d1 := p.Register(Section{
			Name:  "identity",
			Order: 0,
			Render: func(*fiber.Context) (string, error) {
				return "You are hx, a coding agent. Answer concisely and use the provided tools when they help.", nil
			},
		})
		d2 := fiber.Provide[*Prompt](c, "prompt", p)
		return func() error {
			if err := d2(); err != nil {
				return err
			}
			return d1()
		}, nil
	})
}

func init() { plugin.Register(Plugin()) }
