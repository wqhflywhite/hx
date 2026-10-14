// Package tools 提供工具注册表与执行流水线。
package tools

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"hx/core/event"
	"hx/core/fiber"
	"hx/core/plugin"
	"hx/llm"
)

// Tool 是一个可调用的能力；Schema 是其参数的 JSON Schema。
type Tool interface {
	Name() string
	Schema() map[string]any
	Execute(c *fiber.Context, args json.RawMessage) (any, error)
}

// Result 是一次执行的规范化结果，可被
// tools/post-execute 瀑布（waterfall）改写。
type Result struct {
	Content string
	Error   string
}

// Registry 持有已挂载的工具。
type Registry struct {
	mu    sync.RWMutex
	tools map[string]Tool
}

// NewRegistry 创建一个空的注册表。
func NewRegistry() *Registry { return &Registry{tools: map[string]Tool{}} }

// Register 挂载 t 并返回一个卸载器（disposer）；重名注册会 panic。
func (r *Registry) Register(t Tool) fiber.Disposer {
	r.mu.Lock()
	if _, ok := r.tools[t.Name()]; ok {
		r.mu.Unlock()
		panic(fmt.Errorf("tools: duplicate registration %q", t.Name()))
	}
	r.tools[t.Name()] = t
	r.mu.Unlock()
	return func() error {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.tools[t.Name()] == t {
			delete(r.tools, t.Name())
		}
		return nil
	}
}

// Get 按名称解析一个工具。
func (r *Registry) Get(name string) (Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tools[name]
	return t, ok
}

// Schemas 按名称顺序列出所有工具的 schema，供 prompt 组装使用。
func (r *Registry) Schemas() []llm.ToolSchema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.tools))
	for name := range r.tools {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]llm.ToolSchema, 0, len(names))
	for _, name := range names {
		schema := r.tools[name].Schema()
		desc, _ := schema["description"].(string)
		params := make(map[string]any, len(schema))
		for k, v := range schema {
			if k != "description" {
				params[k] = v
			}
		}
		out = append(out, llm.ToolSchema{Name: name, Description: desc, Parameters: params})
	}
	return out
}

// Execute 让一次工具调用走完整流水线：tools/pre-execute 可以改写或
// 拒绝这次调用，然后执行工具，最后 tools/post-execute 可以改写结果。
// 会话日志记录由调用方负责。
func (r *Registry) Execute(c *fiber.Context, call llm.ToolCall) Result {
	call, err := event.Waterfall[llm.ToolCall, llm.ToolCall](c, "tools/pre-execute", call,
		func(_ *fiber.Context, in llm.ToolCall) (llm.ToolCall, error) { return in, nil })
	if err != nil {
		return Result{Error: err.Error(), Content: err.Error()}
	}
	tool, ok := r.Get(call.Name)
	if !ok {
		res := Result{Error: fmt.Sprintf("unknown tool %q", call.Name)}
		res.Content = res.Error
		return res
	}
	value, err := tool.Execute(c, json.RawMessage(call.Arguments))
	res := Result{}
	if err != nil {
		res.Error = err.Error()
		res.Content = err.Error()
	} else if s, ok := value.(string); ok {
		res.Content = s
	} else {
		raw, merr := json.Marshal(value)
		if merr != nil {
			res.Error = merr.Error()
			res.Content = merr.Error()
		} else {
			res.Content = string(raw)
		}
	}
	final, err := event.Waterfall[*Result, *Result](c, "tools/post-execute", &res,
		func(_ *fiber.Context, in *Result) (*Result, error) { return in, nil })
	if err != nil {
		return Result{Error: err.Error(), Content: err.Error()}
	}
	return *final
}

// Plugin 以 "tools" 服务的形式暴露一个空的注册表。
func Plugin() plugin.Plugin { return toolsPlugin{} }

type toolsPlugin struct{}

func (toolsPlugin) Name() string     { return "tools" }
func (toolsPlugin) Inject() []string { return nil }
func (toolsPlugin) Apply(c *fiber.Context, _ map[string]any) error {
	return c.Effect(func(c *fiber.Context) (fiber.Disposer, error) {
		return fiber.Provide[*Registry](c, "tools", NewRegistry()), nil
	})
}

func init() { plugin.Register(Plugin()) }
