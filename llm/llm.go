// Package llm 定义适配器 seam 与流式消息词汇表。
package llm

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"

	"hx/core/event"
	"hx/core/fiber"
	"hx/core/plugin"
)

// Message 是模型历史中的一条消息；role 取值为 system/user/assistant/tool。
type Message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

// ToolCall 是模型发起的一次工具调用；Arguments 为原始 JSON。
type ToolCall struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ToolSchema 向模型描述一个工具。
type ToolSchema struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters"`
}

// Request 是一次补全请求。Provider 指定要使用的注册表适配器；
// 留空则解析注册表默认适配器。
type Request struct {
	Provider string
	Model    string
	Messages []Message
	Tools    []ToolSchema
}

// Usage 报告一次响应的 token 消耗。
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

// Chunk 是流式响应的一个分片；Err 非空表示流终止。
type Chunk struct {
	Delta    string
	ToolCall *ToolCall
	Usage    *Usage
	Err      error
}

// Adapter 是 LLM 能力 seam：Stream 采用拉取模式，对其 range
// 即消费响应，取消 ctx 会切断流。
type Adapter interface {
	Stream(ctx context.Context, req *Request) iter.Seq[Chunk]
}

// Registry 持有按名注册的适配器；provider 负责注册，消费者
// 按 Request.Provider 解析，否则回退到默认适配器。
type Registry struct {
	mu       sync.RWMutex
	adapters map[string]Adapter
	order    []string // 注册顺序；首个条目是隐式默认适配器
	def      string   // 显式默认适配器，仍注册时优先于 order
}

// NewRegistry 创建一个空注册表。
func NewRegistry() *Registry { return &Registry{adapters: map[string]Adapter{}} }

// SetAdapter 以 name 安装 a 并返回卸载器；同名适配器被替换，
// disposer 只回滚自己这一代的注册，不误删后续实例。
func (r *Registry) SetAdapter(name string, a Adapter) fiber.Disposer {
	r.mu.Lock()
	pre, exist := r.adapters[name]
	r.adapters[name] = a
	if !exist {
		r.order = append(r.order, name)
	}
	r.mu.Unlock()
	return func() error {
		r.mu.Lock()
		defer r.mu.Unlock()

		cur, ok := r.adapters[name]
		if !ok || cur != a {
			return nil
		}
		if exist {
			r.adapters[name] = pre
			return nil
		}

		delete(r.adapters, name)
		for i, n := range r.order {
			if n == name {
				r.order = append(r.order[:i], r.order[i+1:]...)
				break
			}
		}
		return nil
	}
}

// SetDefault 将 name 标记为默认适配器，并返回恢复之前默认值的
// disposer；name 未注册时 panic。
func (r *Registry) SetDefault(name string) fiber.Disposer {
	r.mu.Lock()
	if _, ok := r.adapters[name]; !ok {
		r.mu.Unlock()
		panic(fmt.Errorf("llm: default adapter %q is not registered", name))
	}
	prev := r.def
	r.def = name
	r.mu.Unlock()
	return func() error {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.def == name {
			r.def = prev
		}
		return nil
	}
}

// Adapter 解析指定名字的适配器；空名字解析默认适配器——
// 显式默认值仍然注册时取显式默认值，否则取最早注册的
// 适配器。
func (r *Registry) Adapter(name string) (Adapter, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if name != "" {
		a, ok := r.adapters[name]
		return a, ok
	}
	if r.def != "" {
		if a, ok := r.adapters[r.def]; ok {
			return a, true
		}
	}
	for _, n := range r.order {
		if a, ok := r.adapters[n]; ok {
			return a, true
		}
	}
	return nil, false
}

// Stream 解析 req.Provider 指定的适配器（或默认适配器），并经由
// llm/stream 瀑布（waterfall）执行请求，重试、日志与计量以
// 监听器形式挂入。
func Stream(c *fiber.Context, ctx context.Context, req *Request) (iter.Seq[Chunk], error) {
	return event.Waterfall[*Request, iter.Seq[Chunk]](c, ctx, "llm/stream", req,
		func(ctx context.Context, c *fiber.Context, r *Request) (iter.Seq[Chunk], error) {
			reg, ok := fiber.Get[*Registry](c, "llm")
			if !ok {
				return nil, errNoAdapter
			}
			adapter, ok := reg.Adapter(r.Provider)
			if !ok {
				return nil, fmt.Errorf("llm: no adapter registered for provider %q", r.Provider)
			}
			return adapter.Stream(ctx, r), nil
		})
}

var errNoAdapter = errors.New("llm: no adapter registered")

// Plugin 将适配器注册表暴露为 "llm" 服务。
func Plugin() plugin.Plugin { return llmPlugin{} }

type llmPlugin struct{}

func (llmPlugin) Name() string     { return "llm" }
func (llmPlugin) Inject() []string { return nil }
func (llmPlugin) Apply(c *fiber.Context, _ map[string]any) error {
	return c.Effect(func(c *fiber.Context) (fiber.Disposer, error) {
		return fiber.Provide[*Registry](c, "llm", NewRegistry()), nil
	})
}

func init() { plugin.Register(Plugin()) }
