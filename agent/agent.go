// Package agent 提供 agent 注册表与工厂 seam。
package agent

import (
	"fmt"
	"sync"

	"hx/core/fiber"
	"hx/core/plugin"
)

// Agent 是一个运行中的 agent：输入进入它的 inbox，输出通过
// 会话事件观察。
type Agent interface {
	// Send 将一条用户输入排入 agent 的 inbox。
	Send(text string)
	// SessionID 返回该 agent 的会话日志 id。
	SessionID() string
	// Dispose 停止该 agent 并撤销它的副作用。
	Dispose() error
}

// Options 为 agent 创建提供参数；SessionID 为空表示开启新会话，
// 非空表示恢复该会话的日志。
type Options struct {
	SessionID string
}

// Factory 创建 agent；由驱动器（默认 agentloop）注册一个实现。
type Factory interface {
	Create(c *fiber.Context, opts Options) (Agent, error)
}

// Registry 不认识任何 agent 实现，只依赖工厂 seam。
type Registry struct {
	mu      sync.RWMutex
	factory Factory
}

// NewRegistry 创建一个尚无工厂的注册表。
func NewRegistry() *Registry { return &Registry{} }

// SetFactory 安装工厂并返回一个 disposer；已有工厂生效时
// 再次安装会 panic。
func (r *Registry) SetFactory(f Factory) fiber.Disposer {
	r.mu.Lock()
	if r.factory != nil {
		r.mu.Unlock()
		panic("agent: factory already set")
	}
	r.factory = f
	r.mu.Unlock()
	return func() error {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.factory == f {
			r.factory = nil
		}
		return nil
	}
}

// Create 委托给已注册的工厂；没有工厂时显式失败。
func (r *Registry) Create(c *fiber.Context, opts Options) (Agent, error) {
	r.mu.RLock()
	f := r.factory
	r.mu.RUnlock()
	if f == nil {
		return nil, fmt.Errorf("agent: no factory registered (mount a driver such as agentloop)")
	}
	return f.Create(c, opts)
}

// Plugin 将注册表以 "agents" 服务的形式暴露。
func Plugin() plugin.Plugin { return agentPlugin{} }

type agentPlugin struct{}

func (agentPlugin) Name() string     { return "agents" }
func (agentPlugin) Inject() []string { return nil }
func (agentPlugin) Apply(c *fiber.Context, _ map[string]any) error {
	return c.Effect(func(c *fiber.Context) (fiber.Disposer, error) {
		return fiber.Provide[*Registry](c, "agents", NewRegistry()), nil
	})
}

func init() { plugin.Register(Plugin()) }
