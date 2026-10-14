// Package plugin 提供编译期注册表与基于 inject 声明的激活。
package plugin

import (
	"fmt"
	"sync"

	"hx/core/fiber"
)

// Plugin 是组合的基本单元：它声明自己依赖的服务，
// 并在所有依赖都就绪后应用自己的副作用。
type Plugin interface {
	// Name 是注册表中的唯一键。
	Name() string
	// Inject 列出 Apply 运行前必须已提供的服务名。
	Inject() []string
	// Apply 把插件的副作用挂载到 c 上；cfg 携带插件配置。
	Apply(c *fiber.Context, cfg map[string]any) error
}

var (
	registryMu sync.Mutex
	registry   = map[string]Plugin{}
)

// Register 把 p 加入编译期注册表；重名会 panic。
func Register(p Plugin) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, ok := registry[p.Name()]; ok {
		panic(fmt.Errorf("plugin: duplicate registration %q", p.Name()))
	}
	registry[p.Name()] = p
}

// Mount 把指定名字的插件挂载为 root 的子 fiber。如果所有
// 注入的服务都已提供，Apply 会同步运行并返回其错误；否则
// 子 fiber 等待，直到最后一个依赖出现时才激活。挂载未注册
// 的名字会立即报错。
func Mount(root *fiber.Context, name string, cfg map[string]any) error {
	registryMu.Lock()
	p, ok := registry[name]
	registryMu.Unlock()
	if !ok {
		return fmt.Errorf("plugin: %q is not registered", name)
	}

	fb, err := root.Fiber().New("plugin:" + name)
	if err != nil {
		return err
	}
	ctx := fb.Context()
	deps := p.Inject()
	if depsReady(ctx, deps) {
		return activate(fb, ctx, p, cfg)
	}

	// 异步等待：在每次全树的 provide/revoke 后重新检查，
	// 直到所有依赖就绪或 fiber 被 dispose。
	go func() {
		for {
			gen := fb.Generation()
			if depsReady(ctx, deps) {
				if err := activate(fb, ctx, p, cfg); err != nil {
					panic(fmt.Errorf("plugin: async apply %q: %w", name, err))
				}
				return
			}
			if !fb.WaitGeneration(gen) {
				return // 等待期间被 dispose
			}
		}
	}()
	return nil
}

// depsReady 报告每个依赖是否都能从 ctx 解析到。
func depsReady(ctx *fiber.Context, deps []string) bool {
	for _, dep := range deps {
		if _, ok := fiber.Get[any](ctx, dep); !ok {
			return false
		}
	}
	return true
}

// activate 为每个依赖挂上撤销级联钩子，运行 Apply，
// 并把 fiber 标记为 active。任何失败都会 dispose 子 fiber。
func activate(fb *fiber.Fiber, ctx *fiber.Context, p Plugin, cfg map[string]any) error {
	for _, dep := range p.Inject() {
		dep := dep
		if err := ctx.Effect(func(c *fiber.Context) (fiber.Disposer, error) {
			return fiber.OnRevoked(c, dep, func() { _ = fb.Dispose() }), nil
		}); err != nil {
			_ = fb.Dispose()
			return err
		}
	}
	if err := p.Apply(ctx, cfg); err != nil {
		_ = fb.Dispose()
		return fmt.Errorf("plugin: apply %q: %w", p.Name(), err)
	}
	fb.Activate()
	return nil
}
