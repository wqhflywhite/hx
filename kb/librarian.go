package kb

import (
	_ "embed"
	"sync"

	"hx/core/fiber"
	"hx/prompt"
)

// librarianPrompt 是知识库写作规程（vault 双区约定、wiki 页面
// 格式、etag 写入纪律、锚点引用），在 kb 挂载期间注入
// 系统提示词。
//
//go:embed librarian.md
var librarianPrompt string

// librarianSection 是该规程在 prompt 里的注册名。
const librarianSection = "kb-librarian"

// registerLibrarian 把 librarian 规程注册为 "prompt" 服务的一个
// section，并返回负责移除它的卸载器（disposer）。kb 插件并不
// 硬依赖 prompt 插件（prompt 可能更晚激活），所以起一个
// goroutine 等待树上的服务代次，直到 "prompt" 出现；section
// 注册完成或 kb fiber 被 dispose（撤销）时退出。
func registerLibrarian(c *fiber.Context) fiber.Disposer {
	var mu sync.Mutex
	disposed := false
	var unregister fiber.Disposer
	f := c.Fiber()
	go func() {
		for {
			// 先读代次再查服务：若 provide 恰好落在两者之间，
			// 要么被这次查找本身看到，要么让 WaitGeneration
			// 立即返回（gen 已不同），所以不会漏掉
			// 唤醒。
			gen := f.Generation()
			if p, ok := fiber.Get[*prompt.Prompt](c, "prompt"); ok {
				d := p.Register(prompt.Section{
					Name:  librarianSection,
					Order: 30,
					Render: func(*fiber.Context) (string, error) {
						return librarianPrompt, nil
					},
				})
				mu.Lock()
				if disposed {
					// section 落地期间 kb fiber 被 dispose（撤销）了；
					// 立即撤销注册，而不是泄漏它。
					mu.Unlock()
					_ = d()
					return
				}
				unregister = d
				mu.Unlock()
				return
			}
			if !f.WaitGeneration(gen) {
				return
			}
		}
	}()
	return func() error {
		mu.Lock()
		defer mu.Unlock()
		disposed = true
		if unregister != nil {
			return unregister()
		}
		return nil
	}
}
