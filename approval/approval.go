// Package approval 将工具执行置于逐工具审批模式的闸门之后。
package approval

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"hx/core/event"
	"hx/core/fiber"
	"hx/core/plugin"
	"hx/llm"
)

// Prompter 询问用户某次工具调用是否可以执行；CLI 从 stdin 读取，
// TUI 通过 "prompter" 配置键替换为自己的实现。
type Prompter interface {
	// Confirm 报告 call 是否被批准执行。
	Confirm(c *fiber.Context, call llm.ToolCall) (bool, error)
}

// stdinPrompter 是 CLI 的 Prompter：每次工具调用询问一行 y/n。
type stdinPrompter struct {
	in  *bufio.Reader
	out io.Writer
}

// StdinPrompter 创建一个从 in 读取 y/n 回答的 Prompter。
func StdinPrompter(in io.Reader, out io.Writer) Prompter {
	return &stdinPrompter{in: bufio.NewReader(in), out: out}
}

// Confirm 实现 Prompter。
func (p *stdinPrompter) Confirm(_ *fiber.Context, call llm.ToolCall) (bool, error) {
	fmt.Fprintf(p.out, "\n[approval] run %s %s? [y/N] ", call.Name, call.Arguments)
	line, err := p.in.ReadString('\n')
	if err != nil && line == "" {
		return false, err
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true, nil
	default:
		return false, nil
	}
}

// modes 是解析后的审批策略：一个默认模式加上逐工具模式。
type modes struct {
	def string
	per map[string]string
}

// parseMode 解析 "mode" 配置值：字符串 "auto" 或 "ask"，或一个
// 工具名到模式的映射，其中 "*" 覆盖默认模式。兜底默认值
// 是 "ask"，所以未列出的工具总是会询问。
func parseMode(v any) (modes, error) {
	m := modes{def: "ask", per: map[string]string{}}
	switch v := v.(type) {
	case nil:
	case string:
		if err := checkMode(v); err != nil {
			return m, err
		}
		m.def = v
	case map[string]any:
		for tool, raw := range v {
			mode, ok := raw.(string)
			if !ok {
				return m, fmt.Errorf("approval: mode for %q must be a string", tool)
			}
			if err := checkMode(mode); err != nil {
				return m, err
			}
			if tool == "*" {
				m.def = mode
			} else {
				m.per[tool] = mode
			}
		}
	default:
		return m, fmt.Errorf("approval: mode must be a string or a map, got %T", v)
	}
	return m, nil
}

func checkMode(mode string) error {
	if mode != "auto" && mode != "ask" {
		return fmt.Errorf("approval: unknown mode %q (want \"auto\" or \"ask\")", mode)
	}
	return nil
}

// mode 解析单个工具的模式。
func (m modes) mode(tool string) string {
	if mode, ok := m.per[tool]; ok {
		return mode
	}
	return m.def
}

// Plugin 将审批监听器挂载到 tools/pre-execute 瀑布（waterfall）上。
// 配置键："mode"（字符串或逐工具映射，默认 "ask"）、"prompter"
// (Prompter，默认 stdin Prompter)。
func Plugin() plugin.Plugin { return approvalPlugin{} }

type approvalPlugin struct{}

func (approvalPlugin) Name() string     { return "approval" }
func (approvalPlugin) Inject() []string { return nil }

func (approvalPlugin) Apply(c *fiber.Context, cfg map[string]any) error {
	m, err := parseMode(cfg["mode"])
	if err != nil {
		return err
	}
	prompter, _ := cfg["prompter"].(Prompter)
	if prompter == nil {
		prompter = StdinPrompter(os.Stdin, os.Stderr)
	}
	// 瀑布（waterfall）在 agent fiber 上触发，所以监听器必须挂在它的
	// 祖先链上：即挂载点 fiber（本插件 fiber 的父 fiber），agent
	// 就创建在它旁边。卸载器（disposer）绑定在插件 fiber 上，
	// 让 Dispose 自包含。
	parent := c.Fiber().Parent()
	if parent == nil {
		return fmt.Errorf("approval: plugin fiber has no parent to listen on")
	}
	pc := parent.Context()
	return c.Effect(func(*fiber.Context) (fiber.Disposer, error) {
		return event.OnWaterfall[llm.ToolCall, llm.ToolCall](pc, "tools/pre-execute",
			func(c *fiber.Context, call llm.ToolCall, next event.Next[llm.ToolCall, llm.ToolCall]) (llm.ToolCall, error) {
				if m.mode(call.Name) == "auto" {
					return next(c, call)
				}
				ok, err := prompter.Confirm(c, call)
				if err != nil {
					return call, fmt.Errorf("approval: %w", err)
				}
				if !ok {
					// 短路：工具不会执行；tools.Execute 会把
					// 这个错误变成 tool/result 内容。
					return call, fmt.Errorf("approval: %q denied by user", call.Name)
				}
				return next(c, call)
			}), nil
	})
}

func init() { plugin.Register(Plugin()) }
