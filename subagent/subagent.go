// Package subagent 提供 delegate 工具：把子任务委派给子 agent。
package subagent

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"hx/agent"
	"hx/core/event"
	"hx/core/fiber"
	"hx/core/plugin"
	"hx/session"
	"hx/tools"
)

const (
	// defaultMaxDepth 只允许顶层 agent 委派。
	defaultMaxDepth = 1
	// defaultTimeout 在插件配置未设 timeout_seconds 时约束一次委派的 turn（一轮）。
	defaultTimeout = 120 * time.Second
)

// Delegate 是委派 Consumer：它在一个全新的子 agent 上运行一个
// 子任务，并返回子 agent 的最终回答。子 agent 由与父 agent 相同的
// 工厂创建，所以能看到相同的工具注册表——包括本工具；真正阻止
// 委派无限递归的是深度闸门。
type Delegate struct {
	agents   *agent.Registry
	sessions *session.Manager
	maxDepth int
	timeout  time.Duration
}

// Name 实现 tools.Tool。
func (*Delegate) Name() string { return "delegate" }

// Schema 实现 tools.Tool。
func (*Delegate) Schema() map[string]any {
	return map[string]any{
		"description": "Delegate a self-contained subtask to a subagent. The subagent runs in its own session log with its own step budget and returns its final answer. Best for subtasks that can be completed independently of the main conversation.",
		"type":        "object",
		"properties": map[string]any{
			"task":  map[string]any{"type": "string", "description": "The subtask for the subagent to complete."},
			"label": map[string]any{"type": "string", "description": "Optional one-line summary of why this is delegated, for log readability."},
		},
		"required": []string{"task"},
	}
}

type delegateArgs struct {
	Task  string `json:"task"`
	Label string `json:"label"`
}

// Execute 实现 tools.Tool。
func (d *Delegate) Execute(c *fiber.Context, args json.RawMessage) (any, error) {
	var a delegateArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, fmt.Errorf("delegate: bad arguments: %w", err)
	}
	if a.Task == "" {
		return nil, fmt.Errorf("delegate: empty task")
	}
	if depth := delegationDepth(c.Fiber()); depth > d.maxDepth {
		return nil, fmt.Errorf("delegate: delegation depth limit reached (depth %d > max_depth %d)", depth, d.maxDepth)
	}

	sub, err := d.agents.Create(c, agent.Options{})
	if err != nil {
		return nil, fmt.Errorf("delegate: create subagent: %w", err)
	}
	defer sub.Dispose()
	id := sub.SessionID()

	done := make(chan session.TurnEndData, 1)
	defer event.On[session.Event](c, "session/event", func(_ *fiber.Context, ev session.Event) {
		if ev.Type != session.TurnEnd {
			return
		}
		var data session.TurnEndData
		if json.Unmarshal(ev.Data, &data) == nil && data.Session == id {
			select {
			case done <- data:
			default:
			}
		}
	})()

	sub.Send(a.Task)
	timer := time.NewTimer(d.timeout)
	defer timer.Stop()
	select {
	case end := <-done:
		answer := finalAnswer(d.sessions, id)
		if end.Error != "" {
			return fmt.Sprintf("subagent turn ended: %s\n%s", end.Error, answer), nil
		}
		return answer, nil
	case <-c.Fiber().Done():
		// 调用方 fiber 被撤销（父 agent Dispose、热重挂级联）时
		// 即时解开，不再拖到超时；defer 的 Dispose 会取消子 agent
		// 的 turn（一轮）并撤销其 fiber。
		return nil, fmt.Errorf("delegate: subagent %s interrupted: caller fiber disposed%s", id, partialSuffix(d.sessions, id))
	case <-timer.C:
		return nil, fmt.Errorf("delegate: subagent %s timed out after %s%s", id, d.timeout, partialSuffix(d.sessions, id))
	}
}

// delegationDepth 统计祖先链上的 agent fiber 数量；调用方
// 自身的 fiber 也算一个 agent，所以顶层 agent 的深度为 1。
func delegationDepth(f *fiber.Fiber) int {
	n := 0
	for cur := f; cur != nil; cur = cur.Parent() {
		if strings.HasPrefix(cur.Name(), "agent:") {
			n++
		}
	}
	return n
}

// partialSuffix 回读子会话日志的投影，把最后一条非空 assistant
// 消息格式化为「部分答案」后缀；子 agent 尚未产出内容时为空串。
// 投影会把悬而未决的流式分片也收尾成 assistant 消息，因此超时或
// 被撤销时已经吐出的半截回答也能保住。
func partialSuffix(mgr *session.Manager, id string) string {
	msgs, err := mgr.Messages(id)
	if err != nil {
		return ""
	}
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == "assistant" && strings.TrimSpace(msgs[i].Content) != "" {
			return fmt.Sprintf("; partial answer: %s", truncateRunes(msgs[i].Content, 500))
		}
	}
	return ""
}

// truncateRunes 把 s 截断到最多 n 个 rune，截断时追加省略号。
func truncateRunes(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + "…"
}

// finalAnswer 回读子 agent 的会话日志，返回其
// 最后一条 assistant 消息的内容。
func finalAnswer(mgr *session.Manager, id string) string {
	events, err := mgr.Events(id)
	if err != nil {
		return ""
	}
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Type == session.AssistantMessage {
			var d session.AssistantMessageData
			if json.Unmarshal(events[i].Data, &d) == nil {
				return d.Content
			}
		}
	}
	return ""
}

// Plugin 把 delegate 工具挂载进 "tools" 注册表。配置键：
// "max_depth"（int，默认 1）、"timeout_seconds"（int，默认 120）。
func Plugin() plugin.Plugin { return subagentPlugin{} }

type subagentPlugin struct{}

func (subagentPlugin) Name() string     { return "subagent" }
func (subagentPlugin) Inject() []string { return []string{"agents", "tools", "sessions"} }
func (subagentPlugin) Apply(c *fiber.Context, cfg map[string]any) error {
	maxDepth := defaultMaxDepth
	if v, ok := cfg["max_depth"].(int); ok && v > 0 {
		maxDepth = v
	}
	timeout := defaultTimeout
	if v, ok := cfg["timeout_seconds"].(int); ok && v > 0 {
		timeout = time.Duration(v) * time.Second
	}
	return c.Effect(func(c *fiber.Context) (fiber.Disposer, error) {
		agents, ok := fiber.Get[*agent.Registry](c, "agents")
		if !ok {
			return nil, fmt.Errorf("subagent: agents service unavailable")
		}
		reg, ok := fiber.Get[*tools.Registry](c, "tools")
		if !ok {
			return nil, fmt.Errorf("subagent: tools service unavailable")
		}
		mgr, ok := fiber.Get[*session.Manager](c, "sessions")
		if !ok {
			return nil, fmt.Errorf("subagent: sessions service unavailable")
		}
		return reg.Register(&Delegate{agents: agents, sessions: mgr, maxDepth: maxDepth, timeout: timeout}), nil
	})
}

func init() { plugin.Register(Plugin()) }
