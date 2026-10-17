// Package agentloop 是默认的 ReAct 驱动器，作为 agent 工厂挂载。
package agentloop

import (
	"context"
	"fmt"

	"hx/agent"
	"hx/compaction"
	"hx/core/event"
	"hx/core/fiber"
	"hx/core/plugin"
	"hx/llm"
	"hx/prompt"
	"hx/session"
	"hx/tools"
)

// defaultMaxSteps 在插件配置未设置 max_steps 时为一个 turn（一轮）的步数上限。
const defaultMaxSteps = 32

// Loop 是默认的 ReAct 工厂：一个 turn（一轮）领取一个 inbox 输入，
// 运行若干 step，直到模型不再请求工具调用。
type Loop struct {
	maxSteps int
}

// Create 实现 agent.Factory。
func (l *Loop) Create(c *fiber.Context, opts agent.Options) (agent.Agent, error) {
	opener, ok := fiber.Get[session.Opener](c, "sessions")
	if !ok {
		return nil, fmt.Errorf("agentloop: sessions service unavailable")
	}
	var store session.Store
	var id string
	var err error
	if opts.SessionID == "" {
		store, id, err = opener.Create()
	} else {
		store, err = opener.Resume(opts.SessionID)
		id = opts.SessionID
	}
	if err != nil {
		return nil, fmt.Errorf("agentloop: open session: %w", err)
	}

	fb, err := c.Fiber().New("agent:" + id)
	if err != nil {
		return nil, err
	}
	a := &loopAgent{
		id:       id,
		store:    store,
		fiber:    fb,
		inbox:    make(chan string, 16),
		maxSteps: l.maxSteps,
	}
	// 日志文件是 agent fiber 拥有的副作用（effect）。
	if err := fb.Context().Effect(func(*fiber.Context) (fiber.Disposer, error) {
		return func() error {
			if closer, ok := store.(interface{ Close() error }); ok {
				return closer.Close()
			}
			return nil
		}, nil
	}); err != nil {
		_ = fb.Dispose()
		return nil, err
	}
	go a.run()
	fb.Activate()
	return a, nil
}

// loopAgent 是一个运行中的 ReAct agent。
type loopAgent struct {
	id       string
	store    session.Store
	fiber    *fiber.Fiber
	inbox    chan string
	maxSteps int
}

// Send 实现 agent.Agent。
func (a *loopAgent) Send(text string) {
	select {
	case a.inbox <- text:
	case <-a.fiber.Done():
	}
}

// SessionID 实现 agent.Agent。
func (a *loopAgent) SessionID() string { return a.id }

// Dispose 实现 agent.Agent。
func (a *loopAgent) Dispose() error { return a.fiber.Dispose() }

// run 循环领取 inbox 输入，直到 fiber 被 dispose（撤销）。
func (a *loopAgent) run() {
	for {
		select {
		case <-a.fiber.Done():
			return
		case text := <-a.inbox:
			a.turn(text)
		}
	}
}

// append 是铁律的具体落地：先写日志、再广播（store
// 两者都做）；append 失败就中止本轮，而不是让模型
// 看到未落日志的内容。
func (a *loopAgent) append(c *fiber.Context, typ string, data any) error {
	return a.store.Append(c, session.Event{Type: typ, Data: session.MarshalData(data)})
}

// turn 把一个用户输入跑到完成：零或多个 step，每个 step 是一次
// 模型流式响应加上它的工具执行。
func (a *loopAgent) turn(text string) {
	c := a.fiber.Context()
	// 取消是双通道的：context.Context 用于 HTTP 流和工具
	// 进程，fiber 的 done 通道用于生命周期。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-a.fiber.Done():
			cancel()
		case <-ctx.Done():
		}
	}()

	turnErr := ""
	if err := a.append(c, session.UserMessage, session.UserMessageData{Content: text}); err != nil {
		return
	}
	if err := a.append(c, session.TurnStart, session.TurnStartData{Session: a.id}); err != nil {
		return
	}
	for step := 0; ; step++ {
		if ctx.Err() != nil {
			turnErr = "canceled"
			break
		}
		if step >= a.maxSteps {
			turnErr = fmt.Sprintf("max steps (%d) reached", a.maxSteps)
			break
		}
		if err := a.append(c, session.StepStart, session.StepStartData{Session: a.id, Step: step}); err != nil {
			return
		}
		calls, stepErr := a.step(ctx)
		if stepErr == nil {
			for i, call := range calls {
				if err := a.append(c, session.ToolCall, session.ToolCallData{ID: call.ID, Name: call.Name, Arguments: call.Arguments}); err != nil {
					return
				}
				reg, err := a.tools(c)
				if err != nil {
					for _, rest := range calls[i:] {
						if aerr := a.append(c, session.ToolResult, session.ToolResultData{CallID: rest.ID, Name: rest.Name, Error: err.Error()}); aerr != nil {
							return
						}
					}
					stepErr = err
					break
				}
				res := reg.Execute(c, call)
				if err := a.append(c, session.ToolResult, session.ToolResultData{CallID: call.ID, Name: call.Name, Content: res.Content, Error: res.Error}); err != nil {
					return
				}
			}
		}
		if stepErr != nil {
			turnErr = stepErr.Error()
		}
		if err := a.append(c, session.StepEnd, session.StepEndData{Session: a.id, Step: step}); err != nil {
			return
		}
		if stepErr != nil || len(calls) == 0 {
			break
		}
	}
	_ = a.append(c, session.TurnEnd, session.TurnEndData{Session: a.id, Error: turnErr})
}

// tools 解析工具注册表；热重挂期间服务可能缺席（inject 只保证
// 挂载时的依赖就绪），调用方必须处理 error，不得解引用零值。
func (a *loopAgent) tools(c *fiber.Context) (*tools.Registry, error) {
	reg, ok := fiber.Get[*tools.Registry](c, "tools")
	if !ok {
		return nil, fmt.Errorf("agentloop: tools service unavailable")
	}
	return reg, nil
}

// step 从日志组装请求，流式接收一次模型响应并记录每个
// chunk，返回已完成的工具调用。
func (a *loopAgent) step(ctx context.Context) ([]llm.ToolCall, error) {
	c := a.fiber.Context()
	p, ok := fiber.Get[*prompt.Prompt](c, "prompt")
	if !ok {
		return nil, fmt.Errorf("agentloop: prompt service unavailable")
	}
	system, schemas, err := p.Assemble(c)
	if err != nil {
		return nil, err
	}
	// compaction（压缩）是可选的消费者：没有挂载 provider 时，
	// 历史按原样投影。
	if compactor, ok := fiber.Get[compaction.Compactor](c, "compaction"); ok {
		if _, err := compactor.MaybeCompact(c, ctx, a.store, a.id); err != nil {
			return nil, err
		}
	}
	mgr, ok := fiber.Get[*session.Manager](c, "sessions")
	if !ok {
		return nil, fmt.Errorf("agentloop: sessions service unavailable")
	}
	history, err := mgr.Messages(a.id)
	if err != nil {
		return nil, err
	}
	messages := append([]llm.Message{{Role: "system", Content: system}}, history...)

	req := &llm.Request{Messages: messages, Tools: schemas}
	req, err = event.Waterfall[*llm.Request, *llm.Request](c, "agent/pre-step", req,
		func(_ *fiber.Context, in *llm.Request) (*llm.Request, error) { return in, nil })
	if err != nil {
		return nil, err
	}

	stream, err := llm.Stream(c, ctx, req)
	if err != nil {
		return nil, err
	}
	var content []byte
	var calls []llm.ToolCall
	var streamErr error
	for chunk := range stream {
		if chunk.Err != nil {
			streamErr = chunk.Err
			break
		}
		if chunk.Delta != "" {
			if err := a.append(c, session.AssistantChunk, session.AssistantChunkData{Delta: chunk.Delta}); err != nil {
				return nil, err
			}
			content = append(content, chunk.Delta...)
		}
		if chunk.ToolCall != nil {
			calls = append(calls, *chunk.ToolCall)
		}
	}
	if err := a.append(c, session.AssistantMessage, session.AssistantMessageData{Content: string(content), ToolCalls: calls}); err != nil {
		return nil, err
	}
	return calls, streamErr
}

// Plugin 将该循环挂载为 agent 工厂；配置键 "max_steps"（int）。
func Plugin() plugin.Plugin { return loopPlugin{} }

type loopPlugin struct{}

func (loopPlugin) Name() string { return "agentloop" }
func (loopPlugin) Inject() []string {
	return []string{"agents", "sessions", "llm", "tools", "prompt"}
}
func (loopPlugin) Apply(c *fiber.Context, cfg map[string]any) error {
	maxSteps := defaultMaxSteps
	if v, ok := cfg["max_steps"].(int); ok && v > 0 {
		maxSteps = v
	}
	l := &Loop{maxSteps: maxSteps}
	return c.Effect(func(c *fiber.Context) (fiber.Disposer, error) {
		agents, ok := fiber.Get[*agent.Registry](c, "agents")
		if !ok {
			return nil, fmt.Errorf("agentloop: agents service unavailable")
		}
		return agents.SetFactory(l), nil
	})
}

func init() { plugin.Register(Plugin()) }
