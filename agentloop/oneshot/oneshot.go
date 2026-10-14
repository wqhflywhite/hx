// Package oneshot 是固定形态的替代驱动器：最多一轮工具调用，
// 然后发起一次不带工具的最终请求，迫使模型收尾总结。
package oneshot

import (
	"context"
	"fmt"

	"hx/agent"
	"hx/core/fiber"
	"hx/core/plugin"
	"hx/llm"
	"hx/prompt"
	"hx/session"
	"hx/tools"
)

// Factory 创建 oneshot agent；它验证了工厂 seam——配置
// 只需改一行挂载就能把整套驱动器换掉。
type Factory struct{}

// Create 实现 agent.Factory。
func (Factory) Create(c *fiber.Context, opts agent.Options) (agent.Agent, error) {
	opener, ok := fiber.Get[session.Opener](c, "sessions")
	if !ok {
		return nil, fmt.Errorf("oneshot: sessions service unavailable")
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
		return nil, fmt.Errorf("oneshot: open session: %w", err)
	}

	fb, err := c.Fiber().New("agent:" + id)
	if err != nil {
		return nil, err
	}
	a := &oneshotAgent{id: id, store: store, fiber: fb, inbox: make(chan string, 16)}
	// 日志文件是 agent fiber 拥有的一个副作用（Effect）。
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

// oneshotAgent 是一个正在运行的固定形态 agent。
type oneshotAgent struct {
	id    string
	store session.Store
	fiber *fiber.Fiber
	inbox chan string
}

// Send 实现 agent.Agent。
func (a *oneshotAgent) Send(text string) {
	select {
	case a.inbox <- text:
	case <-a.fiber.Done():
	}
}

// SessionID 实现 agent.Agent。
func (a *oneshotAgent) SessionID() string { return a.id }

// Dispose 实现 agent.Agent。
func (a *oneshotAgent) Dispose() error { return a.fiber.Dispose() }

// run 持续领取 inbox 里的输入，直到 fiber 被 dispose（撤销）。
func (a *oneshotAgent) run() {
	for {
		select {
		case <-a.fiber.Done():
			return
		case text := <-a.inbox:
			a.turn(text)
		}
	}
}

// append 是铁律的具体落地：先写日志，再广播。
func (a *oneshotAgent) append(c *fiber.Context, typ string, data any) error {
	return a.store.Append(c, session.Event{Type: typ, Data: session.MarshalData(data)})
}

// turn 把一个输入跑完固定形态：一个带工具的 step、执行这些工具
// 调用，并且——仅当工具真的执行过——再跑一个不提供工具的最终
// step，让模型收尾总结，而不是继续串联更多调用。
func (a *oneshotAgent) turn(text string) {
	c := a.fiber.Context()
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
	if err := a.append(c, session.StepStart, session.StepStartData{Session: a.id, Step: 0}); err != nil {
		return
	}
	calls, stepErr := a.stream(ctx, true)
	if stepErr != nil {
		turnErr = stepErr.Error()
	} else {
		for _, call := range calls {
			if err := a.append(c, session.ToolCall, session.ToolCallData{ID: call.ID, Name: call.Name, Arguments: call.Arguments}); err != nil {
				return
			}
			reg, _ := fiber.Get[*tools.Registry](c, "tools")
			res := reg.Execute(c, call)
			if err := a.append(c, session.ToolResult, session.ToolResultData{CallID: call.ID, Name: call.Name, Content: res.Content, Error: res.Error}); err != nil {
				return
			}
		}
	}
	if err := a.append(c, session.StepEnd, session.StepEndData{Session: a.id, Step: 0}); err != nil {
		return
	}
	if stepErr == nil && len(calls) > 0 {
		if err := a.append(c, session.StepStart, session.StepStartData{Session: a.id, Step: 1}); err != nil {
			return
		}
		if _, err := a.stream(ctx, false); err != nil {
			turnErr = err.Error()
		}
		if err := a.append(c, session.StepEnd, session.StepEndData{Session: a.id, Step: 1}); err != nil {
			return
		}
	}
	_ = a.append(c, session.TurnEnd, session.TurnEndData{Session: a.id, Error: turnErr})
}

// stream 是一个 step 的请求半边：从日志装配上下文，发起流式
// 请求，并记录 chunk 和最终完整消息。
func (a *oneshotAgent) stream(ctx context.Context, withTools bool) ([]llm.ToolCall, error) {
	c := a.fiber.Context()
	p, ok := fiber.Get[*prompt.Prompt](c, "prompt")
	if !ok {
		return nil, fmt.Errorf("oneshot: prompt service unavailable")
	}
	system, schemas, err := p.Assemble(c)
	if err != nil {
		return nil, err
	}
	mgr, ok := fiber.Get[*session.Manager](c, "sessions")
	if !ok {
		return nil, fmt.Errorf("oneshot: sessions service unavailable")
	}
	history, err := mgr.Messages(a.id)
	if err != nil {
		return nil, err
	}
	messages := append([]llm.Message{{Role: "system", Content: system}}, history...)

	req := &llm.Request{Messages: messages}
	if withTools {
		req.Tools = schemas
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

// Plugin 把工厂挂载为 agent 驱动器；当配置写的是 oneshotloop
// 而不是 agentloop 时，就用它整体替换 agentloop。
func Plugin() plugin.Plugin { return oneshotPlugin{} }

type oneshotPlugin struct{}

func (oneshotPlugin) Name() string { return "oneshotloop" }
func (oneshotPlugin) Inject() []string {
	return []string{"agents", "sessions", "llm", "tools", "prompt"}
}
func (oneshotPlugin) Apply(c *fiber.Context, _ map[string]any) error {
	return c.Effect(func(c *fiber.Context) (fiber.Disposer, error) {
		agents, ok := fiber.Get[*agent.Registry](c, "agents")
		if !ok {
			return nil, fmt.Errorf("oneshot: agents service unavailable")
		}
		return agents.SetFactory(Factory{}), nil
	})
}

func init() { plugin.Register(Plugin()) }
