package subagent_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"hx/agent"
	"hx/core/event"
	"hx/core/fiber"
	"hx/core/plugin"
	"hx/llm"
	"hx/session"
	"hx/tools"

	_ "hx/agentloop"
	_ "hx/llm"
	_ "hx/llm/deepseek"
	_ "hx/prompt"
	_ "hx/subagent"
)

// sseText 流式返回一段完整的文本回答。
func sseText(content string) string {
	return `data: {"choices":[{"delta":{"content":"` + content + `"}}]}

data: {"choices":[{"delta":{},"finish_reason":"stop"}]}

data: [DONE]

`
}

// sseChunk 流式返回一个不完整的文本分片（没有 finish 与 [DONE]），
// 用于编排「说到一半卡住」的子 agent。
func sseChunk(content string) string {
	quoted, _ := json.Marshal(content)
	return `data: {"choices":[{"delta":{"content":` + string(quoted) + `}}]}` + "\n\n"
}

// sseCall 流式返回一次工具调用；args 是原始 JSON 参数串。
func sseCall(id, name, args string) string {
	quoted, _ := json.Marshal(args)
	return `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"` + id + `","function":{"name":"` + name + `","arguments":` + string(quoted) + `}}]}}]}

data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}

data: [DONE]

`
}

// requestCapture 把 OpenAI 兼容的 wire 结构展平为领域消息。
type requestCapture struct {
	Messages []llm.Message
}

// UnmarshalJSON 解码 wire 结构（工具调用嵌套在 "function" 下）。
func (c *requestCapture) UnmarshalJSON(raw []byte) error {
	var wire struct {
		Messages []struct {
			Role      string `json:"role"`
			Content   string `json:"content"`
			ToolCalls []struct {
				ID       string `json:"id"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
			ToolCallID string `json:"tool_call_id"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	for _, m := range wire.Messages {
		msg := llm.Message{Role: m.Role, Content: m.Content, ToolCallID: m.ToolCallID}
		for _, tc := range m.ToolCalls {
			msg.ToolCalls = append(msg.ToolCalls, llm.ToolCall{ID: tc.ID, Name: tc.Function.Name, Arguments: tc.Function.Arguments})
		}
		c.Messages = append(c.Messages, msg)
	}
	return nil
}

// fakeServer 通过检查请求消息来路由编排好的 SSE 响应。
// routeRW 非空时改由它直接写 ResponseWriter，用于需要
// 先吐出部分内容再挂起的编排。
type fakeServer struct {
	*httptest.Server
	route   func(msgs []llm.Message, r *http.Request) string
	routeRW func(msgs []llm.Message, w http.ResponseWriter, r *http.Request)
}

func newFakeServer(route func(msgs []llm.Message, r *http.Request) string) *fakeServer {
	f := &fakeServer{route: route}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serve))
	return f
}

func newFakeServerRW(route func(msgs []llm.Message, w http.ResponseWriter, r *http.Request)) *fakeServer {
	f := &fakeServer{routeRW: route}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serve))
	return f
}

func (f *fakeServer) serve(w http.ResponseWriter, r *http.Request) {
	var body requestCapture
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if f.routeRW != nil {
		f.routeRW(body.Messages, w, r)
		return
	}
	if out := f.route(body.Messages, r); out != "" {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, out)
	}
}

// hasToolMessage 报告请求中是否已携带工具结果。
func hasToolMessage(msgs []llm.Message) bool {
	for _, m := range msgs {
		if m.Role == "tool" {
			return true
		}
	}
	return false
}

// lastUserContent 返回最后一条 user 消息的内容。
func lastUserContent(msgs []llm.Message) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == "user" {
			return msgs[i].Content
		}
	}
	return ""
}

// toolResultContent 返回最后一条 tool 消息的内容。
func toolResultContent(msgs []llm.Message) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == "tool" {
			return msgs[i].Content
		}
	}
	return ""
}

// wire 在全新的 root 上挂载插件栈，返回 context、
// 会话管理器和 agent 注册表。
func wire(t *testing.T, server *fakeServer, subagentCfg map[string]any) (*fiber.Context, *session.Manager, *agent.Registry) {
	t.Helper()
	dir := t.TempDir()
	root := fiber.New()
	ctx := root.Context()
	t.Cleanup(func() { _ = root.Dispose() })

	var n int
	sessionCfg := map[string]any{
		"dir":   dir,
		"clock": func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
		"idgen": func() string { n++; return fmt.Sprintf("sess-%d", n) },
	}
	for _, step := range []struct {
		name string
		cfg  map[string]any
	}{
		{"session", sessionCfg},
		{"llm", nil},
		{"deepseek", map[string]any{"base_url": server.URL, "api_key": "test-key", "model": "test-model"}},
		{"tools", nil},
		{"prompt", nil},
		{"agents", nil},
		{"agentloop", nil},
		{"subagent", subagentCfg},
	} {
		if err := plugin.Mount(ctx, step.name, step.cfg); err != nil {
			t.Fatalf("mount %s: %v", step.name, err)
		}
	}
	mgr, ok := fiber.Get[*session.Manager](ctx, "sessions")
	if !ok {
		t.Fatal("sessions service unavailable")
	}
	agents, ok := fiber.Get[*agent.Registry](ctx, "agents")
	if !ok {
		t.Fatal("agents service unavailable")
	}
	return ctx, mgr, agents
}

// runTurn 向一个全新的 agent 发送文本，并等待它自己的 turn（一轮）结束。
func runTurn(t *testing.T, ctx *fiber.Context, agents *agent.Registry, text string) (agent.Agent, session.TurnEndData) {
	t.Helper()
	ag, err := agents.Create(ctx, agent.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ag.Dispose() })
	turnDone := make(chan session.TurnEndData, 1)
	defer event.On[session.Event](ctx, "session/event", func(_ *fiber.Context, ev session.Event) {
		if ev.Type == session.TurnEnd {
			var d session.TurnEndData
			if json.Unmarshal(ev.Data, &d) == nil && d.Session == ag.SessionID() {
				select {
				case turnDone <- d:
				default:
				}
			}
		}
	})()

	ag.Send(text)
	select {
	case end := <-turnDone:
		return ag, end
	case <-time.After(10 * time.Second):
		t.Fatal("turn did not finish")
		return nil, session.TurnEndData{}
	}
}

// toolResults 按日志顺序返回一个会话的全部 ToolResult 载荷。
func toolResults(t *testing.T, mgr *session.Manager, id string) []session.ToolResultData {
	t.Helper()
	events, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	var out []session.ToolResultData
	for _, ev := range events {
		if ev.Type == session.ToolResult {
			var d session.ToolResultData
			if err := json.Unmarshal(ev.Data, &d); err != nil {
				t.Fatal(err)
			}
			out = append(out, d)
		}
	}
	return out
}

// eventTypes 返回一个会话日志的事件类型序列。
func eventTypes(t *testing.T, mgr *session.Manager, id string) []string {
	t.Helper()
	events, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(events))
	for _, ev := range events {
		out = append(out, ev.Type)
	}
	return out
}

func TestDelegateRunsSubagent(t *testing.T) {
	server := newFakeServer(func(msgs []llm.Message, _ *http.Request) string {
		switch {
		case hasToolMessage(msgs):
			return sseText("parent done")
		case lastUserContent(msgs) == "summarize X":
			return sseText("child answer: X summarized")
		default:
			return sseCall("call_1", "delegate", `{"task":"summarize X","label":"summarize"}`)
		}
	})
	defer server.Close()
	ctx, mgr, agents := wire(t, server, nil)

	ag, end := runTurn(t, ctx, agents, "do it with help")
	if end.Error != "" {
		t.Fatalf("turn error: %s", end.Error)
	}

	parentID := ag.SessionID()
	results := toolResults(t, mgr, parentID)
	if len(results) != 1 {
		t.Fatalf("parent log has %d tool results; want 1", len(results))
	}
	if results[0].Name != "delegate" {
		t.Fatalf("parent tool result is %q; want delegate", results[0].Name)
	}
	if results[0].Error != "" {
		t.Fatalf("delegate result error: %s", results[0].Error)
	}
	if results[0].Content != "child answer: X summarized" {
		t.Fatalf("delegate result = %q; want the child's final answer", results[0].Content)
	}

	// 子 agent 的 turn（一轮）记录在它自己独立的日志里。
	ids, err := mgr.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("session logs = %v; want parent plus child", ids)
	}
	var childID string
	for _, id := range ids {
		if id != parentID {
			childID = id
		}
	}
	types := strings.Join(eventTypes(t, mgr, childID), ",")
	for _, want := range []string{session.UserMessage, session.TurnStart, session.AssistantMessage, session.TurnEnd} {
		if !strings.Contains(types, want) {
			t.Fatalf("child log %s missing %q (types: %s)", childID, want, types)
		}
	}
	events, err := mgr.Events(childID)
	if err != nil {
		t.Fatal(err)
	}
	var d session.UserMessageData
	if err := json.Unmarshal(events[0].Data, &d); err != nil || d.Content != "summarize X" {
		t.Fatalf("child log opens with %+v; want user message %q", events[0], "summarize X")
	}
}

func TestDelegateDepthLimit(t *testing.T) {
	server := newFakeServer(func(msgs []llm.Message, _ *http.Request) string {
		switch {
		case strings.Contains(toolResultContent(msgs), "delegation depth limit"):
			return sseText("depth limited")
		case hasToolMessage(msgs):
			return sseText("parent done")
		case lastUserContent(msgs) == "inner subtask":
			return sseCall("call_inner", "delegate", `{"task":"nested subtask"}`)
		default:
			return sseCall("call_outer", "delegate", `{"task":"inner subtask"}`)
		}
	})
	defer server.Close()
	ctx, mgr, agents := wire(t, server, nil)

	ag, end := runTurn(t, ctx, agents, "outer task")
	if end.Error != "" {
		t.Fatalf("turn error: %s", end.Error)
	}

	// 父 agent 的 delegate 结果就是子 agent 的最终回答。
	results := toolResults(t, mgr, ag.SessionID())
	if len(results) != 1 || results[0].Content != "depth limited" {
		t.Fatalf("parent tool results = %+v; want one delegate result %q", results, "depth limited")
	}

	// 子 agent 自己的 delegate 调用被深度闸门拒绝。
	ids, err := mgr.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("session logs = %v; want exactly parent plus one child (no nested agent)", ids)
	}
	for _, id := range ids {
		if id == ag.SessionID() {
			continue
		}
		results := toolResults(t, mgr, id)
		if len(results) != 1 {
			t.Fatalf("child log has %d tool results; want 1", len(results))
		}
		if !strings.Contains(results[0].Error, "delegation depth limit reached") {
			t.Fatalf("child delegate error = %q; want the depth limit", results[0].Error)
		}
	}
}

// settleGoroutines 等待 goroutine 回落后返回当前数量，
// 用于钉住泄漏断言。
func settleGoroutines() int {
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	return runtime.NumGoroutine()
}

// childSession 返回与 parentID 不同的那份会话日志 id。
func childSession(t *testing.T, mgr *session.Manager, parentID string) string {
	t.Helper()
	ids, err := mgr.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if id != parentID {
			return id
		}
	}
	t.Fatalf("session logs = %v; want a child log besides %s", ids, parentID)
	return ""
}

func TestDelegateTimeoutDisposesSubagent(t *testing.T) {
	before := settleGoroutines()

	server := newFakeServer(func(msgs []llm.Message, r *http.Request) string {
		switch {
		case hasToolMessage(msgs):
			return sseText("parent done")
		case lastUserContent(msgs) == "slow subtask":
			<-r.Context().Done() // 挂起直到客户端断开
			return ""
		default:
			return sseCall("call_1", "delegate", `{"task":"slow subtask"}`)
		}
	})
	ctx, mgr, agents := wire(t, server, map[string]any{"timeout_seconds": 1})

	ag, end := runTurn(t, ctx, agents, "do something slow")
	if end.Error != "" {
		t.Fatalf("turn error: %s", end.Error)
	}

	results := toolResults(t, mgr, ag.SessionID())
	if len(results) != 1 {
		t.Fatalf("parent log has %d tool results; want 1", len(results))
	}
	if !strings.Contains(results[0].Error, "timed out") {
		t.Fatalf("delegate error = %q; want a timeout", results[0].Error)
	}

	// 拆除全部资源后断言没有 goroutine 泄漏：被 dispose 的
	// 子 agent 的 turn 与 watcher goroutine 都必须消失。
	_ = ag.Dispose()
	server.Close()
	if after := settleGoroutines(); after > before {
		t.Fatalf("goroutines: before %d, after %d; subagent leaked", before, after)
	}
}

func TestDelegateTimeoutReturnsPartialAnswer(t *testing.T) {
	server := newFakeServerRW(func(msgs []llm.Message, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		switch {
		case hasToolMessage(msgs):
			fmt.Fprint(w, sseText("parent done"))
		case lastUserContent(msgs) == "slow subtask":
			// 先吐出半截回答再挂起：超时后这半截要作为部分答案返回。
			fmt.Fprint(w, sseChunk("partial child answer"))
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			fmt.Fprint(w, sseCall("call_1", "delegate", `{"task":"slow subtask"}`))
		}
	})
	defer server.Close()
	ctx, mgr, agents := wire(t, server, map[string]any{"timeout_seconds": 1})

	ag, end := runTurn(t, ctx, agents, "do something slow")
	if end.Error != "" {
		t.Fatalf("turn error: %s", end.Error)
	}

	results := toolResults(t, mgr, ag.SessionID())
	if len(results) != 1 {
		t.Fatalf("parent log has %d tool results; want 1", len(results))
	}
	childID := childSession(t, mgr, ag.SessionID())
	for _, want := range []string{"timed out", childID, "partial child answer"} {
		if !strings.Contains(results[0].Error, want) {
			t.Fatalf("delegate error = %q; want it to contain %q", results[0].Error, want)
		}
	}
}

func TestDelegateRevokedByFiberDispose(t *testing.T) {
	before := settleGoroutines()

	started := make(chan struct{})
	var once sync.Once
	server := newFakeServerRW(func(_ []llm.Message, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, sseChunk("partial child answer"))
		w.(http.Flusher).Flush()
		once.Do(func() { close(started) })
		<-r.Context().Done() // 挂起直到客户端断开
	})
	defer server.Close()
	ctx, mgr, _ := wire(t, server, map[string]any{"timeout_seconds": 30})

	reg, ok := fiber.Get[*tools.Registry](ctx, "tools")
	if !ok {
		t.Fatal("tools service unavailable")
	}
	tool, ok := reg.Get("delegate")
	if !ok {
		t.Fatal("delegate tool not registered")
	}

	// 直接以受控 fiber 调用工具：fiber 名字带 "agent:" 前缀，
	// 与 agentloop 传入的 agent fiber 一致，深度计为 1。
	fb, err := ctx.Fiber().New("agent:test-parent")
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		val any
		err error
	}
	resultCh := make(chan outcome, 1)
	go func() {
		val, err := tool.Execute(context.Background(), fb.Context(), json.RawMessage(`{"task":"slow subtask"}`))
		resultCh <- outcome{val, err}
	}()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("subagent did not start")
	}
	childID := childSession(t, mgr, "")

	// 等半截回答真正落进子会话投影再撤销，否则部分答案断言是悬的。
	var partialVisible bool
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		msgs, err := mgr.Messages(childID)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range msgs {
			if m.Role == "assistant" && strings.Contains(m.Content, "partial child answer") {
				partialVisible = true
			}
		}
		if partialVisible {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !partialVisible {
		t.Fatal("partial answer never landed in the child log")
	}

	start := time.Now()
	if err := fb.Dispose(); err != nil {
		t.Fatal(err)
	}
	select {
	case out := <-resultCh:
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("delegate took %s to notice disposal; want it immediate (timeout is 30s)", elapsed)
		}
		if out.err == nil {
			t.Fatalf("delegate returned %+v; want an interruption error", out.val)
		}
		for _, want := range []string{"disposed", childID, "partial child answer"} {
			if !strings.Contains(out.err.Error(), want) {
				t.Fatalf("delegate error = %q; want it to contain %q", out.err.Error(), want)
			}
		}
	case <-time.After(10 * time.Second):
		t.Fatal("delegate did not return after fiber dispose")
	}

	server.Close()
	if after := settleGoroutines(); after > before {
		t.Fatalf("goroutines: before %d, after %d; subagent leaked", before, after)
	}
}
