package oneshot_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"hx/agent"
	"hx/core/event"
	"hx/core/fiber"
	"hx/core/plugin"
	"hx/hxconfig"
	"hx/llm"
	"hx/session"
	"hx/tools"

	_ "hx/agentloop/oneshot"
	_ "hx/llm"
	_ "hx/llm/deepseek"
	_ "hx/prompt"
	_ "hx/session"
	_ "hx/tools"
)

const goldenTime = "2026-01-01T00:00:00Z"

// sseStep1 流式返回一次工具调用；sseStep2 流式返回收尾文本。
const sseStep1 = `data: {"choices":[{"delta":{"content":"Let me check."}}]}

data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"fake_tool","arguments":"{\"x\":1}"}}]}}]}

data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}

data: [DONE]

`

const sseStep2 = `data: {"choices":[{"delta":{"content":"One step only."}}]}

data: {"choices":[{"delta":{},"finish_reason":"stop"}]}

data: [DONE]

`

// goldenLog 是固定形态 turn（一轮）的逐字节期望日志：恰好一轮
// 工具调用，然后是一个不带工具的收尾 step。
var goldenLog = strings.Join([]string{
	`{"type":"user/message","time":"` + goldenTime + `","data":{"content":"do it"}}`,
	`{"type":"turn/start","time":"` + goldenTime + `","data":{"session":"sess-fixed"}}`,
	`{"type":"step/start","time":"` + goldenTime + `","data":{"session":"sess-fixed","step":0}}`,
	`{"type":"assistant/chunk","time":"` + goldenTime + `","data":{"delta":"Let me check."}}`,
	`{"type":"assistant/message","time":"` + goldenTime + `","data":{"content":"Let me check.","tool_calls":[{"id":"call_1","name":"fake_tool","arguments":"{\"x\":1}"}]}}`,
	`{"type":"tool/call","time":"` + goldenTime + `","data":{"id":"call_1","name":"fake_tool","arguments":"{\"x\":1}"}}`,
	`{"type":"tool/result","time":"` + goldenTime + `","data":{"call_id":"call_1","name":"fake_tool","content":"fake-output"}}`,
	`{"type":"step/end","time":"` + goldenTime + `","data":{"session":"sess-fixed","step":0}}`,
	`{"type":"step/start","time":"` + goldenTime + `","data":{"session":"sess-fixed","step":1}}`,
	`{"type":"assistant/chunk","time":"` + goldenTime + `","data":{"delta":"One step only."}}`,
	`{"type":"assistant/message","time":"` + goldenTime + `","data":{"content":"One step only."}}`,
	`{"type":"step/end","time":"` + goldenTime + `","data":{"session":"sess-fixed","step":1}}`,
	`{"type":"turn/end","time":"` + goldenTime + `","data":{"session":"sess-fixed"}}`,
}, "\n") + "\n"

// fakeTool 是内建工具的确定性替身。
type fakeTool struct{}

func (fakeTool) Name() string { return "fake_tool" }
func (fakeTool) Schema() map[string]any {
	return map[string]any{"description": "test double", "type": "object"}
}
func (fakeTool) Execute(_ context.Context, _ *fiber.Context, _ json.RawMessage) (any, error) {
	return "fake-output", nil
}

type requestCapture struct {
	Messages []llm.Message `json:"messages"`
	Tools    []any         `json:"tools"`
}

// fakeServer 回放录制好的响应，并捕获请求体。
type fakeServer struct {
	*httptest.Server
	mu     sync.Mutex
	bodies []requestCapture
}

func newFakeServer() *fakeServer {
	f := &fakeServer{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body requestCapture
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		n := len(f.bodies)
		f.bodies = append(f.bodies, body)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		if n == 0 {
			fmt.Fprint(w, sseStep1)
		} else {
			fmt.Fprint(w, sseStep2)
		}
	}))
	return f
}

func TestOneshotViaConfig(t *testing.T) {
	server := newFakeServer()
	defer server.Close()
	dir := t.TempDir()

	// 整套驱动器替换只是一行配置：在本该写 agentloop 的位置写
	// oneshotloop。clock 和 idgen 通过编程式 overlay 注入。
	path := filepath.Join(dir, "hx.yml")
	body := fmt.Sprintf(`
plugins:
  - id: session
    name: session
    config: {dir: %q}
  - id: llm
    name: llm
  - id: deepseek
    name: deepseek
    config: {base_url: %q, api_key: test-key, model: test-model}
  - id: tools
    name: tools
  - id: prompt
    name: prompt
  - id: agents
    name: agents
  - id: loop
    name: oneshotloop
`, dir, server.URL)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := hxconfig.Load(path)
	if err != nil {
		t.Fatal(err)
	}

	root := fiber.New()
	defer root.Dispose()
	overlays := hxconfig.Overlays{"session": {
		"clock": func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
		"idgen": func() string { return "sess-fixed" },
	}}
	stack, err := hxconfig.MountStack(root.Context(), f, overlays)
	if err != nil {
		t.Fatal(err)
	}
	c := stack.Layer()

	toolsReg, ok := fiber.Get[*tools.Registry](c, "tools")
	if !ok {
		t.Fatal("tools service unavailable")
	}
	defer toolsReg.Register(fakeTool{})()

	agents, ok := fiber.Get[*agent.Registry](c, "agents")
	if !ok {
		t.Fatal("agents service unavailable")
	}
	ag, err := agents.Create(c, agent.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer ag.Dispose()
	// 选择实现的是配置组装，而不是这个测试。
	if got := fmt.Sprintf("%T", ag); !strings.Contains(got, "oneshot") {
		t.Fatalf("agents.Create returned %s; want the oneshot driver", got)
	}

	turnDone := make(chan session.TurnEndData, 1)
	event.On[session.Event](root.Context(), "session/event", func(_ *fiber.Context, ev session.Event) {
		if ev.Type == session.TurnEnd {
			var d session.TurnEndData
			if json.Unmarshal(ev.Data, &d) == nil {
				turnDone <- d
			}
		}
	})

	ag.Send("do it")
	select {
	case end := <-turnDone:
		if end.Error != "" {
			t.Fatalf("turn error: %s", end.Error)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("turn did not finish")
	}

	raw, err := os.ReadFile(filepath.Join(dir, "sess-fixed.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != goldenLog {
		t.Fatalf("session log mismatch:\n--- got ---\n%s\n--- want ---\n%s", raw, goldenLog)
	}

	// 恰好两个请求：一个带工具，一个不带。
	server.mu.Lock()
	bodies := append([]requestCapture{}, server.bodies...)
	server.mu.Unlock()
	if len(bodies) != 2 {
		t.Fatalf("server saw %d requests; want exactly 2", len(bodies))
	}
	if len(bodies[0].Tools) == 0 {
		t.Fatal("the first request must offer tools")
	}
	if len(bodies[1].Tools) != 0 {
		t.Fatal("the wrap-up request must offer no tools")
	}
}

// TestToolsUnavailableMidTurn 覆盖热重挂窗口：turn 执行工具调用时
// tools 服务已被 revoke，driver 不得 panic；它必须为
// assistant/message 已宣布的调用补写失败的 tool/result 保持配对，
// 以错误收尾 turn，且不跑收尾 step——使下一轮请求合法。
func TestToolsUnavailableMidTurn(t *testing.T) {
	server := newFakeServer()
	defer server.Close()
	dir := t.TempDir()

	root := fiber.New()
	defer root.Dispose()
	ctx := root.Context()

	sessCfg := map[string]any{
		"dir":   dir,
		"clock": func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
		"idgen": func() string { return "sess-fixed" },
	}
	for _, step := range []struct {
		name string
		cfg  map[string]any
	}{
		{"session", sessCfg},
		{"llm", nil},
		{"deepseek", map[string]any{"base_url": server.URL, "api_key": "test-key", "model": "test-model"}},
		{"prompt", nil},
		{"agents", nil},
	} {
		if err := plugin.Mount(ctx, step.name, step.cfg); err != nil {
			t.Fatalf("mount %s: %v", step.name, err)
		}
	}
	// tools 由测试手动提供，以便在 turn 前撤销，模拟热重挂窗口。
	revokeTools := fiber.Provide(ctx, "tools", tools.NewRegistry())
	if err := plugin.Mount(ctx, "oneshotloop", nil); err != nil {
		t.Fatalf("mount oneshotloop: %v", err)
	}

	agents, ok := fiber.Get[*agent.Registry](ctx, "agents")
	if !ok {
		t.Fatal("agents service unavailable")
	}
	ag, err := agents.Create(ctx, agent.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer ag.Dispose()

	turnDone := make(chan session.TurnEndData, 1)
	event.On[session.Event](ctx, "session/event", func(_ *fiber.Context, ev session.Event) {
		if ev.Type == session.TurnEnd {
			var d session.TurnEndData
			if json.Unmarshal(ev.Data, &d) == nil {
				turnDone <- d
			}
		}
	})

	if err := revokeTools(); err != nil {
		t.Fatal(err)
	}
	ag.Send("do it")
	select {
	case end := <-turnDone:
		if end.Error == "" {
			t.Fatal("turn must end with an error when the tools service is gone")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("turn did not finish")
	}

	// 日志形态：已宣布的调用有配对的失败 result，没有收尾 step，
	// turn 带错误。
	raw, err := os.ReadFile(filepath.Join(dir, "sess-fixed.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var types []string
	var resultErr string
	for _, ln := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		var ev session.Event
		if err := json.Unmarshal([]byte(ln), &ev); err != nil {
			t.Fatalf("bad log line: %v", err)
		}
		types = append(types, ev.Type)
		if ev.Type == session.ToolResult {
			var d session.ToolResultData
			if err := json.Unmarshal(ev.Data, &d); err != nil {
				t.Fatal(err)
			}
			resultErr = d.Error
		}
	}
	want := []string{
		session.UserMessage, session.TurnStart, session.StepStart,
		session.AssistantChunk, session.AssistantMessage,
		session.ToolCall, session.ToolResult,
		session.StepEnd, session.TurnEnd,
	}
	if strings.Join(types, ",") != strings.Join(want, ",") {
		t.Fatalf("event sequence = %v; want %v", types, want)
	}
	if resultErr == "" {
		t.Fatal("tool/result must carry the failure, keeping the announced call paired")
	}

	// 投影配对合法：带 ToolCalls 的 assistant 消息紧跟同 ID 的 tool 消息。
	mgr, err := session.NewManager(dir, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	events, err := mgr.Events("sess-fixed")
	if err != nil {
		t.Fatal(err)
	}
	msgs := session.DeriveMessages(events)
	for i, m := range msgs {
		for _, tc := range m.ToolCalls {
			if i+1 >= len(msgs) || msgs[i+1].Role != "tool" || msgs[i+1].ToolCallID != tc.ID {
				t.Fatalf("dangling tool call %q in projection: %+v", tc.ID, msgs)
			}
		}
	}
}
