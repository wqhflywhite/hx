package agentloop_test

import (
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
	"hx/llm"
	"hx/session"
	"hx/tools"

	_ "hx/agentloop"
	_ "hx/llm"
	_ "hx/llm/deepseek"
	_ "hx/prompt"
)

const goldenTime = "2026-01-01T00:00:00Z"

// sseStep1 流式输出两段文本增量和一个工具调用。
const sseStep1 = `data: {"choices":[{"delta":{"content":"Hello"}}]}

data: {"choices":[{"delta":{"content":" world"}}]}

data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"fake_tool","arguments":"{\"x\":1}"}}]}}]}

data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}

data: [DONE]

`

// sseStep2 流式输出最终的文本回答。
const sseStep2 = `data: {"choices":[{"delta":{"content":"Done."}}]}

data: {"choices":[{"delta":{},"finish_reason":"stop"}]}

data: [DONE]

`

// goldenLog 是黄金回放那一轮会话日志的逐字节期望值。
var goldenLog = strings.Join([]string{
	`{"type":"user/message","time":"` + goldenTime + `","data":{"content":"say hi"}}`,
	`{"type":"turn/start","time":"` + goldenTime + `","data":{"session":"sess-fixed"}}`,
	`{"type":"step/start","time":"` + goldenTime + `","data":{"session":"sess-fixed","step":0}}`,
	`{"type":"assistant/chunk","time":"` + goldenTime + `","data":{"delta":"Hello"}}`,
	`{"type":"assistant/chunk","time":"` + goldenTime + `","data":{"delta":" world"}}`,
	`{"type":"assistant/message","time":"` + goldenTime + `","data":{"content":"Hello world","tool_calls":[{"id":"call_1","name":"fake_tool","arguments":"{\"x\":1}"}]}}`,
	`{"type":"tool/call","time":"` + goldenTime + `","data":{"id":"call_1","name":"fake_tool","arguments":"{\"x\":1}"}}`,
	`{"type":"tool/result","time":"` + goldenTime + `","data":{"call_id":"call_1","name":"fake_tool","content":"fake-output"}}`,
	`{"type":"step/end","time":"` + goldenTime + `","data":{"session":"sess-fixed","step":0}}`,
	`{"type":"step/start","time":"` + goldenTime + `","data":{"session":"sess-fixed","step":1}}`,
	`{"type":"assistant/chunk","time":"` + goldenTime + `","data":{"delta":"Done."}}`,
	`{"type":"assistant/message","time":"` + goldenTime + `","data":{"content":"Done."}}`,
	`{"type":"step/end","time":"` + goldenTime + `","data":{"session":"sess-fixed","step":1}}`,
	`{"type":"turn/end","time":"` + goldenTime + `","data":{"session":"sess-fixed"}}`,
}, "\n") + "\n"

// fakeTool 是内建工具的确定性替身。
type fakeTool struct{}

func (fakeTool) Name() string { return "fake_tool" }
func (fakeTool) Schema() map[string]any {
	return map[string]any{"description": "test double", "type": "object"}
}
func (fakeTool) Execute(_ *fiber.Context, _ json.RawMessage) (any, error) {
	return "fake-output", nil
}

// fakeServer 回放录制的 SSE 响应并捕获请求体。
type fakeServer struct {
	*httptest.Server
	mu     sync.Mutex
	bodies []requestCapture
}

// requestCapture 以领域消息形式保存一个捕获到的请求体。
type requestCapture struct {
	Messages []llm.Message
}

// UnmarshalJSON 解码 OpenAI 兼容的线上格式（工具调用嵌套在
// "function" 之下）并还原成扁平的领域消息，使模型输入可以与
// 日志投影对比。
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

func TestGoldenReplay(t *testing.T) {
	server := newFakeServer()
	defer server.Close()
	dir := t.TempDir()

	root := fiber.New()
	ctx := root.Context()
	defer root.Dispose()

	cfg := map[string]any{
		"dir":   dir,
		"clock": func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
		"idgen": func() string { return "sess-fixed" },
	}
	for _, step := range []struct {
		name string
		cfg  map[string]any
	}{
		{"session", cfg},
		{"llm", nil},
		{"deepseek", map[string]any{"base_url": server.URL, "api_key": "test-key", "model": "test-model"}},
		{"tools", nil},
		{"prompt", nil},
		{"agents", nil},
		{"agentloop", nil},
	} {
		if err := plugin.Mount(ctx, step.name, step.cfg); err != nil {
			t.Fatalf("mount %s: %v", step.name, err)
		}
	}
	toolsReg, ok := fiber.Get[*tools.Registry](ctx, "tools")
	if !ok {
		t.Fatal("tools service unavailable")
	}
	defer toolsReg.Register(fakeTool{})()

	turnDone := make(chan session.TurnEndData, 1)
	event.On[session.Event](ctx, "session/event", func(_ *fiber.Context, ev session.Event) {
		if ev.Type == session.TurnEnd {
			var d session.TurnEndData
			if json.Unmarshal(ev.Data, &d) == nil {
				turnDone <- d
			}
		}
	})

	agents, _ := fiber.Get[*agent.Registry](ctx, "agents")
	ag, err := agents.Create(ctx, agent.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer ag.Dispose()

	ag.Send("say hi")
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

	// 第二个请求必须恰好携带日志投影加上 system。
	server.mu.Lock()
	bodies := append([]requestCapture{}, server.bodies...)
	server.mu.Unlock()
	if len(bodies) != 2 {
		t.Fatalf("server saw %d requests; want 2", len(bodies))
	}
	mgr, err := session.NewManager(dir, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	events, err := mgr.Events("sess-fixed")
	if err != nil {
		t.Fatal(err)
	}
	// 第二个请求发出时，最后的 assistant 消息（以及收尾的
	// step/turn 事件）尚未写入日志；模型输入必须等于请求时刻
	// 日志状态所对应的投影，也就是截至最后一条
	// tool/result 为止的部分。
	trimmed := events
	for len(trimmed) > 0 && trimmed[len(trimmed)-1].Type != session.ToolResult {
		trimmed = trimmed[:len(trimmed)-1]
	}
	projected := session.DeriveMessages(trimmed)
	gotRaw, _ := json.Marshal(bodies[1].Messages[1:])
	wantRaw, _ := json.Marshal(projected)
	if string(gotRaw) != string(wantRaw) {
		t.Fatalf("model input diverges from log projection:\ngot  %s\nwant %s", gotRaw, wantRaw)
	}
	if len(bodies[1].Messages) == 0 || bodies[1].Messages[0].Role != "system" {
		t.Fatal("first message must be the assembled system prompt")
	}
}
