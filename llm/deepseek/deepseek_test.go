package deepseek_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"hx/core/fiber"
	"hx/core/plugin"
	"hx/llm"
	"hx/llm/deepseek"
)

// sseBody 是一条最小的录制响应，只携带一个文本增量。
func sseBody(text string) string {
	return fmt.Sprintf("data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\ndata: [DONE]\n\n", text)
}

// newServer 统计请求数，并重放一条固定的 SSE 响应。
func newServer(text string, hits *atomic.Int64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, sseBody(text))
	}))
}

// drain 消费一条流式响应并返回其文本。
func drain(t *testing.T, seq func(func(llm.Chunk) bool)) string {
	t.Helper()
	var out strings.Builder
	for chunk := range seq {
		if chunk.Err != nil {
			t.Fatalf("stream error: %v", chunk.Err)
		}
		out.WriteString(chunk.Delta)
	}
	return out.String()
}

func TestNamedProviders(t *testing.T) {
	var hitsDeep, hitsOpen atomic.Int64
	serverDeep := newServer("from-deepseek", &hitsDeep)
	defer serverDeep.Close()
	serverOpen := newServer("from-openrouter", &hitsOpen)
	defer serverOpen.Close()

	root := fiber.New()
	ctx := root.Context()
	defer root.Dispose()

	if err := plugin.Mount(ctx, "llm", nil); err != nil {
		t.Fatal(err)
	}
	if err := plugin.Mount(ctx, "deepseek", map[string]any{"base_url": serverDeep.URL, "model": "m1"}); err != nil {
		t.Fatal(err)
	}
	// 同一个 provider 实现换个名字与端点再挂载一次，
	// 作为 OpenRouter 风格的兄弟实例。
	if err := plugin.Mount(ctx, "deepseek", map[string]any{"name": "openrouter", "base_url": serverOpen.URL, "model": "m2"}); err != nil {
		t.Fatal(err)
	}

	req := &llm.Request{Messages: []llm.Message{{Role: "user", Content: "hi"}}}

	// Provider 留空时回退到第一个注册的适配器。
	seq, err := llm.Stream(ctx, context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got := drain(t, seq); got != "from-deepseek" {
		t.Fatalf("default provider: got %q", got)
	}

	// 指定名字的 Provider 会切换适配器。
	named := &llm.Request{Provider: "openrouter", Messages: req.Messages}
	seq, err = llm.Stream(ctx, context.Background(), named)
	if err != nil {
		t.Fatal(err)
	}
	if got := drain(t, seq); got != "from-openrouter" {
		t.Fatalf("named provider: got %q", got)
	}

	// 未知 provider 会响亮失败，而不是回退。
	bad := &llm.Request{Provider: "missing", Messages: req.Messages}
	if _, err := llm.Stream(ctx, context.Background(), bad); err == nil {
		t.Fatal("unknown provider must fail")
	}

	// SetDefault 重新指定空 Provider 的指向。
	reg, ok := fiber.Get[*llm.Registry](ctx, "llm")
	if !ok {
		t.Fatal("llm service unavailable")
	}
	defer reg.SetDefault("openrouter")()
	seq, err = llm.Stream(ctx, context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got := drain(t, seq); got != "from-openrouter" {
		t.Fatalf("explicit default: got %q", got)
	}

	if got := hitsDeep.Load(); got != 1 {
		t.Fatalf("deepseek server saw %d requests; want 1", got)
	}
	if got := hitsOpen.Load(); got != 2 {
		t.Fatalf("openrouter server saw %d requests; want 2", got)
	}
}

// TestRequestWireShape 锚定 OpenAI 兼容的请求编码：assistant 的
// 工具调用把 name/arguments 嵌套在 "function" 下并带 type "function"，
// tool 结果携带 tool_call_id。真实 API 会以 HTTP 400 拒绝扁平的领域形态，
// 而纯回放测试永远看不到这一点。
func TestRequestWireShape(t *testing.T) {
	var captured []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, sseBody("ok"))
	}))
	defer server.Close()

	a := deepseek.New(deepseek.Config{BaseURL: server.URL, APIKey: "k"}, nil)
	seq := a.Stream(context.Background(), &llm.Request{Messages: []llm.Message{
		{Role: "user", Content: "hi"},
		{Role: "assistant", ToolCalls: []llm.ToolCall{{ID: "c1", Name: "bash", Arguments: `{"command":"ls"}`}}},
		{Role: "tool", ToolCallID: "c1", Content: "out"},
	}})
	if got := drain(t, seq); got != "ok" {
		t.Fatalf("got %q", got)
	}

	var body struct {
		Messages []struct {
			Role      string `json:"role"`
			ToolCalls []struct {
				ID       string `json:"id"`
				Type     string `json:"type"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
			ToolCallID string `json:"tool_call_id"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(captured, &body); err != nil {
		t.Fatalf("request body not JSON: %v", err)
	}
	if len(body.Messages) != 3 {
		t.Fatalf("got %d messages; want 3", len(body.Messages))
	}
	calls := body.Messages[1].ToolCalls
	if len(calls) != 1 {
		t.Fatalf("assistant message carries %d tool calls; want 1", len(calls))
	}
	if calls[0].ID != "c1" || calls[0].Type != "function" ||
		calls[0].Function.Name != "bash" || calls[0].Function.Arguments != `{"command":"ls"}` {
		t.Fatalf("bad tool call wire shape: %+v", calls[0])
	}
	if got := body.Messages[2].ToolCallID; got != "c1" {
		t.Fatalf("tool result tool_call_id = %q; want c1", got)
	}
}
