package compaction_test

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hx/agent"
	"hx/compaction"
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

// fakeAdapter 对每个请求都回复同一段固定内容，并记录收到的请求。
type fakeAdapter struct {
	response string
	reqs     []*llm.Request
}

func (f *fakeAdapter) Stream(_ context.Context, req *llm.Request) iter.Seq[llm.Chunk] {
	f.reqs = append(f.reqs, req)
	response := f.response
	return func(yield func(llm.Chunk) bool) {
		yield(llm.Chunk{Delta: response})
	}
}

// newCompactor 在一个全新的 root 上挂载插件，底层是 fake adapter 和一个
// 临时目录的 session 管理器。
func newCompactor(t *testing.T, cfg map[string]any) (*fiber.Context, compaction.Compactor, *session.Manager, *fakeAdapter) {
	t.Helper()
	root := fiber.New()
	ctx := root.Context()
	t.Cleanup(func() { _ = root.Dispose() })

	adapter := &fakeAdapter{response: "SUMMARY"}
	reg := llm.NewRegistry()
	reg.SetAdapter("fake", adapter)
	fiber.Provide[*llm.Registry](ctx, "llm", reg)

	mgr, err := session.NewManager(t.TempDir(), func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }, func() string { return "s-1" })
	if err != nil {
		t.Fatal(err)
	}
	if err := plugin.Mount(ctx, "compaction", cfg); err != nil {
		t.Fatal(err)
	}
	compactor, ok := fiber.Get[compaction.Compactor](ctx, "compaction")
	if !ok {
		t.Fatal("compaction service unavailable")
	}
	return ctx, compactor, mgr, adapter
}

func appendAll(t *testing.T, ctx *fiber.Context, store session.Store, evs []session.Event) {
	t.Helper()
	for _, ev := range evs {
		if err := store.Append(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCompactsOverBudget(t *testing.T) {
	// keep_first_events 显式置 0：本条用例钉住的是不保留头部的旧行为。
	ctx, compactor, mgr, adapter := newCompactor(t, map[string]any{"max_context_tokens": 1, "keep_recent_events": 2, "keep_first_events": 0})
	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer store.(interface{ Close() error }).Close()
	appendAll(t, ctx, store, []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "old question"})},
		{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{Content: "old answer"})},
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "recent question"})},
		{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{Content: "recent answer"})},
	})

	done, err := compactor.MaybeCompact(ctx, context.Background(), store, id)
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("over-budget history was not compacted")
	}

	// 摘要是一条普通的追加事件，携带模型产出的文本；日志保留所有原始
	// 事件（只追加不删除）。
	events, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 5 {
		t.Fatalf("log has %d events; want the 4 originals plus the summary", len(events))
	}
	last := events[len(events)-1]
	if last.Type != session.Summary {
		t.Fatalf("last event = %q; want %q", last.Type, session.Summary)
	}
	var d session.SummaryData
	if err := json.Unmarshal(last.Data, &d); err != nil || d.Content != "SUMMARY" {
		t.Fatalf("summary payload = %s; want SUMMARY", last.Data)
	}

	// keep_recent 把尾部两条事件挡在了摘要器的转录文本之外，它们不参与
	// 本次摘要。
	if len(adapter.reqs) != 1 {
		t.Fatalf("summarizer saw %d requests; want 1", len(adapter.reqs))
	}
	transcript := adapter.reqs[0].Messages[1].Content
	if !strings.Contains(transcript, "old question") {
		t.Fatalf("transcript misses the old segment: %q", transcript)
	}
	if strings.Contains(transcript, "recent question") {
		t.Fatalf("transcript includes kept-recent events: %q", transcript)
	}

	// 摘要是位置水位线：投影 = 摘要 + 其后落盘的事件（刚压缩完时后面
	// 还没有任何东西）。
	msgs := session.DeriveMessages(events)
	if len(msgs) != 1 || msgs[0].Role != "user" || msgs[0].Content != "SUMMARY" {
		t.Fatalf("projection = %+v; want just the summary", msgs)
	}
}

func TestNoCompactionWithinBudget(t *testing.T) {
	ctx, compactor, mgr, adapter := newCompactor(t, map[string]any{"max_context_tokens": 1 << 20, "keep_recent_events": 2})
	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer store.(interface{ Close() error }).Close()
	appendAll(t, ctx, store, []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "hi"})},
		{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{Content: "hello"})},
	})

	done, err := compactor.MaybeCompact(ctx, context.Background(), store, id)
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Fatal("within-budget history was compacted")
	}
	if len(adapter.reqs) != 0 {
		t.Fatal("summarizer was called within budget")
	}
	events, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if ev.Type == session.Summary {
			t.Fatal("summary event appended within budget")
		}
	}
}

// TestCompactionKeepsToolPairsTogether 覆盖边界规则：朴素切点若落在一次
// 工具调用与其结果之间会被后移，保证摘要器的转录文本里绝不会只有
// 一对中的半边。
func TestCompactionKeepsToolPairsTogether(t *testing.T) {
	ctx, compactor, mgr, adapter := newCompactor(t, map[string]any{"max_context_tokens": 1, "keep_recent_events": 3})
	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer store.(interface{ Close() error }).Close()
	call := llm.ToolCall{ID: "c1", Name: "bash", Arguments: `{"command":"ls"}`}
	appendAll(t, ctx, store, []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "list files"})},
		{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{ToolCalls: []llm.ToolCall{call}})},
		{Type: session.ToolCall, Data: session.MarshalData(session.ToolCallData{ID: "c1", Name: "bash", Arguments: `{"command":"ls"}`})},
		{Type: session.ToolResult, Data: session.MarshalData(session.ToolResultData{CallID: "c1", Name: "bash", Content: "file.txt"})},
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "thanks"})},
		{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{Content: "welcome"})},
	})

	done, err := compactor.MaybeCompact(ctx, context.Background(), store, id)
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("over-budget history was not compacted")
	}
	// keep_recent=3 的朴素切点会落在 tool/call 与 tool/result 之间；后移
	// 边界后，整个工具调用对都进入摘要器的转录文本。
	if len(adapter.reqs) != 1 {
		t.Fatalf("summarizer saw %d requests; want 1", len(adapter.reqs))
	}
	transcript := adapter.reqs[0].Messages[1].Content
	if !strings.Contains(transcript, "Assistant called tool bash") || !strings.Contains(transcript, "file.txt") {
		t.Fatalf("transcript split the tool pair: %q", transcript)
	}
	if strings.Contains(transcript, "thanks") {
		t.Fatalf("transcript includes kept-recent events: %q", transcript)
	}
}

// TestPruneSkipsSummarization 覆盖先剪枝后摘要：超长 tool 输出被
// 改写为头尾摘录并以 session/prune 事件落日志，投影回到预算内后
// 跳过 LLM 摘要（fake adapter 零调用）。
func TestPruneSkipsSummarization(t *testing.T) {
	// 原文 10000 字节（约 2500 token）超预算；剪枝后约 5150 字节
	// （约 1287 token）回到 2000 的预算内。keep_recent=0 保证若剪枝
	// 不生效则一定会走 LLM 摘要。
	ctx, compactor, mgr, adapter := newCompactor(t, map[string]any{"max_context_tokens": 2000, "keep_recent_events": 0, "keep_first_events": 0})
	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer store.(interface{ Close() error }).Close()
	long := strings.Repeat("x", 10000)
	appendAll(t, ctx, store, []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "hi"})},
		{Type: session.ToolResult, Data: session.MarshalData(session.ToolResultData{CallID: "c1", Name: "bash", Content: long})},
	})

	done, err := compactor.MaybeCompact(ctx, context.Background(), store, id)
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("over-budget history was neither pruned nor compacted")
	}
	if len(adapter.reqs) != 0 {
		t.Fatalf("summarizer was called %d times; want pruning to suffice", len(adapter.reqs))
	}

	// 剪枝必须落日志：一条 session/prune 事件按序索引定位被剪事件。
	events, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[2].Type != session.Prune {
		t.Fatalf("log = %d events; want the 2 originals plus a prune", len(events))
	}
	var pd session.PruneData
	if err := json.Unmarshal(events[2].Data, &pd); err != nil || pd.Seq != 1 {
		t.Fatalf("prune payload = %s; want seq 1", events[2].Data)
	}
	if !strings.HasPrefix(pd.Content, strings.Repeat("x", 4096)) || !strings.Contains(pd.Content, "bytes pruned") {
		t.Fatalf("pruned content is not a head/tail excerpt: %.80q...", pd.Content)
	}

	// 投影替换生效：模型可见的是剪枝后的文本。
	msgs := session.DeriveMessages(events)
	if len(msgs) != 2 || msgs[1].Content != pd.Content {
		t.Fatalf("projection = %+v; want the pruned tool content", msgs)
	}
}

// TestPruneThenSummarize 覆盖剪枝仍超预算的情形：prune 落日志后
// 继续走 LLM 摘要。
func TestPruneThenSummarize(t *testing.T) {
	ctx, compactor, mgr, adapter := newCompactor(t, map[string]any{"max_context_tokens": 100, "keep_recent_events": 0, "keep_first_events": 0})
	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer store.(interface{ Close() error }).Close()
	appendAll(t, ctx, store, []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "hi"})},
		{Type: session.ToolResult, Data: session.MarshalData(session.ToolResultData{CallID: "c1", Name: "bash", Content: strings.Repeat("x", 10000)})},
	})

	done, err := compactor.MaybeCompact(ctx, context.Background(), store, id)
	if err != nil {
		t.Fatal(err)
	}
	if !done || len(adapter.reqs) != 1 {
		t.Fatalf("done=%v reqs=%d; want pruning followed by one summarization", done, len(adapter.reqs))
	}
	events, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	if events[2].Type != session.Prune || events[3].Type != session.Summary {
		t.Fatalf("log tail = %q, %q; want prune then summary", events[2].Type, events[3].Type)
	}
}

// TestRollingDistillation 覆盖滚动蒸馏：第二次压缩的 transcript 携带
// 旧摘要内容，压缩 N 次后早期目标不丢。
func TestRollingDistillation(t *testing.T) {
	ctx, compactor, mgr, adapter := newCompactor(t, map[string]any{"max_context_tokens": 1, "keep_recent_events": 1, "keep_first_events": 0})
	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer store.(interface{ Close() error }).Close()
	appendAll(t, ctx, store, []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "round one question"})},
		{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{Content: "round one answer"})},
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "kept recent"})},
	})

	adapter.response = "SUMMARY-1"
	if done, err := compactor.MaybeCompact(ctx, context.Background(), store, id); err != nil || !done {
		t.Fatalf("first compaction: done=%v err=%v", done, err)
	}

	// 第二轮：水位线之后的新事件再次把投影推过预算。
	adapter.response = "SUMMARY-2"
	appendAll(t, ctx, store, []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "round two question"})},
		{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{Content: "round two answer"})},
	})
	if done, err := compactor.MaybeCompact(ctx, context.Background(), store, id); err != nil || !done {
		t.Fatalf("second compaction: done=%v err=%v", done, err)
	}

	if len(adapter.reqs) != 2 {
		t.Fatalf("summarizer saw %d requests; want 2", len(adapter.reqs))
	}
	transcript := adapter.reqs[1].Messages[1].Content
	if !strings.Contains(transcript, "SUMMARY-1") {
		t.Fatalf("second transcript dropped the previous summary: %q", transcript)
	}
	if !strings.Contains(transcript, "round two question") {
		t.Fatalf("second transcript misses the new events: %q", transcript)
	}
}

// TestDefaultSummaryPromptIsStructured 钉住默认摘要提示词的固定段落
// 结构（另一个 agent 能无缝接手）；summary_prompt 配置的覆盖由
// TestAgentloopConsumesCompaction 钉住。
func TestDefaultSummaryPromptIsStructured(t *testing.T) {
	ctx, compactor, mgr, adapter := newCompactor(t, map[string]any{"max_context_tokens": 1, "keep_recent_events": 0, "keep_first_events": 0})
	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer store.(interface{ Close() error }).Close()
	appendAll(t, ctx, store, []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "old question"})},
		{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{Content: "old answer"})},
	})

	if done, err := compactor.MaybeCompact(ctx, context.Background(), store, id); err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
	if len(adapter.reqs) != 1 {
		t.Fatalf("summarizer saw %d requests; want 1", len(adapter.reqs))
	}
	prompt := adapter.reqs[0].Messages[0]
	if prompt.Role != "system" {
		t.Fatalf("prompt role = %q; want system", prompt.Role)
	}
	for _, section := range []string{"## Goal", "## Progress", "## Key decisions", "## Errors", "## Relevant files", "## Next steps"} {
		if !strings.Contains(prompt.Content, section) {
			t.Fatalf("default prompt misses section %q: %q", section, prompt.Content)
		}
	}
}

// TestKeepFirstPinsInitialGoal 覆盖 keep_first：默认保留会话开头第
// 一条事件（初始目标），它不进摘要器的转录文本、在压缩后的投影里
// 按原样排在摘要之前。
func TestKeepFirstPinsInitialGoal(t *testing.T) {
	// 不配 keep_first_events：默认 1。
	ctx, compactor, mgr, adapter := newCompactor(t, map[string]any{"max_context_tokens": 1, "keep_recent_events": 2})
	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer store.(interface{ Close() error }).Close()
	appendAll(t, ctx, store, []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "initial goal"})},
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "middle question"})},
		{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{Content: "middle answer"})},
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "recent question"})},
		{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{Content: "recent answer"})},
	})

	if done, err := compactor.MaybeCompact(ctx, context.Background(), store, id); err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
	if len(adapter.reqs) != 1 {
		t.Fatalf("summarizer saw %d requests; want 1", len(adapter.reqs))
	}
	transcript := adapter.reqs[0].Messages[1].Content
	if strings.Contains(transcript, "initial goal") {
		t.Fatalf("pinned first event leaked into the transcript: %q", transcript)
	}
	if !strings.Contains(transcript, "middle question") {
		t.Fatalf("transcript misses the middle segment: %q", transcript)
	}

	events, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	var d session.SummaryData
	if err := json.Unmarshal(last.Data, &d); err != nil || d.FirstKept != 1 {
		t.Fatalf("summary payload = %s; want first_kept 1", last.Data)
	}

	// 刚压缩完时水位线之后还没有事件：投影 = 钉住的首条 + 摘要
	// 领投。追加一条新事件后它排在摘要之后。
	want := []llm.Message{
		{Role: "user", Content: "initial goal"},
		{Role: "user", Content: "SUMMARY"},
	}
	assertEqual := func(name string, got []llm.Message) {
		t.Helper()
		gotRaw, _ := json.Marshal(got)
		wantRaw, _ := json.Marshal(want)
		if string(gotRaw) != string(wantRaw) {
			t.Fatalf("%s = %s; want %s", name, gotRaw, wantRaw)
		}
	}
	assertEqual("DeriveMessages", session.DeriveMessages(events))
	appendAll(t, ctx, store, []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "after compaction"})},
	})
	want = append(want, llm.Message{Role: "user", Content: "after compaction"})
	events, err = mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	assertEqual("DeriveMessages", session.DeriveMessages(events))
	// 增量投影与全量定义一致。
	msgs, err := mgr.Messages(id)
	if err != nil {
		t.Fatal(err)
	}
	assertEqual("Messages", msgs)
}

// TestKeepFirstKeepsToolPairsTogether 覆盖头部边界的配对规则：
// keep_first 的朴素切点若落在一次工具调用与其结果之间会被后移，
// 钉住的前缀绝不会只剩配对的一半。
func TestKeepFirstKeepsToolPairsTogether(t *testing.T) {
	ctx, compactor, mgr, adapter := newCompactor(t, map[string]any{"max_context_tokens": 1, "keep_recent_events": 2, "keep_first_events": 2})
	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer store.(interface{ Close() error }).Close()
	call := llm.ToolCall{ID: "c1", Name: "bash", Arguments: `{"command":"ls"}`}
	appendAll(t, ctx, store, []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "initial goal"})},
		{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{ToolCalls: []llm.ToolCall{call}})},
		{Type: session.ToolCall, Data: session.MarshalData(session.ToolCallData{ID: "c1", Name: "bash", Arguments: `{"command":"ls"}`})},
		{Type: session.ToolResult, Data: session.MarshalData(session.ToolResultData{CallID: "c1", Name: "bash", Content: "pair result"})},
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "middle question"})},
		{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{Content: "middle answer"})},
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "recent question"})},
		{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{Content: "recent answer"})},
	})

	if done, err := compactor.MaybeCompact(ctx, context.Background(), store, id); err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
	// keep_first=2 的朴素切点落在 tool/call 与 tool/result 之间；后移
	// 后整个工具调用对都钉在头部，摘要携带 first_kept=4。
	events, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	var d session.SummaryData
	if err := json.Unmarshal(events[len(events)-1].Data, &d); err != nil || d.FirstKept != 4 {
		t.Fatalf("summary payload = %s; want first_kept 4", events[len(events)-1].Data)
	}
	transcript := adapter.reqs[0].Messages[1].Content
	if strings.Contains(transcript, "pair result") || strings.Contains(transcript, "initial goal") {
		t.Fatalf("pinned head leaked into the transcript: %q", transcript)
	}
	msgs := session.DeriveMessages(events)
	if len(msgs) < 3 || msgs[2].Role != "tool" || msgs[2].Content != "pair result" {
		t.Fatalf("projection split the pinned tool pair: %+v", msgs)
	}
}

// --- 集成测试：agentloop 消费压缩（compaction）能力 seam ---

// sseText 流式返回一段完整的文本回答。
func sseText(content string) string {
	return `data: {"choices":[{"delta":{"content":"` + content + `"}}]}

data: {"choices":[{"delta":{},"finish_reason":"stop"}]}

data: [DONE]

`
}

// sseCall 流式返回一次工具调用；args 是原始 JSON 参数字符串。
func sseCall(id, name, args string) string {
	quoted, _ := json.Marshal(args)
	return `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"` + id + `","function":{"name":"` + name + `","arguments":` + string(quoted) + `}}]}}]}

data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}

data: [DONE]

`
}

// requestCapture 把 OpenAI 兼容的线上报文形状拍平成领域消息。
type requestCapture struct {
	Messages []llm.Message
}

// UnmarshalJSON 解码线上报文形状（工具调用嵌套在 "function" 下）。
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

// bigTool 返回超大输出，用来把投影历史推过预算。
type bigTool struct{}

func (bigTool) Name() string { return "big_tool" }
func (bigTool) Schema() map[string]any {
	return map[string]any{"description": "test double", "type": "object"}
}
func (bigTool) Execute(_ *fiber.Context, _ json.RawMessage) (any, error) {
	return strings.Repeat("x", 400), nil
}

func TestAgentloopConsumesCompaction(t *testing.T) {
	stepReqs := 0
	var bodies []requestCapture
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body requestCapture
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		bodies = append(bodies, body)
		w.Header().Set("Content-Type", "text/event-stream")
		if body.Messages[0].Content == "COMPACT-PROMPT" {
			fmt.Fprint(w, sseText("SUMMARY-CONTENT"))
			return
		}
		stepReqs++
		if stepReqs == 1 {
			fmt.Fprint(w, sseCall("call_1", "big_tool", `{"x":1}`))
		} else {
			fmt.Fprint(w, sseText("final answer"))
		}
	}))
	defer server.Close()

	dir := t.TempDir()
	root := fiber.New()
	ctx := root.Context()
	defer root.Dispose()

	sessionCfg := map[string]any{
		"dir":   dir,
		"clock": func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
		"idgen": func() string { return "sess-1" },
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
		{"compaction", map[string]any{"max_context_tokens": 20, "keep_recent_events": 4, "keep_first_events": 0, "summary_prompt": "COMPACT-PROMPT"}},
	} {
		if err := plugin.Mount(ctx, step.name, step.cfg); err != nil {
			t.Fatalf("mount %s: %v", step.name, err)
		}
	}
	toolsReg, ok := fiber.Get[*tools.Registry](ctx, "tools")
	if !ok {
		t.Fatal("tools service unavailable")
	}
	defer toolsReg.Register(bigTool{})()

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
	ag.Send("original-task-marker")
	select {
	case end := <-turnDone:
		if end.Error != "" {
			t.Fatalf("turn error: %s", end.Error)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("turn did not finish")
	}

	// 这一 turn（一轮）发生了压缩：日志里应有一条 summary 事件。
	mgr, err := session.NewManager(dir, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	events, err := mgr.Events("sess-1")
	if err != nil {
		t.Fatal(err)
	}
	summaryIdx := -1
	for i, ev := range events {
		if ev.Type == session.Summary {
			summaryIdx = i
		}
	}
	if summaryIdx < 0 {
		t.Fatal("no summary event in the log")
	}
	var d session.SummaryData
	if err := json.Unmarshal(events[summaryIdx].Data, &d); err != nil || d.Content != "SUMMARY-CONTENT" {
		t.Fatalf("summary payload = %s; want SUMMARY-CONTENT", events[summaryIdx].Data)
	}

	// 压缩后的请求携带的必须恰好是请求时刻日志的投影（截至 summary
	// 事件为止）：「模型可见 ⟺ 日志可重建」这条铁律在压缩（compaction）
	// 之后依然成立。
	if len(bodies) != 3 {
		t.Fatalf("server saw %d requests; want 3 (step, summarize, step)", len(bodies))
	}
	projected := session.DeriveMessages(events[:summaryIdx+1])
	gotRaw, _ := json.Marshal(bodies[2].Messages[1:])
	wantRaw, _ := json.Marshal(projected)
	if string(gotRaw) != string(wantRaw) {
		t.Fatalf("post-compaction request diverges from log projection:\ngot  %s\nwant %s", gotRaw, wantRaw)
	}
	if bodies[2].Messages[0].Role != "system" {
		t.Fatal("first message must be the assembled system prompt")
	}
	for _, m := range bodies[2].Messages {
		if strings.Contains(m.Content, "original-task-marker") {
			t.Fatalf("compacted-away content still visible to the model: %+v", bodies[2].Messages)
		}
	}
	if len(bodies[2].Messages) < 2 || bodies[2].Messages[1].Content != "SUMMARY-CONTENT" {
		t.Fatalf("summary not leading the post-compaction history: %+v", bodies[2].Messages)
	}
}
