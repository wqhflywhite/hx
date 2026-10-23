package session_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"hx/core/fiber"
	"hx/llm"
	"hx/session"
)

func fixedClock() func() time.Time {
	return func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
}

func idGen() func() string {
	n := 0
	return func() string { n++; return string(rune('a'-1+n)) + "-session" }
}

func appendEvents(t *testing.T, store session.Store, ctx *fiber.Context, evs []session.Event) {
	t.Helper()
	for _, ev := range evs {
		if err := store.Append(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
}

func sampleEvents() []session.Event {
	return []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "hi"})},
		{Type: session.TurnStart, Data: session.MarshalData(session.TurnStartData{Session: "a-session"})},
		{Type: session.StepStart, Data: session.MarshalData(session.StepStartData{Session: "a-session", Step: 0})},
		{Type: session.AssistantChunk, Data: session.MarshalData(session.AssistantChunkData{Delta: "Hel"})},
		{Type: session.AssistantChunk, Data: session.MarshalData(session.AssistantChunkData{Delta: "lo"})},
		{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{
			Content:   "Hello",
			ToolCalls: []llm.ToolCall{{ID: "c1", Name: "bash", Arguments: `{"command":"ls"}`}},
		})},
		{Type: session.ToolCall, Data: session.MarshalData(session.ToolCallData{ID: "c1", Name: "bash", Arguments: `{"command":"ls"}`})},
		{Type: session.ToolResult, Data: session.MarshalData(session.ToolResultData{CallID: "c1", Name: "bash", Content: "file.txt"})},
		{Type: session.StepEnd, Data: session.MarshalData(session.StepEndData{Session: "a-session", Step: 0})},
		{Type: session.TurnEnd, Data: session.MarshalData(session.TurnEndData{Session: "a-session"})},
	}
}

func TestAppendDeriveFork(t *testing.T) {
	dir := t.TempDir()
	mgr, err := session.NewManager(dir, fixedClock(), idGen())
	if err != nil {
		t.Fatal(err)
	}
	root := fiber.New()
	defer root.Dispose()
	ctx := root.Context()

	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closer, ok := store.(interface{ Close() error }); ok {
			closer.Close()
		}
	}()
	if id != "a-session" {
		t.Fatalf("id = %q; want a-session", id)
	}
	appendEvents(t, store, ctx, sampleEvents())

	// 投影聚合分片并保留完整消息。
	msgs := session.DeriveMessages(mustEvents(t, mgr, id))
	want := []llm.Message{
		{Role: "user", Content: "hi"},
		{Role: "assistant", Content: "Hello", ToolCalls: []llm.ToolCall{{ID: "c1", Name: "bash", Arguments: `{"command":"ls"}`}}},
		{Role: "tool", ToolCallID: "c1", Content: "file.txt"},
	}
	gotRaw, _ := json.Marshal(msgs)
	wantRaw, _ := json.Marshal(want)
	if string(gotRaw) != string(wantRaw) {
		t.Fatalf("DeriveMessages = %s; want %s", gotRaw, wantRaw)
	}

	// Fork 在新 id 下逐字节一致地复制前缀。
	forkID, err := mgr.Fork(id, 5)
	if err != nil {
		t.Fatal(err)
	}
	if forkID != "b-session" {
		t.Fatalf("fork id = %q; want b-session", forkID)
	}
	src, _ := os.ReadFile(filepath.Join(dir, "a-session.jsonl"))
	forked, _ := os.ReadFile(filepath.Join(dir, "b-session.jsonl"))
	lines := 0
	for i, b := range src {
		if b == '\n' {
			lines++
			if lines == 5 {
				if string(src[:i+1]) != string(forked) {
					t.Fatal("fork is not the byte-exact prefix")
				}
			}
		}
	}
}

// TestChunkAggregationCrash 覆盖 turn 中途崩溃的日志跑
// DeriveMessages 的情形：悬而未决的分片仍投影为部分 assistant 消息。
func TestChunkAggregationCrash(t *testing.T) {
	events := []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "hi"})},
		{Type: session.TurnStart, Data: session.MarshalData(session.TurnStartData{Session: "s"})},
		{Type: session.AssistantChunk, Data: session.MarshalData(session.AssistantChunkData{Delta: "par"})},
		{Type: session.AssistantChunk, Data: session.MarshalData(session.AssistantChunkData{Delta: "tial"})},
	}
	msgs := session.DeriveMessages(events)
	if len(msgs) != 2 || msgs[1].Content != "partial" {
		t.Fatalf("msgs = %+v; want dangling chunks aggregated", msgs)
	}
}

func TestList(t *testing.T) {
	dir := t.TempDir()
	mgr, err := session.NewManager(dir, fixedClock(), idGen())
	if err != nil {
		t.Fatal(err)
	}
	ids, err := mgr.List()
	if err != nil || len(ids) != 0 {
		t.Fatalf("empty dir: ids=%v err=%v", ids, err)
	}
	stores := make([]session.Store, 0, 2)
	for range 2 {
		store, _, err := mgr.Create()
		if err != nil {
			t.Fatal(err)
		}
		stores = append(stores, store)
	}
	defer func() {
		for _, s := range stores {
			if closer, ok := s.(interface{ Close() error }); ok {
				closer.Close()
			}
		}
	}()
	// 噪声条目不得混入 id 列表。
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	ids, err = mgr.List()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a-session", "b-session"}
	if len(ids) != len(want) {
		t.Fatalf("ids = %v; want %v", ids, want)
	}
	for i, w := range want {
		if ids[i] != w {
			t.Fatalf("ids = %v; want %v", ids, want)
		}
	}
}

// TestDeriveMessagesWithSummary 覆盖 ThroughSeq 水位线：最新 summary
// 作为一条 user 消息引领投影；水位线之后的事件即使物理上排在 summary
// 之前，也必须和水位线之后追加的事件一样留在模型视图里。
func TestDeriveMessagesWithSummary(t *testing.T) {
	events := append(sampleEvents(),
		session.Event{Type: session.Summary, Data: session.MarshalData(session.SummaryData{Content: "first summary", ThroughSeq: 9})},   // 10
		session.Event{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "covered by latest"})},      // 11
		session.Event{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "retained before latest"})}, // 12
		session.Event{Type: session.Summary, Data: session.MarshalData(session.SummaryData{Content: "latest summary", ThroughSeq: 11})}, // 13
		session.Event{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "after summary"})},          // 14
		session.Event{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{Content: "new answer"})},   // 15
	)
	msgs := session.DeriveMessages(events)
	want := []llm.Message{
		{Role: "user", Content: "latest summary"},
		{Role: "user", Content: "retained before latest"},
		{Role: "user", Content: "after summary"},
		{Role: "assistant", Content: "new answer"},
	}
	gotRaw, _ := json.Marshal(msgs)
	wantRaw, _ := json.Marshal(want)
	if string(gotRaw) != string(wantRaw) {
		t.Fatalf("DeriveMessages = %s; want %s", gotRaw, wantRaw)
	}
}

// TestAppendSyncsToDisk 是 fsync 的行为证据：Append 返回后不关闭、
// 不经任何缓存，新打开的读句柄立即能读到完整行。
func TestAppendSyncsToDisk(t *testing.T) {
	dir := t.TempDir()
	mgr, err := session.NewManager(dir, fixedClock(), idGen())
	if err != nil {
		t.Fatal(err)
	}
	root := fiber.New()
	defer root.Dispose()

	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closer, ok := store.(interface{ Close() error }); ok {
			closer.Close()
		}
	}()
	appendEvents(t, store, root.Context(), []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "durable"})},
	})

	raw, err := os.ReadFile(filepath.Join(dir, id+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var ev session.Event
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(raw))), &ev); err != nil {
		t.Fatalf("appended line not readable on disk: %v", err)
	}
	var d session.UserMessageData
	if err := json.Unmarshal(ev.Data, &d); err != nil || d.Content != "durable" {
		t.Fatalf("disk line = %s; want the appended event", raw)
	}
}

// TestReadEventsTruncatesAtCorruptLine 覆盖坏行容错：崩溃中途写了一
// 半的行及其之后一律不可信，读回时在第一个坏行处截断而不是整个
// 会话报错，会话仍可 resume。
func TestReadEventsTruncatesAtCorruptLine(t *testing.T) {
	good := func(content string) []byte {
		raw, err := json.Marshal(session.Event{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: content})})
		if err != nil {
			t.Fatal(err)
		}
		return append(raw, '\n')
	}
	cases := []struct {
		name  string
		lines [][]byte
		want  []string
	}{
		{"末尾坏行", [][]byte{good("e1"), good("e2"), []byte(`{"type":"user/message","data":{"con`)}, []string{"e1", "e2"}},
		{"中间坏行", [][]byte{good("e1"), []byte("{not json\n"), good("e3")}, []string{"e1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			var buf []byte
			for _, l := range tc.lines {
				buf = append(buf, l...)
			}
			if err := os.WriteFile(filepath.Join(dir, "s.jsonl"), buf, 0o644); err != nil {
				t.Fatal(err)
			}
			mgr, err := session.NewManager(dir, fixedClock(), idGen())
			if err != nil {
				t.Fatal(err)
			}
			events, err := mgr.Events("s")
			if err != nil {
				t.Fatalf("corrupt line must not fail the session: %v", err)
			}
			if len(events) != len(tc.want) {
				t.Fatalf("events = %d; want truncation to %d good events", len(events), len(tc.want))
			}
			for i, w := range tc.want {
				var d session.UserMessageData
				if err := json.Unmarshal(events[i].Data, &d); err != nil || d.Content != w {
					t.Fatalf("events[%d] = %s; want content %q", i, events[i].Data, w)
				}
			}
			store, err := mgr.Resume("s")
			if err != nil {
				t.Fatalf("resume after corrupt line: %v", err)
			}
			if closer, ok := store.(interface{ Close() error }); ok {
				closer.Close()
			}
		})
	}
}

// TestDeriveMessagesWithPrune 覆盖 prune 事件的投影语义：按序索引
// 替换对应 tool/result 事件的投影内容；指向水位线之前的 prune
// 随其目标一起退出窗口，自然失效。
func TestDeriveMessagesWithPrune(t *testing.T) {
	call := llm.ToolCall{ID: "c1", Name: "bash", Arguments: `{"command":"ls"}`}
	events := []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "hi"})},                                   // 0
		{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{ToolCalls: []llm.ToolCall{call}})},       // 1
		{Type: session.ToolResult, Data: session.MarshalData(session.ToolResultData{CallID: "c1", Name: "bash", Content: "x-original"})}, // 2
		{Type: session.Prune, Data: session.MarshalData(session.PruneData{Seq: 2, Content: "x-pruned"})},                                 // 3
		{Type: session.Summary, Data: session.MarshalData(session.SummaryData{Content: "S", ThroughSeq: 4})},                             // 4
		{Type: session.ToolResult, Data: session.MarshalData(session.ToolResultData{CallID: "c2", Name: "bash", Content: "y-original"})}, // 5
		{Type: session.Prune, Data: session.MarshalData(session.PruneData{Seq: 5, Content: "y-pruned"})},                                 // 6
		{Type: session.Prune, Data: session.MarshalData(session.PruneData{Seq: 2, Content: "dead"})},                                     // 7 指向水位线之前，落空
	}
	assertMsgs := func(evs []session.Event, want []llm.Message) {
		t.Helper()
		gotRaw, _ := json.Marshal(session.DeriveMessages(evs))
		wantRaw, _ := json.Marshal(want)
		if string(gotRaw) != string(wantRaw) {
			t.Fatalf("DeriveMessages = %s; want %s", gotRaw, wantRaw)
		}
	}
	// 无 summary 的前缀：prune 直接生效。
	assertMsgs(events[:4], []llm.Message{
		{Role: "user", Content: "hi"},
		{Role: "assistant", ToolCalls: []llm.ToolCall{call}},
		{Role: "tool", ToolCallID: "c1", Content: "x-pruned"},
	})
	// 全量：水位线之前的 prune 失效，之后的生效。
	assertMsgs(events, []llm.Message{
		{Role: "user", Content: "S"},
		{Role: "tool", ToolCallID: "c2", Content: "y-pruned"},
	})
}

// TestDeriveMessagesWithKeepFirst 覆盖 summary 的 FirstKept：日志开头
// 的原始事件钉住在摘要领投消息之前；FirstKept 缺省 0 即旧行为（由
// TestDeriveMessagesWithSummary 钉住）。
func TestDeriveMessagesWithKeepFirst(t *testing.T) {
	events := []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "initial goal"})},
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "old"})},
		{Type: session.Summary, Data: session.MarshalData(session.SummaryData{Content: "S", FirstKept: 1, ThroughSeq: 2})},
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "after"})},
	}
	msgs := session.DeriveMessages(events)
	want := []llm.Message{
		{Role: "user", Content: "initial goal"},
		{Role: "user", Content: "S"},
		{Role: "user", Content: "after"},
	}
	gotRaw, _ := json.Marshal(msgs)
	wantRaw, _ := json.Marshal(want)
	if string(gotRaw) != string(wantRaw) {
		t.Fatalf("DeriveMessages = %s; want %s", gotRaw, wantRaw)
	}
}

// TestDeriveMessagesWithoutSummary 钉住无 summary 的行为：日志
// 没有 summary 事件时投影保持不变。
func TestDeriveMessagesWithoutSummary(t *testing.T) {
	msgs := session.DeriveMessages(sampleEvents())
	if len(msgs) != 3 || msgs[0].Role != "user" || msgs[0].Content != "hi" {
		t.Fatalf("msgs = %+v; want the plain projection", msgs)
	}
}

func mustEvents(t *testing.T, mgr *session.Manager, id string) []session.Event {
	t.Helper()
	events, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	return events
}

// TestResumeRepairsCorruptTail 覆盖坏行后的恢复与追加：Resume 必须
// 先把日志的坏后缀物理截断，之后 Append 的事件才能在换 Manager
// 冷读时恢复；不允许「Append 报成功、冷读却丢失」。末尾无换行的
// 好行也必须妥善处理，追加不得与其粘连。
func TestResumeRepairsCorruptTail(t *testing.T) {
	good := func(content string) []byte {
		raw, err := json.Marshal(session.Event{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: content})})
		if err != nil {
			t.Fatal(err)
		}
		return append(raw, '\n')
	}
	cases := []struct {
		name string
		file []byte   // 崩溃残留的文件内容
		want []string // 修复并追加 e3、e4 后冷读应得到的内容
	}{
		{"末尾半行", append(append(good("e1"), good("e2")...), []byte(`{"type":"user/mess`)...), []string{"e1", "e2", "e3", "e4"}},
		{"中间坏行", append(append(good("e1"), []byte("{not json\n")...), good("e2")...), []string{"e1", "e3", "e4"}},
		{"末尾好行无换行", append(good("e1"), bytes.TrimSuffix(good("e2"), []byte("\n"))...), []string{"e1", "e2", "e3", "e4"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "s.jsonl"), tc.file, 0o644); err != nil {
				t.Fatal(err)
			}
			root := fiber.New()
			defer root.Dispose()
			mgr1, err := session.NewManager(dir, fixedClock(), idGen())
			if err != nil {
				t.Fatal(err)
			}
			store, err := mgr1.Resume("s")
			if err != nil {
				t.Fatalf("resume: %v", err)
			}
			appendEvents(t, store, root.Context(), []session.Event{
				{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "e3"})},
				{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "e4"})},
			})
			if closer, ok := store.(interface{ Close() error }); ok {
				closer.Close()
			}
			// 换 Manager 冷读：绕过投影缓存，直接面对磁盘事实。
			mgr2, err := session.NewManager(dir, fixedClock(), idGen())
			if err != nil {
				t.Fatal(err)
			}
			events, err := mgr2.Events("s")
			if err != nil {
				t.Fatalf("cold read after repair: %v", err)
			}
			if len(events) != len(tc.want) {
				t.Fatalf("cold read = %d events; want %d: %v", len(events), len(tc.want), tc.want)
			}
			for i, w := range tc.want {
				var d session.UserMessageData
				if err := json.Unmarshal(events[i].Data, &d); err != nil || d.Content != w {
					t.Fatalf("events[%d] content = %q err=%v; want %q", i, d.Content, err, w)
				}
			}
		})
	}
}

// TestRejectsPathEscapingID 覆盖会话 id 的路径逃逸防护：任何可能
// 逃出会话目录的 id 在 Manager 边界 fail loud，且不在目录外创建
// 文件。id 最终可来自外部输入（web resume、命令行参数）。
func TestRejectsPathEscapingID(t *testing.T) {
	bad := []string{
		"", ".", "..", "../escape", "../../tmp/x", `..\escape`, "a/b", `a\b`,
		"/abs/path", `C:\evil`, "C:foo", `\share\x`,
	}
	for _, id := range bad {
		t.Run(fmt.Sprintf("%q", id), func(t *testing.T) {
			dir := t.TempDir()
			mgr, err := session.NewManager(dir, fixedClock(), idGen())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := mgr.Resume(id); err == nil {
				t.Fatalf("Resume(%q) must fail", id)
			}
			if _, err := mgr.Events(id); err == nil {
				t.Fatalf("Events(%q) must fail", id)
			}
			if _, err := mgr.Messages(id); err == nil {
				t.Fatalf("Messages(%q) must fail", id)
			}
			if _, err := mgr.Fork(id, 0); err == nil {
				t.Fatalf("Fork(%q) must fail", id)
			}
			// 逃逸目标（dir 的父级兄弟路径）不得被创建。
			sibling := filepath.Join(dir, "..", "escape.jsonl")
			if _, err := os.Stat(sibling); !os.IsNotExist(err) {
				t.Fatalf("escape target %q must not exist", sibling)
			}
		})
	}
}

// TestResumeRequiresExistingLog 钉住 Resume 的新语义：恢复一个
// 不存在的会话必须报错，不得隐式创建空日志——打错的会话名以前
// 会静默造出新文件，把「恢复错会话」变成「数据写进新文件」。
func TestResumeRequiresExistingLog(t *testing.T) {
	dir := t.TempDir()
	mgr, err := session.NewManager(dir, fixedClock(), idGen())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Resume("ghost"); err == nil {
		t.Fatal("Resume of a missing session must fail")
	}
	if _, err := os.Stat(filepath.Join(dir, "ghost.jsonl")); !os.IsNotExist(err) {
		t.Fatal("Resume must not create the log file")
	}
	// 已存在的会话照常恢复。
	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	if closer, ok := store.(interface{ Close() error }); ok {
		closer.Close()
	}
	resumed, err := mgr.Resume(id)
	if err != nil {
		t.Fatalf("Resume of an existing session: %v", err)
	}
	if closer, ok := resumed.(interface{ Close() error }); ok {
		closer.Close()
	}
}

// TestCreateRejectsIDCollision 覆盖 idgen 撞 id：Create 必须排他
// 失败，而不是追加进已有会话的日志造成数据混淆。
func TestCreateRejectsIDCollision(t *testing.T) {
	dir := t.TempDir()
	collide := func() string { return "fixed-id" }
	mgr, err := session.NewManager(dir, fixedClock(), collide)
	if err != nil {
		t.Fatal(err)
	}
	root := fiber.New()
	defer root.Dispose()
	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closer, ok := store.(interface{ Close() error }); ok {
			closer.Close()
		}
	}()
	appendEvents(t, store, root.Context(), []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "original"})},
	})
	if _, _, err := mgr.Create(); err == nil {
		t.Fatal("Create with a colliding id must fail")
	}
	// 旧日志不受影响：换 Manager 冷读仍只有原有的一条事件。
	mgr2, err := session.NewManager(dir, fixedClock(), idGen())
	if err != nil {
		t.Fatal(err)
	}
	events, err := mgr2.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("existing log has %d events; want 1 (untouched)", len(events))
	}
}
