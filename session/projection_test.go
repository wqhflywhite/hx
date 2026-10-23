package session_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"hx/core/fiber"
	"hx/llm"
	"hx/session"
)

// diskEvents 直接从文件读日志，绕过 Manager 缓存，
// 因此等价性断言是把 Messages 与真实的磁盘投影
// 做比较。
func diskEvents(t *testing.T, dir, id string) []session.Event {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, id+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var events []session.Event
	for _, line := range bytes.Split(raw, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var ev session.Event
		if err := json.Unmarshal(line, &ev); err != nil {
			t.Fatalf("corrupt line %q: %v", line, err)
		}
		events = append(events, ev)
	}
	return events
}

func assertProjectionEqual(t *testing.T, mgr *session.Manager, dir, id string, step int) {
	t.Helper()
	got, err := mgr.Messages(id)
	if err != nil {
		t.Fatalf("step %d: Messages: %v", step, err)
	}
	want := session.DeriveMessages(diskEvents(t, dir, id))
	if !reflect.DeepEqual(got, want) {
		gotRaw, _ := json.Marshal(got)
		wantRaw, _ := json.Marshal(want)
		t.Fatalf("step %d: Messages = %s; DeriveMessages(disk) = %s", step, gotRaw, wantRaw)
	}
}

// TestMessagesMatchesDeriveMessages 逐条追加一份编排好的
// 日志——分片流、工具调用与结果、带着悬而未决分片的崩溃
// turn、流中间的 summary、以及无边界收尾的尾巴——并在每次
// 追加后断言增量投影等于对整份磁盘日志跑 DeriveMessages
// 的结果。
func TestMessagesMatchesDeriveMessages(t *testing.T) {
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

	// 空日志必须投影为 nil，与 DeriveMessages(nil) 完全一致。
	assertProjectionEqual(t, mgr, dir, id, 0)

	call := llm.ToolCall{ID: "c1", Name: "bash", Arguments: `{"command":"ls"}`}
	script := []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "hi"})},
		{Type: session.TurnStart, Data: session.MarshalData(session.TurnStartData{Session: id})},
		{Type: session.StepStart, Data: session.MarshalData(session.StepStartData{Session: id, Step: 0})},
		{Type: session.AssistantChunk, Data: session.MarshalData(session.AssistantChunkData{Delta: "Hel"})},
		{Type: session.AssistantChunk, Data: session.MarshalData(session.AssistantChunkData{Delta: "lo"})},
		// 完整消息取代已聚合的分片。
		{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{Content: "Hello", ToolCalls: []llm.ToolCall{call}})},
		{Type: session.ToolCall, Data: session.MarshalData(session.ToolCallData{ID: "c1", Name: "bash", Arguments: `{"command":"ls"}`})},
		{Type: session.ToolResult, Data: session.MarshalData(session.ToolResultData{CallID: "c1", Name: "bash", Content: "file.txt"})},
		// prune 回改上面那条 tool/result（序索引 7）的投影内容。
		{Type: session.Prune, Data: session.MarshalData(session.PruneData{Seq: 7, Content: "[pruned] file list"})},
		{Type: session.StepEnd, Data: session.MarshalData(session.StepEndData{Session: id, Step: 0})},
		{Type: session.TurnEnd, Data: session.MarshalData(session.TurnEndData{Session: id})},
		// 崩溃的 turn：悬而未决的分片在 turn 边界收尾。
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "again"})},
		{Type: session.TurnStart, Data: session.MarshalData(session.TurnStartData{Session: id})},
		{Type: session.AssistantChunk, Data: session.MarshalData(session.AssistantChunkData{Delta: "par"})},
		{Type: session.AssistantChunk, Data: session.MarshalData(session.AssistantChunkData{Delta: "tial"})},
		{Type: session.TurnEnd, Data: session.MarshalData(session.TurnEndData{Session: id, Error: "boom"})},
		// 流中间失败的工具调用：分片在 tool 消息之前收尾。
		{Type: session.TurnStart, Data: session.MarshalData(session.TurnStartData{Session: id})},
		{Type: session.AssistantChunk, Data: session.MarshalData(session.AssistantChunkData{Delta: "abc"})},
		{Type: session.ToolResult, Data: session.MarshalData(session.ToolResultData{CallID: "c2", Name: "bash", Error: "net down"})},
		{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{Content: "final answer"})},
		{Type: session.TurnEnd, Data: session.MarshalData(session.TurnEndData{Session: id})},
		// summary 重置窗口；水位线之前悬而未决的分片被丢弃，但序索引
		// 19、20 虽排在 summary 之前，仍因 ThroughSeq=18 而保留。
		{Type: session.Summary, Data: session.MarshalData(session.SummaryData{Content: "first summary", ThroughSeq: 18})},
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "old after first"})},
		{Type: session.AssistantChunk, Data: session.MarshalData(session.AssistantChunkData{Delta: "x"})},
		{Type: session.AssistantChunk, Data: session.MarshalData(session.AssistantChunkData{Delta: "y"})},
		// FirstKept=1 钉住日志开头的 user/message，随窗口重置重投。
		// ThroughSeq=22 保留序索引 23、24 的分片；当前 summary 自身
		// 作为边界收尾这些分片。
		{Type: session.Summary, Data: session.MarshalData(session.SummaryData{Content: "latest summary", FirstKept: 1, ThroughSeq: 22})},
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "after summary"})},
		{Type: session.AssistantChunk, Data: session.MarshalData(session.AssistantChunkData{Delta: "new "})},
		{Type: session.AssistantMessage, Data: session.MarshalData(session.AssistantMessageData{Content: "new answer"})},
		// summary 之后的 prune：序索引 29 的 tool/result 先按原文
		// 投影，再被下一条事件回改。
		{Type: session.ToolResult, Data: session.MarshalData(session.ToolResultData{CallID: "c3", Name: "bash", Content: "fresh result"})},
		{Type: session.Prune, Data: session.MarshalData(session.PruneData{Seq: 29, Content: "[pruned] fresh"})},
		// 无边界收尾的尾巴：结尾的收尾仍会把它投影出来。
		{Type: session.AssistantChunk, Data: session.MarshalData(session.AssistantChunkData{Delta: "tail"})},
		// 最后一条 summary 只覆盖到序索引 30，保留它之前的
		// tool/result(31) 与 prune(32)；增量重放不能丢剪枝结果。
		{Type: session.ToolResult, Data: session.MarshalData(session.ToolResultData{CallID: "c4", Name: "bash", Content: "retained original"})},
		{Type: session.Prune, Data: session.MarshalData(session.PruneData{Seq: 31, Content: "[pruned] retained"})},
		{Type: session.Summary, Data: session.MarshalData(session.SummaryData{Content: "final summary", ThroughSeq: 30})},
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "after final"})},
		// FirstKept 不得越过 ThroughSeq+1：全量投影会把头部截到
		// 一条，增量若按物理位置重投四条会重复投影序索引 1～3。
		{Type: session.Summary, Data: session.MarshalData(session.SummaryData{Content: "clamped head", FirstKept: 4, ThroughSeq: 0})},
	}
	for i, ev := range script {
		if err := store.Append(ctx, ev); err != nil {
			t.Fatalf("step %d: append: %v", i+1, err)
		}
		assertProjectionEqual(t, mgr, dir, id, i+1)
	}
	// 冷缓存 resume 也必须从同一份磁盘日志重建出完全相同的视图。
	resumed, err := session.NewManager(dir, fixedClock(), idGen())
	if err != nil {
		t.Fatal(err)
	}
	assertProjectionEqual(t, resumed, dir, id, len(script)+1)
}

// TestMessagesAfterRestart 覆盖 kill -9 路径：在同一目录上新建的
// Manager 缓存为空，因此 Messages 与追加都必须从磁盘冷载，
// 且仍与全量投影一致。
func TestMessagesAfterRestart(t *testing.T) {
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
	appendEvents(t, store, ctx, sampleEvents())
	if closer, ok := store.(interface{ Close() error }); ok {
		closer.Close()
	}

	// 以先热后冷的顺序重启：Messages 先冷载。
	mgr2, err := session.NewManager(dir, fixedClock(), idGen())
	if err != nil {
		t.Fatal(err)
	}
	assertProjectionEqual(t, mgr2, dir, id, 0)
	resumed, err := mgr2.Resume(id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closer, ok := resumed.(interface{ Close() error }); ok {
			closer.Close()
		}
	}()
	appendEvents(t, resumed, ctx, []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "post-restart"})},
	})
	assertProjectionEqual(t, mgr2, dir, id, 1)

	// 再次重启并在任何 Messages 之前追加：noteAppend 必须冷载
	// 日志（其中已包含新事件），而不是把它折叠两次。
	mgr3, err := session.NewManager(dir, fixedClock(), idGen())
	if err != nil {
		t.Fatal(err)
	}
	resumed3, err := mgr3.Resume(id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closer, ok := resumed3.(interface{ Close() error }); ok {
			closer.Close()
		}
	}()
	appendEvents(t, resumed3, ctx, []session.Event{
		{Type: session.AssistantChunk, Data: session.MarshalData(session.AssistantChunkData{Delta: "cold-append"})},
	})
	assertProjectionEqual(t, mgr3, dir, id, 2)
}

// TestMessagesAndEventsReturnCopies 钉住调用方无法通过返回的
// 切片篡改缓存的投影。
func TestMessagesAndEventsReturnCopies(t *testing.T) {
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
	appendEvents(t, store, ctx, sampleEvents())

	msgs, err := mgr.Messages(id)
	if err != nil {
		t.Fatal(err)
	}
	msgs[0].Content = "mutated"
	msgs[1].ToolCalls[0].Name = "mutated"
	fresh, err := mgr.Messages(id)
	if err != nil {
		t.Fatal(err)
	}
	if fresh[0].Content != "hi" || fresh[1].ToolCalls[0].Name != "bash" {
		t.Fatalf("Messages shares cached state: %+v", fresh)
	}

	events, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	events[0].Type = "mutated"
	freshEvents, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	if freshEvents[0].Type != session.UserMessage {
		t.Fatalf("Events shares cached state: %+v", freshEvents[0])
	}
}

// TestMessagesConcurrent 跨会话交错执行追加、Messages 与 Events，
// 让 -race 能抓到锁顺序或共享状态的 bug。
func TestMessagesConcurrent(t *testing.T) {
	dir := t.TempDir()
	mgr, err := session.NewManager(dir, fixedClock(), idGen())
	if err != nil {
		t.Fatal(err)
	}
	root := fiber.New()
	defer root.Dispose()
	ctx := root.Context()

	var wg sync.WaitGroup
	var ids []string
	for range 4 {
		store, id, err := mgr.Create()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		wg.Add(1)
		go func(store session.Store, id string) {
			defer wg.Done()
			defer func() {
				if closer, ok := store.(interface{ Close() error }); ok {
					closer.Close()
				}
			}()
			for i := range 20 {
				ev := session.Event{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: fmt.Sprintf("m%d", i)})}
				if err := store.Append(ctx, ev); err != nil {
					t.Error(err)
					return
				}
				if _, err := mgr.Messages(id); err != nil {
					t.Error(err)
					return
				}
				if _, err := mgr.Events(id); err != nil {
					t.Error(err)
					return
				}
			}
		}(store, id)
	}
	wg.Wait()
	for i, id := range ids {
		assertProjectionEqual(t, mgr, dir, id, i)
	}
}
