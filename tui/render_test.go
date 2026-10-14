package tui_test

import (
	"testing"
	"time"

	"hx/session"
	"hx/tui"
)

// ev 构造一条带固定时间戳的会话事件。
func ev(typ string, data any) session.Event {
	return session.Event{Type: typ, Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Data: session.MarshalData(data)}
}

func TestTranscriptProjection(t *testing.T) {
	tr := tui.NewTranscript()
	events := []session.Event{
		ev(session.UserMessage, session.UserMessageData{Content: "list files"}),
		ev(session.TurnStart, session.TurnStartData{Session: "s1"}),
		ev(session.StepStart, session.StepStartData{Session: "s1", Step: 0}),
		ev(session.AssistantChunk, session.AssistantChunkData{Delta: "Let me "}),
		ev(session.AssistantChunk, session.AssistantChunkData{Delta: "check."}),
		ev(session.AssistantMessage, session.AssistantMessageData{Content: "Let me check."}),
		ev(session.ToolCall, session.ToolCallData{ID: "call_1", Name: "bash", Arguments: `{"command":"ls"}`}),
		ev(session.ToolResult, session.ToolResultData{CallID: "call_1", Name: "bash", Content: "a.go\nb.go"}),
		ev(session.StepEnd, session.StepEndData{Session: "s1", Step: 0}),
		ev(session.TurnEnd, session.TurnEndData{Session: "s1"}),
	}
	for _, e := range events {
		tr.Apply(e)
	}
	lines := tr.Lines()
	want := []tui.Line{
		{Kind: tui.KindUser, Text: "list files"},
		// 流式 chunk 追加到当前打开的助手行。
		{Kind: tui.KindAssistant, Text: "Let me check."},
		// 工具调用与其结果折叠成一行摘要。
		{Kind: tui.KindTool, Text: "bash → a.go b.go"},
	}
	if len(lines) != len(want) {
		t.Fatalf("got %d lines: %+v", len(lines), lines)
	}
	for i, w := range want {
		if lines[i] != w {
			t.Fatalf("line %d: got %+v, want %+v", i, lines[i], w)
		}
	}
}

func TestTranscriptToolErrorAndTurnError(t *testing.T) {
	tr := tui.NewTranscript()
	tr.Apply(ev(session.ToolCall, session.ToolCallData{ID: "c1", Name: "bash", Arguments: "{}"}))
	tr.Apply(ev(session.ToolResult, session.ToolResultData{CallID: "c1", Name: "bash", Error: "boom"}))
	tr.Apply(ev(session.TurnEnd, session.TurnEndData{Session: "s1", Error: "max steps (32) reached"}))
	lines := tr.Lines()
	if len(lines) != 2 {
		t.Fatalf("got %d lines: %+v", len(lines), lines)
	}
	if lines[0].Kind != tui.KindTool || lines[0].Text != "bash → error: boom" {
		t.Fatalf("tool error line: %+v", lines[0])
	}
	if lines[1].Kind != tui.KindError || lines[1].Text != "turn failed: max steps (32) reached" {
		t.Fatalf("turn error line: %+v", lines[1])
	}
}

func TestTranscriptResultWithoutCall(t *testing.T) {
	// 重放的日志可能从一轮中途开始；找不到配对调用的结果仍会渲染。
	tr := tui.NewTranscript()
	tr.Apply(ev(session.ToolResult, session.ToolResultData{CallID: "gone", Name: "read_file", Content: "body"}))
	lines := tr.Lines()
	if len(lines) != 1 || lines[0].Text != "read_file → body" {
		t.Fatalf("unmatched result: %+v", lines)
	}
}
