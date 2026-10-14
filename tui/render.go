// Package tui 将会话事件流渲染成交互式对话记录。
package tui

import (
	"encoding/json"
	"fmt"
	"strings"

	"hx/session"
)

// Kind 标记一行对话记录的类别，用于样式渲染。
type Kind int

const (
	// KindUser 是一条用户输入。
	KindUser Kind = iota
	// KindAssistant 是流式输出的助手文本。
	KindAssistant
	// KindTool 是一次工具调用与其结果折叠后的一行。
	KindTool
	// KindError 是一个失败的 turn（一轮）。
	KindError
)

// Line 是对话记录的一行；Text 是原始内容，Kind 是角色类别。
type Line struct {
	Kind Kind
	Text string
}

// Transcript 把会话事件投影成行：助手 chunk 追加到当前打开的
// 助手行，一次 tool/call 与其 tool/result 折叠成一行摘要。
// 它是 TUI 背后的纯视图模型。
type Transcript struct {
	lines     []Line
	openTool  map[string]int // 调用 id → 等待结果的行下标
	streaming bool           // 有一行助手行正在接收 chunk
}

// NewTranscript 创建一个空投影。
func NewTranscript() *Transcript {
	return &Transcript{openTool: map[string]int{}}
}

// Lines 返回投影行的快照。
func (t *Transcript) Lines() []Line {
	return append([]Line(nil), t.lines...)
}

// summary 把 s 截断为单行、最多前 120 个 rune。
func summary(s string) string {
	s = strings.ReplaceAll(strings.TrimSpace(s), "\n", " ")
	r := []rune(s)
	if len(r) > 120 {
		return string(r[:120]) + "…"
	}
	return s
}

// Apply 把一条会话事件折叠进投影。
func (t *Transcript) Apply(ev session.Event) {
	switch ev.Type {
	case session.UserMessage:
		var d session.UserMessageData
		if json.Unmarshal(ev.Data, &d) == nil {
			t.streaming = false
			t.lines = append(t.lines, Line{Kind: KindUser, Text: d.Content})
		}
	case session.AssistantChunk:
		var d session.AssistantChunkData
		if json.Unmarshal(ev.Data, &d) != nil {
			return
		}
		if t.streaming && len(t.lines) > 0 {
			t.lines[len(t.lines)-1].Text += d.Delta
		} else {
			t.streaming = true
			t.lines = append(t.lines, Line{Kind: KindAssistant, Text: d.Delta})
		}
	case session.AssistantMessage:
		// chunk 已经展示过内容；无 chunk 的消息
		//（例如纯工具调用步骤）不新增可见内容。
		t.streaming = false
	case session.ToolCall:
		var d session.ToolCallData
		if json.Unmarshal(ev.Data, &d) != nil {
			return
		}
		t.streaming = false
		t.openTool[d.ID] = len(t.lines)
		t.lines = append(t.lines, Line{Kind: KindTool, Text: fmt.Sprintf("%s %s", d.Name, summary(d.Arguments))})
	case session.ToolResult:
		var d session.ToolResultData
		if json.Unmarshal(ev.Data, &d) != nil {
			return
		}
		text := summary(d.Content)
		if d.Error != "" {
			text = "error: " + summary(d.Error)
		}
		if idx, ok := t.openTool[d.CallID]; ok {
			delete(t.openTool, d.CallID)
			t.lines[idx].Text = fmt.Sprintf("%s → %s", d.Name, text)
		} else {
			t.lines = append(t.lines, Line{Kind: KindTool, Text: fmt.Sprintf("%s → %s", d.Name, text)})
		}
	case session.TurnEnd:
		t.streaming = false
		var d session.TurnEndData
		if json.Unmarshal(ev.Data, &d) == nil && d.Error != "" {
			t.lines = append(t.lines, Line{Kind: KindError, Text: "turn failed: " + d.Error})
		}
	default:
		// turn/start 与 step 边界事件没有可见内容。
	}
}
