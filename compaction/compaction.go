// Package compaction 是压缩（compaction）能力 seam：Compactor 定义和一个
// 基础的摘要 provider。
package compaction

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"hx/core/fiber"
	"hx/core/plugin"
	"hx/llm"
	"hx/session"
)

const (
	// defaultMaxContextTokens 在插件配置未设置 max_context_tokens 时为投影
	// 历史提供预算。
	defaultMaxContextTokens = 16000
	// defaultKeepRecentEvents 限定单次压缩（compaction）后尾部保留不动的
	// 事件条数。
	defaultKeepRecentEvents = 20
	// defaultKeepFirstEvents 钉住会话开头不参与压缩的事件条数；初始目标
	// 通常是第一条 user/message，默认 1 保证它永远不会被压掉。
	defaultKeepFirstEvents = 1
	// defaultPruneBytes 是剪枝阈值：水位线之后投影内容超过它的
	// tool/result 事件会被改写为头尾摘录。
	defaultPruneBytes = 8192
	// defaultSummaryPrompt 是结构化摘要指令：固定段落让另一个 agent
	// 能无缝接手。当部署方想要不同的压缩倾向时，可以用配置项
	// summary_prompt 覆盖它，换成自己的系统提示词模板。
	defaultSummaryPrompt = "You are compacting a long agent session into a handoff summary for another agent that will continue the work with no access to the original transcript. Write the summary with exactly these sections:\n\n## Goal\n## Progress and completed work\n## Key decisions\n## Errors and blockers\n## Relevant files\n## Next steps\n\nBe concise but specific; preserve file paths, commands, identifiers, and numbers verbatim. Reply with the summary only."
)

// Compactor 是压缩（compaction）能力定义：消费方在投影历史之前调用
// MaybeCompact。
type Compactor interface {
	// MaybeCompact 在投影历史超出 token 预算时，先以 session/prune
	// 事件剪枝超长 tool 输出，仍超才把会话里的旧事件摘要为一个
	// summary 事件；返回值报告这一次是否真的发生了压缩（prune 或
	// summary 任一发生即算）。
	MaybeCompact(c *fiber.Context, ctx context.Context, store session.Store, sessionID string) (bool, error)
}

// basic 是基础 provider：超预算后先做无模型调用的剪枝（prune），
// 仍超才把中间区段交给 LLM 摘要成转录文本，再以 summary 事件追加
// 回日志。
type basic struct {
	maxTokens  int
	keepRecent int
	keepFirst  int
	pruneBytes int
	prompt     string
}

// MaybeCompact 实现 Compactor 接口。
func (b *basic) MaybeCompact(c *fiber.Context, ctx context.Context, store session.Store, sessionID string) (bool, error) {
	events, err := store.Events(sessionID)
	if err != nil {
		return false, err
	}
	if estimateTokens(session.DeriveMessages(events)) <= b.maxTokens {
		return false, nil
	}

	// 最新一条 summary 是水位线：只有它之后的事件才是候选。其载荷
	// 同时是滚动蒸馏的前情与 keep_first 的既有口径。
	lastSummary := -1
	prevSummary := ""
	prevFirstKept := 0
	for i, ev := range events {
		if ev.Type == session.Summary {
			lastSummary = i
			var d session.SummaryData
			if json.Unmarshal(ev.Data, &d) == nil {
				prevSummary = d.Content
				prevFirstKept = d.FirstKept
			}
		}
	}

	// 第一步是剪枝：无模型调用，水位线之后投影内容超长的
	// tool/result 事件被改写为头尾摘录。剪枝改变模型可见内容，
	// 因此按「模型可见 ⟺ 已落日志」以 session/prune 事件落日志
	// 再广播（store 两件事都做）。
	pruned := false
	for i := lastSummary + 1; i < len(events); i++ {
		ev := events[i]
		if ev.Type != session.ToolResult {
			continue
		}
		var d session.ToolResultData
		if json.Unmarshal(ev.Data, &d) != nil {
			continue
		}
		content := d.Content
		if d.Error != "" {
			content = d.Error
		}
		if len(content) <= b.pruneBytes {
			continue
		}
		pruneEv := session.Event{Type: session.Prune, Data: session.MarshalData(session.PruneData{Seq: i, Content: pruneText(content)})}
		if err := store.Append(c, pruneEv); err != nil {
			return false, err
		}
		events = append(events, pruneEv)
		pruned = true
	}
	// 剪枝后重新估算：回到预算内就跳过 LLM 摘要。
	if pruned && estimateTokens(session.DeriveMessages(events)) <= b.maxTokens {
		return true, nil
	}

	// 尾部 keepRecent 条事件保持不动。
	head := events[lastSummary+1:]
	if len(head) <= b.keepRecent {
		return pruned, nil
	}
	cut := len(head) - b.keepRecent
	// 绝不把一次工具调用与其结果拆散在摘要边界两侧。
	for cut < len(head) && (head[cut].Type == session.ToolCall || head[cut].Type == session.ToolResult) {
		cut++
	}

	// keep_first：首次压缩钉住日志开头的 keepFirst 条事件（初始
	// 目标），之后的压缩继承前一条 summary 的口径——日志前缀
	// 不可变，两者必然一致。
	keepFirst := prevFirstKept
	start := 0
	if lastSummary < 0 {
		keepFirst = min(b.keepFirst, len(head))
		// 头部边界与尾部同一条规则：绝不落在一次工具调用与其
		// 结果之间，钉住的前缀不会只剩配对的一半。
		for keepFirst < len(head) && (head[keepFirst].Type == session.ToolCall || head[keepFirst].Type == session.ToolResult) {
			keepFirst++
		}
		start = keepFirst
	}
	if start >= cut {
		return pruned, nil
	}
	toSummarize := head[start:cut]

	// 滚动蒸馏：旧摘要不被丢弃，作为前情与新原始事件一起进入
	// transcript，压缩 N 次后早期目标仍留在摘要链里。
	var transcript strings.Builder
	if prevSummary != "" {
		transcript.WriteString("Summary of earlier events:\n")
		transcript.WriteString(prevSummary)
		transcript.WriteString("\n\nEvents since that summary:\n")
	}
	for _, ev := range toSummarize {
		transcript.WriteString(renderEvent(ev))
	}
	req := &llm.Request{Messages: []llm.Message{
		{Role: "system", Content: b.prompt},
		{Role: "user", Content: transcript.String()},
	}}
	stream, err := llm.Stream(c, ctx, req)
	if err != nil {
		return false, fmt.Errorf("compaction: summarize: %w", err)
	}
	var summary []byte
	for chunk := range stream {
		if chunk.Err != nil {
			return false, fmt.Errorf("compaction: summarize: %w", chunk.Err)
		}
		summary = append(summary, chunk.Delta...)
	}
	if len(summary) == 0 {
		return false, fmt.Errorf("compaction: empty summary")
	}

	// 摘要对模型可见，因此它像其他模型输入一样走会话日志：先写入、再
	// 广播（store 两件事都做）。summary 领投后，其覆盖区间的 prune
	// 自然失效——水位线之前的事件不再投影，prune 的回改随之落空。
	data := session.MarshalData(session.SummaryData{Content: string(summary), FirstKept: keepFirst})
	if err := store.Append(c, session.Event{Type: session.Summary, Data: data}); err != nil {
		return false, err
	}
	return true, nil
}

// pruneText 把超长内容改写为头 4096 字节加省略标注加尾 1024 字节。
func pruneText(s string) string {
	const head, tail = 4096, 1024
	if len(s) <= head+tail {
		return s
	}
	return fmt.Sprintf("%s\n... [%d bytes pruned] ...\n%s", s[:head], len(s)-head-tail, s[len(s)-tail:])
}

// estimateTokens 近似估算投影消息的 token 开销；字符/token 比例只是
// 经验值，并非真正的分词器。
func estimateTokens(msgs []llm.Message) int {
	total := 0
	for _, m := range msgs {
		total += len(m.Content) / 4
		for _, tc := range m.ToolCalls {
			total += len(tc.Arguments) / 4
		}
	}
	return total
}

// renderEvent 把一条事件渲染成供摘要器使用的转录行。
func renderEvent(ev session.Event) string {
	switch ev.Type {
	case session.UserMessage:
		var d session.UserMessageData
		if json.Unmarshal(ev.Data, &d) == nil {
			return "User: " + d.Content + "\n"
		}
	case session.AssistantMessage:
		var d session.AssistantMessageData
		if json.Unmarshal(ev.Data, &d) == nil {
			s := "Assistant: " + d.Content + "\n"
			for _, tc := range d.ToolCalls {
				s += fmt.Sprintf("Assistant called tool %s(%s)\n", tc.Name, truncate(tc.Arguments, 100))
			}
			return s
		}
	case session.ToolResult:
		var d session.ToolResultData
		if json.Unmarshal(ev.Data, &d) == nil {
			return fmt.Sprintf("Tool %s returned: %s\n", d.Name, truncate(d.Content, 300))
		}
	}
	return ""
}

// truncate 把 s 截断到 n 字节并加省略号标记。
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// Plugin 把 basic provider 挂载为 "compaction" 服务。配置项：
// "max_context_tokens"（int，默认 16000）、"keep_recent_events"（int，
// 默认 20）、"keep_first_events"（int，默认 1，显式 0 恢复不保留
// 头部的旧行为）、"prune_bytes"（int，默认 8192）、
// "summary_prompt"（string）。
func Plugin() plugin.Plugin { return compactionPlugin{} }

type compactionPlugin struct{}

func (compactionPlugin) Name() string     { return "compaction" }
func (compactionPlugin) Inject() []string { return []string{"llm"} }
func (compactionPlugin) Apply(c *fiber.Context, cfg map[string]any) error {
	maxTokens := defaultMaxContextTokens
	if v, ok := cfg["max_context_tokens"].(int); ok && v > 0 {
		maxTokens = v
	}
	keepRecent := defaultKeepRecentEvents
	if v, ok := cfg["keep_recent_events"].(int); ok && v >= 0 {
		keepRecent = v
	}
	keepFirst := defaultKeepFirstEvents
	if v, ok := cfg["keep_first_events"].(int); ok && v >= 0 {
		keepFirst = v
	}
	pruneBytes := defaultPruneBytes
	if v, ok := cfg["prune_bytes"].(int); ok && v > 0 {
		pruneBytes = v
	}
	prompt := defaultSummaryPrompt
	if v, ok := cfg["summary_prompt"].(string); ok && v != "" {
		prompt = v
	}
	return c.Effect(func(c *fiber.Context) (fiber.Disposer, error) {
		b := &basic{maxTokens: maxTokens, keepRecent: keepRecent, keepFirst: keepFirst, pruneBytes: pruneBytes, prompt: prompt}
		return fiber.Provide[Compactor](c, "compaction", b), nil
	})
}

func init() { plugin.Register(Plugin()) }
