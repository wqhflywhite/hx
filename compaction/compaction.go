// Package compaction 是压缩（compaction）能力 seam：Compactor 定义和一个
// 基础的摘要 provider。
package compaction

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

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

type compactCall struct {
	finished chan struct{}
	done     bool
	err      error
}

// previousSummary 是最近一次摘要留下的覆盖边界与滚动蒸馏前情。
type previousSummary struct {
	index      int
	content    string
	firstKept  int
	throughSeq int
}

// summaryInput 是摘要模型输入及其覆盖元数据。
type summaryInput struct {
	transcript string
	firstKept  int
	throughSeq int
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
	mu         sync.Mutex
	calls      map[string]*compactCall
}

// MaybeCompact 实现 Compactor 接口。同会话并发调用共享一次实际
// 压缩；锁只保护调用表与结果发布，不跨日志或模型调用。
func (b *basic) MaybeCompact(c *fiber.Context, ctx context.Context, store session.Store, sessionID string) (done bool, err error) {
	b.mu.Lock()
	if call, ok := b.calls[sessionID]; ok {
		b.mu.Unlock()
		select {
		case <-call.finished:
			return call.done, call.err
		case <-ctx.Done():
			return false, ctx.Err()
		case <-c.Fiber().Done():
			return false, fiber.ErrDisposed
		}
	}
	if b.calls == nil {
		b.calls = make(map[string]*compactCall)
	}
	call := &compactCall{finished: make(chan struct{})}
	b.calls[sessionID] = call
	defer func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		call.done = done
		call.err = err
		delete(b.calls, sessionID)
		close(call.finished)
	}()
	b.mu.Unlock()

	return b.compact(c, ctx, store, sessionID)
}

// compact 编排一次实际压缩：预算判断、剪枝、摘要输入、模型调用与
// summary 落日志。
func (b *basic) compact(c *fiber.Context, ctx context.Context, store session.Store, sessionID string) (bool, error) {
	events, err := store.Events(sessionID)
	if err != nil {
		return false, err
	}
	if estimateTokens(session.DeriveMessages(events)) <= b.maxTokens {
		return false, nil
	}

	prev := readPreviousSummary(events)
	events, pruned, err := b.pruneResults(c, store, events, prev.index)
	if err != nil {
		return false, err
	}
	if pruned && estimateTokens(session.DeriveMessages(events)) <= b.maxTokens {
		return true, nil
	}

	input, ok := b.summaryWindow(events, prev)
	if !ok {
		return pruned, nil
	}
	summary, err := b.summarize(c, ctx, input.transcript)
	if err != nil {
		return false, err
	}

	// 摘要对模型可见，因此它像其他模型输入一样走会话日志：先写入、再
	// 广播。水位线之前的事件退出投影，其 prune 回改也随之自然失效。
	data := session.MarshalData(session.SummaryData{
		Content: summary, FirstKept: input.firstKept, ThroughSeq: input.throughSeq,
	})
	if err := store.Append(c, session.Event{Type: session.Summary, Data: data}); err != nil {
		return false, err
	}
	return true, nil
}

// readPreviousSummary 找最新一条 summary；无效载荷只更新物理位置，
// 不推进覆盖水位线，与 DeriveMessages 的扫描语义一致。
func readPreviousSummary(events []session.Event) previousSummary {
	prev := previousSummary{index: -1, throughSeq: -1}
	for i, ev := range events {
		if ev.Type != session.Summary {
			continue
		}
		prev.index = i
		var d session.SummaryData
		if json.Unmarshal(ev.Data, &d) == nil {
			prev.content = d.Content
			prev.firstKept = d.FirstKept
			prev.throughSeq = d.ThroughSeq
		}
	}
	return prev
}

// pruneResults 剪枝上一物理 summary 后的超长工具结果。每次剪枝先
// 追加 prune 事件，再更新本次估算使用的事件快照。
func (b *basic) pruneResults(c *fiber.Context, store session.Store, events []session.Event, lastSummary int) ([]session.Event, bool, error) {
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
			return events, pruned, err
		}
		events = append(events, pruneEv)
		pruned = true
	}
	return events, pruned, nil
}

// summaryWindow 选择本次真正覆盖的原始事件，保留 keepFirst 头部与
// keepRecent 尾部，并构造滚动蒸馏 transcript。它是纯计算，不写日志。
func (b *basic) summaryWindow(events []session.Event, prev previousSummary) (summaryInput, bool) {
	head := events[prev.throughSeq+1:]
	if len(head) <= b.keepRecent {
		return summaryInput{}, false
	}
	cut := len(head) - b.keepRecent
	for cut < len(head) && (head[cut].Type == session.ToolCall || head[cut].Type == session.ToolResult) {
		cut++
	}

	keepFirst := prev.firstKept
	start := 0
	if prev.index < 0 {
		keepFirst = min(b.keepFirst, len(head))
		for keepFirst < len(head) && (head[keepFirst].Type == session.ToolCall || head[keepFirst].Type == session.ToolResult) {
			keepFirst++
		}
		start = keepFirst
	}
	if start >= cut {
		return summaryInput{}, false
	}

	var transcript strings.Builder
	if prev.content != "" {
		transcript.WriteString("Summary of earlier events:\n")
		transcript.WriteString(prev.content)
		transcript.WriteString("\n\nEvents not covered by that summary:\n")
	}
	throughSeq := prev.throughSeq
	for i, ev := range head[start:cut] {
		if ev.Type != session.Summary && ev.Type != session.Prune {
			throughSeq = prev.throughSeq + 1 + start + i
		}
		transcript.WriteString(renderEvent(ev))
	}
	if throughSeq == prev.throughSeq {
		return summaryInput{}, false
	}
	return summaryInput{transcript: transcript.String(), firstKept: keepFirst, throughSeq: throughSeq}, true
}

// summarize 调用模型并收集完整摘要文本。
func (b *basic) summarize(c *fiber.Context, ctx context.Context, transcript string) (string, error) {
	req := &llm.Request{Messages: []llm.Message{
		{Role: "system", Content: b.prompt},
		{Role: "user", Content: transcript},
	}}
	stream, err := llm.Stream(c, ctx, req)
	if err != nil {
		return "", fmt.Errorf("compaction: summarize: %w", err)
	}
	var summary []byte
	for chunk := range stream {
		if chunk.Err != nil {
			return "", fmt.Errorf("compaction: summarize: %w", chunk.Err)
		}
		summary = append(summary, chunk.Delta...)
	}
	if len(summary) == 0 {
		return "", fmt.Errorf("compaction: empty summary")
	}
	return string(summary), nil
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
