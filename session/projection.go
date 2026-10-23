package session

import (
	"encoding/json"

	"hx/llm"
)

// projection 是 DeriveMessages 的按会话增量形态：日志的内存
// 拷贝加上已折叠到水位线 wm 为止的模型可见历史。Append 每次只
// 折叠一个事件，因此每个 step 的热路径不再重读并重新投影整份
// 日志。
type projection struct {
	events    []Event       // 日志的内存拷贝
	msgs      []llm.Message // 已折叠的历史，窗口已在最新 summary 处重置
	summary   string        // 最近一次成功解码的 summary 内容
	firstKept int           // 最近一次成功解码的 summary 钉住的头部条数
	toolSeqs  map[int]int   // 事件序索引 -> 其投影出的 tool 消息在 msgs 中的下标（供 prune 回改）
	pending   []byte        // 悬而未决的分片聚合，与 DeriveMessages 对齐
	wm        int           // events[:wm] 已折叠进 msgs
}

// flushPending 收尾悬而未决的分片聚合，与 DeriveMessages 的
// flush 一致。
func (p *projection) flushPending() {
	if len(p.pending) > 0 {
		p.msgs = append(p.msgs, llm.Message{Role: "assistant", Content: string(p.pending)})
		p.pending = nil
	}
}

// foldEvent 折叠一个产出消息（或收尾分片）的事件；seq 是它在日志中
// 的序索引。summary 与 prune 由 fold 顶层处理，落到这里时走 default
// 分支收尾分片，与 DeriveMessages 的投影区间行为一致。
func (p *projection) foldEvent(seq int, ev Event) {
	switch ev.Type {
	case UserMessage:
		p.flushPending()
		var d UserMessageData
		if json.Unmarshal(ev.Data, &d) == nil {
			p.msgs = append(p.msgs, llm.Message{Role: "user", Content: d.Content})
		}
	case AssistantChunk:
		var d AssistantChunkData
		if json.Unmarshal(ev.Data, &d) == nil {
			p.pending = append(p.pending, d.Delta...)
		}
	case AssistantMessage:
		// 完整消息取代已聚合的分片。
		p.pending = nil
		var d AssistantMessageData
		if json.Unmarshal(ev.Data, &d) == nil {
			p.msgs = append(p.msgs, llm.Message{Role: "assistant", Content: d.Content, ToolCalls: d.ToolCalls})
		}
	case ToolResult:
		p.flushPending()
		var d ToolResultData
		if json.Unmarshal(ev.Data, &d) == nil {
			content := d.Content
			if d.Error != "" {
				content = d.Error
			}
			if p.toolSeqs == nil {
				p.toolSeqs = map[int]int{}
			}
			p.toolSeqs[seq] = len(p.msgs)
			p.msgs = append(p.msgs, llm.Message{Role: "tool", ToolCallID: d.CallID, Content: content})
		}
	default:
		// turn/step 边界会收尾任何悬而未决的分片聚合，
		// 使崩溃的 turn 仍贡献其部分内容。
		p.flushPending()
	}
}

func (p *projection) applyPrune(ev Event) {
	var d PruneData
	if json.Unmarshal(ev.Data, &d) == nil {
		if mi, ok := p.toolSeqs[d.Seq]; ok {
			p.msgs[mi].Content = d.Content
		}
	}
}

// fold 折叠一个事件（序索引为 p.wm），以增量方式复刻
// DeriveMessages 的行为：分片聚合、完整消息取代、边界收尾、
// summary 按 ThroughSeq 重建窗口，以及 prune 回改。
func (p *projection) fold(ev Event) {
	switch ev.Type {
	case Summary:
		// 有效 summary 以 ThroughSeq+1 为水位线：重投 FirstKept
		// 头部与摘要，再重放水位线到当前物理位置之间的保留尾部。
		// 解码失败只收尾分片，不改变既有窗口。
		var d SummaryData
		if json.Unmarshal(ev.Data, &d) != nil {
			p.flushPending()
			return
		}

		p.pending = nil
		p.summary = d.Content
		p.firstKept = d.FirstKept
		p.msgs = nil
		p.toolSeqs = nil

		start := d.ThroughSeq + 1

		k := min(p.firstKept, start)
		for i := 0; i < k; i++ {
			p.foldEvent(i, p.events[i])
		}
		if p.summary != "" {
			p.flushPending()
			p.msgs = append(p.msgs, llm.Message{Role: "user", Content: p.summary})
		}

		for i := start; i < p.wm; i++ {
			p.foldEvent(i, p.events[i])
		}

		// 全量投影会先收集 prune 再投影，因此重建后统一回改仍可见的
		// 工具消息；已经退出窗口的目标不在 toolSeqs 中，会自然落空。
		for i := 0; i < p.wm; i++ {
			if p.events[i].Type == Prune {
				p.applyPrune(p.events[i])
			}
		}

		// 当前 summary 自身也是投影边界，负责收尾保留尾部的分片。
		p.flushPending()
	case Prune:
		// 回改已投影的 tool 消息。指向窗口外（序索引未投影出
		// tool 消息）的 prune 自然落空：summary 重置已清掉
		// 覆盖区间的序索引映射。
		p.applyPrune(ev)
	default:
		p.foldEvent(p.wm, ev)
	}
}

// view 返回已折叠的历史加上末尾分片的收尾——与 DeriveMessages
// 在循环后执行的最终收尾相同——并返回拷贝，使调用方无法篡改
// 缓存的投影。
func (p *projection) view() []llm.Message {
	var msgs []llm.Message
	if len(p.msgs) > 0 {
		msgs = make([]llm.Message, len(p.msgs))
		for i, msg := range p.msgs {
			if msg.ToolCalls != nil {
				msg.ToolCalls = append([]llm.ToolCall(nil), msg.ToolCalls...)
			}
			msgs[i] = msg
		}
	}
	if len(p.pending) > 0 {
		msgs = append(msgs, llm.Message{Role: "assistant", Content: string(p.pending)})
	}
	return msgs
}

// Messages 返回会话 id 的模型可见历史，等价于对整份日志跑
// DeriveMessages，但按增量折叠：某会话的首次调用（或重启后的
// 任意调用）冷读一次日志，之后的调用只折叠自上次调用以来追加
// 的事件。
func (m *Manager) Messages(id string) ([]llm.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, err := m.projection(id)
	if err != nil {
		return nil, err
	}
	for p.wm < len(p.events) {
		p.fold(p.events[p.wm])
		p.wm++
	}
	return p.view(), nil
}

// projection 返回 id 的缓存投影，首次见到时从磁盘冷载日志。
// 调用方需持有 m.mu。
func (m *Manager) projection(id string) (*projection, error) {
	if p, ok := m.projs[id]; ok {
		return p, nil
	}
	p, err := m.path(id)
	if err != nil {
		return nil, err
	}
	events, err := readEvents(p)
	if err != nil {
		return nil, err
	}
	pro := &projection{events: events}
	m.projs[id] = pro
	return pro, nil
}

// noteAppend 把已持久化的追加同步进投影缓存。日志行已在
// 盘上（Append 先写盘再广播），因此没有缓存的会话会冷载
// 日志——其中已包含 ev——而不是把 ev 折叠两次。冷载读失败
// 只是放弃缓存：下一次 Messages 会重新冷读。它永远不会
// 回调进 FileLog，因此 Manager 锁与 FileLog.mu 永远不会
// 嵌套。
func (m *Manager) noteAppend(id string, ev Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p, ok := m.projs[id]; ok {
		p.events = append(p.events, ev)
		return
	}
	// 错误被有意丢弃：磁盘日志是兜底。
	_, _ = m.projection(id)
}
