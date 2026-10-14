package web

import (
	"fmt"
	"sync"

	"hx/approval"
	"hx/core/fiber"
	"hx/llm"
)

// ApprovalView 是一条尚未应答的审批，当有客户端在提问仍处于
// 开放状态时连接上来，它会被发送给这些客户端。
type ApprovalView struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// Prompter 在 web UI 上实现 approval.Prompter：Confirm 向每个
// SSE 客户端广播一条审批信封，并阻塞调用它的 agent 协程，直
// 到浏览器通过 POST /api/approval 作答，或 Close 拒绝所有仍在
// 等待的提问。
type Prompter struct {
	mu      sync.Mutex
	seq     uint64
	pending map[string]pendingApproval
	notify  func(id string, call llm.ToolCall)
	done    chan struct{}
	once    sync.Once
}

var _ approval.Prompter = (*Prompter)(nil)

type pendingApproval struct {
	call llm.ToolCall
	ans  chan bool
}

// NewPrompter 创建一个 Prompter；NewServer 负责接线它的 notify 钩子。
func NewPrompter() *Prompter {
	return &Prompter{pending: map[string]pendingApproval{}, done: make(chan struct{})}
}

// setNotify 安装广播钩子；由 NewServer 调用。
func (p *Prompter) setNotify(fn func(id string, call llm.ToolCall)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.notify = fn
}

// Confirm 实现 approval.Prompter。
func (p *Prompter) Confirm(_ *fiber.Context, call llm.ToolCall) (bool, error) {
	p.mu.Lock()
	select {
	case <-p.done:
		p.mu.Unlock()
		return false, nil
	default:
	}
	p.seq++
	id := fmt.Sprintf("a%d", p.seq)
	pa := pendingApproval{call: call, ans: make(chan bool, 1)}
	p.pending[id] = pa
	notify := p.notify
	p.mu.Unlock()
	if notify != nil {
		notify(id, call)
	}
	select {
	case allow := <-pa.ans:
		return allow, nil
	case <-p.done:
		return false, nil
	}
}

// Answer 解除正在等待 id 的 Confirm 的阻塞；返回值表示 id 是否
// 处于待答状态。对同一 id 的第二次作答是无操作，返回 false。
func (p *Prompter) Answer(id string, allow bool) bool {
	p.mu.Lock()
	pa, ok := p.pending[id]
	delete(p.pending, id)
	p.mu.Unlock()
	if !ok {
		return false
	}
	pa.ans <- allow
	return true
}

// Pending 对尚未应答的审批做一次快照；顺序不做保证。
func (p *Prompter) Pending() []ApprovalView {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []ApprovalView
	for id, pa := range p.pending {
		out = append(out, ApprovalView{ID: id, Name: pa.call.Name, Arguments: pa.call.Arguments})
	}
	return out
}

// Close 拒绝所有待处理的审批以及此后的一切 Confirm 调用；它是
// 幂等的。
func (p *Prompter) Close() {
	p.once.Do(func() {
		p.mu.Lock()
		p.pending = map[string]pendingApproval{}
		p.mu.Unlock()
		close(p.done)
	})
}
