package web

import (
	"context"
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
	mu         sync.Mutex
	seq        uint64
	generation uint64
	pending    map[string]pendingApproval
	notify     func(id string, call llm.ToolCall)
	done       chan struct{}
	once       sync.Once
}

var _ approval.Prompter = (*Prompter)(nil)

type pendingApproval struct {
	call       llm.ToolCall
	ans        chan bool
	generation uint64
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
func (p *Prompter) Confirm(ctx context.Context, _ *fiber.Context, call llm.ToolCall) (bool, error) {
	p.mu.Lock()
	select {
	case <-p.done:
		p.mu.Unlock()
		return false, nil
	default:
	}
	p.seq++
	id := fmt.Sprintf("a%d", p.seq)
	pa := pendingApproval{call: call, ans: make(chan bool, 1), generation: p.generation}
	p.pending[id] = pa
	notify := p.notify
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.pending, id)
		p.mu.Unlock()
	}()
	if notify != nil {
		notify(id, call)
	}
	select {
	case allow := <-pa.ans:
		if err := ctx.Err(); err != nil {
			return false, err
		}
		return allow, nil
	case <-p.done:
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// Answer 只接受当前 agent 代际的待答审批；旧代或重复作答返回 false。
func (p *Prompter) Answer(id string, allow bool) bool {
	p.mu.Lock()
	pa, ok := p.pending[id]
	if ok && pa.generation == p.generation {
		delete(p.pending, id)
	} else {
		ok = false
	}
	p.mu.Unlock()
	if !ok {
		return false
	}
	pa.ans <- allow
	return true
}

// Invalidate 切换 agent 代际并拒绝全部旧审批，旧回答此后不能再放行工具。
func (p *Prompter) Invalidate() {
	p.mu.Lock()
	p.generation++
	pending := p.pending
	p.pending = map[string]pendingApproval{}
	p.mu.Unlock()
	for _, pa := range pending {
		select {
		case pa.ans <- false:
		default:
		}
	}
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
