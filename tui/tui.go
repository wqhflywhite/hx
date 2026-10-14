package tui

import (
	"fmt"
	"strings"
	"sync"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"hx/core/event"
	"hx/core/fiber"
	"hx/llm"
	"hx/session"
)

// Prompter 把审批提示路由进 TUI 而不是 stdin；它实现
// approval.Prompter，通过插件配置注入。
type Prompter struct {
	reqs chan askReq
	done chan struct{}
	once sync.Once
}

// NewPrompter 为一次 UI 运行创建 Prompter。
func NewPrompter() *Prompter {
	return &Prompter{reqs: make(chan askReq), done: make(chan struct{})}
}

// askReq 是一个待回答的审批问题。
type askReq struct {
	call llm.ToolCall
	ans  chan bool
}

// Confirm 实现 approval.Prompter：它阻塞调用方的 agent 协程，
// 直到用户在 TUI 内作答或 UI 关闭。
func (p *Prompter) Confirm(_ *fiber.Context, call llm.ToolCall) (bool, error) {
	r := askReq{call: call, ans: make(chan bool, 1)}
	select {
	case p.reqs <- r:
	case <-p.done:
		return false, nil
	}
	return <-r.ans, nil
}

// close 释放仍在等待的 Confirm 调用方；幂等。
func (p *Prompter) close() { p.once.Do(func() { close(p.done) }) }

// 流入 bubbletea 循环的消息。
type (
	askMsg    askReq
	eventMsg  session.Event
	statusMsg string
)

var (
	userStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	toolStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	errorStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	assistantStyle = lipgloss.NewStyle()
	dimStyle       = lipgloss.NewStyle().Faint(true)
)

// model 是 bubbletea 模型：对话记录投影加一行输入框。
type model struct {
	tr      *Transcript
	input   []rune
	send    func(string)
	reqs    <-chan askReq
	pending *askReq
	status  string
	width   int
	height  int
}

// awaitReq 等待下一个审批请求。
func awaitReq(reqs <-chan askReq) tea.Cmd {
	return func() tea.Msg {
		r, ok := <-reqs
		if !ok {
			return nil
		}
		return askMsg(r)
	}
}

// Init 实现 tea.Model。
func (m *model) Init() tea.Cmd { return awaitReq(m.reqs) }

// Update 实现 tea.Model。
func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
	case eventMsg:
		m.tr.Apply(session.Event(msg))
	case statusMsg:
		m.status = string(msg)
	case askMsg:
		r := askReq(msg)
		m.pending = &r
	case tea.KeyMsg:
		if m.pending != nil {
			// 有未决审批问题时键盘归它独占。
			switch msg.String() {
			case "y", "Y":
				m.pending.ans <- true
				m.pending = nil
				return m, awaitReq(m.reqs)
			case "n", "N", "esc":
				m.pending.ans <- false
				m.pending = nil
				return m, awaitReq(m.reqs)
			}
			return m, nil
		}
		switch msg.Type {
		case tea.KeyCtrlC:
			return m, tea.Quit
		case tea.KeyEnter:
			text := strings.TrimSpace(string(m.input))
			m.input = nil
			if text == "/quit" {
				return m, tea.Quit
			}
			if text != "" {
				m.send(text)
			}
		case tea.KeyBackspace:
			if len(m.input) > 0 {
				m.input = m.input[:len(m.input)-1]
			}
		case tea.KeyRunes, tea.KeySpace:
			m.input = append(m.input, msg.Runes...)
		}
	}
	return m, nil
}

// renderLine 按角色为一行对话记录套上样式。
func renderLine(l Line) string {
	switch l.Kind {
	case KindUser:
		return userStyle.Render("you: " + l.Text)
	case KindTool:
		return toolStyle.Render("[tool] " + l.Text)
	case KindError:
		return errorStyle.Render(l.Text)
	default:
		return assistantStyle.Render(l.Text)
	}
}

// View 实现 tea.Model：渲染对话记录的可见尾部、
// 未决的审批问题（若有）以及输入行。
func (m *model) View() string {
	var b strings.Builder
	lines := m.tr.Lines()
	avail := m.height - 3
	if avail < 1 {
		avail = len(lines)
	}
	if len(lines) > avail {
		lines = lines[len(lines)-avail:]
	}
	for _, l := range lines {
		b.WriteString(renderLine(l))
		b.WriteByte('\n')
	}
	if m.pending != nil {
		b.WriteString(toolStyle.Render(fmt.Sprintf("[approval] run %s %s? [y/N]", m.pending.call.Name, summary(m.pending.call.Arguments))))
		b.WriteByte('\n')
	}
	if m.status != "" {
		b.WriteString(dimStyle.Render(m.status))
		b.WriteByte('\n')
	}
	b.WriteString("> " + string(m.input))
	return b.String()
}

// UI 持有 bubbletea 程序：它渲染会话事件流并通过
// send 提交输入。send 必须并发安全，因为配置
// 热重挂会换掉它背后的 agent。
type UI struct {
	send     func(string)
	prompter *Prompter
	program  *tea.Program
}

// NewUI 创建 TUI；prompter 必须是注入审批插件配置的
// 同一个实例。
func NewUI(send func(string), prompter *Prompter) *UI {
	return &UI{send: send, prompter: prompter}
}

// Notify 发布一行状态；Run 返回后为空操作。
func (u *UI) Notify(status string) { u.program.Send(statusMsg(status)) }

// Run 在 ctx（agent fiber 祖先链的上游）上订阅会话事件流，
// 运行程序直到 Ctrl+C 或 /quit。
func (u *UI) Run(ctx *fiber.Context) error {
	u.program = tea.NewProgram(&model{tr: NewTranscript(), send: u.send, reqs: u.prompter.reqs}, tea.WithAltScreen())
	d := event.On[session.Event](ctx, "session/event", func(_ *fiber.Context, ev session.Event) {
		u.program.Send(eventMsg(ev))
	})
	defer func() {
		_ = d()
		u.prompter.close()
	}()
	_, err := u.program.Run()
	return err
}
