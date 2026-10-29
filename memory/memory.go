// Package memory 把文件承载的长期记忆加载进持久历史。
// 每轮（turn）开始时，用户级与项目级记忆文件会以一条
// system-reminder 包裹的 user/message 追加到会话日志，
// 让记忆与模型所见的一切走同一条路径：先记日志、再做投影。
// 按会话维护的 SHA-1 摘要抑制重复注入，而一条比上次注入
// 更新的 summary 事件会重新武装注入
// （压缩（compaction）已把记忆折叠到水位线之后）。
package memory

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unicode/utf8"

	"hx/core/event"
	"hx/core/fiber"
	"hx/core/plugin"
	"hx/session"
)

const (
	// defaultUserFile 是跨项目共享的用户级存储。
	defaultUserFile = "~/.hx/memory.md"
	// defaultProjectFile 是项目级存储，相对路径
	// 以挂载时的工作目录为基准。
	defaultProjectFile = ".hx/memory.md"
	// defaultMaxBytes 限定单条注入记忆消息的字节上限，
	// 与内建工具的输出上限对齐。
	defaultMaxBytes = 30 * 1024
)

const (
	frameOpen  = "<system-reminder>"
	frameClose = "</system-reminder>"
	intro      = "The following long-term memory may be relevant to your work. Use it as guidance when applicable. It does not override system, developer, or direct user instructions."
)

// resolve 通过用户主目录展开开头的 "~"（与 session 插件的
// ~/.hx/sessions 默认路径同一约定），并把相对路径锚定在
// 工作目录上。
func resolve(path string) (string, error) {
	if strings.HasPrefix(path, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		path = filepath.Join(home, path[1:])
	}
	return filepath.Abs(filepath.FromSlash(path))
}

// escape 防止仓库可控的内容闭合插件拥有的框架
// （dsh 的 agent-instructions 规则）。
func escape(content string) string {
	return strings.ReplaceAll(content, frameClose, `<\/system-reminder>`)
}

// truncateUTF8 把 s 截断到至多 n 字节，且不劈开任何 rune。
func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	b := s[:n]
	for len(b) > 0 && !utf8.ValidString(b) {
		b = b[:len(b)-1]
	}
	return b
}

// source 是一个带内容的记忆文件：展示路径与去空白后的文本。
type source struct {
	path    string
	content string
}

// readSource 加载一个记忆文件；文件不存在、不可读
// 或仅含空白时视为确认缺席。
func readSource(path string) (source, bool) {
	const maxSourceBytes = 1 << 20
	f, err := os.Open(path)
	if err != nil {
		return source{}, false
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxSourceBytes))
	if err != nil {
		return source{}, false
	}
	content := strings.TrimSpace(string(data))
	if content == "" {
		return source{}, false
	}
	return source{path: path, content: content}, true
}

// render 在 maxBytes 预算内拼出带框架的记忆消息。预算策略遵循
// dsh agent-instructions：先整体省略较宽泛的来源，再截断最具体的
// 来源，并用标记说明发生了什么。sources 按从最宽泛（用户级）
// 到最具体（项目级）排序。
func render(sources []source, maxBytes int) string {
	if len(sources) == 0 {
		return ""
	}
	if msg := memoryFrame("", sources); len(msg) <= maxBytes {
		return msg
	}
	specific := sources[len(sources)-1]
	omitted := sources[:len(sources)-1]
	return fitSpecificSource(specific, omitted, maxBytes)
}

// memoryFrame 拼装预算标注、说明与各来源内容。
func memoryFrame(marker string, sources []source) string {
	blocks := []string{}
	if marker != "" {
		blocks = append(blocks, marker)
	}
	blocks = append(blocks, intro)
	for _, s := range sources {
		blocks = append(blocks, "Memory from: "+s.path+"\n\n"+escape(s.content))
	}
	return frameOpen + "\n" + strings.Join(blocks, "\n\n") + "\n" + frameClose
}

// memoryBudgetMarker 说明被省略的来源与最具体来源的截断量。
func memoryBudgetMarker(specific source, omitted []source, included, maxBytes int) string {
	var parts []string
	if len(omitted) > 0 {
		paths := make([]string, 0, len(omitted))
		for _, s := range omitted {
			paths = append(paths, s.path)
		}
		parts = append(parts, "omitted "+strings.Join(paths, ", "))
	}
	if included < len(specific.content) {
		parts = append(parts, fmt.Sprintf("truncated %s from %d to %d bytes", specific.path, len(specific.content), included))
	}
	return fmt.Sprintf("Memory budget %d bytes: %s", maxBytes, strings.Join(parts, "; "))
}

// fitSpecificSource 省略宽泛来源，并二分寻找最具体来源的最大可
// 保留前缀；极小预算只截断标注，始终保留完整开闭框架。
func fitSpecificSource(specific source, omitted []source, maxBytes int) string {
	best := ""
	lo, hi := 0, len(specific.content)
	for lo <= hi {
		mid := (lo + hi) / 2
		cut := truncateUTF8(specific.content, mid)
		marker := memoryBudgetMarker(specific, omitted, len(cut), maxBytes)
		msg := memoryFrame(marker, []source{{path: specific.path, content: cut}})
		if len(msg) <= maxBytes {
			best = msg
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if best != "" {
		return best
	}
	marker := memoryBudgetMarker(specific, omitted, 0, maxBytes)
	available := maxBytes - len(frameOpen) - len(frameClose) - 2
	return frameOpen + "\n" + truncateUTF8(marker, available) + "\n" + frameClose
}

// injectState 是每个会话记录最近一次注入的状态。
type injectState struct {
	digest string
	index  int // 被注入事件在日志中的位置
}

// loader 持有配置、按会话的摘要状态与注入逻辑。
type loader struct {
	opener   session.Opener
	userFile string
	projFile string
	maxBytes int

	mu     sync.Mutex
	states map[string]injectState
}

// onEvent 响应 turn 开始；其他事件类型立即返回。
func (l *loader) onEvent(c *fiber.Context, ev session.Event) {
	if ev.Type != session.TurnStart {
		return
	}
	var d session.TurnStartData
	if json.Unmarshal(ev.Data, &d) != nil || d.Session == "" {
		return
	}
	l.inject(c, d.Session)
}

// inject 在记忆消息为"新"时把它追加到会话日志：
// 首次见到、摘要变化，或上次注入之后落下了 summary。
// 没有记忆文件时什么都不追加；恢复的会话只是再注入
// 一次；追加失败则不更新状态，让下一轮（turn）
// 重试。
func (l *loader) inject(c *fiber.Context, sessionID string) {
	var sources []source
	if s, ok := readSource(l.userFile); ok {
		sources = append(sources, s)
	}
	if s, ok := readSource(l.projFile); ok {
		sources = append(sources, s)
	}
	if len(sources) == 0 {
		return
	}
	msg := render(sources, l.maxBytes)
	if msg == "" {
		return
	}
	digest := fmt.Sprintf("%x", sha1.Sum([]byte(msg)))

	l.mu.Lock()
	st, seen := l.states[sessionID]
	l.mu.Unlock()

	store, err := l.opener.Resume(sessionID)
	if err != nil {
		return
	}
	defer func() {
		if closer, ok := store.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}()
	events, err := store.Events(sessionID)
	if err != nil {
		return
	}

	need := !seen || st.digest != digest || summaryAfter(events, st.index)
	if !need {
		return
	}
	// Append 先落盘再广播，所以本轮（turn）的第一次投影
	// 就已经包含这条记忆消息。
	if err := store.Append(c, session.Event{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: msg})}); err != nil {
		return
	}
	l.mu.Lock()
	l.states[sessionID] = injectState{digest: digest, index: len(events)}
	l.mu.Unlock()
}

// summaryAfter 报告 index 之后是否存在 summary 事件，
// 存在即压缩（compaction）已把早前的注入折叠到水位线之后。
func summaryAfter(events []session.Event, index int) bool {
	for i := index + 1; i < len(events); i++ {
		if events[i].Type == session.Summary {
			return true
		}
	}
	return false
}

func configString(cfg map[string]any, key, def string) (string, error) {
	v, ok := cfg[key]
	if !ok || v == nil {
		return def, nil
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("memory: %q must be a string, got %T", key, v)
	}
	if s == "" {
		return def, nil
	}
	return s, nil
}

func configInt(cfg map[string]any, key string, def int) (int, error) {
	v, ok := cfg[key]
	if !ok || v == nil {
		return def, nil
	}
	i, ok := v.(int)
	if !ok {
		return 0, fmt.Errorf("memory: %q must be an int, got %T", key, v)
	}
	if i <= 0 {
		return 0, fmt.Errorf("memory: %q must be positive, got %d", key, i)
	}
	return i, nil
}

// Plugin 挂载 turn 开始时的记忆注入器。配置键："user_file"
// （string，默认 ~/.hx/memory.md）、"project_file"（string，默认
// .hx/memory.md，相对工作目录）、"max_bytes"（int，
// 默认 30720）。
func Plugin() plugin.Plugin { return memoryPlugin{} }

type memoryPlugin struct{}

func (memoryPlugin) Name() string     { return "memory" }
func (memoryPlugin) Inject() []string { return []string{"sessions"} }
func (memoryPlugin) Apply(c *fiber.Context, cfg map[string]any) error {
	userFile, err := configString(cfg, "user_file", defaultUserFile)
	if err != nil {
		return err
	}
	projFile, err := configString(cfg, "project_file", defaultProjectFile)
	if err != nil {
		return err
	}
	maxBytes, err := configInt(cfg, "max_bytes", defaultMaxBytes)
	if err != nil {
		return err
	}
	if minFrame := len(frameOpen) + len(frameClose) + 2; maxBytes < minFrame {
		return fmt.Errorf("memory: max_bytes must be at least %d to preserve the reminder frame, got %d", minFrame, maxBytes)
	}
	userPath, err := resolve(userFile)
	if err != nil {
		return err
	}
	projPath, err := resolve(projFile)
	if err != nil {
		return err
	}
	if userPath == projPath {
		return fmt.Errorf("memory: user_file and project_file resolve to the same path %q", userPath)
	}
	opener, ok := fiber.Get[session.Opener](c, "sessions")
	if !ok {
		return fmt.Errorf("memory: sessions service unavailable")
	}
	// turn 开始在 agent fiber 上触发，所以监听器必须挂在它的
	// 祖先上：挂载点 fiber，agent 正是在它旁边被创建的
	// （approval 先例）。disposer 绑定在插件 fiber 上。
	parent := c.Fiber().Parent()
	if parent == nil {
		return fmt.Errorf("memory: plugin fiber has no parent to listen on")
	}
	l := &loader{opener: opener, userFile: userPath, projFile: projPath, maxBytes: maxBytes, states: map[string]injectState{}}
	return c.Effect(func(*fiber.Context) (fiber.Disposer, error) {
		return event.On[session.Event](parent.Context(), "session/event", l.onEvent), nil
	})
}

func init() { plugin.Register(Plugin()) }
