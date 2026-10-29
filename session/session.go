// Package session 是只追加的 JSONL 日志及其消息投影。
package session

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"hx/core/event"
	"hx/core/fiber"
	"hx/core/plugin"
	"hx/llm"
)

// 判别联合的事件类型标签。
const (
	TurnStart        = "turn/start"
	TurnEnd          = "turn/end"
	StepStart        = "step/start"
	StepEnd          = "step/end"
	UserMessage      = "user/message"
	AssistantChunk   = "assistant/chunk"
	AssistantMessage = "assistant/message"
	ToolCall         = "tool/call"
	ToolResult       = "tool/result"
	Summary          = "session/summary"
	Prune            = "session/prune"
)

// Event 是一行日志；Type 是判别字段，Data 是载荷。
type Event struct {
	Type string          `json:"type"`
	Time time.Time       `json:"time"`
	Data json.RawMessage `json:"data"`
}

// 事件联合的各载荷。

// TurnStartData 开启一个 turn（一轮）。
type TurnStartData struct {
	Session string `json:"session"`
}

// TurnEndData 关闭一个 turn；Error 上报失败的 turn。
type TurnEndData struct {
	Session string `json:"session"`
	Error   string `json:"error,omitempty"`
}

// StepStartData 开启 turn 内的一个 step。
type StepStartData struct {
	Session string `json:"session"`
	Step    int    `json:"step"`
}

// StepEndData 关闭一个 step。
type StepEndData struct {
	Session string `json:"session"`
	Step    int    `json:"step"`
}

// UserMessageData 携带一条用户输入。
type UserMessageData struct {
	Content string `json:"content"`
}

// AssistantChunkData 携带一段流式文本增量。
type AssistantChunkData struct {
	Delta string `json:"delta"`
}

// AssistantMessageData 是一个 step 的完整 assistant 消息。
type AssistantMessageData struct {
	Content   string         `json:"content"`
	ToolCalls []llm.ToolCall `json:"tool_calls,omitempty"`
}

// ToolCallData 记录一次工具调用。
type ToolCallData struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ToolResultData 记录一次工具调用的结果。
type ToolResultData struct {
	CallID  string `json:"call_id"`
	Name    string `json:"name"`
	Content string `json:"content"`
	Error   string `json:"error,omitempty"`
}

// SummaryData 是一条压缩（compaction）摘要。ThroughSeq 记录本次
// 摘要覆盖的最后一条原始事件的序索引：投影以 ThroughSeq+1 为
// 水位线，序索引大于它的事件——无论是摘要前保留的尾部，还是摘要
// 前后并发追加的事件——都留在模型视图里。写入方必须总是赋值；
// 缺省 0 没有特殊含义，只表示「覆盖到序索引 0」。
type SummaryData struct {
	Content string `json:"content"`
	// FirstKept 钉住日志开头不参与压缩的事件条数：投影时前
	// FirstKept 条原始事件排在摘要领投消息之前，永远保持模型
	// 可见。缺省 0 是旧行为（不保留），旧日志无需迁移。
	FirstKept  int `json:"first_kept,omitempty"`
	ThroughSeq int `json:"through_seq"`
}

// PruneData 记录一次剪枝（compaction 的无模型调用前奏）：被剪事件
// 由 Seq 定位——它是事件在会话日志中的序索引。Event 自身没有可用
// 作定位的字段（Time 由注入时钟填充，可以重复），而日志只追加、
// 序索引永不变，因此序索引是唯一稳定的定位方式。Seq 只能指向它
// 之前的事件（增量投影按序折叠，prune 回改已投影的消息）。投影时
// 把 Seq 指向的 tool/result 事件的投影内容替换为 Content。
type PruneData struct {
	Seq     int    `json:"seq"`
	Content string `json:"content"`
}

// MarshalData 把载荷编码进 Event.Data。
func MarshalData(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Errorf("session: marshal event data: %w", err))
	}
	return raw
}

// Store 是一个会话的持久日志；Append 先写盘再广播。
type Store interface {
	// Append 持久化 ev（用 Store 的时钟填 Time）之后才
	// 发出 "session/event"，因此模型永远不会看到未落盘的内容。
	Append(c *fiber.Context, ev Event) error
	// Events 读回一个会话的完整日志。
	Events(id string) ([]Event, error)
	// Fork 把 src 的前 boundary 个事件复制进一个新会话并
	// 返回新会话 id。
	Fork(src string, boundary int) (string, error)
}

// Opener 创建或恢复按会话划分的 Store；"sessions" 服务实现它，
// 使驱动方永远不直接处理路径。
type Opener interface {
	// Create 打开一个新会话并返回其 Store 和 id。
	Create() (Store, string, error)
	// Resume 打开一个已存在会话的日志用于追加。
	Resume(id string) (Store, error)
}

// Manager 持有会话目录、时钟和 id 生成器；它就是作为
// "sessions" 服务提供的对象。它还为每个会话缓存一份
// 增量投影（见 projection.go），因此热路径只折叠新事件，
// 而不是每个 step 重读整份日志。
type Manager struct {
	dir   string
	clock func() time.Time
	idgen func() string

	mu     sync.Mutex
	projs  map[string]*projection
	states map[string]*sync.Mutex
}

// NewManager 创建以 dir 为根的 Manager；clock 为事件打时间戳，
// idgen 命名新会话。两者都必须注入，以保证日志可复现。
func NewManager(dir string, clock func() time.Time, idgen func() string) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Manager{
		dir: dir, clock: clock, idgen: idgen,
		projs: make(map[string]*projection), states: make(map[string]*sync.Mutex),
	}, nil
}

// appendState 返回同一 Manager 内按会话共享的顺序锁。写盘、缓存
// 更新与冷读都按 state → m.mu 的顺序加锁，避免多 Resume 句柄交错。
func (m *Manager) appendState(id string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.states[id]
	if state == nil {
		state = &sync.Mutex{}
		m.states[id] = state
	}
	return state
}

// validateID 校验会话 id：拒绝任何可能让路径逃逸出会话目录的
// 形态。id 最终可来自外部输入（web resume、命令行参数）。
func validateID(id string) error {
	if id == "" {
		return fmt.Errorf("session: empty session id")
	}
	if id == "." || id == ".." {
		return fmt.Errorf("session: invalid session id %q", id)
	}
	if strings.ContainsAny(id, `/\`) {
		return fmt.Errorf("session: session id %q contains path separator", id)
	}
	if filepath.IsAbs(id) {
		return fmt.Errorf("session: session id %q is an absolute path", id)
	}
	if filepath.VolumeName(id) != "" {
		return fmt.Errorf("session: session id %q contains a volume name", id)
	}
	return nil
}

func (m *Manager) path(id string) (string, error) {
	if err := validateID(id); err != nil {
		return "", err
	}
	p := filepath.Join(m.dir, id+".jsonl")
	rel, err := filepath.Rel(m.dir, p)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("session: session id %q escapes the sessions dir", id)
	}
	return p, nil
}

// Create 实现 Opener。
func (m *Manager) Create() (Store, string, error) {
	id := m.idgen()
	store, err := m.open(id, os.O_CREATE|os.O_EXCL)
	if err != nil {
		return nil, "", err
	}
	return store, id, nil
}

func (m *Manager) open(id string, flag int) (Store, error) {
	path, err := m.path(id)
	if err != nil {
		return nil, err
	}
	state := m.appendState(id)
	state.Lock()
	defer state.Unlock()
	f, err := os.OpenFile(path, flag|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("session: open %q: %w", id, err)
	}
	off, err := repairTail(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("session: open %q: %w", id, err)
	}
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("session: open %q: %w", id, err)
	}
	return &FileLog{m: m, id: id, f: f, state: state}, nil
}

// Resume 实现 Opener。打开和坏后缀修复与其他追加共用会话顺序锁。
// 句柄不带 O_APPEND（Windows 上 O_APPEND 句柄无法 Truncate），因此
// 每次 Append 都在锁内重新 Seek 到当前文件末尾。
func (m *Manager) Resume(id string) (Store, error) {
	return m.open(id, 0)
}

// repairTail 扫描日志并返回 Append 应使用的写入偏移。崩溃残留的
// 坏后缀（第一个无法解析为 Event 的行及其之后的一切，与 readEvents
// 的逻辑截断语义一致）被物理截断；末尾无换行的好行补写换行符，
// 使后续 Append 永不与其粘连。空行不计入事件但计入偏移，与
// readEvents 的跳过一致。修复落盘后立即 fsync。本函数在 Resume
// 返回句柄之前运行，因此被截断的字节不可能已存在于任何投影缓存
// 中。IO 错误 fail loud，绝不当坏数据处理——那会误删好数据。
func repairTail(f *os.File) (int64, error) {
	r := bufio.NewReader(f)
	var off int64
	var bad, dirty bool
	for {
		line, err := r.ReadBytes('\n')
		if len(bytes.TrimRight(line, "\r\n")) > 0 {
			var ev Event
			if json.Unmarshal(line, &ev) != nil {
				bad = true
				break
			}
		}
		off += int64(len(line))
		if err == nil {
			continue
		}
		if err != io.EOF {
			return 0, fmt.Errorf("session: scan log: %w", err)
		}
		if len(line) > 0 {
			// 末尾好行缺换行符：补写一个，否则 Append 会与其
			// 粘连成一条新的坏行。
			if _, werr := f.WriteAt([]byte{'\n'}, off); werr != nil {
				return 0, fmt.Errorf("session: repair missing newline: %w", werr)
			}
			off++
			dirty = true
		}
		break
	}
	if bad {
		if err := f.Truncate(off); err != nil {
			return 0, fmt.Errorf("session: truncate corrupt tail: %w", err)
		}
		dirty = true
	}
	if dirty {
		if err := f.Sync(); err != nil {
			return 0, fmt.Errorf("session: sync repaired log: %w", err)
		}
	}
	return off, nil
}

func cloneEvent(ev Event) Event {
	ev.Data = append(json.RawMessage(nil), ev.Data...)
	return ev
}

func cloneEvents(events []Event) []Event {
	out := make([]Event, len(events))
	for i, ev := range events {
		out[i] = cloneEvent(ev)
	}
	return out
}

// Events 读回一个会话的完整日志；若投影缓存已热，
// 则直接从内存返回防御性拷贝，不再重读文件。
func (m *Manager) Events(id string) ([]Event, error) {
	path, err := m.path(id)
	if err != nil {
		return nil, err
	}
	state := m.appendState(id)
	state.Lock()
	defer state.Unlock()
	m.mu.Lock()
	p, ok := m.projs[id]
	m.mu.Unlock()
	if ok {
		return cloneEvents(p.events), nil
	}
	return readEvents(path)
}

// List 返回目录中所有会话日志的 id，已排序。
func (m *Manager) List() ([]string, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		ids = append(ids, strings.TrimSuffix(e.Name(), ".jsonl"))
	}
	sort.Strings(ids)
	return ids, nil
}

// Fork 把 src 的前 boundary 个事件复制进一个新会话并返回新会话
// id；新日志是源日志逐字节一致的前缀。
func (m *Manager) Fork(src string, boundary int) (string, error) {
	events, err := m.Events(src)
	if err != nil {
		return "", err
	}
	if boundary < 0 || boundary > len(events) {
		return "", fmt.Errorf("session: fork boundary %d out of range [0,%d]", boundary, len(events))
	}
	id := m.idgen()
	path, err := m.path(id)
	if err != nil {
		return "", err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return "", err
	}
	defer f.Close()
	for _, ev := range events[:boundary] {
		raw, err := json.Marshal(ev)
		if err != nil {
			return "", err
		}
		if _, err := f.Write(append(raw, '\n')); err != nil {
			return "", err
		}
	}
	return id, nil
}

func readEvents(path string) ([]Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var events []Event
	r := bufio.NewReader(f)
	for {
		line, readErr := r.ReadBytes('\n')
		if readErr != nil && readErr != io.EOF {
			return events, readErr
		}
		bad := false
		if len(bytes.TrimRight(line, "\r\n")) > 0 {
			var ev Event
			if err := json.Unmarshal(line, &ev); err != nil {
				// 崩溃中途写了一半的行及其之后一律不可信：在第一个
				// 坏行处截断，返回之前的好事件，而不是让整个会话报错。
				bad = true
			} else {
				events = append(events, ev)
			}
		}
		if bad || readErr == io.EOF {
			break
		}
	}
	return events, nil
}

// FileLog 是一个会话的 Store：一个只追加的 JSONL 文件。
type FileLog struct {
	m  *Manager
	id string

	mu    sync.Mutex
	state *sync.Mutex
	f     *os.File
}

// ID 返回这份日志的会话 id。
func (l *FileLog) ID() string { return l.id }

// Append 实现 Store：在同会话顺序锁内定位末尾、写盘、fsync 并更新
// 缓存，随后释放所有锁再广播。缓存事件与广播载荷使用不同拷贝，
// 监听器无法借 RawMessage 改写尚未投影的内存日志。
func (l *FileLog) Append(c *fiber.Context, ev Event) error {
	l.mu.Lock()
	if l.f == nil {
		l.mu.Unlock()
		return fmt.Errorf("session: log %q is closed", l.id)
	}
	ev = cloneEvent(ev)
	ev.Time = l.m.clock()
	raw, err := json.Marshal(ev)
	if err != nil {
		l.mu.Unlock()
		return err
	}
	l.state.Lock()
	if _, err := l.f.Seek(0, io.SeekEnd); err != nil {
		l.state.Unlock()
		l.mu.Unlock()
		return err
	}
	if _, err := l.f.Write(append(raw, '\n')); err != nil {
		l.state.Unlock()
		l.mu.Unlock()
		return err
	}
	if err := l.f.Sync(); err != nil {
		l.state.Unlock()
		l.mu.Unlock()
		return err
	}
	l.m.noteAppend(l.id, ev)
	l.state.Unlock()
	l.mu.Unlock()
	event.Emit[Event](c, "session/event", cloneEvent(ev))
	return nil
}

// Events 实现 Store。
func (l *FileLog) Events(id string) ([]Event, error) { return l.m.Events(id) }

// Fork 实现 Store。
func (l *FileLog) Fork(src string, boundary int) (string, error) { return l.m.Fork(src, boundary) }

// Close 关闭日志文件。
func (l *FileLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f != nil {
		err := l.f.Close()
		l.f = nil
		return err
	}
	return nil
}

// DeriveMessages 是从日志到模型历史的唯一投影：assistant
// 分片聚合成一条 assistant 消息，完整的 assistant/message
// 取代分片缓冲。最新有效 summary 以 ThroughSeq+1 为水位线：
// 其内容作为 user 消息领投，水位线之后的事件无论物理上位于
// summary 前后都参与投影；已覆盖事件仍留在日志里，但退出模型
// 视图。FirstKept 钉住日志开头的原始事件，排在摘要之前且不得
// 越过水位线。prune 按序索引回改 tool/result 的投影内容。
func DeriveMessages(events []Event) []llm.Message {
	window := scanDeriveWindow(events)
	builder := messageBuilder{pruned: window.pruned}
	for i := 0; i < window.firstKept; i++ {
		builder.add(i, events[i])
	}
	if window.summary != "" {
		builder.flush()
		builder.msgs = append(builder.msgs, llm.Message{Role: "user", Content: window.summary})
	}
	for i := window.start; i < len(events); i++ {
		builder.add(i, events[i])
	}
	builder.flush()
	return builder.msgs
}

// deriveWindow 是全量投影的覆盖范围与全部 prune 回改。
type deriveWindow struct {
	start     int
	summary   string
	firstKept int
	pruned    map[int]string
}

// scanDeriveWindow 找最新有效 summary，并收集最后生效的 prune 内容。
func scanDeriveWindow(events []Event) deriveWindow {
	window := deriveWindow{pruned: map[int]string{}}
	for _, ev := range events {
		switch ev.Type {
		case Summary:
			var d SummaryData
			if json.Unmarshal(ev.Data, &d) == nil {
				window.summary = d.Content
				window.firstKept = d.FirstKept
				window.start = d.ThroughSeq + 1
			}
		case Prune:
			var d PruneData
			if json.Unmarshal(ev.Data, &d) == nil {
				window.pruned[d.Seq] = d.Content
			}
		}
	}
	// 防御手写的越界 FirstKept：钉住的前缀不得越过水位线。
	window.firstKept = min(window.firstKept, window.start)
	return window
}

// messageBuilder 按事件序构造全量投影，聚合分片并应用 prune。
type messageBuilder struct {
	msgs    []llm.Message
	pending []byte
	pruned  map[int]string
}

func (b *messageBuilder) flush() {
	if len(b.pending) > 0 {
		b.msgs = append(b.msgs, llm.Message{Role: "assistant", Content: string(b.pending)})
		b.pending = nil
	}
}

func (b *messageBuilder) add(seq int, ev Event) {
	switch ev.Type {
	case UserMessage:
		b.flush()
		var d UserMessageData
		if json.Unmarshal(ev.Data, &d) == nil {
			b.msgs = append(b.msgs, llm.Message{Role: "user", Content: d.Content})
		}
	case AssistantChunk:
		var d AssistantChunkData
		if json.Unmarshal(ev.Data, &d) == nil {
			b.pending = append(b.pending, d.Delta...)
		}
	case AssistantMessage:
		// 完整消息取代已聚合的分片。
		b.pending = nil
		var d AssistantMessageData
		if json.Unmarshal(ev.Data, &d) == nil {
			b.msgs = append(b.msgs, llm.Message{Role: "assistant", Content: d.Content, ToolCalls: d.ToolCalls})
		}
	case ToolResult:
		b.flush()
		var d ToolResultData
		if json.Unmarshal(ev.Data, &d) == nil {
			content := d.Content
			if d.Error != "" {
				content = d.Error
			}
			if pc, ok := b.pruned[seq]; ok {
				content = pc
			}
			b.msgs = append(b.msgs, llm.Message{Role: "tool", ToolCallID: d.CallID, Content: content})
		}
	default:
		// turn/step 边界和控制事件会收尾悬而未决的分片。
		b.flush()
	}
}

// Plugin 把 Manager 暴露为 "sessions" 服务。配置键："dir"、
// "clock"（func() time.Time）、"idgen"（func() string）。
func Plugin() plugin.Plugin { return sessionPlugin{} }

type sessionPlugin struct{}

func (sessionPlugin) Name() string     { return "session" }
func (sessionPlugin) Inject() []string { return nil }
func (sessionPlugin) Apply(c *fiber.Context, cfg map[string]any) error {
	dir, _ := cfg["dir"].(string)
	if dir == "" {
		dir = os.Getenv("HX_SESSIONS_DIR")
	}
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		dir = filepath.Join(home, ".hx", "sessions")
	}
	clock, _ := cfg["clock"].(func() time.Time)
	if clock == nil {
		clock = time.Now
	}
	idgen, _ := cfg["idgen"].(func() string)
	if idgen == nil {
		idgen = defaultIDGen
	}
	return c.Effect(func(c *fiber.Context) (fiber.Disposer, error) {
		mgr, err := NewManager(dir, clock, idgen)
		if err != nil {
			return nil, err
		}
		return fiber.Provide[*Manager](c, "sessions", mgr), nil
	})
}

var idCounter uint64
var idMu sync.Mutex

// defaultIDGen 用墙钟时间加计数器命名会话。
func defaultIDGen() string {
	idMu.Lock()
	defer idMu.Unlock()
	idCounter++
	return fmt.Sprintf("%d-%06d", time.Now().UnixNano(), idCounter)
}

func init() { plugin.Register(Plugin()) }
