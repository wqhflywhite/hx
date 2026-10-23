package memory_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"hx/core/fiber"
	"hx/core/plugin"
	"hx/memory"
	"hx/session"
)

// mount 在全新 root 上接线 session + memory，session 管理器
// 指向临时目录，两个记忆文件也重定向进临时目录。
func mount(t *testing.T, memCfg map[string]any) (*fiber.Context, *session.Manager, string, string) {
	t.Helper()
	root := fiber.New()
	ctx := root.Context()
	t.Cleanup(func() { _ = root.Dispose() })
	dir := t.TempDir()
	n := 0
	sessionCfg := map[string]any{
		"dir":   filepath.Join(dir, "sessions"),
		"clock": func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
		"idgen": func() string { n++; return fmt.Sprintf("s-%d", n) },
	}
	if err := plugin.Mount(ctx, "session", sessionCfg); err != nil {
		t.Fatal(err)
	}
	userFile := filepath.Join(dir, "home", ".hx", "memory.md")
	projectFile := filepath.Join(dir, "proj", ".hx", "memory.md")
	cfg := map[string]any{"user_file": userFile, "project_file": projectFile}
	for k, v := range memCfg {
		cfg[k] = v
	}
	if err := plugin.Mount(ctx, "memory", cfg); err != nil {
		t.Fatal(err)
	}
	mgr, ok := fiber.Get[*session.Manager](ctx, "sessions")
	if !ok {
		t.Fatal("sessions service unavailable")
	}
	return ctx, mgr, userFile, projectFile
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// createSession 打开一个新会话，并在 cleanup 时关闭其存储句柄。
func createSession(t *testing.T, mgr *session.Manager) string {
	t.Helper()
	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.(interface{ Close() error }).Close() })
	return id
}

// appendEvent 从 root 上下文向会话日志追加一个事件，
// 这正是 turn 开始监听器所看到的发射上下文。
func appendEvent(t *testing.T, ctx *fiber.Context, mgr *session.Manager, id string, ev session.Event) {
	t.Helper()
	store, err := mgr.Resume(id)
	if err != nil {
		t.Fatal(err)
	}
	defer store.(interface{ Close() error }).Close()
	if err := store.Append(ctx, ev); err != nil {
		t.Fatal(err)
	}
}

func startTurn(t *testing.T, ctx *fiber.Context, mgr *session.Manager, id string) {
	t.Helper()
	appendEvent(t, ctx, mgr, id, session.Event{Type: session.TurnStart, Data: session.MarshalData(session.TurnStartData{Session: id})})
}

// memoryMessages 返回日志中所有被注入记忆消息的内容，
// 按顺序排列。
func memoryMessages(t *testing.T, mgr *session.Manager, id string) []string {
	t.Helper()
	events, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, ev := range events {
		if ev.Type != session.UserMessage {
			continue
		}
		var d session.UserMessageData
		if json.Unmarshal(ev.Data, &d) == nil && strings.Contains(d.Content, "<system-reminder>") {
			out = append(out, d.Content)
		}
	}
	return out
}

func TestInjectsOnTurnStart(t *testing.T) {
	ctx, mgr, userFile, projectFile := mount(t, nil)
	writeFile(t, userFile, "- prefers concise answers\n")
	writeFile(t, projectFile, "- module is hx, Go 1.26\n")

	id := createSession(t, mgr)
	startTurn(t, ctx, mgr, id)

	msgs := memoryMessages(t, mgr, id)
	if len(msgs) != 1 {
		t.Fatalf("got %d memory messages; want 1", len(msgs))
	}
	for _, want := range []string{
		"<system-reminder>", "</system-reminder>",
		"Memory from: " + userFile, "prefers concise answers",
		"Memory from: " + projectFile, "module is hx, Go 1.26",
	} {
		if !strings.Contains(msgs[0], want) {
			t.Fatalf("injected message misses %q:\n%s", want, msgs[0])
		}
	}

	// 注入是持久历史：投影把它作为 user 角色的
	// 消息携带。
	projected, err := mgr.Messages(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(projected) != 1 || projected[0].Role != "user" || !strings.Contains(projected[0].Content, "module is hx") {
		t.Fatalf("projection = %+v; want the memory message as user", projected)
	}
}

func TestNoReinjectWhenUnchanged(t *testing.T) {
	ctx, mgr, _, projectFile := mount(t, nil)
	writeFile(t, projectFile, "- fact\n")
	id := createSession(t, mgr)
	startTurn(t, ctx, mgr, id)
	appendEvent(t, ctx, mgr, id, session.Event{Type: session.TurnEnd, Data: session.MarshalData(session.TurnEndData{Session: id})})
	startTurn(t, ctx, mgr, id)

	if msgs := memoryMessages(t, mgr, id); len(msgs) != 1 {
		t.Fatalf("got %d memory messages after two turns; want 1 (digest unchanged)", len(msgs))
	}
}

func TestReinjectsOnChange(t *testing.T) {
	ctx, mgr, _, projectFile := mount(t, nil)
	writeFile(t, projectFile, "- old decision\n")
	id := createSession(t, mgr)
	startTurn(t, ctx, mgr, id)
	writeFile(t, projectFile, "- new decision\n")
	startTurn(t, ctx, mgr, id)

	msgs := memoryMessages(t, mgr, id)
	if len(msgs) != 2 {
		t.Fatalf("got %d memory messages; want 2 after a file change", len(msgs))
	}
	if !strings.Contains(msgs[1], "new decision") {
		t.Fatalf("second injection has stale content:\n%s", msgs[1])
	}
}

func TestReinjectsAfterSummary(t *testing.T) {
	ctx, mgr, _, projectFile := mount(t, nil)
	writeFile(t, projectFile, "- fact\n")
	id := createSession(t, mgr)
	startTurn(t, ctx, mgr, id)
	// 压缩（compaction）把这次注入折叠到 summary 水位线之后：
	// 序列此刻为 turn/start(0)、注入的 user/message(1)，summary
	// 落在序索引 2，ThroughSeq 即其自身位置。
	appendEvent(t, ctx, mgr, id, session.Event{Type: session.Summary, Data: session.MarshalData(session.SummaryData{Content: "SUMMARY", ThroughSeq: 2})})
	startTurn(t, ctx, mgr, id)

	msgs := memoryMessages(t, mgr, id)
	if len(msgs) != 2 {
		t.Fatalf("got %d memory messages; want 2 (re-injected after summary)", len(msgs))
	}
	// 投影应展示 summary 加上重新注入的记忆，而不是
	// 最初那次注入。
	projected, err := mgr.Messages(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(projected) != 2 || projected[0].Content != "SUMMARY" || !strings.Contains(projected[1].Content, "- fact") {
		t.Fatalf("projection = %+v; want summary + re-injected memory", projected)
	}
}

func TestMaxBytesTruncates(t *testing.T) {
	ctx, mgr, _, projectFile := mount(t, map[string]any{"max_bytes": 256})
	writeFile(t, projectFile, "- "+strings.Repeat("x", 4000)+"-TAIL\n")
	id := createSession(t, mgr)
	startTurn(t, ctx, mgr, id)

	msgs := memoryMessages(t, mgr, id)
	if len(msgs) != 1 {
		t.Fatalf("got %d memory messages; want 1", len(msgs))
	}
	if len(msgs[0]) > 256 {
		t.Fatalf("injected message is %d bytes; budget is 256", len(msgs[0]))
	}
	if !strings.Contains(msgs[0], "Memory budget 256 bytes: truncated") {
		t.Fatalf("missing truncation marker:\n%s", msgs[0])
	}
	if strings.Contains(msgs[0], "TAIL") {
		t.Fatal("over-budget tail survived truncation")
	}
}

func TestMaxBytesOmitsBroaderFile(t *testing.T) {
	ctx, mgr, userFile, projectFile := mount(t, nil)
	writeFile(t, userFile, "- "+strings.Repeat("u", 40000)+"\n")
	writeFile(t, projectFile, "- small project fact\n")
	id := createSession(t, mgr)
	startTurn(t, ctx, mgr, id)

	msgs := memoryMessages(t, mgr, id)
	if len(msgs) != 1 {
		t.Fatalf("got %d memory messages; want 1", len(msgs))
	}
	if len(msgs[0]) > 30*1024 {
		t.Fatalf("injected message is %d bytes; default budget is 30720", len(msgs[0]))
	}
	if !strings.Contains(msgs[0], "omitted "+userFile) {
		t.Fatalf("missing omission marker:\n%s", msgs[0])
	}
	if !strings.Contains(msgs[0], "small project fact") {
		t.Fatalf("most specific file was dropped instead of the broader one:\n%s", msgs[0])
	}
}

func TestZeroFilesZeroInjection(t *testing.T) {
	ctx, mgr, userFile, _ := mount(t, nil)
	id := createSession(t, mgr)
	startTurn(t, ctx, mgr, id)

	events, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != session.TurnStart {
		t.Fatalf("log has %d events; want exactly the turn/start", len(events))
	}
	projected, err := mgr.Messages(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(projected) != 0 {
		t.Fatalf("projection = %+v; want empty", projected)
	}

	// 存在但为空的文件同样不注入任何内容。
	writeFile(t, userFile, "  \n")
	startTurn(t, ctx, mgr, id)
	events, err = mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("log has %d events after an empty-file turn; want 2 turn/starts only", len(events))
	}
	if msgs := memoryMessages(t, mgr, id); len(msgs) != 0 {
		t.Fatalf("empty memory file produced an injection: %q", msgs[0])
	}
}

func TestConfigFailLoud(t *testing.T) {
	for name, cfg := range map[string]map[string]any{
		"non-string user_file":    {"user_file": 42},
		"non-string project_file": {"project_file": true},
		"non-int max_bytes":       {"max_bytes": "30720"},
		"zero max_bytes":          {"max_bytes": 0},
	} {
		t.Run(name, func(t *testing.T) {
			root := fiber.New()
			ctx := root.Context()
			t.Cleanup(func() { _ = root.Dispose() })
			if err := plugin.Mount(ctx, "session", map[string]any{"dir": t.TempDir()}); err != nil {
				t.Fatal(err)
			}
			if err := plugin.Mount(ctx, "memory", cfg); err == nil {
				t.Fatalf("config %v accepted; want fail loud", cfg)
			}
		})
	}

	t.Run("same resolved path", func(t *testing.T) {
		root := fiber.New()
		ctx := root.Context()
		t.Cleanup(func() { _ = root.Dispose() })
		if err := plugin.Mount(ctx, "session", map[string]any{"dir": t.TempDir()}); err != nil {
			t.Fatal(err)
		}
		same := filepath.Join(t.TempDir(), "memory.md")
		err := plugin.Mount(ctx, "memory", map[string]any{"user_file": same, "project_file": same})
		if err == nil {
			t.Fatal("identical user/project files accepted; want fail loud")
		}
	})
}

func TestDisposeStopsInjection(t *testing.T) {
	root := fiber.New()
	ctx := root.Context()
	t.Cleanup(func() { _ = root.Dispose() })
	n := 0
	sessionCfg := map[string]any{
		"dir":   filepath.Join(t.TempDir(), "sessions"),
		"clock": func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
		"idgen": func() string { n++; return fmt.Sprintf("s-%d", n) },
	}
	if err := plugin.Mount(ctx, "session", sessionCfg); err != nil {
		t.Fatal(err)
	}
	mgr, _ := fiber.Get[*session.Manager](ctx, "sessions")

	dir := t.TempDir()
	projectFile := filepath.Join(dir, "project.md")
	writeFile(t, projectFile, "- fact v1\n")

	// 在专用子 fiber 上应用插件，以便单独 dispose（撤销）它。
	fb, err := root.New("plugin:memory")
	if err != nil {
		t.Fatal(err)
	}
	err = memory.Plugin().Apply(fb.Context(), map[string]any{
		"user_file":    filepath.Join(dir, "user.md"),
		"project_file": projectFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	fb.Activate()

	id := createSession(t, mgr)
	startTurn(t, ctx, mgr, id)
	if msgs := memoryMessages(t, mgr, id); len(msgs) != 1 {
		t.Fatalf("got %d memory messages; want 1 before dispose", len(msgs))
	}

	if err := fb.Dispose(); err != nil {
		t.Fatal(err)
	}
	writeFile(t, projectFile, "- fact v2\n")
	startTurn(t, ctx, mgr, id)
	if msgs := memoryMessages(t, mgr, id); len(msgs) != 1 {
		t.Fatalf("got %d memory messages after dispose; want still 1", len(msgs))
	}
}
