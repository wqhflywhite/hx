// Command hx 是 hx 的 CLI：单发任务、行模式聊天、TUI、web UI，
// 以及可选的由配置文件驱动的组合与热重挂。
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"

	"hx/agent"
	"hx/core/event"
	"hx/core/fiber"
	"hx/core/plugin"
	"hx/hxconfig"
	"hx/session"
	"hx/tui"
	"hx/web"

	_ "hx/agentloop"     // 默认 ReAct 驱动
	_ "hx/approval"      // 工具审批监听器
	_ "hx/compaction"    // 历史压缩（compaction）提供者
	_ "hx/kb"            // 知识库（无 vault 配置时为空操作）
	_ "hx/llm"           // llm 注册表
	_ "hx/llm/deepseek"  // DeepSeek 提供者
	_ "hx/memory"        // 长期记忆段落 + memory_save 工具
	_ "hx/prompt"        // prompt 组装
	_ "hx/subagent"      // delegate 工具（subagent 委派）
	_ "hx/toolgate"      // 工具闸门（检索式按需加载）
	_ "hx/tools"         // 工具注册表
	_ "hx/tools/builtin" // bash、read_file、write_file、edit
)

// flatWire 是不使用配置文件时的默认组合——它是数据而非
// 机制（与 dsh 的基础 cordis.yml 同角色）。
var flatWire = []string{"session", "llm", "deepseek", "tools", "builtin-tools", "kb", "prompt", "memory", "agents", "approval", "agentloop", "compaction", "subagent"}

// agentCell 在配置热重挂时替换存活的 agent；并发安全。
type agentCell struct {
	mu sync.Mutex
	ag agent.Agent
}

// send 将文本送入当前 agent。
func (c *agentCell) send(text string) {
	c.mu.Lock()
	ag := c.ag
	c.mu.Unlock()
	if ag != nil {
		ag.Send(text)
	}
}

// swap 替换当前 agent 并 dispose（撤销）旧的。
func (c *agentCell) swap(ag agent.Agent) {
	c.mu.Lock()
	old := c.ag
	c.ag = ag
	c.mu.Unlock()
	if old != nil {
		_ = old.Dispose()
	}
}

// sessionID 返回当前 agent 的会话 id，无 agent 时返回 ""。
func (c *agentCell) sessionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ag == nil {
		return ""
	}
	return c.ag.SessionID()
}

func main() {
	args := os.Args[1:]
	mode := ""
	if len(args) > 0 && (args[0] == "chat" || args[0] == "tui" || args[0] == "web") {
		mode = args[0]
		args = args[1:]
	}
	fs := flag.NewFlagSet("hx", flag.ExitOnError)
	resume := fs.String("resume", "", "resume the given session id and continue")
	configFlag := fs.String("config", "", "path to hx.yml (default ./hx.yml, then ~/.hx/hx.yml)")
	watch := fs.Bool("watch", false, "reload the config file on change (config mode only)")
	addr := fs.String("addr", "127.0.0.1:7681", "listen address for web mode")
	allowRemote := fs.Bool("allow-remote", false, "allow non-loopback web listen (requires an auth token)")
	authToken := fs.String("auth-token", os.Getenv("HX_WEB_TOKEN"), "auth token for all web endpoints (or HX_WEB_TOKEN)")
	_ = fs.Parse(args)
	rest := fs.Args()
	if mode == "" && len(rest) == 0 {
		mode = "tui"
	}

	root := fiber.New()
	ctx := root.Context()
	defer root.Dispose()
	configPath, found, err := hxconfig.Find(*configFlag)
	if err != nil {
		fatal(err)
	}
	if *watch && !found {
		fatal(fmt.Errorf("--watch needs a config file (use --config or create ./hx.yml)"))
	}

	prompter := tui.NewPrompter()
	webPrompter := web.NewPrompter()
	stack, agentCtx, err := mountForMode(ctx, mode, configPath, found, prompter, webPrompter)
	if err != nil {
		fatal(err)
	}
	if stack != nil {
		defer stack.Dispose()
	}

	cell := &agentCell{}
	defer cell.swap(nil)
	if _, err := createAgent(agentCtx, stack, cell, *resume); err != nil {
		fatal(err)
	}
	fmt.Fprintf(os.Stderr, "session: %s\n", cell.sessionID())

	// TUI 和 web 启动后会把 notify 改接到各自的 UI。
	notify := func(text string) { fmt.Fprintf(os.Stderr, "hx: %s\n", text) }
	if found && *watch {
		if err := watchConfig(stack, configPath, cell, notify); err != nil {
			fatal(err)
		}
	}

	switch mode {
	case "tui":
		ui := tui.NewUI(cell.send, prompter)
		notify = ui.Notify
		if err := ui.Run(ctx); err != nil {
			fatal(err)
		}
	case "web":
		srv := newWebServer(ctx, stack, cell, webPrompter, *authToken, *allowRemote)
		notify = srv.Notify
		fmt.Fprintf(os.Stderr, "hx: web UI listening on %s\n", *addr)
		if err := srv.Run(ctx, *addr); err != nil {
			fatal(err)
		}
	case "chat":
		chat(ctx, cell)
	default:
		oneshot(ctx, cell, rest[0])
	}
}

// mountForMode 按界面模式挂载 flat 默认组合或配置驱动的双层 Stack。
func mountForMode(ctx *fiber.Context, mode, configPath string, found bool, prompter *tui.Prompter, webPrompter *web.Prompter) (*hxconfig.Stack, *fiber.Context, error) {
	if found {
		f, err := hxconfig.Load(configPath)
		if err != nil {
			return nil, nil, err
		}
		var overlays hxconfig.Overlays
		switch mode {
		case "tui":
			overlays = hxconfig.Overlays{"approval": {"prompter": prompter}}
		case "web":
			overlays = hxconfig.Overlays{"approval": {"prompter": webPrompter}}
		}
		stack, err := hxconfig.MountStack(ctx, f, overlays)
		if err != nil {
			return nil, nil, err
		}
		return stack, stack.Layer(), nil
	}

	// 声明式装配；激活顺序由依赖图决定，与 flatWire 顺序无关。
	for _, name := range flatWire {
		var cfg map[string]any
		if name == "approval" {
			switch mode {
			case "tui":
				cfg = map[string]any{"prompter": prompter}
			case "web":
				cfg = map[string]any{"prompter": webPrompter}
			}
		}
		if err := plugin.Mount(ctx, name, cfg); err != nil {
			return nil, nil, err
		}
	}
	return nil, ctx, nil
}

// createAgent 在同一个 layer 租约内创建并发布 agent，避免 Reload
// 已撤销旧代后，旧请求又把旧 agent 写回 cell。
func createAgent(rootCtx *fiber.Context, stack *hxconfig.Stack, cell *agentCell, sessionID string) (string, error) {
	id := ""
	create := func(ctx *fiber.Context) error {
		reg, ok := fiber.Get[*agent.Registry](ctx, "agents")
		if !ok {
			return fmt.Errorf("agents service unavailable")
		}
		ag, err := reg.Create(ctx, agent.Options{SessionID: sessionID})
		if err != nil {
			return err
		}
		cell.swap(ag)
		id = ag.SessionID()
		return nil
	}
	if stack != nil {
		if err := stack.WithLayer(create); err != nil {
			return "", err
		}
	} else if err := create(rootCtx); err != nil {
		return "", err
	}
	return id, nil
}

// watchConfig 在插件层重建后，用同一会话 id 替换当前 agent。
func watchConfig(stack *hxconfig.Stack, configPath string, cell *agentCell, notify func(string)) error {
	return stack.Watch(configPath, func(err error) {
		if err != nil {
			var reloadErr *hxconfig.ReloadError
			if !errors.As(err, &reloadErr) || !reloadErr.Committed {
				notify("config reload failed: " + err.Error())
				return
			}
			notify("config reload committed with cleanup warning: " + err.Error())
		}
		id, err := createAgent(nil, stack, cell, cell.sessionID())
		if err != nil {
			notify("config reload could not restart agent: " + err.Error())
			return
		}
		notify("config reloaded; agent restarted on session " + id)
	})
}

// newWebServer 把 web 服务器接入 harness：消息发往 cell，
// 会话切换时在当前层上重建 agent。
func newWebServer(ctx *fiber.Context, stack *hxconfig.Stack, cell *agentCell, prompter *web.Prompter, authToken string, allowRemote bool) *web.Server {
	mgr, ok := fiber.Get[*session.Manager](ctx, "sessions")
	if !ok {
		fatal(fmt.Errorf("sessions service unavailable"))
	}
	recreate := func(sessionID string) (string, error) {
		return createAgent(ctx, stack, cell, sessionID)
	}
	srv, err := web.NewServer(web.Options{
		Prompter:       prompter,
		Send:           cell.send,
		CurrentSession: cell.sessionID,
		LoadEvents:     mgr.Events,
		ListSessions:   mgr.List,
		NewSession:     func() (string, error) { return recreate("") },
		ResumeSession:  recreate,
		AuthToken:      authToken,
		AllowRemote:    allowRemote,
	})
	if err != nil {
		fatal(err)
	}
	// 在 agent 尚未开始工作时，用当前会话的日志预填 hub，
	// 让首个订阅者的回填能看到它。
	events, err := mgr.Events(cell.sessionID())
	if err != nil {
		fatal(err)
	}
	srv.SetSession(cell.sessionID(), events)
	return srv
}

// transcript 为行模式打印事件流，并在返回的 channel 上
// 汇报 turn（一轮）结束。
func transcript(ctx *fiber.Context) chan string {
	turnDone := make(chan string, 1)
	event.On[session.Event](ctx, "session/event", func(_ *fiber.Context, ev session.Event) {
		switch ev.Type {
		case session.AssistantChunk:
			var d session.AssistantChunkData
			if json.Unmarshal(ev.Data, &d) == nil {
				fmt.Print(d.Delta)
			}
		case session.ToolCall:
			var d session.ToolCallData
			if json.Unmarshal(ev.Data, &d) == nil {
				fmt.Fprintf(os.Stderr, "\n[tool] %s %s\n", d.Name, d.Arguments)
			}
		case session.TurnEnd:
			var d session.TurnEndData
			if json.Unmarshal(ev.Data, &d) == nil {
				turnDone <- d.Error
			}
			fmt.Println()
		}
	})
	return turnDone
}

// oneshot 运行单个任务直到完成。
func oneshot(ctx *fiber.Context, cell *agentCell, task string) {
	turnDone := transcript(ctx)
	cell.send(task)
	if turnErr := <-turnDone; turnErr != "" {
		fatal(fmt.Errorf("turn failed: %s", turnErr))
	}
}

// chat 是行模式：每行一个 turn。
func chat(ctx *fiber.Context, cell *agentCell) {
	turnDone := transcript(ctx)
	fmt.Fprintln(os.Stderr, "chat mode; Ctrl-D to exit")
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Fprint(os.Stderr, "> ")
		if !scanner.Scan() {
			return
		}
		line := scanner.Text()
		if line == "" {
			continue
		}
		cell.send(line)
		if turnErr := <-turnDone; turnErr != "" {
			fmt.Fprintf(os.Stderr, "turn failed: %s\n", turnErr)
		}
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "hx:", err)
	os.Exit(1)
}
