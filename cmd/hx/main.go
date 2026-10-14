// Command hx 是 hx 的 CLI：单发任务、行模式聊天、TUI、web UI，
// 以及可选的由配置文件驱动的组合与热重挂。
package main

import (
	"bufio"
	"encoding/json"
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
	addr := fs.String("addr", ":7681", "listen address for web mode")
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

	// prompter 把审批问题路由进 TUI，webPrompter 路由进 web UI；
	// 行模式则让审批插件继续使用默认的 stdin
	// Prompter。
	prompter := tui.NewPrompter()
	webPrompter := web.NewPrompter()
	var stack *hxconfig.Stack
	agentCtx := ctx
	if found {
		f, err := hxconfig.Load(configPath)
		if err != nil {
			fatal(err)
		}
		var overlays hxconfig.Overlays
		switch mode {
		case "tui":
			overlays = hxconfig.Overlays{"approval": {"prompter": prompter}}
		case "web":
			overlays = hxconfig.Overlays{"approval": {"prompter": webPrompter}}
		}
		stack, err = hxconfig.MountStack(ctx, f, overlays)
		if err != nil {
			fatal(err)
		}
		defer stack.Dispose()
		agentCtx = stack.Layer()
	} else {
		// 声明式装配；激活顺序由依赖图决定，与此处顺序无关。
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
				fatal(err)
			}
		}
	}

	agents, ok := fiber.Get[*agent.Registry](agentCtx, "agents")
	if !ok {
		fatal(fmt.Errorf("agents service unavailable"))
	}
	cell := &agentCell{}
	defer cell.swap(nil)
	ag, err := agents.Create(agentCtx, agent.Options{SessionID: *resume})
	if err != nil {
		fatal(err)
	}
	cell.swap(ag)
	fmt.Fprintf(os.Stderr, "session: %s\n", cell.sessionID())

	// notify 用于上报配置热重挂的结果；TUI 和 web 模式运行后
	// 会把它改接到各自的 UI。
	notify := func(text string) { fmt.Fprintf(os.Stderr, "hx: %s\n", text) }
	if found && *watch {
		if err := stack.Watch(configPath, func(err error) {
			if err != nil {
				notify("config reload failed: " + err.Error())
				return
			}
			// 插件层已重建；在同一份会话日志上重启 agent。
			reg, ok := fiber.Get[*agent.Registry](stack.Layer(), "agents")
			if !ok {
				return
			}
			fresh, err := reg.Create(stack.Layer(), agent.Options{SessionID: cell.sessionID()})
			if err != nil {
				return
			}
			cell.swap(fresh)
			notify("config reloaded; agent restarted on session " + cell.sessionID())
		}); err != nil {
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
		srv := newWebServer(ctx, stack, cell, webPrompter)
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

// newWebServer 把 web 服务器接入 harness：消息发往 cell，
// 会话切换时在当前层上重建 agent。
func newWebServer(ctx *fiber.Context, stack *hxconfig.Stack, cell *agentCell, prompter *web.Prompter) *web.Server {
	mgr, ok := fiber.Get[*session.Manager](ctx, "sessions")
	if !ok {
		fatal(fmt.Errorf("sessions service unavailable"))
	}
	// layerCtx 解析新 agent 创建所在的 fiber；配置热重挂会重建
	// 该层，因此惰性解析。
	layerCtx := func() *fiber.Context {
		if stack != nil {
			return stack.Layer()
		}
		return ctx
	}
	recreate := func(sessionID string) (string, error) {
		lc := layerCtx()
		reg, ok := fiber.Get[*agent.Registry](lc, "agents")
		if !ok {
			return "", fmt.Errorf("agents service unavailable")
		}
		ag, err := reg.Create(lc, agent.Options{SessionID: sessionID})
		if err != nil {
			return "", err
		}
		cell.swap(ag)
		return cell.sessionID(), nil
	}
	srv, err := web.NewServer(web.Options{
		Prompter:       prompter,
		Send:           cell.send,
		CurrentSession: cell.sessionID,
		LoadEvents:     mgr.Events,
		ListSessions:   mgr.List,
		NewSession:     func() (string, error) { return recreate("") },
		ResumeSession:  recreate,
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
