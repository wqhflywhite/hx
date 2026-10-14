package eval_test

import (
	"encoding/json"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"hx/agent"
	"hx/core/event"
	"hx/core/fiber"
	"hx/core/plugin"
	"hx/kb/eval"
	"hx/llm"
	"hx/session"

	_ "hx/agentloop"
	_ "hx/llm/deepseek"
	_ "hx/prompt"
)

// Agent 级真实语料评测：每道 gold 题交给一个完整 hx 运行时
// （agentloop ReAct 循环 + kb 插件挂载 testdata/realvault +
// 真实 bge-m3 嵌入向量服务 + 真实 DeepSeek），一题一个独立
// 会话。它度量迭代检索相对单轮检索腿
// （TestRealVaultFTSOnly / TestRealVaultRealEmbedder）多救回
// 多少。
//
// 环境门槛：HX_EMBED_BASE_URL + HX_EMBED_MODEL + DEEPSEEK_API_KEY
// 必须全部设置；key 只经环境变量传递，绝不落盘。结果写入
// $HX_AGENTLVL_DIR（默认 $TMPDIR/hx-agentlvl）下的 <idx>.json；
// 已有结果文件的题目会被跳过，中断后重跑不必为已完成的题目再次付费。

type agentlvlCall struct {
	Step   int    `json:"step"`
	Name   string `json:"name"`
	Args   string `json:"args"`
	Result string `json:"result"` // 截断到 2000 个 rune
}

type agentlvlResult struct {
	Index     int            `json:"index"`
	Query     string         `json:"query"`
	Group     string         `json:"group"`
	Expect    []string       `json:"expect"`
	Note      string         `json:"note"`
	Answer    string         `json:"answer"`
	Steps     int            `json:"steps"`
	ElapsedMS int64          `json:"elapsed_ms"`
	TurnError string         `json:"turn_error,omitempty"`
	Retries   int            `json:"retries,omitempty"`
	PromptTok int            `json:"prompt_tokens"`
	ComplTok  int            `json:"completion_tokens"`
	ToolCalls []agentlvlCall `json:"tool_calls"`
}

// agentlvlCollector 累计一道题的 turn 遥测。整个运行严格串行（同一时刻
// 只有一个 agent），所以一个加锁保护的 current 指针就足以把会话事件
// 归属到正在进行的那道题。
type agentlvlCollector struct {
	mu      sync.Mutex
	current *agentlvlResult
	usageP  int
	usageC  int
}

func (col *agentlvlCollector) reset(res *agentlvlResult) {
	col.mu.Lock()
	defer col.mu.Unlock()
	col.current = res
	col.usageP, col.usageC = 0, 0
}

func (col *agentlvlCollector) finalize() {
	col.mu.Lock()
	defer col.mu.Unlock()
	if col.current != nil {
		col.current.PromptTok = col.usageP
		col.current.ComplTok = col.usageC
	}
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		return string(r[:n]) + "…(truncated)"
	}
	return s
}

func agentlvlDir(t *testing.T) string {
	t.Helper()
	dir := os.Getenv("HX_AGENTLVL_DIR")
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "hx-agentlvl")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestRealVaultAgentLevel 用完整 hx agent 端到端跑真实语料 gold 题集。
// 缺少 HX_EMBED_* 与 DEEPSEEK_API_KEY 环境时自动跳过，且不触碰
// 确定性腿的断言。
func TestRealVaultAgentLevel(t *testing.T) {
	if os.Getenv("HX_EMBED_BASE_URL") == "" || os.Getenv("HX_EMBED_MODEL") == "" ||
		os.Getenv("DEEPSEEK_API_KEY") == "" {
		t.Skip("HX_EMBED_BASE_URL / HX_EMBED_MODEL / DEEPSEEK_API_KEY not set")
	}
	gold := loadRealGold(t)
	dir := agentlvlDir(t)

	root := fiber.New()
	ctx := root.Context()
	defer root.Dispose()

	// 整个运行共用一个运行时；kb 的首次 Sync（解析 + 嵌入 706 个 chunk
	// 的语料）只在挂载时发生一次。
	vault := copyRealVault(t)
	kbCfg := map[string]any{
		"vault": vault,
		"index": filepath.Join(t.TempDir(), "kb.db"),
		"embedder": map[string]any{
			"base_url": os.Getenv("HX_EMBED_BASE_URL"),
			"api_key":  os.Getenv("HX_EMBED_API_KEY"),
			"model":    os.Getenv("HX_EMBED_MODEL"),
		},
	}
	for _, step := range []struct {
		name string
		cfg  map[string]any
	}{
		{"session", map[string]any{"dir": t.TempDir()}},
		{"llm", nil},
		{"deepseek", map[string]any{"model": "deepseek-chat"}},
		{"tools", nil},
		{"kb", kbCfg},
		{"prompt", nil},
		{"agents", nil},
		{"agentloop", nil}, // kb 场景默认 max_steps
	} {
		if err := plugin.Mount(ctx, step.name, step.cfg); err != nil {
			t.Fatalf("mount %s: %v", step.name, err)
		}
	}

	col := &agentlvlCollector{}
	// llm/stream 瀑布（waterfall）上的计量中间件：把 usage chunk 从流里
	// 旁路复制出来（agentloop 自身不记录它们）。
	event.OnWaterfall(ctx, "llm/stream", func(c *fiber.Context, req *llm.Request, next event.Next[*llm.Request, iter.Seq[llm.Chunk]]) (iter.Seq[llm.Chunk], error) {
		seq, err := next(c, req)
		if err != nil {
			return nil, err
		}
		return func(yield func(llm.Chunk) bool) {
			for chunk := range seq {
				if chunk.Usage != nil {
					col.mu.Lock()
					col.usageP += chunk.Usage.PromptTokens
					col.usageC += chunk.Usage.CompletionTokens
					col.mu.Unlock()
				}
				if !yield(chunk) {
					return
				}
			}
		}, nil
	})

	event.On(ctx, "session/event", func(_ *fiber.Context, ev session.Event) {
		col.mu.Lock()
		res := col.current
		col.mu.Unlock()
		if res == nil {
			return
		}
		switch ev.Type {
		case session.StepStart:
			col.mu.Lock()
			res.Steps++
			col.mu.Unlock()
		case session.ToolCall:
			var d session.ToolCallData
			if json.Unmarshal(ev.Data, &d) == nil {
				col.mu.Lock()
				res.ToolCalls = append(res.ToolCalls, agentlvlCall{Name: d.Name, Args: d.Arguments, Step: res.Steps - 1})
				col.mu.Unlock()
			}
		case session.ToolResult:
			var d session.ToolResultData
			if json.Unmarshal(ev.Data, &d) == nil {
				col.mu.Lock()
				for i := len(res.ToolCalls) - 1; i >= 0; i-- {
					if res.ToolCalls[i].Result == "" {
						res.ToolCalls[i].Result = truncateRunes(d.Content, 2000)
						break
					}
				}
				col.mu.Unlock()
			}
		case session.AssistantMessage:
			var d session.AssistantMessageData
			if json.Unmarshal(ev.Data, &d) == nil && len(d.ToolCalls) == 0 && strings.TrimSpace(d.Content) != "" {
				col.mu.Lock()
				res.Answer = d.Content
				col.mu.Unlock()
			}
		}
	})

	agents, ok := fiber.Get[*agent.Registry](ctx, "agents")
	if !ok {
		t.Fatal("agents service unavailable")
	}

	for idx, q := range gold.Questions {
		out := filepath.Join(dir, fmt.Sprintf("%02d.json", idx))
		if _, err := os.Stat(out); err == nil {
			t.Logf("[%02d] skip (result exists)", idx)
			continue
		}
		res := runAgentlvlQuestion(ctx, t, agents, col, idx, q)
		data, err := json.MarshalIndent(res, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(out, data, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("[%02d] %s steps=%d %dms tools=%d err=%q → %s",
			idx, q.Group, res.Steps, res.ElapsedMS, len(res.ToolCalls), res.TurnError, out)
	}
}

// runAgentlvlQuestion 在一个全新会话里提问并等待 turn（一轮）
// 结束（上限 3 分钟）。限流/过载类失败会带退避重试，
// 绝不记为题目级结果；重试计数器记录发生了
// 多少次。
func runAgentlvlQuestion(ctx *fiber.Context, t *testing.T, agents *agent.Registry, col *agentlvlCollector, idx int, q eval.Question) *agentlvlResult {
	t.Helper()
	retries := 0
	for {
		res := &agentlvlResult{
			Index:  idx,
			Query:  q.Query,
			Group:  q.Group,
			Expect: q.Expect,
			Note:   q.Note,
		}
		col.reset(res)
		ag, err := agents.Create(ctx, agent.Options{})
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		turnDone := make(chan string, 1)
		dispose := event.On(ctx, "session/event", func(_ *fiber.Context, ev session.Event) {
			if ev.Type == session.TurnEnd {
				var d session.TurnEndData
				if json.Unmarshal(ev.Data, &d) == nil {
					select {
					case turnDone <- d.Error:
					default:
					}
				}
			}
		})
		ag.Send(q.Query)
		var turnErr string
		select {
		case turnErr = <-turnDone:
		case <-time.After(3 * time.Minute):
			turnErr = "timeout (3m)"
		}
		dispose()
		_ = ag.Dispose()
		col.finalize()
		res.ElapsedMS = time.Since(start).Milliseconds()
		res.TurnError = turnErr
		res.Retries = retries

		lower := strings.ToLower(turnErr)
		if turnErr != "" && (strings.Contains(lower, "429") || strings.Contains(lower, "rate") ||
			strings.Contains(lower, "overloaded") || strings.Contains(lower, "timeout")) && retries < 3 {
			retries++
			t.Logf("[%02d] transient %q; backoff retry %d", idx, truncateRunes(turnErr, 120), retries)
			time.Sleep(time.Duration(retries*20) * time.Second)
			continue
		}
		return res
	}
}
