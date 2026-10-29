// Package deepseek 提供基于 OpenAI 兼容 HTTP SSE 的 DeepSeek provider。
package deepseek

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"os"
	"sort"
	"strings"

	"hx/core/fiber"
	"hx/core/plugin"
	"hx/llm"
)

// Config 配置适配器；留空的字段先回退到环境变量，
// 再回退到默认值。
type Config struct {
	BaseURL string // 环境变量 DEEPSEEK_BASE_URL，默认 https://api.deepseek.com
	APIKey  string // 环境变量 DEEPSEEK_API_KEY
	Model   string // 环境变量 DEEPSEEK_MODEL，默认 deepseek-chat
}

// resolve 应用环境变量与默认值回退。
func (c Config) resolve() Config {
	if c.BaseURL == "" {
		c.BaseURL = os.Getenv("DEEPSEEK_BASE_URL")
	}
	if c.BaseURL == "" {
		c.BaseURL = "https://api.deepseek.com"
	}
	c.BaseURL = strings.TrimRight(c.BaseURL, "/")
	if c.APIKey == "" {
		c.APIKey = os.Getenv("DEEPSEEK_API_KEY")
	}
	if c.Model == "" {
		c.Model = os.Getenv("DEEPSEEK_MODEL")
	}
	if c.Model == "" {
		c.Model = "deepseek-chat"
	}
	return c
}

// Adapter 对接 OpenAI 兼容的 chat completions 端点。
type Adapter struct {
	cfg    Config
	client *http.Client
}

// New 创建适配器；HTTP client 可注入以便测试。
func New(cfg Config, client *http.Client) *Adapter {
	if client == nil {
		client = http.DefaultClient
	}
	return &Adapter{cfg: cfg.resolve(), client: client}
}

type requestBody struct {
	Model    string           `json:"model"`
	Messages []wireMessage    `json:"messages"`
	Tools    []toolDefinition `json:"tools,omitempty"`
	Stream   bool             `json:"stream"`
}

// wireMessage 是 llm.Message 的 OpenAI 兼容线上形态：工具调用的
// name 与 arguments 嵌套在 "function" 下，并带 type "function"。
// 领域类型保持扁平，转换只发生在 provider 边界。
type wireMessage struct {
	Role       string         `json:"role"`
	Content    string         `json:"content"`
	ToolCalls  []wireToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

// wireToolCall 是 llm.ToolCall 的 OpenAI 兼容线上形态。
type wireToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// wireMessages 把领域消息转换成线上形态。
func wireMessages(msgs []llm.Message) []wireMessage {
	out := make([]wireMessage, 0, len(msgs))
	for _, m := range msgs {
		wm := wireMessage{Role: m.Role, Content: m.Content, ToolCallID: m.ToolCallID}
		for _, tc := range m.ToolCalls {
			var wc wireToolCall
			wc.ID = tc.ID
			wc.Type = "function"
			wc.Function.Name = tc.Name
			wc.Function.Arguments = tc.Arguments
			wm.ToolCalls = append(wm.ToolCalls, wc)
		}
		out = append(out, wm)
	}
	return out
}

type toolDefinition struct {
	Type     string `json:"type"`
	Function struct {
		Name        string         `json:"name"`
		Description string         `json:"description,omitempty"`
		Parameters  map[string]any `json:"parameters"`
	} `json:"function"`
}

type streamResponse struct {
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
	Choices []struct {
		Delta struct {
			Content   string `json:"content"`
			ToolCalls []struct {
				Index    int    `json:"index"`
				ID       string `json:"id"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage *llm.Usage `json:"usage"`
}

// Stream 以 stream=true 发送请求，并产出解析后的 SSE chunk；
// 工具调用的增量会被累积，完整后一次性产出。
func (a *Adapter) Stream(ctx context.Context, req *llm.Request) iter.Seq[llm.Chunk] {
	return func(yield func(llm.Chunk) bool) {
		resp, err := a.openStream(ctx, a.streamBody(req))
		if err != nil {
			yield(llm.Chunk{Err: err})
			return
		}
		defer resp.Body.Close()
		consumeStream(ctx, resp.Body, yield)
	}
}

// streamBody 构造 OpenAI 兼容的流式请求体。
func (a *Adapter) streamBody(req *llm.Request) requestBody {
	model := req.Model
	if model == "" {
		model = a.cfg.Model
	}
	body := requestBody{Model: model, Messages: wireMessages(req.Messages), Stream: true}
	for _, t := range req.Tools {
		var def toolDefinition
		def.Type = "function"
		def.Function.Name = t.Name
		def.Function.Description = t.Description
		def.Function.Parameters = t.Parameters
		body.Tools = append(body.Tools, def)
	}
	return body
}

// openStream 发起 HTTP 请求；只有 200 响应才交给 SSE 消费方。
func (a *Adapter) openStream(ctx context.Context, body requestBody) (*http.Response, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		a.cfg.BaseURL+"/chat/completions", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	if a.cfg.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+a.cfg.APIKey)
	}

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		var msg [4096]byte
		n, _ := resp.Body.Read(msg[:])
		return nil, fmt.Errorf("deepseek: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg[:n])))
	}
	return resp, nil
}

// toolCallAccumulator 按工具调用 index 聚合 SSE 增量。
type toolCallAccumulator struct {
	pending map[int]*llm.ToolCall
	args    map[int]*strings.Builder
}

func newToolCallAccumulator() *toolCallAccumulator {
	return &toolCallAccumulator{
		pending: map[int]*llm.ToolCall{},
		args:    map[int]*strings.Builder{},
	}
}

func (a *toolCallAccumulator) add(index int, id, name, args string) {
	call, ok := a.pending[index]
	if !ok {
		call = &llm.ToolCall{}
		a.pending[index] = call
		a.args[index] = &strings.Builder{}
	}
	if id != "" {
		call.ID = id
	}
	if name != "" {
		call.Name = name
	}
	a.args[index].WriteString(args)
}

// flush 按实际 index 排序产出完整调用，稀疏或非连续 index 不会
// 触发 nil 解引用；随后重置下一组调用的累积状态。
func (a *toolCallAccumulator) flush(yield func(llm.Chunk) bool) bool {
	indices := make([]int, 0, len(a.pending))
	for index := range a.pending {
		indices = append(indices, index)
	}
	sort.Ints(indices)
	for _, index := range indices {
		call := a.pending[index]
		call.Arguments = a.args[index].String()
		if !yield(llm.Chunk{ToolCall: call}) {
			return false
		}
	}
	a.pending = map[int]*llm.ToolCall{}
	a.args = map[int]*strings.Builder{}
	return true
}

// consumeStream 逐行消费 SSE，输出文本、usage 和完整工具调用。
// 只有显式 [DONE] 是正常终态；取消、扫描错误和缺失 DONE 的 EOF
// 都必须向上层报告，不能把半截响应当成成功。
func consumeStream(ctx context.Context, body io.Reader, yield func(llm.Chunk) bool) {
	calls := newToolCallAccumulator()
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		data, ok := strings.CutPrefix(line, "data:")
		if !ok {
			continue
		}
		data = strings.TrimSpace(data)
		if data == "[DONE]" {
			calls.flush(yield)
			return
		}
		var sr streamResponse
		if err := json.Unmarshal([]byte(data), &sr); err != nil {
			yield(llm.Chunk{Err: fmt.Errorf("deepseek: bad SSE data: %w", err)})
			return
		}
		if sr.Error != nil {
			yield(llm.Chunk{Err: fmt.Errorf("deepseek: SSE error: %s", sr.Error.Message)})
			return
		}
		if sr.Usage != nil {
			if !yield(llm.Chunk{Usage: sr.Usage}) {
				return
			}
		}
		for _, choice := range sr.Choices {
			if choice.Delta.Content != "" {
				if !yield(llm.Chunk{Delta: choice.Delta.Content}) {
					return
				}
			}
			for _, tc := range choice.Delta.ToolCalls {
				calls.add(tc.Index, tc.ID, tc.Function.Name, tc.Function.Arguments)
			}
			if choice.FinishReason != "" && len(calls.pending) > 0 {
				if !calls.flush(yield) {
					return
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		yield(llm.Chunk{Err: fmt.Errorf("deepseek: read SSE: %w", err)})
		return
	}
	if err := ctx.Err(); err != nil {
		yield(llm.Chunk{Err: err})
		return
	}
	yield(llm.Chunk{Err: io.ErrUnexpectedEOF})
}

// Plugin 把 DeepSeek 适配器注册进 "llm" 注册表，名字取自
// 配置键 "name"（默认 "deepseek"）；换一个名字与端点再挂载一次，
// 即可让多个 OpenAI 兼容 provider 并存。
func Plugin() plugin.Plugin { return deepseekPlugin{} }

type deepseekPlugin struct{}

func (deepseekPlugin) Name() string     { return "deepseek" }
func (deepseekPlugin) Inject() []string { return []string{"llm"} }
func (deepseekPlugin) Apply(c *fiber.Context, cfg map[string]any) error {
	var ccfg Config
	name := "deepseek"
	if cfg != nil {
		ccfg.BaseURL, _ = cfg["base_url"].(string)
		ccfg.APIKey, _ = cfg["api_key"].(string)
		ccfg.Model, _ = cfg["model"].(string)
		if v, ok := cfg["name"].(string); ok && v != "" {
			name = v
		}
	}
	return c.Effect(func(c *fiber.Context) (fiber.Disposer, error) {
		reg, ok := fiber.Get[*llm.Registry](c, "llm")
		if !ok {
			return nil, fmt.Errorf("deepseek: llm service unavailable")
		}
		return reg.SetAdapter(name, New(ccfg, nil)), nil
	})
}

func init() { plugin.Register(Plugin()) }
