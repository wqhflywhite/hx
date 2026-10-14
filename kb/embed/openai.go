package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// Config 配置 OpenAI 兼容的 embeddings provider；空字段
// 回退到 HX_EMBED_BASE_URL / HX_EMBED_API_KEY / HX_EMBED_MODEL。
type Config struct {
	BaseURL string // 环境变量 HX_EMBED_BASE_URL；必填
	APIKey  string // 环境变量 HX_EMBED_API_KEY；可选（本地服务）
	Model   string // 环境变量 HX_EMBED_MODEL；必填
}

// resolve 应用环境变量回退，并报告
// 仍然缺失的必填字段。
func (c Config) resolve() (Config, []string) {
	if c.BaseURL == "" {
		c.BaseURL = os.Getenv("HX_EMBED_BASE_URL")
	}
	c.BaseURL = strings.TrimRight(c.BaseURL, "/")
	if c.APIKey == "" {
		c.APIKey = os.Getenv("HX_EMBED_API_KEY")
	}
	if c.Model == "" {
		c.Model = os.Getenv("HX_EMBED_MODEL")
	}
	var missing []string
	if c.BaseURL == "" {
		missing = append(missing, "base_url")
	}
	if c.Model == "" {
		missing = append(missing, "model")
	}
	return c, missing
}

// Provider 与一个 OpenAI 兼容的 embeddings 端点通信。
type Provider struct {
	cfg    Config
	client *http.Client
}

// New 创建 provider；HTTP client 可注入以便测试。环境变量回退后
// 仍缺失的必填字段会响亮报错（fail loud）。
func New(cfg Config, client *http.Client) (*Provider, error) {
	resolved, missing := cfg.resolve()
	if len(missing) > 0 {
		return nil, fmt.Errorf("embed: missing required fields %s (config or HX_EMBED_* env)", strings.Join(missing, ", "))
	}
	if client == nil {
		client = http.DefaultClient
	}
	return &Provider{cfg: resolved, client: client}, nil
}

type embedRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embedResponse struct {
	Data []struct {
		Index     int       `json:"index"`
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

// Embed 实现 Embedder：每次调用发起一次 POST {base_url}/embeddings，
// 并按返回行的 index 字段重排，无论 provider
// 返回顺序如何，调用方拿到的都是输入顺序。
func (p *Provider) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	raw, err := json.Marshal(embedRequest{Model: p.cfg.Model, Input: texts})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.BaseURL+"/embeddings", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.cfg.APIKey)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var msg [4096]byte
		n, _ := resp.Body.Read(msg[:])
		return nil, fmt.Errorf("embed: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg[:n])))
	}
	var body embedResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("embed: bad response: %w", err)
	}
	if len(body.Data) != len(texts) {
		return nil, fmt.Errorf("embed: provider returned %d vectors for %d texts", len(body.Data), len(texts))
	}
	out := make([][]float32, len(texts))
	for _, row := range body.Data {
		if row.Index < 0 || row.Index >= len(texts) {
			return nil, fmt.Errorf("embed: provider returned out-of-range index %d", row.Index)
		}
		out[row.Index] = row.Embedding
	}
	for i, v := range out {
		if len(v) == 0 {
			return nil, fmt.Errorf("embed: provider returned no vector for input %d", i)
		}
	}
	return out, nil
}
