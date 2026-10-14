package kb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"hx/core/fiber"
	"hx/kb/index"
)

// maxSnippet 限制 kb_search 结果中单条命中的文本长度。
const maxSnippet = 300

// searchTool 是 kb_search 工具：对已索引 vault 做 FTS 检索。
type searchTool struct {
	svc *Service
}

// Name 实现 tools.Tool。
func (searchTool) Name() string { return "kb_search" }

// Schema 实现 tools.Tool。
func (searchTool) Schema() map[string]any {
	return map[string]any{
		"description": "Search the knowledge base. Returns ranked note chunks as path + heading anchor + snippet.",
		"type":        "object",
		"properties": map[string]any{
			"query": map[string]any{"type": "string", "description": "The search query; whitespace-separated terms are OR-combined."},
			"limit": map[string]any{"type": "integer", "description": "Optional maximum number of hits."},
		},
		"required": []string{"query"},
	}
}

type searchArgs struct {
	Query string `json:"query"`
	Limit int    `json:"limit"`
}

// Execute 实现 tools.Tool。
func (t searchTool) Execute(c *fiber.Context, args json.RawMessage) (any, error) {
	var a searchArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, fmt.Errorf("kb_search: bad arguments: %w", err)
	}
	if strings.TrimSpace(a.Query) == "" {
		return nil, fmt.Errorf("kb_search: empty query")
	}
	hits, err := t.svc.Search(context.Background(), a.Query, index.SearchOpts{Limit: a.Limit})
	if err != nil {
		return nil, err
	}
	if len(hits) == 0 {
		return "no hits", nil
	}
	var b strings.Builder
	for _, h := range hits {
		anchor := h.Path
		if len(h.HeadingPath) > 0 {
			anchor += "#" + strings.Join(h.HeadingPath, " / ")
		}
		snippet := h.Text
		if len(snippet) > maxSnippet {
			snippet = snippet[:maxSnippet] + "..."
		}
		fmt.Fprintf(&b, "%s  score %.2f\n%s\n\n", anchor, h.Score, snippet)
	}
	return strings.TrimRight(b.String(), "\n"), nil
}

// readTool 是 kb_read 工具：整篇或按小节读取，并附带 etag
// 供 kb_write 做乐观并发。
type readTool struct {
	svc *Service
}

// Name 实现 tools.Tool。
func (readTool) Name() string { return "kb_read" }

// Schema 实现 tools.Tool。
func (readTool) Schema() map[string]any {
	return map[string]any{
		"description": "Read a knowledge-base note (vault-relative path), optionally only the section under a heading path. The result ends with an etag line; pass its sha1 and mtime to kb_write.",
		"type":        "object",
		"properties": map[string]any{
			"path":    map[string]any{"type": "string", "description": "Vault-relative note path, e.g. \"papers/attention.md\"."},
			"heading": map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Optional heading path selecting one section, e.g. [\"实验方法\", \"数据集\"]."},
		},
		"required": []string{"path"},
	}
}

type readArgs struct {
	Path    string   `json:"path"`
	Heading []string `json:"heading"`
}

// Execute 实现 tools.Tool。
func (t readTool) Execute(c *fiber.Context, args json.RawMessage) (any, error) {
	var a readArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, fmt.Errorf("kb_read: bad arguments: %w", err)
	}
	content, etag, err := t.svc.Read(context.Background(), a.Path, a.Heading)
	if err != nil {
		return nil, err
	}
	return fmt.Sprintf("%s\n\n[etag] sha1=%s mtime=%d", content, etag.SHA1, etag.MTime.UnixNano()), nil
}

// writeTool 是 kb_write 工具：对 vault 做乐观并发写入。etag
// 参数必须是 kb_read 返回的那一份；etag 过期会被拒绝，
// 并提示重新读取。
type writeTool struct {
	svc *Service
}

// Name 实现 tools.Tool。
func (writeTool) Name() string { return "kb_write" }

// Schema 实现 tools.Tool。
func (writeTool) Schema() map[string]any {
	return map[string]any{
		"description": "Replace a knowledge-base note's content. Requires the sha1 and mtime from the last kb_read (empty sha1 and mtime 0 to create a new note); a stale etag is rejected — re-read and retry.",
		"type":        "object",
		"properties": map[string]any{
			"path":    map[string]any{"type": "string", "description": "Vault-relative note path."},
			"content": map[string]any{"type": "string", "description": "The full new content."},
			"sha1":    map[string]any{"type": "string", "description": "The etag sha1 from kb_read (\"\" to create)."},
			"mtime":   map[string]any{"type": "integer", "description": "The etag mtime from kb_read (0 to create)."},
		},
		"required": []string{"path", "content", "sha1", "mtime"},
	}
}

type writeArgs struct {
	Path    string `json:"path"`
	Content string `json:"content"`
	SHA1    string `json:"sha1"`
	MTime   int64  `json:"mtime"`
}

// Execute 实现 tools.Tool。
func (t writeTool) Execute(c *fiber.Context, args json.RawMessage) (any, error) {
	var a writeArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, fmt.Errorf("kb_write: bad arguments: %w", err)
	}
	etag := ETag{SHA1: a.SHA1, MTime: timeFromNanos(a.MTime)}
	fresh, err := t.svc.Write(context.Background(), a.Path, []byte(a.Content), etag)
	if err != nil {
		return nil, err
	}
	return fmt.Sprintf("wrote %d bytes to %s; new etag sha1=%s mtime=%d", len(a.Content), a.Path, fresh.SHA1, fresh.MTime.UnixNano()), nil
}

// lintTool 是 kb_lint 工具：确定性的 vault 健康检查（断链
// wikilink、孤儿笔记、未索引文件、超长笔记）。它不接受
// 参数，也从不调用 LLM 或 embedder。
type lintTool struct {
	svc *Service
}

// Name 实现 tools.Tool。
func (lintTool) Name() string { return "kb_lint" }

// Schema 实现 tools.Tool。
func (lintTool) Schema() map[string]any {
	return map[string]any{
		"description": "Check knowledge-base health: wikilinks to missing notes, orphan notes (no inbound links, raw/ exempt), notes not yet indexed, and notes over the chunk threshold. No arguments.",
		"type":        "object",
		"properties":  map[string]any{},
	}
}

// Execute 实现 tools.Tool。
func (t lintTool) Execute(_ *fiber.Context, args json.RawMessage) (any, error) {
	if len(args) > 0 && string(args) != "{}" && string(args) != "null" {
		return nil, fmt.Errorf("kb_lint: takes no arguments")
	}
	report, err := t.svc.Lint(context.Background())
	if err != nil {
		return nil, err
	}
	if report.Empty() {
		return "vault healthy: no broken links, orphans, unindexed files, or long notes", nil
	}
	var b strings.Builder
	if len(report.BrokenLinks) > 0 {
		fmt.Fprintf(&b, "broken links (%d):\n", len(report.BrokenLinks))
		for _, l := range report.BrokenLinks {
			fmt.Fprintf(&b, "  %s -> [[%s]]\n", l.Src, l.Dst)
		}
	}
	if len(report.Orphans) > 0 {
		fmt.Fprintf(&b, "orphan notes (%d):\n", len(report.Orphans))
		for _, f := range report.Orphans {
			fmt.Fprintf(&b, "  %s\n", f)
		}
	}
	if len(report.Unindexed) > 0 {
		fmt.Fprintf(&b, "unindexed files (%d):\n", len(report.Unindexed))
		for _, f := range report.Unindexed {
			fmt.Fprintf(&b, "  %s\n", f)
		}
	}
	if len(report.LongNotes) > 0 {
		fmt.Fprintf(&b, "long notes (%d):\n", len(report.LongNotes))
		for _, n := range report.LongNotes {
			fmt.Fprintf(&b, "  %s (%d chunks, threshold %d)\n", n.Path, n.Chunks, t.svc.lintMaxChunks)
		}
	}
	return strings.TrimRight(b.String(), "\n"), nil
}
