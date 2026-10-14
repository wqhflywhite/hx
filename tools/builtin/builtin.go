// Package builtin 提供四个内置工具：bash、read_file、write_file、edit。
package builtin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"hx/core/fiber"
	"hx/core/plugin"
	"hx/tools"
)

const (
	// maxOutput 是捕获工具输出的上限；超出部分截断并附加提示。
	maxOutput = 30 * 1024
	// defaultBashTimeout 约束未显式指定超时的 bash 命令。
	defaultBashTimeout = 120 * time.Second
)

// Bash 带超时地运行 shell 命令，并返回截断后的捕获输出。
type Bash struct{}

// Name 实现 tools.Tool。
func (Bash) Name() string { return "bash" }

// Schema 实现 tools.Tool。
func (Bash) Schema() map[string]any {
	return map[string]any{
		"description": "Run a shell command and return its combined output.",
		"type":        "object",
		"properties": map[string]any{
			"command":         map[string]any{"type": "string", "description": "The command to run."},
			"timeout_seconds": map[string]any{"type": "integer", "description": "Optional timeout in seconds (default 120)."},
		},
		"required": []string{"command"},
	}
}

type bashArgs struct {
	Command        string `json:"command"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

// Execute 实现 tools.Tool。
func (Bash) Execute(_ *fiber.Context, args json.RawMessage) (any, error) {
	var a bashArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, fmt.Errorf("bash: bad arguments: %w", err)
	}
	if a.Command == "" {
		return nil, fmt.Errorf("bash: empty command")
	}
	timeout := defaultBashTimeout
	if a.TimeoutSeconds > 0 {
		timeout = time.Duration(a.TimeoutSeconds) * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := shellCommand(ctx, a.Command)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	runErr := cmd.Run()
	out := buf.String()
	if len(out) > maxOutput {
		out = out[:maxOutput] + fmt.Sprintf("\n... [truncated, %d bytes total]", len(out))
	}
	if ctx.Err() == context.DeadlineExceeded {
		return out + fmt.Sprintf("\n[killed: timeout after %s]", timeout), nil
	}
	if runErr != nil {
		return out + fmt.Sprintf("\n[exit: %v]", runErr), nil
	}
	return out, nil
}

// shellCommand 构造平台相关的 shell 调用；这是唯一允许
// 出现平台分支的地方。
func shellCommand(ctx context.Context, command string) *exec.Cmd {
	if runtime.GOOS == "windows" {
		return exec.CommandContext(ctx, "cmd", "/C", command)
	}
	return exec.CommandContext(ctx, "sh", "-c", command)
}

// ReadFile 返回文件的（可能被截断的）内容。
type ReadFile struct{}

// Name 实现 tools.Tool。
func (ReadFile) Name() string { return "read_file" }

// Schema 实现 tools.Tool。
func (ReadFile) Schema() map[string]any {
	return map[string]any{
		"description": "Read the contents of a file.",
		"type":        "object",
		"properties": map[string]any{
			"path": map[string]any{"type": "string", "description": "Path of the file to read."},
		},
		"required": []string{"path"},
	}
}

type readFileArgs struct {
	Path string `json:"path"`
}

// Execute 实现 tools.Tool。
func (ReadFile) Execute(_ *fiber.Context, args json.RawMessage) (any, error) {
	var a readFileArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, fmt.Errorf("read_file: bad arguments: %w", err)
	}
	data, err := os.ReadFile(a.Path)
	if err != nil {
		return nil, err
	}
	out := string(data)
	if len(out) > maxOutput {
		out = out[:maxOutput] + fmt.Sprintf("\n... [truncated, %d bytes total]", len(out))
	}
	return out, nil
}

// WriteFile 将内容写入文件，必要时创建父目录。
type WriteFile struct{}

// Name 实现 tools.Tool。
func (WriteFile) Name() string { return "write_file" }

// Schema 实现 tools.Tool。
func (WriteFile) Schema() map[string]any {
	return map[string]any{
		"description": "Write content to a file, replacing it entirely.",
		"type":        "object",
		"properties": map[string]any{
			"path":    map[string]any{"type": "string", "description": "Path of the file to write."},
			"content": map[string]any{"type": "string", "description": "The full new content."},
		},
		"required": []string{"path", "content"},
	}
}

type writeFileArgs struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

// Execute 实现 tools.Tool。
func (WriteFile) Execute(_ *fiber.Context, args json.RawMessage) (any, error) {
	var a writeFileArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, fmt.Errorf("write_file: bad arguments: %w", err)
	}
	if dir := filepath.Dir(a.Path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	if err := os.WriteFile(a.Path, []byte(a.Content), 0o644); err != nil {
		return nil, err
	}
	return fmt.Sprintf("wrote %d bytes to %s", len(a.Content), a.Path), nil
}

// Edit 替换文件中唯一出现的一处字符串。
type Edit struct{}

// Name 实现 tools.Tool。
func (Edit) Name() string { return "edit" }

// Schema 实现 tools.Tool。
func (Edit) Schema() map[string]any {
	return map[string]any{
		"description": "Replace old_string with new_string in a file; old_string must match exactly once.",
		"type":        "object",
		"properties": map[string]any{
			"path":       map[string]any{"type": "string", "description": "Path of the file to edit."},
			"old_string": map[string]any{"type": "string", "description": "The text to replace; must occur exactly once."},
			"new_string": map[string]any{"type": "string", "description": "The replacement text."},
		},
		"required": []string{"path", "old_string", "new_string"},
	}
}

type editArgs struct {
	Path      string `json:"path"`
	OldString string `json:"old_string"`
	NewString string `json:"new_string"`
}

// Execute 实现 tools.Tool。
func (Edit) Execute(_ *fiber.Context, args json.RawMessage) (any, error) {
	var a editArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, fmt.Errorf("edit: bad arguments: %w", err)
	}
	data, err := os.ReadFile(a.Path)
	if err != nil {
		return nil, err
	}
	content := string(data)
	n := strings.Count(content, a.OldString)
	if n != 1 {
		return nil, fmt.Errorf("edit: old_string occurs %d times in %s, want exactly 1", n, a.Path)
	}
	updated := strings.Replace(content, a.OldString, a.NewString, 1)
	if err := os.WriteFile(a.Path, []byte(updated), 0o644); err != nil {
		return nil, err
	}
	return fmt.Sprintf("edited %s", a.Path), nil
}

// All 列出全部内置工具。
func All() []tools.Tool {
	return []tools.Tool{Bash{}, ReadFile{}, WriteFile{}, Edit{}}
}

// Plugin 将所有内置工具挂载进 "tools" 注册表。
func Plugin() plugin.Plugin { return builtinPlugin{} }

type builtinPlugin struct{}

func (builtinPlugin) Name() string     { return "builtin-tools" }
func (builtinPlugin) Inject() []string { return []string{"tools"} }
func (builtinPlugin) Apply(c *fiber.Context, _ map[string]any) error {
	return c.Effect(func(c *fiber.Context) (fiber.Disposer, error) {
		reg, ok := fiber.Get[*tools.Registry](c, "tools")
		if !ok {
			return nil, fmt.Errorf("builtin-tools: tools service unavailable")
		}
		var disposers []fiber.Disposer
		for _, t := range All() {
			disposers = append(disposers, reg.Register(t))
		}
		return func() error {
			for i := len(disposers) - 1; i >= 0; i-- {
				if err := disposers[i](); err != nil {
					return err
				}
			}
			return nil
		}, nil
	})
}

func init() { plugin.Register(Plugin()) }
