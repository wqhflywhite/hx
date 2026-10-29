package approval_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"hx/approval"
	"hx/core/fiber"
	"hx/core/plugin"
	"hx/llm"
	"hx/tools"

	_ "hx/tools"
)

// fakeTool 记录自己的执行次数。
type fakeTool struct{ ran *int }

func (t fakeTool) Name() string { return "fake_tool" }
func (t fakeTool) Schema() map[string]any {
	return map[string]any{"description": "test double", "type": "object"}
}
func (t fakeTool) Execute(_ context.Context, _ *fiber.Context, _ json.RawMessage) (any, error) {
	*t.ran++
	return "fake-output", nil
}

// fakePrompter 以固定决定作答并记录每次询问。
type fakePrompter struct {
	approve bool
	asked   []string
}

func (p *fakePrompter) Confirm(_ context.Context, _ *fiber.Context, call llm.ToolCall) (bool, error) {
	p.asked = append(p.asked, call.Name)
	return p.approve, nil
}

// setup 用 cfg 挂载 tools 和 approval，返回一个类 agent 的上下文
// （挂载点的子 fiber 的上下文）和工具注册表。
func setup(t *testing.T, cfg map[string]any) (*fiber.Context, *tools.Registry, func()) {
	t.Helper()
	root := fiber.New()
	ctx := root.Context()
	if err := plugin.Mount(ctx, "tools", nil); err != nil {
		t.Fatal(err)
	}
	if err := plugin.Mount(ctx, "approval", cfg); err != nil {
		t.Fatal(err)
	}
	reg, ok := fiber.Get[*tools.Registry](ctx, "tools")
	if !ok {
		t.Fatal("tools service unavailable")
	}
	agentFiber, err := root.New("agent:test")
	if err != nil {
		t.Fatal(err)
	}
	return agentFiber.Context(), reg, func() { _ = root.Dispose() }
}

func TestRejectShortCircuits(t *testing.T) {
	ran := 0
	prompter := &fakePrompter{approve: false}
	c, reg, cleanup := setup(t, map[string]any{"prompter": prompter})
	defer cleanup()
	defer reg.Register(fakeTool{ran: &ran})()

	res := reg.Execute(context.Background(), c, llm.ToolCall{ID: "call_1", Name: "fake_tool", Arguments: "{}"})
	if ran != 0 {
		t.Fatal("rejected call must not execute the tool")
	}
	if !strings.Contains(res.Error, "denied") {
		t.Fatalf("rejection must surface in the result, got %q", res.Error)
	}
	if len(prompter.asked) != 1 {
		t.Fatalf("prompter saw %d calls; want 1", len(prompter.asked))
	}
}

func TestApproveExecutes(t *testing.T) {
	ran := 0
	prompter := &fakePrompter{approve: true}
	c, reg, cleanup := setup(t, map[string]any{"prompter": prompter})
	defer cleanup()
	defer reg.Register(fakeTool{ran: &ran})()

	res := reg.Execute(context.Background(), c, llm.ToolCall{ID: "call_1", Name: "fake_tool", Arguments: "{}"})
	if ran != 1 {
		t.Fatalf("approved call ran %d times; want 1", ran)
	}
	if res.Error != "" || res.Content != "fake-output" {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestModes(t *testing.T) {
	ran := 0
	prompter := &fakePrompter{approve: false}
	cfg := map[string]any{
		"prompter": prompter,
		"mode":     map[string]any{"fake_tool": "auto", "bash": "ask"},
	}
	c, reg, cleanup := setup(t, cfg)
	defer cleanup()
	defer reg.Register(fakeTool{ran: &ran})()

	// "auto" 模式完全跳过询问。
	res := reg.Execute(context.Background(), c, llm.ToolCall{ID: "call_1", Name: "fake_tool", Arguments: "{}"})
	if ran != 1 || res.Error != "" {
		t.Fatalf("auto mode must execute without asking: %+v", res)
	}
	if len(prompter.asked) != 0 {
		t.Fatalf("auto mode must not prompt, asked %v", prompter.asked)
	}

	// 未列出的工具回退到默认的 "ask"。
	res = reg.Execute(context.Background(), c, llm.ToolCall{ID: "call_2", Name: "bash", Arguments: "{}"})
	if !strings.Contains(res.Error, "denied") || len(prompter.asked) != 1 {
		t.Fatalf("ask mode must prompt and reject: %+v asked=%v", res, prompter.asked)
	}
}

func TestBadModeFailsLoud(t *testing.T) {
	root := fiber.New()
	defer root.Dispose()
	if err := plugin.Mount(root.Context(), "approval", map[string]any{"mode": "sometimes"}); err == nil {
		t.Fatal("unknown mode must fail at mount time")
	}
}

func TestStdinPrompter(t *testing.T) {
	p := approval.StdinPrompter(strings.NewReader("y\n\n"), &strings.Builder{})
	ok, err := p.Confirm(context.Background(), nil, llm.ToolCall{Name: "bash"})
	if err != nil || !ok {
		t.Fatalf("y must approve: ok=%v err=%v", ok, err)
	}
	ok, err = p.Confirm(context.Background(), nil, llm.ToolCall{Name: "bash"})
	if err != nil || ok {
		t.Fatalf("empty answer must deny: ok=%v err=%v", ok, err)
	}
}
