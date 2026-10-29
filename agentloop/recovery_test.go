package agentloop

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"sync"
	"testing"
	"time"

	"hx/agent"
	"hx/core/event"
	"hx/core/fiber"
	"hx/llm"
	"hx/prompt"
	"hx/session"
	"hx/tools"
)

type recoveryAdapter struct {
	mu   sync.Mutex
	reqs []*llm.Request
}

func (a *recoveryAdapter) Stream(_ context.Context, req *llm.Request) iter.Seq[llm.Chunk] {
	a.mu.Lock()
	a.reqs = append(a.reqs, req)
	call := len(a.reqs)
	a.mu.Unlock()
	return func(yield func(llm.Chunk) bool) {
		if call == 1 {
			yield(llm.Chunk{ToolCall: &llm.ToolCall{ID: "broken", Name: "bash", Arguments: "{}"}})
			yield(llm.Chunk{Err: errors.New("stream disconnected")})
			return
		}
		yield(llm.Chunk{Delta: "recovered"})
	}
}

func TestBrokenToolCallStreamCanContinueNextTurn(t *testing.T) {
	root := fiber.New()
	ctx := root.Context()
	mgr, err := session.NewManager(t.TempDir(), time.Now, func() string { return "recovery" })
	if err != nil {
		t.Fatal(err)
	}
	fiber.Provide[session.Opener](ctx, "sessions", mgr)
	fiber.Provide[*session.Manager](ctx, "session-manager", mgr)
	adapter := &recoveryAdapter{}
	llms := llm.NewRegistry()
	llms.SetAdapter("fake", adapter)
	fiber.Provide[*llm.Registry](ctx, "llm", llms)
	fiber.Provide[*tools.Registry](ctx, "tools", tools.NewRegistry())
	fiber.Provide[*prompt.Prompt](ctx, "prompt", prompt.New())
	agents := agent.NewRegistry()
	agents.SetFactory(&Loop{maxSteps: 4})
	fiber.Provide[*agent.Registry](ctx, "agents", agents)
	ag, err := agents.Create(ctx, agent.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ends := make(chan session.TurnEndData, 2)
	event.On[session.Event](ctx, "session/event", func(_ *fiber.Context, ev session.Event) {
		if ev.Type == session.TurnEnd {
			var data session.TurnEndData
			_ = jsonUnmarshal(ev.Data, &data)
			ends <- data
		}
	})
	ag.Send("first")
	end := <-ends
	if end.Error == "" {
		t.Fatal("broken stream must fail the first turn")
	}
	ag.Send("second")
	select {
	case end = <-ends:
		if end.Error != "" {
			t.Fatalf("second turn failed: %s", end.Error)
		}
	case <-time.After(time.Second):
		t.Fatal("second turn did not complete")
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if len(adapter.reqs) != 2 {
		t.Fatalf("requests = %d; want 2", len(adapter.reqs))
	}
	for _, msg := range adapter.reqs[1].Messages {
		if len(msg.ToolCalls) != 0 {
			t.Fatalf("second request contains dangling tool calls: %+v", msg)
		}
	}
	if err := ag.Dispose(); err != nil {
		t.Fatal(err)
	}
	if err := root.Dispose(); err != nil {
		t.Fatal(err)
	}
}

func jsonUnmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
