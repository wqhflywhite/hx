package toolgate_test

import (
	"testing"
	"time"
)

func TestFirstAssembleConcurrentSessionDispose(t *testing.T) {
	ctx, _, reg, p := wire(t, map[string]any{"max_inline": 1})
	addTools(t, reg, fakeTool{"alpha", "alpha"}, fakeTool{"beta", "beta"})
	for range 50 {
		agent, err := ctx.Fiber().New("agent:dispose-race")
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, _, err := p.Assemble(agent.Context())
			done <- err
		}()
		disposed := make(chan error, 1)
		go func() { disposed <- agent.Dispose() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("Assemble deadlocked with Dispose")
		}
		select {
		case err := <-disposed:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("Dispose deadlocked with Assemble")
		}
	}
}
