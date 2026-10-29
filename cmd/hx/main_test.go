package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"hx/agent"
	"hx/core/fiber"
	"hx/core/plugin"
	"hx/hxconfig"
)

type testAgent struct {
	fb *fiber.Fiber
	id string
}

func (a *testAgent) Send(string)       {}
func (a *testAgent) SessionID() string { return a.id }
func (a *testAgent) Dispose() error    { return a.fb.Dispose() }
func (a *testAgent) disposed() bool    { return a.fb.State() == fiber.StateDisposed }

type testAgentFactory struct{ next atomic.Uint64 }

func (f *testAgentFactory) Create(c *fiber.Context, opts agent.Options) (agent.Agent, error) {
	fb, err := c.Fiber().New("test-agent")
	if err != nil {
		return nil, err
	}
	id := opts.SessionID
	if id == "" {
		id = fmt.Sprintf("test-%d", f.next.Add(1))
	}
	fb.Activate()
	return &testAgent{fb: fb, id: id}, nil
}

type testAgentDriver struct{ factory testAgentFactory }

func (p *testAgentDriver) Name() string     { return "test-agent-driver" }
func (p *testAgentDriver) Inject() []string { return []string{"agents"} }
func (p *testAgentDriver) Apply(c *fiber.Context, _ map[string]any) error {
	reg, ok := fiber.Get[*agent.Registry](c, "agents")
	if !ok {
		return fmt.Errorf("test agent driver: agents unavailable")
	}
	return c.Effect(func(c *fiber.Context) (fiber.Disposer, error) {
		return reg.SetFactory(&p.factory), nil
	})
}

func init() { plugin.Register(&testAgentDriver{}) }

func TestCreateAgentNeverPublishesDisposedGeneration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hx.yml")
	body := []byte(`
plugins:
  - id: agents
    name: agents
  - id: driver
    name: test-agent-driver
`)
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	file, err := hxconfig.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	root := fiber.New()
	t.Cleanup(func() { _ = root.Dispose() })
	stack, err := hxconfig.MountStack(root.Context(), file, nil)
	if err != nil {
		t.Fatal(err)
	}
	cell := &agentCell{}
	if _, err := createAgent(nil, stack, cell, ""); err != nil {
		t.Fatal(err)
	}

	const iterations = 20
	errs := make(chan error, iterations*2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range iterations {
			_, err := createAgent(nil, stack, cell, "")
			errs <- err
		}
	}()
	go func() {
		defer wg.Done()
		for range iterations {
			errs <- stack.Reload(file)
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := createAgent(nil, stack, cell, ""); err != nil {
		t.Fatal(err)
	}
	cell.mu.Lock()
	current := cell.ag.(*testAgent)
	cell.mu.Unlock()
	if current.disposed() {
		t.Fatal("cell contains an agent from a disposed generation")
	}
}
