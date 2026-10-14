package plugin_test

import (
	"testing"
	"time"

	"hx/core/fiber"
	"hx/core/plugin"
)

// testPlugin 是为测试用字段拼出来的 Plugin；Apply 注册一个
// 副作用，其 disposer 会向 disposed 通道报告撤销。
type testPlugin struct {
	name     string
	inject   []string
	applied  chan<- string
	disposed chan<- string
}

func (p *testPlugin) Name() string     { return p.name }
func (p *testPlugin) Inject() []string { return p.inject }
func (p *testPlugin) Apply(c *fiber.Context, _ map[string]any) error {
	return c.Effect(func(*fiber.Context) (fiber.Disposer, error) {
		if p.applied != nil {
			p.applied <- p.name
		}
		return func() error {
			if p.disposed != nil {
				p.disposed <- p.name
			}
			return nil
		}, nil
	})
}

func TestInjectWaitActivateCascade(t *testing.T) {
	root := fiber.New()
	ctx := root.Context()

	applied := make(chan string, 2)
	disposed := make(chan string, 2)
	plugin.Register(&testPlugin{name: "test/dependent", inject: []string{"test-svc"}, applied: applied, disposed: disposed})
	plugin.Register(&testPlugin{name: "test/other", inject: []string{"test-svc"}, applied: applied, disposed: disposed})

	// 在依赖存在之前挂载：Apply 此时还不能运行。
	if err := plugin.Mount(ctx, "test/dependent", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case name := <-applied:
		t.Fatalf("plugin %q applied before its dependency was provided", name)
	case <-time.After(50 * time.Millisecond):
	}

	// 提供依赖：等待者被激活。
	release := fiber.Provide[string](ctx, "test-svc", "up")
	select {
	case name := <-applied:
		if name != "test/dependent" {
			t.Fatalf("activated %q; want test/dependent", name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("plugin not activated after dependency provided")
	}

	// 在依赖存在之后挂载：Apply 同步运行。
	if err := plugin.Mount(ctx, "test/other", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case name := <-applied:
		if name != "test/other" {
			t.Fatalf("activated %q; want test/other", name)
		}
	case <-time.After(time.Second):
		t.Fatal("synchronous mount did not apply")
	}

	// 撤销服务：两个依赖方都被级联 dispose。
	if err := release(); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for range 2 {
		select {
		case name := <-disposed:
			got[name] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("dependents not cascade-disposed after revoke: got %v", got)
		}
	}
	if !got["test/dependent"] || !got["test/other"] {
		t.Fatalf("cascade dispose incomplete: %v", got)
	}
	if err := root.Dispose(); err != nil {
		t.Fatal(err)
	}
}

func TestMountUnknownFails(t *testing.T) {
	root := fiber.New()
	defer root.Dispose()
	if err := plugin.Mount(root.Context(), "test/does-not-exist", nil); err == nil {
		t.Fatal("mounting an unregistered plugin must fail loud")
	}
}

func TestDuplicateRegisterPanics(t *testing.T) {
	plugin.Register(&testPlugin{name: "test/dup"})
	defer func() {
		if recover() == nil {
			t.Fatal("duplicate registration must panic")
		}
	}()
	plugin.Register(&testPlugin{name: "test/dup"})
}
