package hxconfig_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"hx/core/fiber"
	"hx/core/plugin"
	"hx/hxconfig"
	"hx/llm"

	_ "hx/llm/deepseek"
)

var errStageDispose = errors.New("stage dispose failure")

// stagePlugin 提供可控制挂载失败和卸载失败的 Reload 测试服务。
type stagePlugin struct{}

func (stagePlugin) Name() string     { return "stage" }
func (stagePlugin) Inject() []string { return nil }
func (stagePlugin) Apply(c *fiber.Context, cfg map[string]any) error {
	if fail, _ := cfg["fail"].(bool); fail {
		return fmt.Errorf("stage mount failure")
	}
	value, _ := cfg["value"].(string)
	disposeError, _ := cfg["dispose_error"].(bool)
	return c.Effect(func(c *fiber.Context) (fiber.Disposer, error) {
		dispose := fiber.Provide[string](c, "stage", value)
		return func() error {
			if err := dispose(); err != nil {
				return err
			}
			if disposeError {
				return errStageDispose
			}
			return nil
		}, nil
	})
}

// stageConsumerPlugin 把实际解析到的 stage 值固定下来，暴露跨代绑定。
type stageConsumerPlugin struct{}

func (stageConsumerPlugin) Name() string     { return "stage-consumer" }
func (stageConsumerPlugin) Inject() []string { return []string{"stage"} }
func (stageConsumerPlugin) Apply(c *fiber.Context, _ map[string]any) error {
	return c.Effect(func(c *fiber.Context) (fiber.Disposer, error) {
		value, ok := fiber.Get[string](c, "stage")
		if !ok {
			return nil, fmt.Errorf("stage consumer: stage unavailable")
		}
		return fiber.Provide[string](c, "stage-consumer", value), nil
	})
}

func init() {
	plugin.Register(stagePlugin{})
	plugin.Register(stageConsumerPlugin{})
}

func deepseekReloadConfig(dir, model string) string {
	return fmt.Sprintf(`
plugins:
  - id: session
    name: session
    config: {dir: %q}
  - id: llm
    name: llm
  - id: deepseek
    name: deepseek
    config: {base_url: http://127.0.0.1, api_key: test, model: %q}
`, dir, model)
}

func failedDeepseekReloadConfig(dir string) string {
	return fmt.Sprintf(`
plugins:
  - id: session
    name: session
    config: {dir: %q}
  - id: llm
    name: llm
  - id: deepseek
    name: deepseek
    config: {base_url: http://127.0.0.1, api_key: test, model: broken}
  - id: fail
    name: stage
    config: {fail: true}
`, dir)
}

func generationReloadConfig(dir, value string) string {
	return fmt.Sprintf(`
plugins:
  - id: session
    name: session
    config: {dir: %q}
  - id: llm
    name: llm
  - id: stage
    name: stage
    config: {value: %q}
  - id: consumer
    name: stage-consumer
`, dir, value)
}

func loadReloadConfig(t *testing.T, dir, body string) *hxconfig.File {
	t.Helper()
	f, err := hxconfig.Load(writeConfig(t, dir, body))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func mountReloadStack(t *testing.T, f *hxconfig.File) *hxconfig.Stack {
	t.Helper()
	root := fiber.New()
	t.Cleanup(func() { _ = root.Dispose() })
	stack, err := hxconfig.MountStack(root.Context(), f, nil)
	if err != nil {
		t.Fatal(err)
	}
	return stack
}

func TestReloadDeepseekReplacesAdapter(t *testing.T) {
	dir := t.TempDir()
	stack := mountReloadStack(t, loadReloadConfig(t, dir, deepseekReloadConfig(dir, "v1")))
	reg, ok := fiber.Get[*llm.Registry](stack.Layer(), "llm")
	if !ok {
		t.Fatal("llm registry unavailable")
	}
	v1, ok := reg.Adapter("deepseek")
	if !ok {
		t.Fatal("initial adapter unavailable")
	}

	if err := stack.Reload(loadReloadConfig(t, dir, deepseekReloadConfig(dir, "v2"))); err != nil {
		t.Fatal(err)
	}
	v2, _ := reg.Adapter("deepseek")
	if v2 == v1 {
		t.Fatal("reload kept the old adapter instance")
	}
	if err := stack.Reload(loadReloadConfig(t, dir, deepseekReloadConfig(dir, "v3"))); err != nil {
		t.Fatal(err)
	}
	v3, _ := reg.Adapter("deepseek")
	if v3 == v2 || v3 == v1 {
		t.Fatal("second reload did not install a fresh adapter")
	}
}

func TestReloadFailureRestoresPreviousAdapter(t *testing.T) {
	dir := t.TempDir()
	stack := mountReloadStack(t, loadReloadConfig(t, dir, deepseekReloadConfig(dir, "v1")))
	reg, _ := fiber.Get[*llm.Registry](stack.Layer(), "llm")
	v1, _ := reg.Adapter("deepseek")
	oldLayer := stack.Layer()

	if err := stack.Reload(loadReloadConfig(t, dir, failedDeepseekReloadConfig(dir))); err == nil {
		t.Fatal("broken reload must fail")
	}
	if got := stack.Layer(); got.Fiber() != oldLayer.Fiber() {
		t.Fatal("failed reload replaced the running layer")
	}
	if got, _ := reg.Adapter("deepseek"); got != v1 {
		t.Fatal("failed reload did not roll back the previous adapter")
	}
	if err := stack.Reload(loadReloadConfig(t, dir, deepseekReloadConfig(dir, "v2"))); err != nil {
		t.Fatalf("reload after rollback failed: %v", err)
	}
}

func TestReloadConsumerBindsCurrentGeneration(t *testing.T) {
	dir := t.TempDir()
	stack := mountReloadStack(t, loadReloadConfig(t, dir, generationReloadConfig(dir, "v1")))
	if err := stack.Reload(loadReloadConfig(t, dir, generationReloadConfig(dir, "v2"))); err != nil {
		t.Fatal(err)
	}
	if got, ok := fiber.Get[string](stack.Layer(), "stage-consumer"); !ok || got != "v2" {
		t.Fatalf("consumer = %q, %v; want v2, true", got, ok)
	}
}

func TestReloadCommittedCleanupError(t *testing.T) {
	dir := t.TempDir()
	body := func(value string, disposeError bool) string {
		return fmt.Sprintf(`
plugins:
  - id: stage
    name: stage
    config: {value: %q, dispose_error: %t}
`, value, disposeError)
	}
	stack := mountReloadStack(t, loadReloadConfig(t, dir, body("v1", true)))
	err := stack.Reload(loadReloadConfig(t, dir, body("v2", false)))
	var reloadErr *hxconfig.ReloadError
	if !errors.As(err, &reloadErr) || !reloadErr.Committed || !errors.Is(err, errStageDispose) {
		t.Fatalf("Reload error = %v; want committed errStageDispose", err)
	}
	if got, ok := fiber.Get[string](stack.Layer(), "stage"); !ok || got != "v2" {
		t.Fatalf("committed layer stage = %q, %v; want v2, true", got, ok)
	}
	if err := stack.Reload(loadReloadConfig(t, dir, body("v3", false))); err != nil {
		t.Fatalf("reload after committed cleanup error: %v", err)
	}
}

func TestWithLayerBlocksReloadUntilCallbackReturns(t *testing.T) {
	dir := t.TempDir()
	stack := mountReloadStack(t, loadReloadConfig(t, dir, generationReloadConfig(dir, "v1")))
	entered := make(chan struct{})
	release := make(chan struct{})
	leaseDone := make(chan error, 1)
	go func() {
		leaseDone <- stack.WithLayer(func(ctx *fiber.Context) error {
			close(entered)
			<-release
			if got, ok := fiber.Get[string](ctx, "stage"); !ok || got != "v1" {
				return fmt.Errorf("leased stage = %q, %v; want v1, true", got, ok)
			}
			return nil
		})
	}()
	<-entered
	reloadDone := make(chan error, 1)
	go func() {
		reloadDone <- stack.Reload(loadReloadConfig(t, dir, generationReloadConfig(dir, "v2")))
	}()
	select {
	case err := <-reloadDone:
		t.Fatalf("Reload returned before lease release: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-leaseDone; err != nil {
		t.Fatal(err)
	}
	if err := <-reloadDone; err != nil {
		t.Fatal(err)
	}
	if got, ok := fiber.Get[string](stack.Layer(), "stage"); !ok || got != "v2" {
		t.Fatalf("stage after lease = %q, %v; want v2, true", got, ok)
	}
}
