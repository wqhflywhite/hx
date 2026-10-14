package hxconfig_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"hx/core/fiber"
	"hx/core/plugin"
	"hx/hxconfig"

	_ "hx/llm"
	_ "hx/session"
)

// probeState 记录 probe 插件的挂载与 dispose（撤销）。
var probeState = struct {
	sync.Mutex
	mounted  []string
	disposed []string
}{}

// probePlugin 把自己的配置 "value" 作为 "probe" 服务提供，并记录
// 自己的 dispose，用来扮演一个可热重挂的 provider。
type probePlugin struct{}

func (probePlugin) Name() string     { return "probe" }
func (probePlugin) Inject() []string { return nil }
func (probePlugin) Apply(c *fiber.Context, cfg map[string]any) error {
	value, _ := cfg["value"].(string)
	return c.Effect(func(c *fiber.Context) (fiber.Disposer, error) {
		probeState.Lock()
		probeState.mounted = append(probeState.mounted, value)
		probeState.Unlock()
		d := fiber.Provide[string](c, "probe", value)
		return func() error {
			probeState.Lock()
			probeState.disposed = append(probeState.disposed, value)
			probeState.Unlock()
			return d()
		}, nil
	})
}

func init() { plugin.Register(probePlugin{}) }

func writeConfig(t *testing.T, dir, body string) string {
	t.Helper()
	path := filepath.Join(dir, "hx.yml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadPatchAndDisabled(t *testing.T) {
	path := writeConfig(t, t.TempDir(), `
plugins:
  - id: main
    name: probe
    config: {value: old}
  - id: off
    name: probe
    disabled: true
patch:
  - id: main
    config: {value: new}
  - id: extra
    name: probe
    config: {value: inserted}
`)
	f, err := hxconfig.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	entries := f.Entries()
	if len(entries) != 2 {
		t.Fatalf("disabled entry must be skipped, got %d entries", len(entries))
	}
	// patch 整体替换 config 块；条目保留自己的 name。
	if got := entries[0].Config["value"]; got != "new" {
		t.Fatalf("patch replace: got %v", got)
	}
	if entries[0].Name != "probe" {
		t.Fatalf("patched entry keeps its name, got %q", entries[0].Name)
	}
	// id 不匹配的 patch 会插入一个新条目。
	if entries[1].ID != "extra" || entries[1].Config["value"] != "inserted" {
		t.Fatalf("patch insert: got %+v", entries[1])
	}
}

func TestLoadMissingNameFailsLoud(t *testing.T) {
	path := writeConfig(t, t.TempDir(), `
plugins:
  - id: main
    config: {value: x}
`)
	if _, err := hxconfig.Load(path); err == nil {
		t.Fatal("entry without name must fail to load")
	}
}

func TestLoadEnvExpansion(t *testing.T) {
	t.Setenv("HX_TEST_KEY", "secret-value")
	path := writeConfig(t, t.TempDir(), `
plugins:
  - id: main
    name: probe
    config: {value: "key=${HX_TEST_KEY}"}
`)
	f, err := hxconfig.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := f.Entries()[0].Config["value"]; got != "key=secret-value" {
		t.Fatalf("env expansion: got %v", got)
	}
}

// configBody 渲染一份栈配置，其中 probe 的 value 设为 value。
func configBody(dir, value string) string {
	return fmt.Sprintf(`
plugins:
  - id: session
    name: session
    config: {dir: %q}
  - id: llm
    name: llm
  - id: main
    name: probe
    config: {value: %q}
`, dir, value)
}

func TestReloadSwapsLayer(t *testing.T) {
	dir := t.TempDir()
	path := writeConfig(t, dir, configBody(dir, "v1"))
	f, err := hxconfig.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	root := fiber.New()
	defer root.Dispose()
	stack, err := hxconfig.MountStack(root.Context(), f, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := fiber.Get[string](stack.Layer(), "probe"); got != "v1" {
		t.Fatalf("initial mount: got %q", got)
	}

	f2, err := hxconfig.Load(writeConfig(t, dir, configBody(dir, "v2")))
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Reload(f2); err != nil {
		t.Fatal(err)
	}
	probeState.Lock()
	defer probeState.Unlock()
	if len(probeState.disposed) != 1 || probeState.disposed[0] != "v1" {
		t.Fatalf("old layer disposers must run, disposed %v", probeState.disposed)
	}
	if got, _ := fiber.Get[string](stack.Layer(), "probe"); got != "v2" {
		t.Fatalf("reloaded mount: got %q", got)
	}
}

func TestWatchTriggersReload(t *testing.T) {
	dir := t.TempDir()
	path := writeConfig(t, dir, configBody(dir, "v1"))
	f, err := hxconfig.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	root := fiber.New()
	defer root.Dispose()
	stack, err := hxconfig.MountStack(root.Context(), f, nil)
	if err != nil {
		t.Fatal(err)
	}
	reloaded := make(chan error, 1)
	if err := stack.Watch(path, func(err error) { reloaded <- err }); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(path, []byte(configBody(dir, "v2")), 0o644); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-reloaded:
		if err != nil {
			t.Fatalf("reload failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("config change did not trigger a reload")
	}
	if got, _ := fiber.Get[string](stack.Layer(), "probe"); got != "v2" {
		t.Fatalf("watched reload: got %q", got)
	}
	probeState.Lock()
	defer probeState.Unlock()
	if len(probeState.disposed) == 0 {
		t.Fatal("old layer disposers must run on watched reload")
	}
}

// configBodyPinned 渲染一份类似 configBody 的栈配置，但带一行显式的
// pinned 列表。
func configBodyPinned(dir, value, pinned string) string {
	return fmt.Sprintf(`
pinned: %s
plugins:
  - id: session
    name: session
    config: {dir: %q}
  - id: llm
    name: llm
  - id: main
    name: probe
    config: {value: %q}
`, pinned, dir, value)
}

func reload(t *testing.T, dir string, stack *hxconfig.Stack, body string) {
	t.Helper()
	f2, err := hxconfig.Load(writeConfig(t, dir, body))
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Reload(f2); err != nil {
		t.Fatal(err)
	}
}

func mountStack(t *testing.T, path string) *hxconfig.Stack {
	t.Helper()
	f, err := hxconfig.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	stack, err := hxconfig.MountStack(fiber.New().Context(), f, nil)
	if err != nil {
		t.Fatal(err)
	}
	return stack
}

func TestPinnedDefaultSurvivesReload(t *testing.T) {
	dir := t.TempDir()
	stack := mountStack(t, writeConfig(t, dir, configBody(dir, "v1")))
	before, ok := fiber.Get[any](stack.Layer(), "sessions")
	if !ok {
		t.Fatal("sessions service must resolve")
	}
	reload(t, dir, stack, configBody(dir, "v2"))
	after, _ := fiber.Get[any](stack.Layer(), "sessions")
	if before != after {
		t.Fatal("default pinned session service must survive a reload")
	}
}

func TestPinnedExplicitOverride(t *testing.T) {
	dir := t.TempDir()
	stack := mountStack(t, writeConfig(t, dir, configBodyPinned(dir, "v1", "[probe]")))
	sessionsBefore, _ := fiber.Get[any](stack.Layer(), "sessions")
	reload(t, dir, stack, configBodyPinned(dir, "v2", "[probe]"))
	// probe 是 pinned 的：base 层的实例带着旧值存活。
	if got, _ := fiber.Get[string](stack.Layer(), "probe"); got != "v1" {
		t.Fatalf("pinned probe must survive reload, got %q", got)
	}
	// session 不再 pinned：它随 layer 一起被重新挂载。
	if after, _ := fiber.Get[any](stack.Layer(), "sessions"); after == sessionsBefore {
		t.Fatal("unpinned session service must be remounted on reload")
	}
}

func TestPinnedEmptyList(t *testing.T) {
	dir := t.TempDir()
	stack := mountStack(t, writeConfig(t, dir, configBodyPinned(dir, "v1", "[]")))
	sessionsBefore, _ := fiber.Get[any](stack.Layer(), "sessions")
	reload(t, dir, stack, configBodyPinned(dir, "v2", "[]"))
	if after, _ := fiber.Get[any](stack.Layer(), "sessions"); after == sessionsBefore {
		t.Fatal("explicit empty pinned list must remount everything on reload")
	}
}

func TestPinnedChangeRejectsReload(t *testing.T) {
	dir := t.TempDir()
	stack := mountStack(t, writeConfig(t, dir, configBody(dir, "v1")))
	f2, err := hxconfig.Load(writeConfig(t, dir, configBodyPinned(dir, "v2", "[session, llm, probe]")))
	if err != nil {
		t.Fatal(err)
	}
	err = stack.Reload(f2)
	if err == nil {
		t.Fatal("pinned set change must reject the reload")
	}
	// 错误文案必须报出新旧名单，并说明需要重启。
	for _, want := range []string{"[llm session]", "[llm probe session]", "restart"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error must mention %q, got %v", want, err)
		}
	}
	// 拒绝重载时旧层保持不动：probe 服务仍是旧值。
	if got, _ := fiber.Get[string](stack.Layer(), "probe"); got != "v1" {
		t.Fatalf("old layer must stay running on rejected reload, got %q", got)
	}
	// 被拒绝的重载不留下任何状态：pinned 不变的下一次重载照常成功。
	reload(t, dir, stack, configBody(dir, "v3"))
	if got, _ := fiber.Get[string](stack.Layer(), "probe"); got != "v3" {
		t.Fatalf("reload after rejection must succeed, got %q", got)
	}
}

func TestPinnedUnchangedReloadSucceeds(t *testing.T) {
	dir := t.TempDir()
	stack := mountStack(t, writeConfig(t, dir, configBodyPinned(dir, "v1", "[session, llm]")))
	reload(t, dir, stack, configBodyPinned(dir, "v2", "[session, llm]"))
	if got, _ := fiber.Get[string](stack.Layer(), "probe"); got != "v2" {
		t.Fatalf("unchanged pinned set must reload normally, got %q", got)
	}
}

func TestPinnedReorderReloadSucceeds(t *testing.T) {
	dir := t.TempDir()
	stack := mountStack(t, writeConfig(t, dir, configBodyPinned(dir, "v1", "[session, llm]")))
	// 顺序变化但集合相同，不算变更。
	reload(t, dir, stack, configBodyPinned(dir, "v2", "[llm, session]"))
	// 显式名单与默认名单集合相同，也不算变更。
	reload(t, dir, stack, configBody(dir, "v3"))
	if got, _ := fiber.Get[string](stack.Layer(), "probe"); got != "v3" {
		t.Fatalf("same pinned set must reload normally, got %q", got)
	}
}
