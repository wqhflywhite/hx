package hxconfig

import (
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"hx/core/fiber"
	"hx/core/plugin"
)

// Overlays 把程序内生成的配置值（比如 TUI 的 Prompter）注入文件声明的
// 条目，按条目 id 索引，在挂载时做浅合并。
type Overlays map[string]map[string]any

// Stack 是已挂载的配置：一个 base fiber 承载 pinned（底座层）插件，
// 一个可热重挂的 layer fiber 承载它们之上的所有插件。
type Stack struct {
	fiber    *fiber.Fiber
	base     *fiber.Fiber
	layer    *fiber.Fiber
	overlays Overlays
	pinned   []string
	mu       sync.RWMutex
}

// MountStack 把 f 以两层栈的形式挂载到 root 上。
func MountStack(root *fiber.Context, f *File, overlays Overlays) (*Stack, error) {
	fb, err := root.Fiber().New("hxconfig")
	if err != nil {
		return nil, err
	}
	s := &Stack{fiber: fb, overlays: overlays, pinned: f.PinnedSet()}
	base, err := s.mountLayer(s.fiber, "base", f, true)
	if err != nil {
		_ = fb.Dispose()
		return nil, err
	}
	s.base = base
	layer, err := s.mountLayer(s.base, "layer", f, false)
	if err != nil {
		_ = fb.Dispose()
		return nil, err
	}
	s.layer = layer
	return s, nil
}

// mountLayer 把 f 中 pinned（或可热重挂）的条目挂载到栈的一个新建
// 子 fiber 上。
func (s *Stack) mountLayer(parent *fiber.Fiber, name string, f *File, pinnedLayer bool) (*fiber.Fiber, error) {
	fb, err := parent.NewGeneration(name)
	if err != nil {
		return nil, err
	}

	for _, e := range f.Entries() {
		if f.IsPinned(e.Name) != pinnedLayer {
			continue
		}

		cfg := e.Config
		if overlay, ok := s.overlays[e.ID]; ok {
			merged := make(map[string]any, len(cfg)+len(overlay))
			for k, v := range cfg {
				merged[k] = v
			}
			for k, v := range overlay {
				merged[k] = v
			}
			cfg = merged
		}

		if err := plugin.Mount(fb.Context(), e.Name, cfg); err != nil {
			_ = fb.Dispose()
			return nil, fmt.Errorf("hxconfig: mount entry %q (%s): %w", e.ID, e.Name, err)
		}
	}
	fb.Activate()
	return fb, nil
}

// Layer 返回当前可热重挂层的 context。需要让 agent 创建与发布保持在
// 同一代的调用方应使用 WithLayer，而不是在解锁后继续使用此句柄。
func (s *Stack) Layer() *fiber.Context {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.layer.Context()
}

// WithLayer 在读租约内执行 fn；回调返回前 Reload 和 Dispose 不会
// 撤销该代 layer。
func (s *Stack) WithLayer(fn func(*fiber.Context) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fn(s.layer.Context())
}

// ReloadError 报告新 layer 已发布、但旧 layer 清理失败的状态。
// Committed 为 true 时调用方必须改用新 layer，不能继续保留旧 agent。
type ReloadError struct {
	Committed bool
	Err       error
}

func (e *ReloadError) Error() string {
	return fmt.Sprintf("hxconfig: reload committed=%t; old layer cleanup: %v", e.Committed, e.Err)
}

func (e *ReloadError) Unwrap() error { return e.Err }

// Reload 把可热重挂层换成按 f 挂载的新层：先构建新层，成功后才会
// 替换旧层，所以配置出错时在跑的层保持不变。旧层的所有
// disposer（卸载器）都会执行。
// pinned 集合不支持热改：base 层只在启动时挂载一次，改 pinned 名单会让
// 新 pinned 的插件两头都不挂、被取消 pinned 的插件挂两份，因此 Reload
// 在任何重建动作前响亮拒绝，重启进程后才生效。
func (s *Stack) Reload(f *File) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if next := f.PinnedSet(); !slices.Equal(s.pinned, next) {
		return fmt.Errorf("hxconfig: pinned set changed (old %v, new %v); reload rejected, restart required", s.pinned, next)
	}
	old := s.layer
	next, err := s.mountLayer(s.base, "layer", f, false)
	if err != nil {
		return err
	}
	s.layer = next
	if err := old.Dispose(); err != nil {
		return &ReloadError{Committed: true, Err: err}
	}
	return nil
}

// Dispose 撤销（dispose）整个栈，包括 base 层。
func (s *Stack) Dispose() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fiber.Dispose()
}

// debounce 把热重挂延迟到写入方消停下来；编辑器一次保存常常发出
// 一串事件。
const debounce = 150 * time.Millisecond

// Watch 在 path 变化时热重挂整个栈。它一直运行到栈被 dispose
// 为止；加载与 watcher 错误通过 onReload 上报，并且让正在运行的
// 层保持不动。
func (s *Stack) Watch(path string, onReload func(error)) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	name := filepath.Base(path)
	if err := watcher.Add(dir); err != nil {
		_ = watcher.Close()
		return err
	}
	return s.fiber.Context().Effect(func(*fiber.Context) (fiber.Disposer, error) {
		go s.watchLoop(watcher.Events, watcher.Errors, path, name, onReload)
		return watcher.Close, nil
	})
}

// watchLoop 排空 fsnotify 事件，在每次防抖后的变化触发热重挂。
func (s *Stack) watchLoop(events <-chan fsnotify.Event, errs <-chan error, path, name string, onReload func(error)) {
	var timer *time.Timer
	var fired <-chan time.Time
	for {
		select {
		case <-s.fiber.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			if filepath.Base(ev.Name) != name {
				continue
			}
			if timer == nil {
				timer = time.NewTimer(debounce)
				fired = timer.C
			} else {
				timer.Reset(debounce)
			}
		case err, ok := <-errs:
			if !ok {
				return
			}
			if err != nil {
				onReload(fmt.Errorf("hxconfig: watch %s: %w", path, err))
			}
		case <-fired:
			fired = nil
			f, err := Load(path)
			if err == nil {
				err = s.Reload(f)
			}
			onReload(err)
		}
	}
}
