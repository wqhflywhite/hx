package kb

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"hx/core/fiber"
)

// watch 在 vault 上启动一个递归的 fsnotify watcher：变更事件先
// 收集起来，静默防抖期满后被触碰的笔记重新索引（已删除的则
// 从索引中清除）。fsnotify 本身不递归，所以每个子目录都要
// 注册，运行期新建的目录也要动态加入。goroutine 随 fiber
// 结束，返回的卸载器（disposer）负责关闭 watcher。goroutine
// 内部的索引错误直接丢弃：索引是可丢弃的衍生物，下一个事件
// 或启动时的 Sync 会自愈。
func watch(c *fiber.Context, svc *Service, root string, debounce time.Duration) (fiber.Disposer, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	err = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return nil
		}
		if p != root && strings.HasPrefix(d.Name(), ".") {
			return filepath.SkipDir
		}
		return w.Add(p)
	})
	if err != nil {
		_ = w.Close()
		return nil, err
	}

	done := c.Fiber().Done()
	pending := map[string]bool{}
	var timer *time.Timer
	var timerC <-chan time.Time
	// fsnotify 的 Windows 后端在并发 Close 时会死锁；watcher
	// goroutine 和 disposer 各自的路径都会关闭它，所以用
	// Once 收口。
	var closeOnce sync.Once
	closeW := func() { closeOnce.Do(func() { _ = w.Close() }) }
	go func() {
		flush := func() {
			paths := pending
			pending = map[string]bool{}
			for p := range paths {
				_ = svc.SyncFile(p)
			}
		}
		for {
			select {
			case <-done:
				closeW()
				return
			case ev, ok := <-w.Events:
				if !ok {
					return
				}
				if ev.Op&fsnotify.Create != 0 {
					if info, err := os.Stat(ev.Name); err == nil && info.IsDir() {
						if !strings.HasPrefix(info.Name(), ".") {
							_ = w.Add(ev.Name)
						}
						continue
					}
				}
				if ev.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename) == 0 {
					continue
				}
				if !strings.HasSuffix(ev.Name, ".md") {
					continue
				}
				pending[ev.Name] = true
				if timer != nil {
					timer.Stop()
				}
				timer = time.NewTimer(debounce)
				timerC = timer.C
			case _, ok := <-w.Errors:
				if !ok {
					return
				}
			case <-timerC:
				timer, timerC = nil, nil
				flush()
			}
		}
	}()
	return func() error {
		closeW()
		return nil
	}, nil
}
