// Package fiber 提供可撤销的副作用容器树。
package fiber

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// Disposer 撤销一个已注册的副作用；它必须是幂等的。
type Disposer func() error

// State 是 Fiber 的生命周期状态。
type State int

const (
	// StateLoading 表示 fiber 已存在但属主尚未完成装配。
	StateLoading State = iota
	// StateActive 表示 fiber 已装配完成，正在接受副作用。
	StateActive
	// StateDisposing 表示首个 Dispose 已开始，但清理尚未完成。
	StateDisposing
	// StateDisposed 表示 fiber 已 dispose 完成，拒绝新的副作用。
	StateDisposed
)

// String 返回状态名，用于诊断。
func (s State) String() string {
	switch s {
	case StateLoading:
		return "loading"
	case StateActive:
		return "active"
	case StateDisposing:
		return "disposing"
	case StateDisposed:
		return "disposed"
	}
	return "unknown"
}

// ErrDisposed 由已 dispose fiber 上的操作返回。
var ErrDisposed = errors.New("fiber: disposed")

type entry struct {
	value any
	typ   reflect.Type
}

// Fiber 是副作用树中的一个节点：它持有服务、disposer 和子 fiber，
// Dispose 会按逆序撤销它持有的一切。
type Fiber struct {
	parent      *Fiber
	root        *Fiber
	name        string
	services    map[string]entry
	revokes     map[string][]*revokeHook // 服务被移除时触发
	disposers   []Disposer
	children    map[*Fiber]struct{}
	state       State
	done        chan struct{}
	disposeDone chan struct{}
	disposeErr  error
	scope       *Fiber // 兄弟服务回退搜索的子树根

	// mu 保护整棵树（一律经由 root 加锁）；gen 和 cond 实现
	// 插件依赖等待所用的 provide 通知。
	mu   sync.Mutex
	gen  uint64
	cond *sync.Cond
}

// New 创建一个处于 active 状态的根 fiber。
func New() *Fiber {
	f := &Fiber{
		name:        "root",
		services:    map[string]entry{},
		revokes:     map[string][]*revokeHook{},
		children:    map[*Fiber]struct{}{},
		state:       StateActive,
		done:        make(chan struct{}),
		disposeDone: make(chan struct{}),
	}
	f.root = f
	f.cond = sync.NewCond(&f.mu)
	f.scope = f
	return f
}

// New 创建一个处于 loading 状态的子 fiber；父 fiber 的 Dispose 会级联
// dispose 子 fiber。父 fiber 已 dispose 时返回 ErrDisposed。
func (f *Fiber) New(name string) (*Fiber, error) {
	f.root.mu.Lock()
	defer f.root.mu.Unlock()
	if f.state >= StateDisposing {
		return nil, ErrDisposed
	}
	child := &Fiber{
		parent:      f,
		root:        f.root,
		name:        name,
		services:    map[string]entry{},
		revokes:     map[string][]*revokeHook{},
		children:    map[*Fiber]struct{}{},
		state:       StateLoading,
		done:        make(chan struct{}),
		disposeDone: make(chan struct{}),
		scope:       f.scope,
	}
	f.children[child] = struct{}{}
	return child, nil
}

func (f *Fiber) NewGeneration(name string) (*Fiber, error) {
	f.root.mu.Lock()
	defer f.root.mu.Unlock()
	if f.state >= StateDisposing {
		return nil, ErrDisposed
	}
	child := &Fiber{
		parent:      f,
		root:        f.root,
		name:        name,
		services:    map[string]entry{},
		revokes:     map[string][]*revokeHook{},
		children:    map[*Fiber]struct{}{},
		state:       StateLoading,
		done:        make(chan struct{}),
		disposeDone: make(chan struct{}),
	}
	child.scope = child
	f.children[child] = struct{}{}
	return child, nil
}

// Activate 把 fiber 从 loading 状态切换为 active。
func (f *Fiber) Activate() {
	f.root.mu.Lock()
	defer f.root.mu.Unlock()
	if f.state == StateLoading {
		f.state = StateActive
	}
}

// Context 返回指向该 fiber 的轻量句柄。
func (f *Fiber) Context() *Context { return &Context{fiber: f} }

// Parent 返回父 fiber，根 fiber 返回 nil。
func (f *Fiber) Parent() *Fiber { return f.parent }

// Name 返回 fiber 的名字。
func (f *Fiber) Name() string { return f.name }

// State 返回 fiber 的生命周期状态。
func (f *Fiber) State() State {
	f.root.mu.Lock()
	defer f.root.mu.Unlock()
	return f.state
}

// Done 返回一个在 fiber 被 dispose 时关闭的 channel；插件启动的
// goroutine 必须把生命周期挂在它上面。
func (f *Fiber) Done() <-chan struct{} { return f.done }

// Generation 返回全树共享的服务代际号，每次 provide 和 revoke
// 都会递增。
func (f *Fiber) Generation() uint64 {
	f.root.mu.Lock()
	defer f.root.mu.Unlock()
	return f.root.gen
}

// WaitGeneration 阻塞直到树的服务代际号与 gen 不同或 fiber 被
// dispose；返回值报告 fiber 是否仍然存活。
func (f *Fiber) WaitGeneration(gen uint64) bool {
	f.root.mu.Lock()
	defer f.root.mu.Unlock()
	for f.state < StateDisposing && f.root.gen == gen {
		f.root.cond.Wait()
	}
	return f.state < StateDisposing
}

// Dispose 撤销 fiber 持有的一切：先子 fiber（与构建顺序相反），
// 再按 LIFO 顺序执行自身的 disposer。并发调用者等待首个清理完成，
// 并获得同一份聚合错误；完成后的重复调用也返回该结果。
func (f *Fiber) Dispose() error {
	f.root.mu.Lock()
	if f.state == StateDisposed {
		err := f.disposeErr
		f.root.mu.Unlock()
		return err
	}
	if f.state == StateDisposing {
		done := f.disposeDone
		f.root.mu.Unlock()
		<-done
		f.root.mu.Lock()
		err := f.disposeErr
		f.root.mu.Unlock()
		return err
	}
	f.state = StateDisposing
	close(f.done)
	if f.parent != nil {
		delete(f.parent.children, f)
	}
	children := make([]*Fiber, 0, len(f.children))
	for child := range f.children {
		children = append(children, child)
	}
	f.children = map[*Fiber]struct{}{}
	disposers := f.disposers
	f.disposers = nil
	services := f.services
	f.services = map[string]entry{}
	hooks := f.revokes
	f.revokes = map[string][]*revokeHook{}
	f.root.cond.Broadcast()
	f.root.mu.Unlock()

	var errs []error
	for _, child := range children {
		if err := child.Dispose(); err != nil {
			errs = append(errs, err)
		}
	}
	for i := len(disposers) - 1; i >= 0; i-- {
		if err := disposers[i](); err != nil {
			errs = append(errs, err)
		}
	}
	for name := range services {
		for _, hook := range hooks[name] {
			hook.fn()
		}
	}
	f.root.mu.Lock()
	f.disposeErr = errors.Join(errs...)
	f.state = StateDisposed
	close(f.disposeDone)
	f.root.cond.Broadcast()
	err := f.disposeErr
	f.root.mu.Unlock()
	return err
}

// Context 是指向 fiber 的轻量句柄，到处传递。
type Context struct {
	fiber *Fiber
}

// Fiber 返回该 Context 所属的 fiber。
func (c *Context) Fiber() *Fiber { return c.fiber }

// Effect 是唯一的注册入口：fn 立即执行，其返回的 disposer
// 注册到 fiber 上，因此 Dispose 会撤销它。若 fn 返回前 fiber
// 已被 dispose，disposer 会立刻执行并返回
// ErrDisposed。
func (c *Context) Effect(fn func(*Context) (Disposer, error)) error {
	f := c.fiber
	f.root.mu.Lock()
	if f.state >= StateDisposing {
		f.root.mu.Unlock()
		return ErrDisposed
	}
	f.root.mu.Unlock()

	d, err := fn(c)
	if err != nil {
		return err
	}
	if d == nil {
		return nil
	}

	f.root.mu.Lock()
	defer f.root.mu.Unlock()
	if f.state >= StateDisposing {
		f.root.mu.Unlock()
		derr := d()
		f.root.mu.Lock()
		return errors.Join(ErrDisposed, derr)
	}
	f.disposers = append(f.disposers, d)
	return nil
}

// Provide 在 Context 所属 fiber 上注册服务 name，并返回一个
// 撤销它的 disposer。provide 会唤醒依赖等待者；revoke 会触发
// 撤销钩子（级联 dispose 依赖方）并唤醒等待者重新检查。对已
// dispose 的 fiber 会 panic。
func Provide[T any](c *Context, name string, svc T) Disposer {
	f := c.fiber
	f.root.mu.Lock()
	if f.state >= StateDisposing {
		f.root.mu.Unlock()
		panic(fmt.Errorf("fiber: provide %q on disposed fiber %q", name, f.name))
	}
	f.services[name] = entry{value: svc, typ: reflect.TypeFor[T]()}
	f.root.gen++
	f.root.cond.Broadcast()
	f.root.mu.Unlock()

	removed := false
	return func() error {
		f.root.mu.Lock()
		if removed {
			f.root.mu.Unlock()
			return nil
		}
		removed = true
		delete(f.services, name)
		hooks := f.revokes[name]
		delete(f.revokes, name)
		f.root.gen++
		f.root.cond.Broadcast()
		f.root.mu.Unlock()
		for _, hook := range hooks {
			hook.fn()
		}
		return nil
	}
}

// lookup 沿 fiber 链向上查找服务，找不到则回退到 scope 子树。
// 普通 fiber 的 scope 是树根，保持既有全树搜索；generation 的
// 子孙只搜索本代，再搜索外层 generation 的非 generation 部分，
// 使新 layer 能解析 pinned 插件提供的服务，却看不到旧 layer。
func (f *Fiber) lookup(name string) (entry, *Fiber, bool) {
	for cur := f; cur != nil; cur = cur.parent {
		if e, ok := cur.services[name]; ok {
			return e, cur, true
		}
	}
	if e, owner, ok := f.scope.lookupDown(name); ok {
		return e, owner, true
	}
	for cur := f.parent; cur != nil; cur = cur.parent {
		if cur.scope == cur {
			if e, owner, ok := cur.lookupDownLocal(name); ok {
				return e, owner, true
			}
		}
	}
	return entry{}, nil, false
}

// lookupDown 在 f 的完整子树中查找 name，先查自身服务。
func (f *Fiber) lookupDown(name string) (entry, *Fiber, bool) {
	if e, ok := f.services[name]; ok {
		return e, f, true
	}
	for child := range f.children {
		if e, owner, ok := child.lookupDown(name); ok {
			return e, owner, true
		}
	}
	return entry{}, nil, false
}

// lookupDownLocal 查找 generation 的 pinned 区域，遇到嵌套
// generation 即停止，避免把旧 layer 的服务暴露给新 layer。
func (f *Fiber) lookupDownLocal(name string) (entry, *Fiber, bool) {
	if e, ok := f.services[name]; ok {
		return e, f, true
	}
	for child := range f.children {
		if child.scope == child {
			continue
		}
		if e, owner, ok := child.lookupDownLocal(name); ok {
			return e, owner, true
		}
	}
	return entry{}, nil, false
}

// Get 沿 fiber 链向上解析服务 name 并把它断言为 T。
// 第二个返回值报告服务是否存在且类型
// 匹配。
func Get[T any](c *Context, name string) (T, bool) {
	var zero T
	f := c.fiber
	f.root.mu.Lock()
	defer f.root.mu.Unlock()
	e, _, ok := f.lookup(name)
	if !ok {
		return zero, false
	}
	v, ok := e.value.(T)
	if !ok {
		return zero, false
	}
	return v, true
}

// revokeHook 把撤销回调包装出稳定身份，便于移除。
type revokeHook struct {
	fn func()
}

// OnRevoked 注册 fn，当服务 name 从该 Context 变得不可解析时执行：
// 提供方 fiber 撤销它时触发；若此刻服务就不存在则立即执行。返回的
// disposer 用于注销该钩子。
func OnRevoked(c *Context, name string, fn func()) Disposer {
	f := c.fiber
	h := &revokeHook{fn: fn}
	f.root.mu.Lock()
	_, owner, ok := f.lookup(name)
	if !ok {
		f.root.mu.Unlock()
		fn()
		return func() error { return nil }
	}
	owner.revokes[name] = append(owner.revokes[name], h)
	f.root.mu.Unlock()

	removed := false
	return func() error {
		f.root.mu.Lock()
		defer f.root.mu.Unlock()
		if removed {
			return nil
		}
		removed = true
		hooks := owner.revokes[name]
		for i, candidate := range hooks {
			if candidate == h {
				owner.revokes[name] = append(hooks[:i], hooks[i+1:]...)
				break
			}
		}
		return nil
	}
}
