// Package event 提供带四种分发模式的通用事件总线。
package event

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

	"hx/core/fiber"
)

// kind 标识一种分发模式，用于签名冲突检查。
type kind int

const (
	kindOn kind = iota
	kindSerial
	kindWaterfall
)

// sig 记录注册在某个事件名下的类型签名。
type sig struct {
	kind    kind
	payload reflect.Type
	result  reflect.Type
}

func (s sig) String() string {
	return fmt.Sprintf("kind=%d payload=%v result=%v", s.kind, s.payload, s.result)
}

type listener struct {
	fiber *fiber.Fiber
	fn    any
	dead  bool
}

var (
	mu        sync.RWMutex
	sigs      = map[string]sig{}
	listeners = map[string][]*listener{}
)

// checkSig 注册或校验事件名的签名；签名冲突时
// 立即 panic，而不是拖到分发时才失败。
func checkSig(name string, want sig) {
	mu.Lock()
	defer mu.Unlock()
	if got, ok := sigs[name]; ok {
		if got != want {
			panic(fmt.Errorf("event: type conflict on %q: registered %v, got %v", name, got, want))
		}
		return
	}
	sigs[name] = want
}

func addListener(c *fiber.Context, name string, fn any) fiber.Disposer {
	l := &listener{fiber: c.Fiber(), fn: fn}
	mu.Lock()
	listeners[name] = append(listeners[name], l)
	mu.Unlock()

	d := fiber.Disposer(func() error {
		mu.Lock()
		defer mu.Unlock()
		if l.dead {
			return nil
		}
		l.dead = true
		ls := listeners[name]
		for i, candidate := range ls {
			if candidate == l {
				listeners[name] = append(ls[:i], ls[i+1:]...)
				break
			}
		}
		return nil
	})
	// 把清理绑定到 fiber 生命周期，保证 Dispose 后不残留任何监听器。
	if err := c.Effect(func(*fiber.Context) (fiber.Disposer, error) { return d, nil }); err != nil {
		panic(fmt.Errorf("event: register listener on %q: %w", name, err))
	}
	return d
}

// collect 快照从 ctx 可见的 name 的监听器：注册在 ctx 的 fiber
// 或其任意祖先上的监听器，按注册顺序返回。
func collect(c *fiber.Context, name string) []*listener {
	mu.RLock()
	defer mu.RUnlock()
	var out []*listener
	for f := c.Fiber(); f != nil; f = f.Parent() {
		for _, l := range listeners[name] {
			if l.fiber == f && !l.dead {
				out = append(out, l)
			}
		}
	}
	return out
}

// On 把 fn 订阅到广播事件 name 并返回一个 disposer。载荷类型在注册时
// 记录；同名但类型冲突的注册会 panic。
func On[P any](c *fiber.Context, name string, fn func(*fiber.Context, P)) fiber.Disposer {
	checkSig(name, sig{kind: kindOn, payload: reflect.TypeFor[P]()})
	return addListener(c, name, func(cc *fiber.Context, p any) { fn(cc, p.(P)) })
}

// Emit 把 p 同步分发给从 ctx 可见的 name 的所有订阅者。
func Emit[P any](c *fiber.Context, name string, p P) {
	mu.RLock()
	s, ok := sigs[name]
	mu.RUnlock()
	if ok {
		if s.kind != kindOn || s.payload != reflect.TypeFor[P]() {
			panic(fmt.Errorf("event: emit %q with type %v conflicts with registered %v", name, reflect.TypeFor[P](), s))
		}
	}
	for _, l := range collect(c, name) {
		l.fn.(func(*fiber.Context, any))(c, p)
	}
}

// Parallel 把 p 并发扇出给 name 的所有订阅者，并把它们的 panic
// 合并成一个 error 返回，相当于不引入依赖的 errgroup。
func Parallel[P any](c *fiber.Context, name string, p P) error {
	mu.RLock()
	s, ok := sigs[name]
	mu.RUnlock()
	if ok {
		if s.kind != kindOn || s.payload != reflect.TypeFor[P]() {
			panic(fmt.Errorf("event: parallel %q conflicts with registered %v", name, s))
		}
	}
	ls := collect(c, name)
	var wg sync.WaitGroup
	var emu sync.Mutex
	var errs []error
	for _, l := range ls {
		wg.Add(1)
		go func(l *listener) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					emu.Lock()
					errs = append(errs, fmt.Errorf("event: parallel %q listener panicked: %v", name, r))
					emu.Unlock()
				}
			}()
			l.fn.(func(*fiber.Context, any))(c, p)
		}(l)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// OnSerial 把 fn 订阅到串行事件 name；fn 通过把第二个返回值
// 置为 true 来报告一个中断（bail）值。
func OnSerial[P, R any](c *fiber.Context, name string, fn func(*fiber.Context, P) (R, bool)) fiber.Disposer {
	checkSig(name, sig{kind: kindSerial, payload: reflect.TypeFor[P](), result: reflect.TypeFor[R]()})
	return addListener(c, name, func(cc *fiber.Context, p any) (any, bool) {
		r, ok := fn(cc, p.(P))
		return r, ok
	})
}

// Serial 按注册顺序运行 name 的串行监听器，遇到第一个中断值即停止；
// 第二个返回值报告是否有监听器中断。
func Serial[P, R any](c *fiber.Context, name string, p P) (R, bool) {
	var zero R
	mu.RLock()
	s, ok := sigs[name]
	mu.RUnlock()
	if ok {
		if s.kind != kindSerial || s.payload != reflect.TypeFor[P]() || s.result != reflect.TypeFor[R]() {
			panic(fmt.Errorf("event: serial %q conflicts with registered %v", name, s))
		}
	}
	for _, l := range collect(c, name) {
		r, bail := l.fn.(func(*fiber.Context, any) (any, bool))(c, p)
		if bail {
			return r.(R), true
		}
	}
	return zero, false
}

// Next 是在瀑布（waterfall）链中传递的延续函数。
type Next[I, O any] func(*fiber.Context, I) (O, error)

// OnWaterfall 把 mw 作为洋葱模型中间件订阅到 name；不调用 next 的
// 中间件会按设计短路整条链。
func OnWaterfall[I, O any](c *fiber.Context, name string, mw func(*fiber.Context, I, Next[I, O]) (O, error)) fiber.Disposer {
	checkSig(name, sig{kind: kindWaterfall, payload: reflect.TypeFor[I](), result: reflect.TypeFor[O]()})
	return addListener(c, name, mw)
}

// Waterfall 让 in 依次穿过 name 的洋葱链，最外层先执行（按注册
// 顺序），最终在 terminal 收尾。
func Waterfall[I, O any](c *fiber.Context, name string, in I, terminal Next[I, O]) (O, error) {
	mu.RLock()
	s, ok := sigs[name]
	mu.RUnlock()
	if ok {
		if s.kind != kindWaterfall || s.payload != reflect.TypeFor[I]() || s.result != reflect.TypeFor[O]() {
			panic(fmt.Errorf("event: waterfall %q conflicts with registered %v", name, s))
		}
	}
	ls := collect(c, name)
	next := terminal
	for i := len(ls) - 1; i >= 0; i-- {
		mw := ls[i].fn.(func(*fiber.Context, I, Next[I, O]) (O, error))
		inner := next
		next = func(cc *fiber.Context, v I) (O, error) { return mw(cc, v, inner) }
	}
	return next(c, in)
}
