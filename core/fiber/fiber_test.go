package fiber_test

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"hx/core/fiber"
)

// settleGoroutines 等调度器稳定后返回 goroutine 数量。
func settleGoroutines() int {
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	return runtime.NumGoroutine()
}

func TestProvideGetDisposeClean(t *testing.T) {
	before := settleGoroutines()

	root := fiber.New()
	ctx := root.Context()

	child, err := root.New("child")
	if err != nil {
		t.Fatal(err)
	}
	child.Activate()
	cctx := child.Context()

	disposeSvc := fiber.Provide[string](ctx, "greeting", "hello")
	if got, ok := fiber.Get[string](cctx, "greeting"); !ok || got != "hello" {
		t.Fatalf("Get = %q, %v; want hello, true", got, ok)
	}
	// 子 fiber 的服务沿自己的链遮蔽父 fiber 的服务。
	disposeChildSvc := fiber.Provide[string](cctx, "greeting", "hi")
	if got, _ := fiber.Get[string](cctx, "greeting"); got != "hi" {
		t.Fatalf("shadowed Get = %q; want hi", got)
	}
	if got, _ := fiber.Get[string](ctx, "greeting"); got != "hello" {
		t.Fatalf("parent Get = %q; want hello", got)
	}

	// Effect 的 disposer 按 LIFO 顺序执行。
	var order []int
	for i := range 3 {
		i := i
		if err := ctx.Effect(func(*fiber.Context) (fiber.Disposer, error) {
			return func() error { order = append(order, i); return nil }, nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	if err := root.Dispose(); err != nil {
		t.Fatal(err)
	}
	if len(order) != 3 || order[0] != 2 || order[2] != 0 {
		t.Fatalf("LIFO order = %v; want [2 1 0]", order)
	}
	if _, ok := fiber.Get[string](ctx, "greeting"); ok {
		t.Fatal("service still resolvable after dispose")
	}
	if child.State() != fiber.StateDisposed {
		t.Fatalf("child state = %v; want disposed (cascade)", child.State())
	}
	select {
	case <-child.Done():
	default:
		t.Fatal("child done channel not closed")
	}

	// Dispose 是幂等的；遗留的 disposer 是无害的空操作。
	if err := root.Dispose(); err != nil {
		t.Fatal(err)
	}
	_ = disposeSvc()
	_ = disposeChildSvc()

	if err := ctx.Effect(func(*fiber.Context) (fiber.Disposer, error) {
		return func() error { return nil }, nil
	}); !errors.Is(err, fiber.ErrDisposed) {
		t.Fatalf("Effect on disposed fiber = %v; want ErrDisposed", err)
	}

	if after := settleGoroutines(); after > before {
		t.Fatalf("goroutine leak: before=%d after=%d", before, after)
	}
}

func TestRevokeFiresHooks(t *testing.T) {
	root := fiber.New()
	ctx := root.Context()

	revoked := make(chan struct{}, 1)
	unhook := fiber.OnRevoked(ctx, "svc", func() { revoked <- struct{}{} })
	release := fiber.Provide[int](ctx, "svc", 42)
	if err := release(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-revoked:
	case <-time.After(time.Second):
		t.Fatal("revocation hook did not fire")
	}
	_ = unhook()
	if err := root.Dispose(); err != nil {
		t.Fatal(err)
	}
}

func TestOnRevokedAbsentFiresImmediately(t *testing.T) {
	root := fiber.New()
	ctx := root.Context()
	fired := false
	fiber.OnRevoked(ctx, "missing", func() { fired = true })
	if !fired {
		t.Fatal("hook for absent service did not fire immediately")
	}
	_ = root.Dispose()
}

func TestWaitGeneration(t *testing.T) {
	root := fiber.New()
	ctx := root.Context()
	gen := root.Generation()

	woke := make(chan bool, 1)
	go func() { woke <- root.WaitGeneration(gen) }()
	time.Sleep(20 * time.Millisecond)
	release := fiber.Provide[int](ctx, "svc", 1)
	select {
	case alive := <-woke:
		if !alive {
			t.Fatal("waiter reported disposed")
		}
	case <-time.After(time.Second):
		t.Fatal("waiter not woken by provide")
	}

	// dispose 整棵树会唤醒等待者并报告已 dispose。
	gen = root.Generation()
	done := make(chan bool, 1)
	go func() { done <- root.WaitGeneration(gen) }()
	time.Sleep(20 * time.Millisecond)
	_ = release()
	_ = root.Dispose()
	select {
	case alive := <-done:
		if alive {
			t.Fatal("waiter should report disposal")
		}
	case <-time.After(time.Second):
		t.Fatal("waiter not woken by dispose")
	}
}
