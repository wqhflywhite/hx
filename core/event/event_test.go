package event_test

import (
	"context"
	"strings"
	"testing"

	"hx/core/event"
	"hx/core/fiber"
)

func TestOnEmit(t *testing.T) {
	root := fiber.New()
	defer root.Dispose()
	ctx := root.Context()

	var got []string
	event.On[string](ctx, "test/on", func(_ *fiber.Context, p string) { got = append(got, "a:"+p) })
	event.On[string](ctx, "test/on", func(_ *fiber.Context, p string) { got = append(got, "b:"+p) })
	event.Emit[string](ctx, "test/on", "x")
	if strings.Join(got, ",") != "a:x,b:x" {
		t.Fatalf("got %v", got)
	}

	// 向未注册的事件名 Emit 是空操作。
	event.Emit[int](ctx, "test/nobody", 1)
}

func TestParallelJoinsPanics(t *testing.T) {
	root := fiber.New()
	defer root.Dispose()
	ctx := root.Context()

	event.On[string](ctx, "test/par", func(_ *fiber.Context, _ string) { panic("boom") })
	event.On[string](ctx, "test/par", func(_ *fiber.Context, _ string) { panic("crash") })
	err := event.Parallel[string](ctx, "test/par", "x")
	if err == nil || !strings.Contains(err.Error(), "boom") || !strings.Contains(err.Error(), "crash") {
		t.Fatalf("err = %v; want both panics joined", err)
	}
	if err := event.Parallel[string](ctx, "test/none", "x"); err != nil {
		t.Fatalf("err = %v; want nil", err)
	}
}

func TestSerialBail(t *testing.T) {
	root := fiber.New()
	defer root.Dispose()
	ctx := root.Context()

	event.OnSerial[int, string](ctx, "test/serial", func(_ *fiber.Context, p int) (string, bool) {
		if p > 1 {
			return "big", true
		}
		return "", false
	})
	runs := 0
	event.OnSerial[int, string](ctx, "test/serial", func(_ *fiber.Context, _ int) (string, bool) {
		runs++
		return "late", true
	})

	r, ok := event.Serial[int, string](ctx, "test/serial", 2)
	if !ok || r != "big" {
		t.Fatalf("Serial = %q, %v; want big, true", r, ok)
	}
	if runs != 0 {
		t.Fatal("listener after bail must not run")
	}
	r, ok = event.Serial[int, string](ctx, "test/serial", 0)
	if !ok || r != "late" {
		t.Fatalf("Serial = %q, %v; want late, true", r, ok)
	}

	event.OnSerial[int, string](ctx, "test/serial-none", func(_ *fiber.Context, _ int) (string, bool) {
		return "", false
	})
	if _, ok := event.Serial[int, string](ctx, "test/serial-none", 1); ok {
		t.Fatal("Serial with no bail should report false")
	}
}

func TestWaterfallOnionAndShortCircuit(t *testing.T) {
	root := fiber.New()
	defer root.Dispose()
	ctx := root.Context()

	event.OnWaterfall[string, string](ctx, "test/wf", func(_ context.Context, _ *fiber.Context, in string, next event.Next[string, string]) (string, error) {
		out, err := next(context.Background(), ctx2(ctx), "("+in)
		return out + ")", err
	})
	event.OnWaterfall[string, string](ctx, "test/wf", func(_ context.Context, _ *fiber.Context, in string, next event.Next[string, string]) (string, error) {
		out, err := next(context.Background(), ctx2(ctx), "["+in)
		return out + "]", err
	})
	out, err := event.Waterfall[string, string](ctx, context.Background(), "test/wf", "x", func(_ context.Context, _ *fiber.Context, in string) (string, error) {
		return in + ".", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if out != "[(x.])" {
		t.Fatalf("out = %q; want [(x.]) (prefixes on the way in, suffixes on the way out)", out)
	}

	// 跳过 next 的中间件会短路整条链。
	event.OnWaterfall[string, string](ctx, "test/wf2", func(_ context.Context, _ *fiber.Context, in string, _ event.Next[string, string]) (string, error) {
		return "cut:" + in, nil
	})
	terminalRan := false
	out, err = event.Waterfall[string, string](ctx, context.Background(), "test/wf2", "x", func(_ context.Context, _ *fiber.Context, in string) (string, error) {
		terminalRan = true
		return in, nil
	})
	if err != nil || out != "cut:x" || terminalRan {
		t.Fatalf("short-circuit broken: out=%q err=%v terminalRan=%v", out, err, terminalRan)
	}
}

// ctx2 是恒等函数；它的存在是为了让中间件签名保持真实调用形态。
func ctx2(c *fiber.Context) *fiber.Context { return c }

func TestTypeConflictPanics(t *testing.T) {
	root := fiber.New()
	defer root.Dispose()
	ctx := root.Context()

	event.On[string](ctx, "test/conflict", func(_ *fiber.Context, _ string) {})
	defer func() {
		if recover() == nil {
			t.Fatal("same-name different-type registration must panic")
		}
	}()
	event.On[int](ctx, "test/conflict", func(_ *fiber.Context, _ int) {})
}

func TestKindConflictPanics(t *testing.T) {
	root := fiber.New()
	defer root.Dispose()
	ctx := root.Context()

	event.On[string](ctx, "test/kind", func(_ *fiber.Context, _ string) {})
	defer func() {
		if recover() == nil {
			t.Fatal("same-name different-kind registration must panic")
		}
	}()
	event.OnWaterfall[string, string](ctx, "test/kind", func(_ context.Context, _ *fiber.Context, in string, next event.Next[string, string]) (string, error) {
		return next(context.Background(), ctx2(ctx), in)
	})
}

func TestListenerDisposeAndError(t *testing.T) {
	root := fiber.New()
	defer root.Dispose()
	ctx := root.Context()

	runs := 0
	d := event.On[string](ctx, "test/off", func(_ *fiber.Context, _ string) { runs++ })
	event.Emit[string](ctx, "test/off", "x")
	if err := d(); err != nil {
		t.Fatal(err)
	}
	event.Emit[string](ctx, "test/off", "x")
	if runs != 1 {
		t.Fatalf("runs = %d; want 1 after listener dispose", runs)
	}

	// fiber 的 dispose 会移除监听器，即使调用方丢掉了 disposer。
	event.On[string](ctx, "test/off2", func(_ *fiber.Context, _ string) { runs++ })
	child, err := root.New("sub")
	if err != nil {
		t.Fatal(err)
	}
	child.Activate()
	event.On[string](child.Context(), "test/off2", func(_ *fiber.Context, _ string) { runs += 10 })
	if err := child.Dispose(); err != nil {
		t.Fatal(err)
	}
	event.Emit[string](ctx, "test/off2", "x")
	if runs != 2 {
		t.Fatalf("runs = %d; want 2 after child fiber dispose", runs)
	}
}
