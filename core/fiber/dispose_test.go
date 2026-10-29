package fiber_test

import (
	"errors"
	"testing"
	"time"

	"hx/core/fiber"
)

func TestConcurrentDisposeWaitsForSameResult(t *testing.T) {
	root := fiber.New()
	started := make(chan struct{})
	release := make(chan struct{})
	sentinel := errors.New("cleanup failed")
	if err := root.Context().Effect(func(*fiber.Context) (fiber.Disposer, error) {
		return func() error {
			close(started)
			<-release
			return sentinel
		}, nil
	}); err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { first <- root.Dispose() }()
	<-started
	second := make(chan error, 1)
	go func() { second <- root.Dispose() }()
	select {
	case err := <-second:
		t.Fatalf("second Dispose returned before cleanup completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	for _, result := range []chan error{first, second} {
		select {
		case err := <-result:
			if !errors.Is(err, sentinel) {
				t.Fatalf("Dispose error = %v; want sentinel", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Dispose did not complete")
		}
	}
	if err := root.Dispose(); !errors.Is(err, sentinel) {
		t.Fatalf("repeated Dispose error = %v; want same sentinel", err)
	}
}
