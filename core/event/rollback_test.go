package event_test

import (
	"testing"

	"hx/core/event"
	"hx/core/fiber"
)

func TestListenerRegistrationFailureRollsBack(t *testing.T) {
	root := fiber.New()
	ctx := root.Context()
	if err := root.Dispose(); err != nil {
		t.Fatal(err)
	}
	runs := 0
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("registration on a disposed fiber must panic")
			}
		}()
		event.On[string](ctx, "test/rollback", func(_ *fiber.Context, _ string) { runs++ })
	}()
	event.Emit[string](ctx, "test/rollback", "x")
	if runs != 0 {
		t.Fatal("failed registration left a ghost listener")
	}
}
