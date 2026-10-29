package web_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hx/llm"
	"hx/web"
)

func TestPrompterConfirmHonorsContextCancellation(t *testing.T) {
	prompter := web.NewPrompter()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := prompter.Confirm(ctx, nil, llm.ToolCall{Name: "bash"})
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Confirm error = %v; want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Confirm did not exit on cancellation")
	}
	if pending := prompter.Pending(); len(pending) != 0 {
		t.Fatalf("canceled approval remains pending: %+v", pending)
	}
}
