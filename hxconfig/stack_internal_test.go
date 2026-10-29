package hxconfig

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"

	"hx/core/fiber"
)

func TestWatchLoopReportsWatcherErrors(t *testing.T) {
	root := fiber.New()
	t.Cleanup(func() { _ = root.Dispose() })
	stack := &Stack{fiber: root}
	events := make(chan fsnotify.Event)
	errs := make(chan error)
	reported := make(chan error, 1)
	loopDone := make(chan struct{})
	go func() {
		stack.watchLoop(events, errs, "hx.yml", "hx.yml", func(err error) { reported <- err })
		close(loopDone)
	}()

	sentinel := errors.New("watcher failure")
	errs <- sentinel
	select {
	case err := <-reported:
		if !errors.Is(err, sentinel) || !strings.Contains(err.Error(), "hx.yml") {
			t.Fatalf("reported error = %v; want wrapped sentinel with path", err)
		}
	case <-time.After(time.Second):
		t.Fatal("watcher error was not reported")
	}
	close(events)
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("watch loop did not exit")
	}
}
