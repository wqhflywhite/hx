package session_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"hx/core/event"
	"hx/core/fiber"
	"hx/session"
)

func newHardeningManager(t *testing.T, dir string) *session.Manager {
	t.Helper()
	mgr, err := session.NewManager(dir, func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }, func() string { return "s-1" })
	if err != nil {
		t.Fatal(err)
	}
	return mgr
}

func closeSessionStore(t *testing.T, store session.Store) {
	t.Helper()
	t.Cleanup(func() { _ = store.(interface{ Close() error }).Close() })
}

func TestConcurrentResumeWritersMatchColdLog(t *testing.T) {
	dir := t.TempDir()
	mgr := newHardeningManager(t, dir)
	first, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	closeSessionStore(t, first)
	second, err := mgr.Resume(id)
	if err != nil {
		t.Fatal(err)
	}
	closeSessionStore(t, second)
	root := fiber.New()
	defer root.Dispose()
	ctx := root.Context()
	if err := first.Append(ctx, session.Event{Type: session.ToolResult, Data: session.MarshalData(session.ToolResultData{CallID: "c", Name: "bash", Content: "original"})}); err != nil {
		t.Fatal(err)
	}

	stores := []session.Store{first, second}
	start := make(chan struct{})
	errs := make(chan error, 128)
	var writers sync.WaitGroup
	for writer, store := range stores {
		writers.Add(1)
		go func() {
			defer writers.Done()
			<-start
			for i := range 40 {
				data := session.MarshalData(session.UserMessageData{Content: fmt.Sprintf("writer-%d-event-%d", writer, i)})
				if err := store.Append(ctx, session.Event{Type: session.UserMessage, Data: data}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	var reader sync.WaitGroup
	reader.Add(1)
	go func() {
		defer reader.Done()
		<-start
		for range 80 {
			if _, err := mgr.Messages(id); err != nil {
				errs <- err
				return
			}
			if _, err := mgr.Events(id); err != nil {
				errs <- err
				return
			}
		}
	}()
	close(start)
	writers.Wait()
	reader.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if err := first.Append(ctx, session.Event{Type: session.Prune, Data: session.MarshalData(session.PruneData{Seq: 0, Content: "pruned"})}); err != nil {
		t.Fatal(err)
	}

	hot, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	cold, err := newHardeningManager(t, dir).Events(id)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(hot, cold) {
		t.Fatalf("hot cache and cold log differ: hot=%d cold=%d", len(hot), len(cold))
	}
	if len(hot) != 82 {
		t.Fatalf("log has %d events; want 82 without duplicates", len(hot))
	}
	hotMessages, err := mgr.Messages(id)
	if err != nil {
		t.Fatal(err)
	}
	coldMessages, err := newHardeningManager(t, dir).Messages(id)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(hotMessages, coldMessages) {
		t.Fatal("incremental and cold projections differ")
	}
	if hotMessages[0].Content != "pruned" {
		t.Fatalf("prune changed the wrong tool result: %+v", hotMessages[0])
	}
}

func TestEventDataIsCopiedAtBoundaries(t *testing.T) {
	mgr := newHardeningManager(t, t.TempDir())
	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	closeSessionStore(t, store)
	root := fiber.New()
	defer root.Dispose()
	dispose := event.On[session.Event](root.Context(), "session/event", func(_ *fiber.Context, ev session.Event) {
		copy(ev.Data, json.RawMessage(`{"content":"XXXXXX"}`))
	})
	defer dispose()
	raw := json.RawMessage(`{"content":"before"}`)
	if err := store.Append(root.Context(), session.Event{Type: session.UserMessage, Data: raw}); err != nil {
		t.Fatal(err)
	}
	copy(raw, json.RawMessage(`{"content":"after!"}`))
	events, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	var data session.UserMessageData
	if err := json.Unmarshal(events[0].Data, &data); err != nil {
		t.Fatal(err)
	}
	if data.Content != "before" {
		t.Fatalf("cached event was mutated to %q", data.Content)
	}
	copy(events[0].Data, json.RawMessage(`{"content":"return"}`))
	fresh, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(fresh[0].Data, &data); err != nil {
		t.Fatal(err)
	}
	if data.Content != "before" {
		t.Fatalf("returned Events shares cached data: %q", data.Content)
	}
}

func TestEventLargerThanScannerLimitCanBeRecovered(t *testing.T) {
	dir := t.TempDir()
	mgr := newHardeningManager(t, dir)
	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	closeSessionStore(t, store)
	content := strings.Repeat("x", 16*1024*1024+1024)
	if err := store.Append(fiber.New().Context(), session.Event{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: content})}); err != nil {
		t.Fatal(err)
	}
	events, err := newHardeningManager(t, dir).Events(id)
	if err != nil {
		t.Fatal(err)
	}
	var data session.UserMessageData
	if err := json.Unmarshal(events[0].Data, &data); err != nil {
		t.Fatal(err)
	}
	if len(data.Content) != len(content) {
		t.Fatalf("recovered content has %d bytes; want %d", len(data.Content), len(content))
	}
}
