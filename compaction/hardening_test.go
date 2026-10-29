package compaction_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"hx/session"
)

func TestPruneRetriesAreIdempotent(t *testing.T) {
	ctx, compactor, mgr, _ := newCompactor(t, map[string]any{
		"max_context_tokens": 1, "keep_recent_events": 100, "prune_bytes": 100,
	})
	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer store.(interface{ Close() error }).Close()
	appendAll(t, ctx, store, []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "question"})},
		{Type: session.ToolResult, Data: session.MarshalData(session.ToolResultData{CallID: "c", Name: "bash", Content: strings.Repeat("x", 1000)})},
	})
	for range 2 {
		if _, err := compactor.MaybeCompact(ctx, context.Background(), store, id); err != nil {
			t.Fatal(err)
		}
	}
	events, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[2].Type != session.Prune {
		t.Fatalf("retry produced duplicate prune events: %+v", events)
	}
}

func TestSmallPruneBudgetSkipsInvalidPrune(t *testing.T) {
	ctx, compactor, mgr, _ := newCompactor(t, map[string]any{
		"max_context_tokens": 1, "keep_recent_events": 100, "prune_bytes": 1,
	})
	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer store.(interface{ Close() error }).Close()
	appendAll(t, ctx, store, []session.Event{
		{Type: session.ToolResult, Data: session.MarshalData(session.ToolResultData{CallID: "c", Name: "bash", Content: "ab"})},
	})
	done, err := compactor.MaybeCompact(ctx, context.Background(), store, id)
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Fatal("an ineffective prune was reported as compaction")
	}
	events, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("small budget appended an invalid prune: %+v", events)
	}
}

func TestPruneSurvivesSummaryFailureAndRetry(t *testing.T) {
	ctx, compactor, mgr, adapter := newCompactor(t, map[string]any{
		"max_context_tokens": 1, "keep_recent_events": 0, "keep_first_events": 0, "prune_bytes": 100,
	})
	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer store.(interface{ Close() error }).Close()
	appendAll(t, ctx, store, []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "question"})},
		{Type: session.ToolResult, Data: session.MarshalData(session.ToolResultData{CallID: "c", Name: "bash", Content: strings.Repeat("x", 1000)})},
	})
	sentinel := errors.New("summary failed")
	adapter.err = sentinel
	done, err := compactor.MaybeCompact(ctx, context.Background(), store, id)
	if !done || !errors.Is(err, sentinel) {
		t.Fatalf("MaybeCompact = %t, %v; want true, summary failure", done, err)
	}
	adapter.err = nil
	if _, err := compactor.MaybeCompact(ctx, context.Background(), store, id); err != nil {
		t.Fatal(err)
	}
	events, err := mgr.Events(id)
	if err != nil {
		t.Fatal(err)
	}
	prunes, summaries := 0, 0
	for _, ev := range events {
		switch ev.Type {
		case session.Prune:
			prunes++
		case session.Summary:
			summaries++
		}
	}
	if prunes != 1 || summaries != 1 {
		t.Fatalf("after retry prunes=%d summaries=%d; want one each", prunes, summaries)
	}
}

func TestSummaryTranscriptIncludesToolError(t *testing.T) {
	ctx, compactor, mgr, adapter := newCompactor(t, map[string]any{
		"max_context_tokens": 1, "keep_recent_events": 0, "keep_first_events": 0, "prune_bytes": 10000,
	})
	store, id, err := mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer store.(interface{ Close() error }).Close()
	appendAll(t, ctx, store, []session.Event{
		{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: "run the command"})},
		{Type: session.ToolResult, Data: session.MarshalData(session.ToolResultData{CallID: "c", Name: "bash", Content: "ordinary output", Error: "permission denied"})},
	})
	done, err := compactor.MaybeCompact(ctx, context.Background(), store, id)
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("over-budget tool error was not compacted")
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if len(adapter.reqs) != 1 {
		t.Fatalf("summary requests = %d; want 1", len(adapter.reqs))
	}
	transcript := adapter.reqs[0].Messages[1].Content
	if !strings.Contains(transcript, "permission denied") || strings.Contains(transcript, "ordinary output") {
		t.Fatalf("summary transcript = %q; want tool error", transcript)
	}
}
