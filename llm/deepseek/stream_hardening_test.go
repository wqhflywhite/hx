package deepseek

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"hx/llm"
)

func collectStream(t *testing.T, ctx context.Context, input string) []llm.Chunk {
	t.Helper()
	var chunks []llm.Chunk
	consumeStream(ctx, strings.NewReader(input), func(chunk llm.Chunk) bool {
		chunks = append(chunks, chunk)
		return true
	})
	return chunks
}

func TestConsumeStreamRequiresDone(t *testing.T) {
	chunks := collectStream(t, context.Background(), "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
	if len(chunks) != 2 || !errors.Is(chunks[1].Err, io.ErrUnexpectedEOF) {
		t.Fatalf("chunks = %+v; want partial delta then unexpected EOF", chunks)
	}
}

func TestConsumeStreamSparseIndicesAreSorted(t *testing.T) {
	input := "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":4,\"id\":\"b\",\"function\":{\"name\":\"second\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":1,\"id\":\"a\",\"function\":{\"name\":\"first\"}}]}}]}\n\n" +
		"data: [DONE]\n\n"
	chunks := collectStream(t, context.Background(), input)
	if len(chunks) != 2 || chunks[0].ToolCall.ID != "a" || chunks[1].ToolCall.ID != "b" {
		t.Fatalf("sparse calls = %+v; want a,b without panic", chunks)
	}
}

func TestConsumeStreamReportsErrorFrame(t *testing.T) {
	chunks := collectStream(t, context.Background(), "data: {\"error\":{\"message\":\"quota exhausted\"}}\n\n")
	if len(chunks) != 1 || chunks[0].Err == nil || !strings.Contains(chunks[0].Err.Error(), "quota exhausted") {
		t.Fatalf("error frame chunks = %+v", chunks)
	}
}

func TestConsumeStreamReportsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	chunks := collectStream(t, ctx, "")
	if len(chunks) != 1 || !errors.Is(chunks[0].Err, context.Canceled) {
		t.Fatalf("cancellation chunks = %+v", chunks)
	}
}
