package builtin

import (
	"context"
	"encoding/json"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestBashOutputIsBounded(t *testing.T) {
	command := "yes 1234567890 | head -n 5000"
	if runtime.GOOS == "windows" {
		command = "for /L %i in (1,1,5000) do @echo 1234567890"
	}
	args := json.RawMessage(`{"command":` + strconvQuote(command) + `}`)
	value, err := (Bash{}).Execute(context.Background(), nil, args)
	if err != nil {
		t.Fatal(err)
	}
	out := value.(string)
	if !strings.Contains(out, "truncated") || !strings.Contains(out, "bytes total") {
		t.Fatalf("output lacks truncation marker: %q", out[len(out)-min(len(out), 100):])
	}
	if len(out) > maxOutput+100 {
		t.Fatalf("output length = %d; want bounded prefix plus marker", len(out))
	}
}

func TestBashHonorsParentCancellation(t *testing.T) {
	command := "sleep 20"
	if runtime.GOOS == "windows" {
		command = "ping 127.0.0.1 -n 20 >nul"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	value, err := (Bash{}).Execute(ctx, nil, json.RawMessage(`{"command":`+strconvQuote(command)+`}`))
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("cancellation took %s", elapsed)
	}
	if !strings.Contains(value.(string), "killed") {
		t.Fatalf("cancellation output = %q; want killed marker", value)
	}
}

func strconvQuote(s string) string {
	raw, _ := json.Marshal(s)
	return string(raw)
}
