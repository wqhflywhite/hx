package memory

import (
	"strings"
	"testing"
)

func TestTinyBudgetKeepsCompleteReminderFrame(t *testing.T) {
	budget := len(frameOpen) + len(frameClose) + 12
	msg := render([]source{{path: "project", content: strings.Repeat("x", 1000)}}, budget)
	if len(msg) > budget || !strings.HasPrefix(msg, frameOpen+"\n") || !strings.HasSuffix(msg, "\n"+frameClose) {
		t.Fatalf("invalid frame for tiny budget: %q", msg)
	}
}
