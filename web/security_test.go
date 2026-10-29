package web_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hx/core/fiber"
	"hx/llm"
	"hx/web"
)

func TestAuthenticationContentTypeAndOrigin(t *testing.T) {
	prompter := web.NewPrompter()
	fake := newFakeHarness()
	opts := fake.options(prompter)
	opts.AuthToken = "secret"
	srv, err := web.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	defer srv.Close()

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized index status = %d", resp.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/events", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized SSE status = %d", resp.StatusCode)
	}

	post := func(origin, contentType string) *http.Response {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/send", strings.NewReader(`{"text":"hello"}`))
		req.Header.Set("X-HX-Token", "secret")
		req.Header.Set("Content-Type", contentType)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}
	if resp := post(ts.URL, "text/plain"); resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("wrong Content-Type status = %d", resp.StatusCode)
	}
	if resp := post("https://attacker.example", "application/json"); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin status = %d", resp.StatusCode)
	}
	if resp := post(ts.URL, "application/json"); resp.StatusCode != http.StatusOK {
		t.Fatalf("same-origin status = %d", resp.StatusCode)
	}
}

func TestRemoteListenRequiresOptInAndToken(t *testing.T) {
	for _, tc := range []struct {
		allow bool
		token string
		want  string
	}{
		{false, "", "remote opt-in"},
		{true, "", "auth token"},
	} {
		prompter := web.NewPrompter()
		opts := newFakeHarness().options(prompter)
		opts.AllowRemote = tc.allow
		opts.AuthToken = tc.token
		srv, err := web.NewServer(opts)
		if err != nil {
			t.Fatal(err)
		}
		root := fiber.New()
		err = srv.Run(root.Context(), "0.0.0.0:0")
		_ = root.Dispose()
		srv.Close()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("Run remote allow=%t token=%q error = %v; want %q", tc.allow, tc.token, err, tc.want)
		}
	}
}

func TestSessionChangeInvalidatesOldApproval(t *testing.T) {
	_, prompter, _, base := setup(t)
	confirmed := make(chan bool, 1)
	go func() {
		allow, _ := prompter.Confirm(context.Background(), nil, llm.ToolCall{Name: "bash"})
		confirmed <- allow
	}()
	var id string
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		pending := prompter.Pending()
		if len(pending) == 1 {
			id = pending[0].ID
			break
		}
		time.Sleep(time.Millisecond)
	}
	if id == "" {
		t.Fatal("approval did not become pending")
	}
	resp := postJSON(t, base, "/api/session/new", `{}`)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("session change status = %d", resp.StatusCode)
	}
	select {
	case allow := <-confirmed:
		if allow {
			t.Fatal("invalidated approval was allowed")
		}
	case <-time.After(time.Second):
		t.Fatal("invalidated approval remained blocked")
	}
	resp = postJSON(t, base, "/api/approval", fmt.Sprintf(`{"id":%q,"allow":true}`, id))
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("old approval answer status = %d; want 404", resp.StatusCode)
	}
}
