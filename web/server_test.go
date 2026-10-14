package web_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"hx/core/event"
	"hx/core/fiber"
	"hx/llm"
	"hx/session"
	"hx/web"
)

// sseEnvelope 是一条 SSE 消息的客户端侧解码结果。
type sseEnvelope struct {
	Kind      string         `json:"kind"`
	Event     *session.Event `json:"event"`
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Arguments string         `json:"arguments"`
	Text      string         `json:"text"`
}

// fakeHarness 基于内存状态实现所有 Options 回调。
type fakeHarness struct {
	mu        sync.Mutex
	sent      []string
	sessions  []string
	current   string
	events    map[string][]session.Event
	resumeErr error
}

func newFakeHarness() *fakeHarness {
	return &fakeHarness{current: "s1", sessions: []string{"s1"}, events: map[string][]session.Event{"s1": nil}}
}

func (f *fakeHarness) options(p *web.Prompter) web.Options {
	return web.Options{
		Prompter: p,
		Send: func(text string) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.sent = append(f.sent, text)
		},
		CurrentSession: func() string {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.current
		},
		LoadEvents: func(id string) ([]session.Event, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			events, ok := f.events[id]
			if !ok {
				return nil, fmt.Errorf("unknown session %q", id)
			}
			return events, nil
		},
		ListSessions: func() ([]string, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return append([]string(nil), f.sessions...), nil
		},
		NewSession: func() (string, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			id := fmt.Sprintf("s%d", len(f.sessions)+1)
			f.sessions = append(f.sessions, id)
			f.events[id] = nil
			f.current = id
			return id, nil
		},
		ResumeSession: func(id string) (string, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.resumeErr != nil {
				return "", f.resumeErr
			}
			if _, ok := f.events[id]; !ok {
				return "", fmt.Errorf("unknown session %q", id)
			}
			f.current = id
			return id, nil
		},
	}
}

func (f *fakeHarness) sentTexts() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.sent...)
}

// setup 在 fake harness 上构建一个 server 并真实启动服务，使
// SSE 测试走真实连接。
func setup(t *testing.T) (*web.Server, *web.Prompter, *fakeHarness, string) {
	t.Helper()
	prompter := web.NewPrompter()
	fake := newFakeHarness()
	srv, err := web.NewServer(fake.options(prompter))
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ts.Close()
		srv.Close()
	})
	return srv, prompter, fake, ts.URL
}

// sseConn 是测试用 SSE 客户端，从活跃连接上解码信封。
type sseConn struct {
	cancel context.CancelFunc
	envs   chan sseEnvelope
}

func openSSE(t *testing.T, baseURL, sessionID string) *sseConn {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/events?session="+sessionID, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		cancel()
		t.Fatalf("Content-Type = %q; want text/event-stream", ct)
	}
	c := &sseConn{cancel: cancel, envs: make(chan sseEnvelope, 4096)}
	t.Cleanup(cancel)
	go c.read(resp.Body)
	return c
}

func (c *sseConn) read(body io.Reader) {
	defer close(c.envs)
	r := bufio.NewReader(body)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		data, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}
		var env sseEnvelope
		if json.Unmarshal([]byte(data), &env) == nil {
			c.envs <- env
		}
	}
}

// next 返回下一个信封，超过宽松的超时时间则判失败。
func (c *sseConn) next(t *testing.T) sseEnvelope {
	t.Helper()
	select {
	case env, ok := <-c.envs:
		if !ok {
			t.Fatal("SSE stream closed unexpectedly")
		}
		return env
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for SSE envelope")
		return sseEnvelope{}
	}
}

func postJSON(t *testing.T, baseURL, path, body string) *http.Response {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	resp, err := http.Post(baseURL+path, "application/json", r)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func userEvent(text string) session.Event {
	return session.Event{Type: session.UserMessage, Data: session.MarshalData(session.UserMessageData{Content: text})}
}

func TestIndexServesEmbeddedHTML(t *testing.T) {
	_, _, _, base := setup(t)
	resp, err := http.Get(base + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d; want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("Content-Type = %q; want text/html", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "<title>hx</title>") {
		t.Fatal("embedded index.html not served")
	}
}

func TestSessionsEndpoint(t *testing.T) {
	_, _, fake, base := setup(t)
	resp, err := http.Get(base + "/api/sessions")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Sessions []string `json:"sessions"`
		Current  string   `json:"current"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Sessions) != 1 || body.Sessions[0] != fake.sessions[0] || body.Current != "s1" {
		t.Fatalf("body = %+v; want sessions=[s1] current=s1", body)
	}
}

func TestSendValidation(t *testing.T) {
	_, _, fake, base := setup(t)
	for name, body := range map[string]string{
		"empty body":    "",
		"not json":      "nope",
		"empty text":    `{"text":""}`,
		"blank text":    `{"text":"  "}`,
		"unknown field": `{"text":"hi","bogus":1}`,
	} {
		resp := postJSON(t, base, "/api/send", body)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s: status = %d; want 400", name, resp.StatusCode)
		}
		resp.Body.Close()
	}
	resp := postJSON(t, base, "/api/send", `{"text":"hello"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("valid send: status = %d; want 200", resp.StatusCode)
	}
	resp.Body.Close()
	if got := fake.sentTexts(); len(got) != 1 || got[0] != "hello" {
		t.Fatalf("sent = %v; want [hello]", got)
	}
}

func TestApprovalValidation(t *testing.T) {
	_, _, _, base := setup(t)
	cases := []struct {
		name string
		body string
		want int
	}{
		{"bad json", "nope", http.StatusBadRequest},
		{"missing id", `{"allow":true}`, http.StatusBadRequest},
		{"unknown id", `{"id":"a99","allow":true}`, http.StatusNotFound},
	}
	for _, tc := range cases {
		resp := postJSON(t, base, "/api/approval", tc.body)
		if resp.StatusCode != tc.want {
			t.Fatalf("%s: status = %d; want %d", tc.name, resp.StatusCode, tc.want)
		}
		resp.Body.Close()
	}
}

// TestApprovalRoundTrip 驱动完整回路：agent 协程阻塞在 Confirm
// 中，浏览器看到 SSE 信封并 POST 作答，Confirm 带着决定解除阻
// 塞。
func TestApprovalRoundTrip(t *testing.T) {
	_, prompter, _, base := setup(t)
	client := openSSE(t, base, "s1")

	call := llm.ToolCall{ID: "call_1", Name: "bash", Arguments: `{"command":"ls"}`}
	type outcome struct {
		allow bool
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		allow, err := prompter.Confirm(nil, call)
		done <- outcome{allow, err}
	}()

	env := client.next(t)
	if env.Kind != "approval" || env.Name != "bash" || env.Arguments != call.Arguments || env.ID == "" {
		t.Fatalf("approval envelope = %+v", env)
	}
	resp := postJSON(t, base, "/api/approval", fmt.Sprintf(`{"id":%q,"allow":true}`, env.ID))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("answer: status = %d; want 200", resp.StatusCode)
	}
	resp.Body.Close()
	select {
	case o := <-done:
		if o.err != nil || !o.allow {
			t.Fatalf("Confirm = (%v, %v); want (true, nil)", o.allow, o.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Confirm did not unblock")
	}
	// 已作答的审批必须不再是待处理状态。
	resp = postJSON(t, base, "/api/approval", fmt.Sprintf(`{"id":%q,"allow":false}`, env.ID))
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("re-answer: status = %d; want 404", resp.StatusCode)
	}
	resp.Body.Close()
}

// TestApprovalBackfillForLateJoin：在提问仍处于开放状态时连接
// 的客户端仍然能收到审批信封。
func TestApprovalBackfillForLateJoin(t *testing.T) {
	_, prompter, _, base := setup(t)
	done := make(chan struct{})
	defer close(done)
	go func() {
		_, _ = prompter.Confirm(nil, llm.ToolCall{ID: "c1", Name: "edit", Arguments: "{}"})
	}()
	// 等待提问注册完成。
	deadline := time.Now().Add(10 * time.Second)
	for len(prompter.Pending()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("approval never became pending")
		}
		time.Sleep(time.Millisecond)
	}
	client := openSSE(t, base, "s1")
	env := client.next(t)
	if env.Kind != "approval" || env.Name != "edit" {
		t.Fatalf("late-join envelope = %+v; want approval for edit", env)
	}
}

// TestPrompterCloseDeniesPending：关闭 server 会让每个阻塞中的
// Confirm 以拒绝结束。
func TestPrompterCloseDeniesPending(t *testing.T) {
	prompter := web.NewPrompter()
	fake := newFakeHarness()
	srv, err := web.NewServer(fake.options(prompter))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan bool, 1)
	go func() {
		allow, _ := prompter.Confirm(nil, llm.ToolCall{Name: "bash"})
		done <- allow
	}()
	deadline := time.Now().Add(10 * time.Second)
	for len(prompter.Pending()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("approval never became pending")
		}
		time.Sleep(time.Millisecond)
	}
	srv.Close()
	select {
	case allow := <-done:
		if allow {
			t.Fatal("close must deny the pending approval")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Confirm did not unblock on Close")
	}
	if allow, err := prompter.Confirm(nil, llm.ToolCall{Name: "bash"}); err != nil || allow {
		t.Fatalf("Confirm after Close = (%v, %v); want (false, nil)", allow, err)
	}
}

// TestBackfillLiveNoGapNoDup 在客户端订阅期间用并发发布者猛轰
// hub；每个事件必须恰好到达一次。
func TestBackfillLiveNoGapNoDup(t *testing.T) {
	srv, _, _, base := setup(t)
	root := fiber.New()
	defer root.Dispose()
	dispose := srv.SubscribeEvents(root.Context())
	defer func() { _ = dispose() }()

	// 回填种子：事件 e0..e49 已在日志副本中。
	seed := make([]session.Event, 50)
	for i := range seed {
		seed[i] = userEvent(fmt.Sprintf("e%d", i))
	}
	srv.SetSession("s1", seed)

	// 发布者与订阅者竞速；按协程划分的偏移保证标记唯一。
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				event.Emit[session.Event](root.Context(), "session/event", userEvent(fmt.Sprintf("e%d", 50+g*50+i)))
			}
		}(g)
	}
	client := openSSE(t, base, "s1")
	wg.Wait()

	seen := map[string]int{}
	for range 250 {
		env := client.next(t)
		if env.Kind != "session" || env.Event == nil {
			t.Fatalf("envelope = %+v; want session event", env)
		}
		var d session.UserMessageData
		if err := json.Unmarshal(env.Event.Data, &d); err != nil {
			t.Fatal(err)
		}
		seen[d.Content]++
	}
	for i := 0; i < 250; i++ {
		marker := fmt.Sprintf("e%d", i)
		if seen[marker] != 1 {
			t.Fatalf("event %q seen %d times; want exactly 1", marker, seen[marker])
		}
	}
}

func TestNotice(t *testing.T) {
	srv, _, _, base := setup(t)
	client := openSSE(t, base, "s1")
	srv.Notify("config reloaded")
	env := client.next(t)
	if env.Kind != "notice" || env.Text != "config reloaded" {
		t.Fatalf("envelope = %+v; want notice", env)
	}
}

func TestSessionNewAndResume(t *testing.T) {
	srv, _, fake, base := setup(t)
	fake.events["s1"] = []session.Event{userEvent("old turn")}
	srv.SetSession("s1", fake.events["s1"])
	client := openSSE(t, base, "s1")
	if env := client.next(t); env.Kind != "session" {
		t.Fatalf("backfill = %+v; want the seeded event", env)
	}

	// 新会话：响应携带 id，客户端收到 session-changed，下一个
	// 订阅者回填新的（空）日志。
	resp := postJSON(t, base, "/api/session/new", `{}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("new: status = %d; want 200", resp.StatusCode)
	}
	var newBody struct {
		ID string `json:"id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&newBody)
	resp.Body.Close()
	if newBody.ID != "s2" {
		t.Fatalf("new id = %q; want s2", newBody.ID)
	}
	if env := client.next(t); env.Kind != "session-changed" || env.ID != "s2" {
		t.Fatalf("envelope = %+v; want session-changed s2", env)
	}

	// Resume 校验：缺少 id、未知 id。
	resp = postJSON(t, base, "/api/session/resume", `{}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("resume without id: status = %d; want 400", resp.StatusCode)
	}
	resp.Body.Close()
	resp = postJSON(t, base, "/api/session/resume", `{"id":"ghost"}`)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("resume unknown id: status = %d; want 500", resp.StatusCode)
	}
	resp.Body.Close()

	// Resume 会为下一个订阅者回填被恢复的日志。
	resp = postJSON(t, base, "/api/session/resume", `{"id":"s1"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("resume: status = %d; want 200", resp.StatusCode)
	}
	resp.Body.Close()
	if env := client.next(t); env.Kind != "session-changed" || env.ID != "s1" {
		t.Fatalf("envelope = %+v; want session-changed s1", env)
	}
	resumed := openSSE(t, base, "s1")
	env := resumed.next(t)
	if env.Kind != "session" || env.Event == nil {
		t.Fatalf("resumed backfill = %+v; want the old turn", env)
	}
	var d session.UserMessageData
	if json.Unmarshal(env.Event.Data, &d) != nil || d.Content != "old turn" {
		t.Fatalf("resumed backfill content = %+v; want old turn", env.Event)
	}
}

// TestUnknownSessionEvents：订阅一个没有日志的会话返回 404，
// 且连接不会被挂起。
func TestUnknownSessionEvents(t *testing.T) {
	_, _, _, base := setup(t)
	resp, err := http.Get(base + "/api/events?session=ghost")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d; want 404", resp.StatusCode)
	}
}

// TestSlowConsumerDoesNotBlockBroadcast：一个从不读取的客户端
// 不得拖住健康客户端的流；hub 会丢弃它已满的缓冲区而不是阻
// 塞。
func TestSlowConsumerDoesNotBlockBroadcast(t *testing.T) {
	srv, _, _, base := setup(t)
	srv.SetSession("s1", nil)
	root := fiber.New()
	defer root.Dispose()
	dispose := srv.SubscribeEvents(root.Context())
	defer func() { _ = dispose() }()

	// 慢客户端连上后一个字节也不读。
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/events?session=s1", nil)
	if err != nil {
		t.Fatal(err)
	}
	slow, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Body.Close()

	good := openSSE(t, base, "s1")
	const n = 300 // 超过 256 深的客户端缓冲区
	for i := 0; i < n; i++ {
		event.Emit[session.Event](root.Context(), "session/event", userEvent(fmt.Sprintf("e%d", i)))
		if i%32 == 31 {
			// 健康客户端的排水每条信封都要走一次 TCP 写，零延迟突发
			// 300 条必然超过其 256 深缓冲而被误丢； pacing 让排水跟上，
			// 测试钉的是「慢客户端被丢弃」而不是「广播比 TCP 快」。
			time.Sleep(time.Millisecond)
		}
	}
	for i := 0; i < n; i++ {
		env := good.next(t)
		if env.Kind != "session" || env.Event == nil {
			t.Fatalf("envelope = %+v; want session event", env)
		}
		var d session.UserMessageData
		if err := json.Unmarshal(env.Event.Data, &d); err != nil {
			t.Fatal(err)
		}
		if want := fmt.Sprintf("e%d", i); d.Content != want {
			t.Fatalf("event %d: content = %q; want %q", i, d.Content, want)
		}
	}
}
