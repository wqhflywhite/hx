// Package web 提供浏览器 UI：一个内嵌的单页客户端、
// 一条承载会话事件、审批与通知的 SSE 流，以及用于命令
// 和审批应答的 POST 端点。JSON 信封集合是封闭的、手工
// 编写的；没有 schema 生成器。
package web

import (
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"hx/core/event"
	"hx/core/fiber"
	"hx/llm"
	"hx/session"
)

//go:embed static/index.html
var indexHTML []byte

// 封闭 SSE 协议的信封种类。
const (
	kindSession        = "session"
	kindApproval       = "approval"
	kindNotice         = "notice"
	kindSessionChanged = "session-changed"
)

// envelope 是一条 SSE 消息；Kind 是判别字段，只设置该种类
// 对应的字段：session 携带 Event，approval 携带 ID/Name/
// Arguments，notice 携带 Text，session-changed 携带 ID。
type envelope struct {
	Kind      string         `json:"kind"`
	Event     *session.Event `json:"event,omitempty"`
	ID        string         `json:"id,omitempty"`
	Name      string         `json:"name,omitempty"`
	Arguments string         `json:"arguments,omitempty"`
	Text      string         `json:"text,omitempty"`
}

// mustEnvelope 序列化一个信封；所有载荷都源自合法 JSON，
// 因此失败即编程错误。
func mustEnvelope(env envelope) []byte {
	raw, err := json.Marshal(env)
	if err != nil {
		panic(fmt.Errorf("web: marshal %q envelope: %w", env.Kind, err))
	}
	return raw
}

// clientBuffer 限定单个 SSE 客户端队列的上限；消费慢的
// 客户端会被丢弃，而不是阻塞广播。
const clientBuffer = 256

// hub 把会话事件扇出给各个 SSE 客户端。一把互斥锁串行化
// 回填快照、实时广播与客户端注册，因此订阅者恰好一次地看
// 到当前会话的每个事件：事件既不会落在快照与注册之间（缺
// 口），也不会在两边都出现（重复），因为两者都与广播在同
// 一把锁下发生。
type hub struct {
	mu      sync.Mutex
	session string
	events  []session.Event
	clients map[chan []byte]struct{}
}

func newHub() *hub { return &hub{clients: map[chan []byte]struct{}{}} }

// fanout 把 raw 排入每个客户端的队列，缓冲区已满的消费者
// 会被丢弃。调用时必须持有 h.mu。
func (h *hub) fanout(raw []byte) {
	for ch := range h.clients {
		select {
		case ch <- raw:
		default:
			delete(h.clients, ch)
			close(ch)
		}
	}
}

// publishEvent 把 ev 追加到当前会话的日志副本并原子地广播；
// 若把这两步拆到两次加锁中，会重新引入 subscribe 已消除的
// 回填竞态。
func (h *hub) publishEvent(ev session.Event) {
	raw := mustEnvelope(envelope{Kind: kindSession, Event: &ev})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, ev)
	h.fanout(raw)
}

// publishApproval 广播一条待处理的审批。
func (h *hub) publishApproval(id string, call llm.ToolCall) {
	raw := mustEnvelope(envelope{Kind: kindApproval, ID: id, Name: call.Name, Arguments: call.Arguments})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fanout(raw)
}

// publishNotice 广播一行状态通知。
func (h *hub) publishNotice(text string) {
	raw := mustEnvelope(envelope{Kind: kindNotice, Text: text})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fanout(raw)
}

// subscribe 在与广播相同的锁下注册客户端并对 id
// 的回填做快照，这正是事件流无缺口、无重复的原因。
// 当前会话从 hub 的内存副本回填（若改为从文件播种，
// 会与并发的 Append 产生竞态：日志写入先于广播落盘，
// 因此一个在途事件可能同时出现在快照和实时流两处）；
// 其他会话则从它自己的日志文件回填，该文件没有活跃
// 的写入者。
func (h *hub) subscribe(id string, load func(string) ([]session.Event, error)) (chan []byte, []session.Event, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var backfill []session.Event
	if id == "" || id == h.session {
		backfill = append([]session.Event(nil), h.events...)
	} else {
		if load == nil {
			return nil, nil, fmt.Errorf("web: no log loader configured")
		}
		events, err := load(id)
		if err != nil {
			return nil, nil, err
		}
		backfill = events
	}
	ch := make(chan []byte, clientBuffer)
	h.clients[ch] = struct{}{}
	return ch, backfill, nil
}

// unsubscribe 移除 handler 已返回的客户端。
func (h *hub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[ch]; ok {
		delete(h.clients, ch)
		close(ch)
	}
}

// setSession 把 id 设为当前会话、以 events 作为它目前为止的
// 日志，并广播 "session-changed"。
func (h *hub) setSession(id string, events []session.Event) {
	raw := mustEnvelope(envelope{Kind: kindSessionChanged, ID: id})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.session = id
	h.events = append([]session.Event(nil), events...)
	h.fanout(raw)
}

// close 丢弃所有客户端，结束它们的 SSE 流。
func (h *hub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		delete(h.clients, ch)
		close(ch)
	}
}

// Options 把 server 接入 harness。AuthToken 设置后保护全部 HTTP
// 端点；AllowRemote 是非 loopback 监听的显式安全确认。
type Options struct {
	Prompter       *Prompter
	Send           func(text string)
	CurrentSession func() string
	LoadEvents     func(id string) ([]session.Event, error)
	ListSessions   func() ([]string, error)
	NewSession     func() (string, error)
	ResumeSession  func(id string) (string, error)
	AuthToken      string
	AllowRemote    bool
}

// Server 是 HTTP 前端：内嵌 UI、SSE hub 与命令端点。
type Server struct {
	opts        Options
	hub         *hub
	mux         *http.ServeMux
	handler     http.Handler
	allowedHost string
	done        chan struct{}
	once        sync.Once
}

// NewServer 构建 server 并把 Prompter 的提问路由进 hub；
// 接线不完整时直接报错。
func NewServer(opts Options) (*Server, error) {
	if opts.Prompter == nil || opts.Send == nil || opts.CurrentSession == nil ||
		opts.LoadEvents == nil || opts.ListSessions == nil ||
		opts.NewSession == nil || opts.ResumeSession == nil {
		return nil, fmt.Errorf("web: every Options callback must be set")
	}
	s := &Server{opts: opts, hub: newHub(), mux: http.NewServeMux(), done: make(chan struct{})}
	opts.Prompter.setNotify(s.hub.publishApproval)
	s.mux.HandleFunc("GET /", s.handleIndex)
	s.mux.HandleFunc("GET /api/events", s.handleEvents)
	s.mux.HandleFunc("POST /api/send", s.handleSend)
	s.mux.HandleFunc("POST /api/approval", s.handleApproval)
	s.mux.HandleFunc("GET /api/sessions", s.handleSessions)
	s.mux.HandleFunc("POST /api/session/new", s.handleSessionNew)
	s.mux.HandleFunc("POST /api/session/resume", s.handleSessionResume)
	s.handler = s.secure(s.mux)
	return s, nil
}

func hostName(value string) string {
	if host, _, err := net.SplitHostPort(value); err == nil {
		return host
	}
	return strings.Trim(value, "[]")
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (s *Server) secure(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.opts.AuthToken != "" {
			token := r.Header.Get("X-HX-Token")
			if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
				token = strings.TrimPrefix(auth, "Bearer ")
			}
			if subtle.ConstantTimeCompare([]byte(token), []byte(s.opts.AuthToken)) != 1 {
				http.Error(w, "web: unauthorized", http.StatusUnauthorized)
				return
			}
		}
		if s.allowedHost != "" {
			host := hostName(r.Host)
			if host != s.allowedHost && !(loopbackHost(host) && loopbackHost(s.allowedHost)) {
				http.Error(w, "web: forbidden host", http.StatusForbidden)
				return
			}
		}
		if r.Method == http.MethodPost && r.Header.Get("Origin") != "" {
			origin, err := url.Parse(r.Header.Get("Origin"))
			if err != nil || origin.Host != r.Host {
				http.Error(w, "web: cross-origin request rejected", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// Handler 返回带鉴权与来源校验的 HTTP handler。
func (s *Server) Handler() http.Handler { return s.handler }

// SubscribeEvents 把从 ctx 可见的会话事件路由进 hub。
func (s *Server) SubscribeEvents(ctx *fiber.Context) fiber.Disposer {
	return event.On[session.Event](ctx, "session/event", func(_ *fiber.Context, ev session.Event) {
		s.hub.publishEvent(ev)
	})
}

func (s *Server) SetSession(id string, events []session.Event) { s.hub.setSession(id, events) }
func (s *Server) Notify(text string)                           { s.hub.publishNotice(text) }

func (s *Server) Close() {
	s.once.Do(func() {
		close(s.done)
		s.hub.close()
		s.opts.Prompter.Close()
	})
}

// Run 校验监听安全边界后持续服务，直到 ctx 被撤销或监听器失败。
func (s *Server) Run(ctx *fiber.Context, addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("web: invalid listen address %q: %w", addr, err)
	}
	if !loopbackHost(host) {
		if !s.opts.AllowRemote {
			return fmt.Errorf("web: non-loopback listen requires explicit remote opt-in")
		}
		if s.opts.AuthToken == "" {
			return fmt.Errorf("web: non-loopback listen requires an auth token")
		}
	}
	s.allowedHost = host
	d := s.SubscribeEvents(ctx)
	defer func() { _ = d() }()
	defer s.Close()
	hs := &http.Server{
		Addr: addr, Handler: s.handler,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second,
	}
	go func() {
		select {
		case <-ctx.Fiber().Done():
		case <-s.done:
		}
		_ = hs.Close()
	}()
	err = hs.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) handleIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}

// writeSSE 封装一条消息并返回写错误；raw 不得包含换行。
func writeSSE(w http.ResponseWriter, raw []byte) error {
	_, err := fmt.Fprintf(w, "data: %s\n\n", raw)
	return err
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "web: streaming unsupported", http.StatusInternalServerError)
		return
	}
	ch, backfill, err := s.hub.subscribe(r.URL.Query().Get("session"), s.opts.LoadEvents)
	if err != nil {
		http.Error(w, "web: "+err.Error(), http.StatusNotFound)
		return
	}
	defer s.hub.unsubscribe(ch)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	for _, ev := range backfill {
		if r.Context().Err() != nil {
			return
		}
		ev := ev
		if writeSSE(w, mustEnvelope(envelope{Kind: kindSession, Event: &ev})) != nil {
			return
		}
	}
	for _, p := range s.opts.Prompter.Pending() {
		if writeSSE(w, mustEnvelope(envelope{Kind: kindApproval, ID: p.ID, Name: p.Name, Arguments: p.Arguments})) != nil {
			return
		}
	}
	flusher.Flush()
	for {
		select {
		case raw, ok := <-ch:
			if !ok || writeSSE(w, raw) != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		case <-s.done:
			return
		}
	}
}

// decodeJSON 校验 JSON Content-Type，并拒绝未知字段与过大载荷。
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" {
		http.Error(w, "web: Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return false
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		http.Error(w, "web: bad request body: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Text string `json:"text"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.Text) == "" {
		http.Error(w, "web: empty text", http.StatusBadRequest)
		return
	}
	s.opts.Send(req.Text)
	writeJSON(w, map[string]any{"ok": true})
}

func (s *Server) handleApproval(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID    string `json:"id"`
		Allow bool   `json:"allow"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.ID == "" {
		http.Error(w, "web: missing approval id", http.StatusBadRequest)
		return
	}
	if !s.opts.Prompter.Answer(req.ID, req.Allow) {
		http.Error(w, "web: no pending approval "+req.ID, http.StatusNotFound)
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func (s *Server) handleSessions(w http.ResponseWriter, _ *http.Request) {
	ids, err := s.opts.ListSessions()
	if err != nil {
		http.Error(w, "web: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"sessions": ids, "current": s.opts.CurrentSession()})
}

// changeSession 执行重建回调，重载 hub 的日志副本，并广播
// "session-changed"。
func (s *Server) changeSession(recreate func() (string, error)) (string, error) {
	s.opts.Prompter.Invalidate()
	id, err := recreate()
	if err != nil {
		return "", err
	}
	events, err := s.opts.LoadEvents(id)
	if err != nil {
		return "", fmt.Errorf("web: load session %q: %w", id, err)
	}
	s.hub.setSession(id, events)
	return id, nil
}

func (s *Server) handleSessionNew(w http.ResponseWriter, _ *http.Request) {
	id, err := s.changeSession(s.opts.NewSession)
	if err != nil {
		http.Error(w, "web: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"id": id})
}

func (s *Server) handleSessionResume(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID string `json:"id"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.ID == "" {
		http.Error(w, "web: missing session id", http.StatusBadRequest)
		return
	}
	id, err := s.changeSession(func() (string, error) { return s.opts.ResumeSession(req.ID) })
	if err != nil {
		http.Error(w, "web: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"id": id})
}
