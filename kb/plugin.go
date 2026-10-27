package kb

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"hx/core/fiber"
	"hx/core/plugin"
	"hx/kb/embed"
	"hx/kb/index"
	"hx/tools"
)

const (
	// defaultDebounceMs 限定 watcher 的静默期：被触碰的笔记
	// 静默这么久之后才重新索引。
	defaultDebounceMs = 200
	// defaultSearchLimit 在调用方和配置都没设 limit 时
	// 限制 kb_search 的命中条数。
	defaultSearchLimit = 10
	// defaultLintMaxChunks 是配置未设 lint_max_chunks 时
	// kb_lint 的长笔记阈值。
	defaultLintMaxChunks = 50
)

// resolve 把开头的 "~" 展开为用户主目录，并把相对路径锚定到
// 工作目录（沿用 memory 插件的约定）。
func resolve(path string) (string, error) {
	if strings.HasPrefix(path, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		path = filepath.Join(home, path[1:])
	}
	return filepath.Abs(filepath.FromSlash(path))
}

func configString(cfg map[string]any, key, def string) (string, error) {
	v, ok := cfg[key]
	if !ok || v == nil {
		return def, nil
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("kb: %q must be a string, got %T", key, v)
	}
	if s == "" {
		return def, nil
	}
	return s, nil
}

func configInt(cfg map[string]any, key string, def int) (int, error) {
	v, ok := cfg[key]
	if !ok || v == nil {
		return def, nil
	}
	i, ok := v.(int)
	if !ok {
		return 0, fmt.Errorf("kb: %q must be an int, got %T", key, v)
	}
	if i <= 0 {
		return 0, fmt.Errorf("kb: %q must be positive, got %d", key, i)
	}
	return i, nil
}

func configFloatPtr(cfg map[string]any, key string) (*float64, error) {
	v, ok := cfg[key]
	if !ok || v == nil {
		return nil, nil
	}
	switch n := v.(type) {
	case float64:
		return &n, nil
	case int:
		f := float64(n)
		return &f, nil
	default:
		return nil, fmt.Errorf("kb: %q must be a number, got %T", key, v)
	}
}

// configEmbedder 解析可选的 "embedder" 配置节。该节缺省表示
// 纯 FTS（embedder 为 nil）；存在时必须是 map，且必填字段
// （经 HX_EMBED_* 环境变量回退之后的 base_url、model）缺失时
// 响亮报错。
func configEmbedder(cfg map[string]any) (embed.Embedder, error) {
	v, ok := cfg["embedder"]
	if !ok || v == nil {
		return nil, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("kb: %q must be a map, got %T", "embedder", v)
	}
	var ec embed.Config
	for _, key := range []string{"base_url", "api_key", "model"} {
		if raw, present := m[key]; present {
			s, ok := raw.(string)
			if !ok {
				return nil, fmt.Errorf("kb: embedder.%q must be a string, got %T", key, raw)
			}
			switch key {
			case "base_url":
				ec.BaseURL = s
			case "api_key":
				ec.APIKey = s
			case "model":
				ec.Model = s
			}
		}
	}
	return embed.New(ec, nil)
}

// timeFromNanos 解码 kb_write 的 mtime 参数；0 表示零时间
// （即「断言文件不存在」的 etag）。
func timeFromNanos(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// Plugin 挂载 KB 服务、它的 vault watcher、kb_search / kb_read /
// kb_write / kb_lint 四个工具，以及 librarian 写作规程的 prompt
// section（"prompt" 服务出现后注册，dispose 时移除）。
// 配置键："vault"（string，必填——带配置挂载 kb 却没有 vault
// 会响亮报错；flatWire 的无配置挂载是空操作）、"index"
// （string，默认 <vault>/.hx/kb.db）、"debounce_ms"（int，
// 默认 200）、"search_limit"（int，默认 10）、"lint_max_chunks"
// （int，默认 50）、"fts_weight" 和
// "vector_weight"（数字，可选——显式写 0 会关闭该信号；
// 配置了 embedder 而两个权重都没设时，两者都默认取
// 1）、"embedder"（map，字符串键 base_url / api_key / model，
// 依次回退到 HX_EMBED_BASE_URL / HX_EMBED_API_KEY /
// HX_EMBED_MODEL 环境变量；该节存在但回退后仍缺 base_url 或
// model 时响亮报错，该节缺省时检索保持纯 FTS）。
func Plugin() plugin.Plugin { return kbPlugin{} }

type kbPlugin struct{}

func (kbPlugin) Name() string     { return "kb" }
func (kbPlugin) Inject() []string { return []string{"tools"} }
func (kbPlugin) Apply(c *fiber.Context, raw map[string]any) error {
	if raw == nil {
		// 默认装配不带配置挂载 kb；没配 vault 时插件是空操作。
		return nil
	}
	cfg, err := parseKBConfig(raw)
	if err != nil {
		return err
	}
	if info, err := os.Stat(cfg.vaultRoot); err != nil || !info.IsDir() {
		return fmt.Errorf("kb: vault %q is not a directory: %w", cfg.vaultRoot, err)
	}
	if err := os.MkdirAll(filepath.Dir(cfg.indexPath), 0o755); err != nil {
		return err
	}
	return c.Effect(func(c *fiber.Context) (fiber.Disposer, error) {
		return mountKB(c, cfg)
	})
}

// kbConfig 是完成路径解析和默认值求值后的 KB 挂载配置。
type kbConfig struct {
	vaultRoot     string
	indexPath     string
	debounceMs    int
	searchLimit   int
	lintMaxChunks int
	ftsWeight     *float64
	vecWeight     *float64
	embedder      embed.Embedder
}

// parseKBConfig 只解析配置，不创建目录或打开索引。
func parseKBConfig(raw map[string]any) (kbConfig, error) {
	var cfg kbConfig
	vaultRaw, err := configString(raw, "vault", "")
	if err != nil {
		return cfg, err
	}
	if vaultRaw == "" {
		return cfg, fmt.Errorf("kb: %q is required", "vault")
	}
	if cfg.vaultRoot, err = resolve(vaultRaw); err != nil {
		return cfg, err
	}
	indexRaw, err := configString(raw, "index", filepath.Join(cfg.vaultRoot, ".hx", "kb.db"))
	if err != nil {
		return cfg, err
	}
	if cfg.indexPath, err = resolve(indexRaw); err != nil {
		return cfg, err
	}
	if cfg.debounceMs, err = configInt(raw, "debounce_ms", defaultDebounceMs); err != nil {
		return cfg, err
	}
	if cfg.searchLimit, err = configInt(raw, "search_limit", defaultSearchLimit); err != nil {
		return cfg, err
	}
	if cfg.lintMaxChunks, err = configInt(raw, "lint_max_chunks", defaultLintMaxChunks); err != nil {
		return cfg, err
	}
	if cfg.ftsWeight, err = configFloatPtr(raw, "fts_weight"); err != nil {
		return cfg, err
	}
	if cfg.vecWeight, err = configFloatPtr(raw, "vector_weight"); err != nil {
		return cfg, err
	}
	if cfg.embedder, err = configEmbedder(raw); err != nil {
		return cfg, err
	}
	if cfg.embedder != nil && cfg.ftsWeight == nil && cfg.vecWeight == nil {
		// 配置 embedder 即声明要混合检索；显式 0 仍关闭对应信号。
		one := 1.0
		cfg.ftsWeight, cfg.vecWeight = &one, &one
	}
	return cfg, nil
}

// mountKB 打开并同步索引，注册 watcher、服务与工具，返回逆序清理器。
func mountKB(c *fiber.Context, cfg kbConfig) (fiber.Disposer, error) {
	idx, err := index.Open(cfg.indexPath)
	if err != nil {
		return nil, err
	}
	svc := NewService(cfg.vaultRoot, idx, cfg.embedder, cfg.ftsWeight, cfg.vecWeight, cfg.searchLimit, cfg.lintMaxChunks)
	fail := func(err error) (fiber.Disposer, error) {
		_ = idx.Close()
		return nil, err
	}
	if err := svc.Sync(); err != nil {
		return fail(err)
	}
	wd, err := watch(c, svc, cfg.vaultRoot, time.Duration(cfg.debounceMs)*time.Millisecond)
	if err != nil {
		return fail(err)
	}
	reg, ok := fiber.Get[*tools.Registry](c, "tools")
	if !ok {
		_ = wd()
		return fail(fmt.Errorf("kb: tools service unavailable"))
	}
	d0 := fiber.Provide[KB](c, "kb", svc)
	d1 := reg.Register(searchTool{svc})
	d2 := reg.Register(readTool{svc})
	d3 := reg.Register(writeTool{svc})
	d4 := reg.Register(lintTool{svc})
	d5 := registerLibrarian(c)
	return func() error {
		for _, d := range []fiber.Disposer{d5, d4, d3, d2, d1, d0, wd} {
			if err := d(); err != nil {
				return err
			}
		}
		return idx.Close()
	}, nil
}

func init() { plugin.Register(Plugin()) }
