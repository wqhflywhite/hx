// Package kb 是知识库能力 seam：Markdown vault 是事实源，内嵌
// 索引是可随时丢弃的衍生物，工具则是 agent 的混合检索与受控
// 写入界面。
package kb

import (
	"context"
	"crypto/sha1"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"hx/kb/embed"
	"hx/kb/index"
	"hx/kb/vault"
)

// ETag 是单个 vault 文件的乐观并发令牌：调用方上次
// 读到的 sha1 和修改时间。零值 ETag 断言
// 该文件尚不存在。
type ETag struct {
	SHA1  string
	MTime time.Time
}

// KB 是知识库的能力定义。路径相对 vault 且以斜杠分隔；
// heading 路径选中一篇笔记下的一个小节子树。
type KB interface {
	// Search 在索引上执行一次查询，返回按相关度排序的命中。
	Search(ctx context.Context, query string, opts index.SearchOpts) ([]index.Hit, error)
	// Read 返回一篇笔记的内容及其当前 ETag。heading 为空时读取
	// 整个文件，否则读取该 heading 路径下的 chunk。
	Read(ctx context.Context, path string, heading []string) (string, ETag, error)
	// Write 在 etag 仍与磁盘文件一致时替换一篇笔记的内容，重新
	// 索引它并返回新的 ETag。etag 过期是冲突，而不是
	// 覆盖写入。
	Write(ctx context.Context, path string, content []byte, etag ETag) (ETag, error)
	// Backlinks 列出所有链接到 target 的笔记。
	Backlinks(ctx context.Context, target string) ([]string, error)
}

// Service 是 KB 的提供者：一个 vault 目录加上它的索引。
// embedder 为 nil 时所有操作保持纯 FTS。
type Service struct {
	root          string
	idx           *index.Index
	emb           embed.Embedder
	ftsWeight     *float64
	vecWeight     *float64
	searchLimit   int
	lintMaxChunks int
}

// NewService 把 root 处的 vault 绑定到 idx。emb 可以为 nil（纯
// FTS）。权重指针是检索默认值；nil 表示用索引默认值（权重 1
// 的纯 FTS）。lintMaxChunks 是 kb_lint 的长笔记阈值。
func NewService(root string, idx *index.Index, emb embed.Embedder, ftsWeight, vecWeight *float64, searchLimit, lintMaxChunks int) *Service {
	return &Service{root: root, idx: idx, emb: emb, ftsWeight: ftsWeight, vecWeight: vecWeight, searchLimit: searchLimit, lintMaxChunks: lintMaxChunks}
}

// resolve 把 vault 相对路径映射为文件系统路径，拒绝逃逸
// 出 vault 的路径。
func (s *Service) resolve(rel string) (string, error) {
	if rel == "" {
		return "", fmt.Errorf("kb: empty path")
	}
	clean := filepath.Clean(filepath.FromSlash(rel))
	if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("kb: path %q escapes the vault", rel)
	}
	return filepath.Join(s.root, clean), nil
}

// relative 把文件系统路径映射回 vault 相对形式。
func (s *Service) relative(abs string) (string, error) {
	rel, err := filepath.Rel(s.root, abs)
	if err != nil {
		return "", err
	}
	return filepath.ToSlash(rel), nil
}

// etagOf 计算 abs 处文件的 ETag。
func etagOf(abs string) (ETag, error) {
	data, err := os.ReadFile(abs)
	if err != nil {
		return ETag{}, err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return ETag{}, err
	}
	return ETag{SHA1: fmt.Sprintf("%x", sha1.Sum(data)), MTime: info.ModTime()}, nil
}

// Search 实现 KB。配置了 embedder 时，查询会转成嵌入向量，
// 检索混合 FTS 与向量两种信号；嵌入失败会让检索失败，
// 而不是静默降级为纯 FTS。
func (s *Service) Search(ctx context.Context, query string, opts index.SearchOpts) ([]index.Hit, error) {
	if opts.Limit <= 0 {
		opts.Limit = s.searchLimit
	}
	if opts.FTSWeight == nil {
		opts.FTSWeight = s.ftsWeight
	}
	if opts.VectorWeight == nil {
		opts.VectorWeight = s.vecWeight
	}
	if s.emb != nil && strings.TrimSpace(query) != "" {
		vecs, err := s.emb.Embed(ctx, []string{query})
		if err != nil {
			return nil, fmt.Errorf("kb: embed query: %w", err)
		}
		if len(vecs) == 1 {
			opts.Vector = vecs[0]
		}
	}
	return s.idx.Search(ctx, query, opts)
}

// Read 实现 KB。
func (s *Service) Read(_ context.Context, path string, heading []string) (string, ETag, error) {
	abs, err := s.resolve(path)
	if err != nil {
		return "", ETag{}, err
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return "", ETag{}, fmt.Errorf("kb: read %q: %w", path, err)
	}
	etag, err := etagOf(abs)
	if err != nil {
		return "", ETag{}, err
	}
	if len(heading) == 0 {
		return string(data), etag, nil
	}
	doc, err := vault.Parse(data)
	if err != nil {
		return "", ETag{}, err
	}
	var parts []string
	for _, c := range doc.Chunks {
		if hasPrefix(c.Path, heading) {
			parts = append(parts, c.Text)
		}
	}
	if len(parts) == 0 {
		return "", ETag{}, fmt.Errorf("kb: %q has no section %q", path, strings.Join(heading, " / "))
	}
	return strings.Join(parts, "\n"), etag, nil
}

// hasPrefix 报告 path 是否以 prefix 的各个元素开头。
func hasPrefix(path, prefix []string) bool {
	if len(path) < len(prefix) {
		return false
	}
	for i, p := range prefix {
		if path[i] != p {
			return false
		}
	}
	return true
}

// conflictError 拒绝 etag 已不再匹配文件的写入。
func conflictError(path string) error {
	return fmt.Errorf("kb: %q 文件已被修改，请重新 kb_read 后重试", path)
}

// Write 实现 KB。乐观并发校验把 etag 的两个字段都与
// 磁盘上的文件比较，任何不一致都是冲突。写入成功后
// 先重新索引再返回，所以下一次检索
// 就能看到它。
func (s *Service) Write(_ context.Context, path string, content []byte, etag ETag) (ETag, error) {
	abs, err := s.resolve(path)
	if err != nil {
		return ETag{}, err
	}
	cur, err := etagOf(abs)
	switch {
	case os.IsNotExist(err):
		if etag.SHA1 != "" || !etag.MTime.IsZero() {
			return ETag{}, conflictError(path)
		}
	case err != nil:
		return ETag{}, err
	default:
		if cur.SHA1 != etag.SHA1 || !cur.MTime.Equal(etag.MTime) {
			return ETag{}, conflictError(path)
		}
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return ETag{}, err
	}
	if err := os.WriteFile(abs, content, 0o644); err != nil {
		return ETag{}, err
	}
	if err := s.SyncFile(abs); err != nil {
		return ETag{}, err
	}
	return etagOf(abs)
}

// Backlinks 实现 KB。
func (s *Service) Backlinks(_ context.Context, target string) ([]string, error) {
	return s.idx.Backlinks(target)
}

// Sync 对整个 vault 做增量索引：有变化或新增的笔记重新索引，
// 磁盘上已消失的笔记从索引中清除。它是启动时的一遍扫描，
// 也是 watcher 漏事件之后的自愈路径。
func (s *Service) Sync() error {
	seen := map[string]bool{}
	disk, err := s.vaultFiles()
	if err != nil {
		return err
	}
	for _, rel := range disk {
		seen[rel] = true
		if err := s.syncFile(filepath.Join(s.root, filepath.FromSlash(rel)), rel); err != nil {
			return err
		}
	}
	indexed, err := s.idx.Files()
	if err != nil {
		return err
	}
	for _, rel := range indexed {
		if !seen[rel] {
			if err := s.idx.RemoveFile(rel); err != nil {
				return err
			}
		}
	}
	return nil
}

// vaultFiles 列出 vault 的笔记路径（斜杠分隔、相对 vault）：
// 点目录以外的所有 .md 文件。
func (s *Service) vaultFiles() ([]string, error) {
	var out []string
	err := filepath.WalkDir(s.root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// 点目录（.hx、.obsidian、.git 等）不算内容。
			if p != s.root && strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".md") {
			return nil
		}
		rel, err := s.relative(p)
		if err != nil {
			return err
		}
		out = append(out, rel)
		return nil
	})
	return out, err
}

// SyncFile 重新索引一个文件系统路径；已消失的文件会从索引中
// 清除。它是 watcher 防抖后的入口。
func (s *Service) SyncFile(abs string) error {
	rel, err := s.relative(abs)
	if err != nil {
		return err
	}
	if _, err := os.Stat(abs); os.IsNotExist(err) {
		return s.idx.RemoveFile(rel)
	}
	return s.syncFile(abs, rel)
}

// syncFile 在某个已存在文件的 sha1 变化时重新索引它。配置了
// embedder 时，chunk 按每个文件一批转入嵌入向量。
func (s *Service) syncFile(abs, rel string) error {
	etag, err := etagOf(abs)
	if err != nil {
		return err
	}
	need, err := s.idx.NeedsReindex(rel, etag.SHA1)
	if err != nil || !need {
		return err
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return err
	}
	doc, err := vault.Parse(data)
	if err != nil {
		return fmt.Errorf("kb: parse %q: %w", rel, err)
	}
	var vectors [][]float32
	if s.emb != nil && len(doc.Chunks) > 0 {
		texts := make([]string, len(doc.Chunks))
		for i, c := range doc.Chunks {
			texts[i] = c.Text
		}
		vectors, err = s.emb.Embed(context.Background(), texts)
		if err != nil {
			return fmt.Errorf("kb: embed %q: %w", rel, err)
		}
	}
	return s.idx.ReindexFile(index.File{
		Path:    rel,
		SHA1:    etag.SHA1,
		MTime:   etag.MTime,
		Chunks:  doc.Chunks,
		Links:   doc.Links,
		Vectors: vectors,
	})
}
