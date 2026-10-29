// Package index 是 vault 的嵌入式 SQLite 索引：Markdown 文件的
// 可丢弃派生物，任何时候都能从它们重建。它使用
// modernc.org/sqlite（纯 Go 实现），所以 CGO_ENABLED=0 的
// 静态二进制不受影响。
package index

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"

	"hx/kb/vault"

	_ "modernc.org/sqlite"
)

// schema 是幂等的，因此 Open 对新库和已有库都适用。trigram
// 分词器让 CJK 文本可以做子串匹配（unicode61 会把一段不间断的
// CJK 文本当作一个不透明 token）；代价是短于三个字符的查询词
// 匹配不到任何内容。
const schema = `
CREATE TABLE IF NOT EXISTS files(path TEXT PRIMARY KEY, sha1 TEXT NOT NULL, mtime INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS chunks(file TEXT NOT NULL, idx INTEGER NOT NULL, heading_path TEXT NOT NULL, text TEXT NOT NULL, PRIMARY KEY(file, idx));
CREATE TABLE IF NOT EXISTS links(src TEXT NOT NULL, dst TEXT NOT NULL, PRIMARY KEY(src, dst));
CREATE TABLE IF NOT EXISTS embeddings(file TEXT NOT NULL, idx INTEGER NOT NULL, vector BLOB NOT NULL, PRIMARY KEY(file, idx));
CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(text, file UNINDEXED, idx UNINDEXED, tokenize='trigram');
`

// File 是一篇 vault 笔记的索引载荷：用于变更检测的身份信息加上
// 解析后的内容。Path 是 vault 相对路径，以斜杠分隔。
type File struct {
	Path   string
	SHA1   string
	MTime  time.Time
	Chunks []vault.Chunk
	Links  []string
	// Vectors 非 nil 时保存每个 chunk 对应的嵌入向量（与 Chunks
	// 对齐），并替换该笔记在 embeddings 表中的行；Vectors 为 nil
	// 时索引该笔记但不带嵌入向量。
	Vectors [][]float32
}

// SearchOpts 参数化一次搜索。权重指针用于区分显式的零（该信号
// 不贡献任何分数）与未设置；两个都未设置时是权重为 1 的纯 FTS
// 搜索，与 P1 行为一致。混合分数 = 权重 × 单侧 min-max 归一化
// 后的信号。
type SearchOpts struct {
	// Limit 限制命中数上限；非正数表示 10。
	Limit int
	// FTSWeight 缩放归一化后的 FTS rank。
	FTSWeight *float64
	// VectorWeight 缩放归一化后的向量余弦值。
	VectorWeight *float64
	// Vector 是查询嵌入向量；为 nil 时搜索只做 FTS。由调用方
	// 对查询做嵌入，让索引不沾染 provider 的事务。
	Vector []float32
}

// Hit 是一个命中的 chunk。Score 是加权归一化信号的混合值，
// 越大越好。
type Hit struct {
	Path        string
	Idx         int
	HeadingPath []string
	Text        string
	Score       float64
}

// Index 持有 SQLite 数据库句柄。
type Index struct {
	db *sql.DB
}

// Open 打开（必要时创建）path 处的索引数据库并确保 schema 就位。
// 调用方负责 Close。
func Open(path string) (*Index, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// modernc 的 PRAGMA 是连接级状态；单连接确保 WAL/busy_timeout
	// 覆盖本 Index 的所有后续操作，也避免写入互相覆盖。
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := db.Exec(pragma); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("index: %s: %w", pragma, err)
		}
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("index: schema: %w", err)
	}
	return &Index{db: db}, nil
}

// Close 关闭数据库句柄。
func (x *Index) Close() error { return x.db.Close() }

// NeedsReindex 报告 path 已索引的 sha1 是否与给定 sha1 不同，
// path 完全没有被索引时也返回 true。
func (x *Index) NeedsReindex(path, sha1 string) (bool, error) {
	var have string
	err := x.db.QueryRow(`SELECT sha1 FROM files WHERE path = ?`, path).Scan(&have)
	if err == sql.ErrNoRows {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return have != sha1, nil
}

// Files 列出已索引的路径；vault 扫描用它剔除已不存在的
// 笔记。
func (x *Index) Files() ([]string, error) {
	rows, err := x.db.Query(`SELECT path FROM files ORDER BY path`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ReindexFile 原子地替换一篇笔记的索引条目：旧 chunk、FTS
// 行和链接被删除，新条目在同一事务中插入。内容未变的笔记
// 由调用方通过 NeedsReindex 跳过，因此重复调用是
// no-op。
func (x *Index) ReindexFile(f File) error {
	tx, err := x.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := deleteFile(tx, f.Path); err != nil {
		return err
	}
	if err := insertFile(tx, f); err != nil {
		return err
	}
	return tx.Commit()
}

// RemoveFile 删除一篇笔记的索引条目；笔记不存在不算
// 错误。
func (x *Index) RemoveFile(path string) error {
	tx, err := x.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := deleteFile(tx, path); err != nil {
		return err
	}
	return tx.Commit()
}

// Rebuild 用 files 原子地替换整个索引：这是「索引可丢弃」的
// 入口。
func (x *Index) Rebuild(files []File) error {
	tx, err := x.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, q := range []string{
		`DELETE FROM chunks_fts`,
		`DELETE FROM chunks`,
		`DELETE FROM links`,
		`DELETE FROM embeddings`,
		`DELETE FROM files`,
	} {
		if _, err := tx.Exec(q); err != nil {
			return fmt.Errorf("index: rebuild: %w", err)
		}
	}
	for _, f := range files {
		if err := insertFile(tx, f); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// deleteFile 从每张表删除一篇笔记的行；调用方持有
// 事务。
func deleteFile(tx *sql.Tx, path string) error {
	for _, q := range []string{
		`DELETE FROM chunks_fts WHERE file = ?`,
		`DELETE FROM chunks WHERE file = ?`,
		`DELETE FROM links WHERE src = ?`,
		`DELETE FROM embeddings WHERE file = ?`,
		`DELETE FROM files WHERE path = ?`,
	} {
		if _, err := tx.Exec(q, path); err != nil {
			return fmt.Errorf("index: delete %q: %w", path, err)
		}
	}
	return nil
}

// insertFile 把一篇笔记的行写入每张表；调用方持有
// 事务。
func insertFile(tx *sql.Tx, f File) error {
	if f.Vectors != nil && len(f.Vectors) != len(f.Chunks) {
		return fmt.Errorf("index: %q has %d chunks but %d vectors", f.Path, len(f.Chunks), len(f.Vectors))
	}
	if _, err := tx.Exec(`INSERT INTO files(path, sha1, mtime) VALUES (?, ?, ?)`, f.Path, f.SHA1, f.MTime.UnixNano()); err != nil {
		return fmt.Errorf("index: insert %q: %w", f.Path, err)
	}
	for i, c := range f.Chunks {
		pathJSON, err := json.Marshal(c.Path)
		if err != nil {
			return fmt.Errorf("index: marshal heading path: %w", err)
		}
		if _, err := tx.Exec(`INSERT INTO chunks(file, idx, heading_path, text) VALUES (?, ?, ?, ?)`, f.Path, i, string(pathJSON), c.Text); err != nil {
			return fmt.Errorf("index: insert %q chunk %d: %w", f.Path, i, err)
		}
		if _, err := tx.Exec(`INSERT INTO chunks_fts(text, file, idx) VALUES (?, ?, ?)`, c.Text, f.Path, i); err != nil {
			return fmt.Errorf("index: insert %q fts %d: %w", f.Path, i, err)
		}
	}
	for i, v := range f.Vectors {
		if _, err := tx.Exec(`INSERT INTO embeddings(file, idx, vector) VALUES (?, ?, ?)`, f.Path, i, encodeVector(v)); err != nil {
			return fmt.Errorf("index: insert %q embedding %d: %w", f.Path, i, err)
		}
	}
	for _, dst := range f.Links {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO links(src, dst) VALUES (?, ?)`, f.Path, dst); err != nil {
			return fmt.Errorf("index: insert link %q -> %q: %w", f.Path, dst, err)
		}
	}
	return nil
}

// candidateK 是喂给混合阶段的单侧候选深度。
func candidateK(limit int) int {
	if k := limit * 4; k > 20 {
		return k
	}
	return 20
}

// Search 执行混合检索：FTS top-K 与向量 top-K（暴力余弦
// 扫描）各自 min-max 归一化到 [0,1]，再按 权重 × 归一化
// 分数 混合，合并后的列表按混合分数重排。查询词被加双引号
// 并以 OR 组合，任意用户文本都无法破坏 MATCH 语法。查询
// 文本为空但带查询向量时是纯向量搜索；两者都没有时 Search
// 不返回任何结果。
func (x *Index) Search(ctx context.Context, query string, opts SearchOpts) ([]Hit, error) {
	ftsW, vecW, limit := searchWeights(opts)
	k := candidateK(limit)
	merged := blendedHits{}
	if ftsW > 0 {
		cands, err := x.ftsCandidates(ctx, query, k)
		if err != nil {
			return nil, err
		}
		merged.add(cands, ftsW)
	}
	if vecW > 0 {
		cands, err := x.vectorCandidates(ctx, opts.Vector, k)
		if err != nil {
			return nil, err
		}
		merged.add(cands, vecW)
	}
	return merged.top(limit), nil
}

// searchWeights 解析检索权重与返回条数；缺省为纯 FTS。
func searchWeights(opts SearchOpts) (ftsW, vecW float64, limit int) {
	if opts.FTSWeight != nil {
		ftsW = *opts.FTSWeight
	}
	if opts.VectorWeight != nil {
		vecW = *opts.VectorWeight
	}
	if opts.FTSWeight == nil && opts.VectorWeight == nil {
		ftsW = 1
	}
	if len(opts.Vector) == 0 {
		vecW = 0
	}
	limit = opts.Limit
	if limit <= 0 {
		limit = 10
	}
	return ftsW, vecW, limit
}

type blendKey struct {
	path string
	idx  int
}

type blendedHits map[blendKey]*Hit

// add 先归一化单侧候选，再按权重加入混合分数。
func (m blendedHits) add(cands []candidate, weight float64) {
	minMaxNormalize(cands)
	for _, c := range cands {
		key := blendKey{path: c.hit.Path, idx: c.hit.Idx}
		if h, ok := m[key]; ok {
			h.Score += weight * c.score
			continue
		}
		h := c.hit
		h.Score = weight * c.score
		m[key] = &h
	}
}

// top 按分数、路径和 chunk 序索引确定性排序并截断。
func (m blendedHits) top(limit int) []Hit {
	var hits []Hit
	for _, h := range m {
		hits = append(hits, *h)
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		if hits[i].Path != hits[j].Path {
			return hits[i].Path < hits[j].Path
		}
		return hits[i].Idx < hits[j].Idx
	})
	if len(hits) > limit {
		hits = hits[:limit]
	}
	return hits
}

// ftsCandidates 返回 query 的 k 个最佳 FTS 匹配，分数是原始
// bm25 系分数（取负的 rank，越大越好）。
func (x *Index) ftsCandidates(ctx context.Context, query string, k int) ([]candidate, error) {
	match := matchQuery(query)
	if match == "" {
		return nil, nil
	}
	rows, err := x.db.QueryContext(ctx,
		`SELECT f.file, f.idx, c.heading_path, c.text, f.rank
		 FROM chunks_fts f JOIN chunks c ON c.file = f.file AND c.idx = f.idx
		 WHERE chunks_fts MATCH ?
		 ORDER BY f.rank
		 LIMIT ?`, match, k)
	if err != nil {
		return nil, fmt.Errorf("index: search %q: %w", query, err)
	}
	defer rows.Close()
	var cands []candidate
	for rows.Next() {
		var c candidate
		var pathJSON string
		var rank float64
		if err := rows.Scan(&c.hit.Path, &c.hit.Idx, &pathJSON, &c.hit.Text, &rank); err != nil {
			return nil, err
		}
		if err := decodeHeadingPath(pathJSON, &c.hit); err != nil {
			return nil, err
		}
		c.score = -rank
		cands = append(cands, c)
	}
	return cands, rows.Err()
}

// decodeHeadingPath 把存储的 heading path 反序列化到 h 中。
func decodeHeadingPath(pathJSON string, h *Hit) error {
	if err := json.Unmarshal([]byte(pathJSON), &h.HeadingPath); err != nil {
		return fmt.Errorf("index: decode heading path of %q: %w", h.Path, err)
	}
	return nil
}

// ChunkCounts 返回每个文件路径下已索引的 chunk 数量。
func (x *Index) ChunkCounts() (map[string]int, error) {
	rows, err := x.db.Query(`SELECT file, COUNT(*) FROM chunks GROUP BY file`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var f string
		var n int
		if err := rows.Scan(&f, &n); err != nil {
			return nil, err
		}
		out[f] = n
	}
	return out, rows.Err()
}

// OutboundLinks 返回每篇已索引笔记的 wikilink 目标（解析时已去掉
// "#anchor" 和 "|alias"），用于全 vault 的链接分析。
func (x *Index) OutboundLinks() (map[string][]string, error) {
	rows, err := x.db.Query(`SELECT src, dst FROM links ORDER BY src, dst`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var src, dst string
		if err := rows.Scan(&src, &dst); err != nil {
			return nil, err
		}
		out[src] = append(out[src], dst)
	}
	return out, rows.Err()
}

// Backlinks 按路径顺序列出链接到 target 的已索引笔记。target
// 是已去掉 "#anchor" 和 "|alias" 的 wikilink 目标。
func (x *Index) Backlinks(target string) ([]string, error) {
	rows, err := x.db.Query(`SELECT src FROM links WHERE dst = ? ORDER BY src`, target)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var src string
		if err := rows.Scan(&src); err != nil {
			return nil, err
		}
		out = append(out, src)
	}
	return out, rows.Err()
}

// matchQuery 从自由形式的自然语言构建安全的 FTS5
// MATCH 表达式。拉丁字母/数字的连续片段变成加引号
// 的词。短于三个字符的 CJK 片段被丢弃（trigram 分词器
// 匹配不到它们）；更长的片段展开成重叠的三字符窗口词，
// 因为一个三字符窗口恰好对应一次 trigram 查找，而自然
// 语言从句作为整体短语本来就什么都匹配不到。所有词以
// OR 组合：召回优先，精度交给 bm25 排序和向量侧。所有
// 标点都被丢弃，所以任意用户文本都无法破坏 MATCH
// 语法。
func matchQuery(query string) string {
	var terms []string
	var latin, cjk []rune
	quote := func(s string) {
		terms = append(terms, `"`+strings.ReplaceAll(s, `"`, `""`)+`"`)
	}
	flushCJK := func() {
		if len(cjk) >= 3 {
			for i := 0; i+3 <= len(cjk); i++ {
				quote(string(cjk[i : i+3]))
			}
		}
		cjk = cjk[:0]
	}
	flushLatin := func() {
		if len(latin) > 0 {
			quote(string(latin))
			latin = latin[:0]
		}
	}
	for _, r := range query {
		switch {
		case unicode.Is(unicode.Han, r):
			flushLatin()
			cjk = append(cjk, r)
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			flushCJK()
			latin = append(latin, r)
		default:
			flushLatin()
			flushCJK()
		}
	}
	flushLatin()
	flushCJK()
	return strings.Join(terms, " OR ")
}
