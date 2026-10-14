package kb

import (
	"context"
	"sort"
	"strings"
)

// BrokenLink 是目标解析不到任何 vault 笔记的 wikilink。
type BrokenLink struct {
	Src string
	Dst string
}

// LongNote 是 chunk 数超过配置阈值的笔记。
type LongNote struct {
	Path   string
	Chunks int
}

// LintReport 是对 vault 的确定性健康检查：断链 wikilink、
// 孤儿笔记、磁盘上有但索引里缺失的笔记，以及超长笔记。
// 每个切片都排好序，保证报告稳定。
type LintReport struct {
	BrokenLinks []BrokenLink
	Orphans     []string
	Unindexed   []string
	LongNotes   []LongNote
}

// Empty 报告 vault 是否通过了全部检查。
func (r *LintReport) Empty() bool {
	return len(r.BrokenLinks) == 0 && len(r.Orphans) == 0 && len(r.Unindexed) == 0 && len(r.LongNotes) == 0
}

// resolveLink 把 wikilink 目标映射为 vault 相对的笔记路径：
// 目标本身、或目标加上 ".md" 后缀，必须是一个已索引的
// 文件。
func resolveLink(dst string, files map[string]bool) (string, bool) {
	if files[dst] {
		return dst, true
	}
	if withMD := dst + ".md"; files[withMD] {
		return withMD, true
	}
	return "", false
}

// Lint 对照索引和文件系统执行 vault 健康检查。它
// 从不调用 embedder 或 LLM。raw/ 下的笔记是原始
// 素材，豁免孤儿检查；自链接不计入
// 入链。
func (s *Service) Lint(_ context.Context) (*LintReport, error) {
	indexed, err := s.idx.Files()
	if err != nil {
		return nil, err
	}
	files := map[string]bool{}
	for _, f := range indexed {
		files[f] = true
	}
	links, err := s.idx.OutboundLinks()
	if err != nil {
		return nil, err
	}
	counts, err := s.idx.ChunkCounts()
	if err != nil {
		return nil, err
	}
	onDisk, err := s.vaultFiles()
	if err != nil {
		return nil, err
	}

	report := &LintReport{}
	inbound := map[string]int{}
	for _, src := range indexed {
		for _, dst := range links[src] {
			target, ok := resolveLink(dst, files)
			if !ok {
				report.BrokenLinks = append(report.BrokenLinks, BrokenLink{Src: src, Dst: dst})
				continue
			}
			if target != src {
				inbound[target]++
			}
		}
	}
	for _, f := range indexed {
		if !strings.HasPrefix(f, "raw/") && inbound[f] == 0 {
			report.Orphans = append(report.Orphans, f)
		}
	}
	for _, f := range onDisk {
		if !files[f] {
			report.Unindexed = append(report.Unindexed, f)
		}
	}
	for f, n := range counts {
		if n > s.lintMaxChunks {
			report.LongNotes = append(report.LongNotes, LongNote{Path: f, Chunks: n})
		}
	}

	sort.Slice(report.BrokenLinks, func(i, j int) bool {
		if report.BrokenLinks[i].Src != report.BrokenLinks[j].Src {
			return report.BrokenLinks[i].Src < report.BrokenLinks[j].Src
		}
		return report.BrokenLinks[i].Dst < report.BrokenLinks[j].Dst
	})
	sort.Strings(report.Orphans)
	sort.Strings(report.Unindexed)
	sort.Slice(report.LongNotes, func(i, j int) bool {
		if report.LongNotes[i].Chunks != report.LongNotes[j].Chunks {
			return report.LongNotes[i].Chunks > report.LongNotes[j].Chunks
		}
		return report.LongNotes[i].Path < report.LongNotes[j].Path
	})
	return report, nil
}
