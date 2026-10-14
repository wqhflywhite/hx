// Package eval 是 kb 插件的离线评测工具：包含一个合成 fixture vault
// （testdata/vault，16 篇 wiki 概念页 + 24 篇带 wikilink 的原始
// 笔记）和一个真实语料 vault（testdata/realvault，真实的中文技术
// 笔记），各自配一套 gold 题集（testdata/gold.json /
// testdata/realgold.json，分单跳与多跳两组），并产出命中率/延迟
// 报告。确定性模式用 embed.Fake 搭配题集的 Synonyms 运行；
// 真实的 OpenAI 兼容嵌入向量服务只在 HX_EMBED_* 环境变量
// 存在时才参与。
// 合成 fixture 的报告数字供项目 README 引用；真实语料的运行结果
// 用真实文档对这些数字做交叉验证。
package eval

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"hx/kb/index"
)

// Question 是一条 gold 查询。Group 为 "single"（单篇笔记即可回答）或
// "multi"（期望笔记距 Anchor 至少两跳 wikilink，且与查询没有任何
// 可检索的公共关键词）。
type Question struct {
	Query  string   `json:"query"`
	Expect []string `json:"expect"`
	Group  string   `json:"group"`
	Anchor string   `json:"anchor"`
	Hops   int      `json:"hops"`
	// Note 记录答案在期望笔记中的位置（小节与行范围），供真实语料 gold
	// 题集使用；合成题集里为空。
	Note string `json:"note"`
}

// GoldSet 是题目列表加上 Fake 同义词表，同义词表用于把查询
// 词汇桥接到笔记词汇上（顶替真实多语言嵌入向量模型
// 的角色）。
type GoldSet struct {
	Synonyms  map[string]string `json:"synonyms"`
	Questions []Question        `json:"questions"`
}

// LoadGold 读取一个 gold 题集 JSON 文件。
func LoadGold(path string) (*GoldSet, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var g GoldSet
	if err := json.Unmarshal(data, &g); err != nil {
		return nil, fmt.Errorf("eval: gold %q: %w", path, err)
	}
	return &g, nil
}

// Result 是一道题的结果：top-5 命中的路径，以及期望笔记是否进入
// top-1 / top-5。
type Result struct {
	Question Question
	Hits     []string
	Top1     bool
	Top5     bool
}

// GroupStats 累计一个分组的命中数。
type GroupStats struct {
	N    int
	Top1 int
	Top5 int
}

func (s GroupStats) rate(n int) float64 {
	if s.N == 0 {
		return 0
	}
	return 100 * float64(n) / float64(s.N)
}

// Top1Rate 是 top-1 命中为期望笔记的题目百分比。
func (s GroupStats) Top1Rate() float64 { return s.rate(s.Top1) }

// Top5Rate 是 top-5 内含期望笔记的题目百分比。
func (s GroupStats) Top5Rate() float64 { return s.rate(s.Top5) }

// Report 是对 gold 题集的一轮评测结果。
type Report struct {
	Mode    string
	Results []Result
	Groups  map[string]*GroupStats
	Total   GroupStats
}

// SearchFunc 是评测所需的 kb.KB 子集。
type SearchFunc func(ctx context.Context, query string, opts index.SearchOpts) ([]index.Hit, error)

// Run 用 opts 逐题检索并记录 top-1 / top-5 命中。
func Run(ctx context.Context, mode string, questions []Question, opts index.SearchOpts, search SearchFunc) (*Report, error) {
	r := &Report{Mode: mode, Groups: map[string]*GroupStats{}}
	for _, q := range questions {
		hits, err := search(ctx, q.Query, opts)
		if err != nil {
			return nil, fmt.Errorf("eval: query %q: %w", q.Query, err)
		}
		res := Result{Question: q}
		for i, h := range hits {
			if i >= 5 {
				break
			}
			res.Hits = append(res.Hits, h.Path)
			for _, want := range q.Expect {
				if h.Path == want {
					if i == 0 {
						res.Top1 = true
					}
					res.Top5 = true
				}
			}
		}
		stats, ok := r.Groups[q.Group]
		if !ok {
			stats = &GroupStats{}
			r.Groups[q.Group] = stats
		}
		stats.N++
		r.Total.N++
		if res.Top1 {
			stats.Top1++
			r.Total.Top1++
		}
		if res.Top5 {
			stats.Top5++
			r.Total.Top5++
		}
		r.Results = append(r.Results, res)
	}
	return r, nil
}

// String 渲染按名称排序的分组命中率表格。
func (r *Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "mode=%s\n", r.Mode)
	groups := make([]string, 0, len(r.Groups))
	for g := range r.Groups {
		groups = append(groups, g)
	}
	sort.Strings(groups)
	for _, g := range groups {
		s := r.Groups[g]
		fmt.Fprintf(&b, "  %-6s top-1 %2d/%d (%5.1f%%)  top-5 %2d/%d (%5.1f%%)\n",
			g, s.Top1, s.N, s.Top1Rate(), s.Top5, s.N, s.Top5Rate())
	}
	fmt.Fprintf(&b, "  %-6s top-1 %2d/%d (%5.1f%%)  top-5 %2d/%d (%5.1f%%)",
		"total", r.Total.Top1, r.Total.N, r.Total.Top1Rate(), r.Total.Top5, r.Total.N, r.Total.Top5Rate())
	return b.String()
}

// Misses 列出 top-5 中不含期望笔记的题目，用于调 fixture 时
// 排查失败。
func (r *Report) Misses() []Result {
	var out []Result
	for _, res := range r.Results {
		if !res.Top5 {
			out = append(out, res)
		}
	}
	return out
}

// Percentile 返回样本的第 p 百分位（0-100）；输入会被原地排序。
// 空输入返回 0。
func Percentile(samples []time.Duration, p float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	i := int(p / 100 * float64(len(samples)-1))
	return samples[i]
}
