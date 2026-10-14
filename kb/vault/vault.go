// Package vault 解析 Obsidian 风格 vault 的 Markdown 笔记。vault
// 文件是事实源；这里派生出的一切都喂给可丢弃的索引，索引可随时
// 重建。
package vault

import (
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Doc 是一篇解析后的笔记：frontmatter 元数据、wikilink 目标，以及按标题
// 层级切分的正文 chunk。
type Doc struct {
	// Frontmatter 是开头 "---" 围栏之间的 YAML 块；笔记没有 frontmatter
	// 时为 nil。
	Frontmatter map[string]any
	// Links 按出现顺序存放去重后的 wikilink 目标，每个目标都已剥离
	// "#锚点" 和 "|别名" 部分。
	Links []string
	// Chunks 是按标题边界切分的正文，按文件顺序排列。
	Chunks []Chunk
}

// Chunk 是笔记中一段由标题界定的内容。Path 是从顶层一直到开启该
// chunk 的标题的标题层级；第一个标题之前的 chunk 的 Path 为空。
// Start 和 End 是原始文件中的 1-based 行号（frontmatter 行也计入
// 编号），End 为闭区间，因此一个 chunk 可以用 "path#Lstart-Lend"
// 寻址。
type Chunk struct {
	Path  []string
	Start int
	End   int
	Text  string
}

// HeadingPath 把 chunk 的标题路径拼接成展示和存储用的字符串。
func (c Chunk) HeadingPath() string { return strings.Join(c.Path, " / ") }

var wikilinkRe = regexp.MustCompile(`\[\[([^\[\]]+)\]\]`)

// Parse 把 data 拆成 frontmatter、wikilink 和标题 chunk。没有开头
// "---" 围栏的笔记解析结果 Frontmatter 为 nil；开头围栏始终未闭合的
// 笔记也是如此（围栏行留在正文里）。frontmatter YAML 块格式错误则
// 返回错误。
func Parse(data []byte) (*Doc, error) {
	text := strings.ReplaceAll(string(data), "\r\n", "\n")
	lines := strings.Split(text, "\n")
	body, bodyOffset := lines, 0
	doc := &Doc{}
	if len(lines) > 1 && lines[0] == "---" {
		for i := 1; i < len(lines); i++ {
			if lines[i] != "---" {
				continue
			}
			fm := map[string]any{}
			if err := yaml.Unmarshal([]byte(strings.Join(lines[1:i], "\n")), &fm); err != nil {
				return nil, fmt.Errorf("vault: frontmatter: %w", err)
			}
			doc.Frontmatter = fm
			body, bodyOffset = lines[i+1:], i+1
			break
		}
	}
	doc.Links = extractLinks(body)
	doc.Chunks = chunk(body, bodyOffset)
	return doc, nil
}

// extractLinks 返回正文行中去重后的 wikilink 目标，
// 按出现顺序排列。
func extractLinks(lines []string) []string {
	var links []string
	seen := map[string]bool{}
	for _, line := range lines {
		for _, m := range wikilinkRe.FindAllStringSubmatch(line, -1) {
			target := m[1]
			if i := strings.Index(target, "|"); i >= 0 {
				target = target[:i]
			}
			if i := strings.Index(target, "#"); i >= 0 {
				target = target[:i]
			}
			target = strings.TrimSpace(target)
			if target != "" && !seen[target] {
				seen[target] = true
				links = append(links, target)
			}
		}
	}
	return links
}

// chunk 在 ATX 标题处切分正文。标题行本身开启自己的 chunk；只含空白
// 的 chunk 会被丢弃。行号会加上 frontmatter 的长度偏移，从而对应原始
// 文件。
func chunk(body []string, offset int) []Chunk {
	type heading struct {
		level int
		text  string
	}
	var chunks []Chunk
	var stack []heading
	start := 0 // 当前 chunk 第一行在 body 内的相对下标
	flush := func(end int) {
		text := strings.Join(body[start:end], "\n")
		if strings.TrimSpace(text) == "" {
			return
		}
		var path []string
		for _, h := range stack {
			path = append(path, h.text)
		}
		chunks = append(chunks, Chunk{
			Path:  path,
			Start: offset + start + 1,
			End:   offset + end,
			Text:  text,
		})
	}
	for i, line := range body {
		level, text, ok := parseHeading(line)
		if !ok {
			continue
		}
		flush(i)
		for len(stack) > 0 && stack[len(stack)-1].level >= level {
			stack = stack[:len(stack)-1]
		}
		stack = append(stack, heading{level, text})
		start = i
	}
	flush(len(body))
	return chunks
}

// parseHeading 识别 ATX 标题（"#" 到 "######" 后跟一个空格或行尾）；
// 标题文本是去掉首尾空白后的剩余部分。
func parseHeading(line string) (level int, text string, ok bool) {
	for level < len(line) && line[level] == '#' {
		level++
	}
	if level == 0 || level > 6 {
		return 0, "", false
	}
	if rest := line[level:]; rest != "" && rest[0] != ' ' && rest[0] != '\t' {
		return 0, "", false
	}
	return level, strings.TrimSpace(line[level:]), true
}
