package embed

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"math"
	"unicode"
)

// Fake 是确定性的哈希投影 Embedder，仅供测试与评估
// 使用——它没有语义理解能力。每个 token 投影为一个
// 由其 SHA-1 派生的伪随机带符号向量，文档向量
// 是其各 token 投影之和再作 L2 归一化，因此余弦相似度
// 反映共享的 token。Synonyms 映射在投影前把 token
// 归一为其规范形式，同时兼任 CJK 分词词典（贪心最长
// 匹配），这让测试能从自然语言查询构造零关键词重叠的
// "语义"匹配（如 Synonyms{"车辆": "car",
// "汽车": "car"}）。严禁用于生产。
type Fake struct {
	// Dim 是向量维度；非正值表示 64。
	Dim int
	// Synonyms 在投影前把 token 映射为其规范形式。
	Synonyms map[string]string
}

// Embed 实现 Embedder。
func (f Fake) Embed(_ context.Context, texts []string) ([][]float32, error) {
	dim := f.Dim
	if dim <= 0 {
		dim = 64
	}
	out := make([][]float32, len(texts))
	for i, text := range texts {
		vec := make([]float32, dim)
		for _, token := range f.tokens(text) {
			project(token, vec)
		}
		normalize(vec)
		out[i] = vec
	}
	return out, nil
}

// tokens 提取拉丁字母/数字连续段（转小写），并按 Synonyms 键的
// 贪心最长匹配切分 CJK 连续段，因此自然语言文本
// （没有手工插入的空格）也能走通同义词桥；未匹配的 CJK
// 片段保留为各自的 token。同义词键映射到其规范形式，
// 其余照旧在 Synonyms 中查找。
func (f Fake) tokens(text string) []string {
	var out []string
	var latin, cjk []rune
	emit := func(tok string) {
		if canonical, ok := f.Synonyms[tok]; ok {
			tok = canonical
		}
		out = append(out, tok)
	}
	flushCJK := func() {
		for len(cjk) > 0 {
			key, n := longestKey(f.Synonyms, cjk)
			if n > 0 {
				emit(key)
				cjk = cjk[n:]
				continue
			}
			// 累积未匹配片段，直到下一个键命中处。
			i := 1
			for i < len(cjk) {
				if _, n := longestKey(f.Synonyms, cjk[i:]); n > 0 {
					break
				}
				i++
			}
			out = append(out, string(cjk[:i]))
			cjk = cjk[i:]
		}
	}
	for _, r := range text {
		switch {
		case unicode.Is(unicode.Han, r):
			if len(latin) > 0 {
				emit(string(latin))
				latin = latin[:0]
			}
			cjk = append(cjk, r)
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			flushCJK()
			latin = append(latin, unicode.ToLower(r))
		default:
			if len(latin) > 0 {
				emit(string(latin))
				latin = latin[:0]
			}
			flushCJK()
		}
	}
	if len(latin) > 0 {
		emit(string(latin))
	}
	flushCJK()
	return out
}

// longestKey 返回能作为 run 前缀的最长 Synonyms 键
// 及其 rune 长度；无键匹配时 n 为 0。
func longestKey(synonyms map[string]string, run []rune) (string, int) {
	best, bestN := "", 0
	for key := range synonyms {
		kr := []rune(key)
		if len(kr) <= bestN || len(kr) > len(run) {
			continue
		}
		if string(run[:len(kr)]) == key {
			best, bestN = key, len(kr)
		}
	}
	return best, bestN
}

// project 把 token 的带符号伪随机投影累加进 vec。
func project(token string, vec []float32) {
	sum := sha1.Sum([]byte(token))
	seed := binary.LittleEndian.Uint64(sum[:8])
	for i := range vec {
		// xorshift64；最低位决定维度 i 的符号。
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		if seed&1 == 0 {
			vec[i] += 1
		} else {
			vec[i] -= 1
		}
	}
}

// normalize 把 vec 缩放到单位长度；零向量保持为零。
func normalize(vec []float32) {
	var norm float64
	for _, v := range vec {
		norm += float64(v) * float64(v)
	}
	norm = math.Sqrt(norm)
	if norm == 0 {
		return
	}
	for i := range vec {
		vec[i] /= float32(norm)
	}
}
