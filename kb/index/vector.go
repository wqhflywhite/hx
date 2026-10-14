package index

import (
	"context"
	"encoding/binary"
	"math"
	"runtime"
	"sync"
)

// candidate 是混合之前的一个搜索候选。
type candidate struct {
	hit   Hit
	score float64
}

// encodeVector 把 v 按小端序打包，供 embeddings 表存储。
func encodeVector(v []float32) []byte {
	buf := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(buf[4*i:], math.Float32bits(f))
	}
	return buf
}

// decodeVector 把 embeddings 表中存储的小端序向量解包出来。
func decodeVector(buf []byte) []float32 {
	v := make([]float32, len(buf)/4)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[4*i:]))
	}
	return v
}

// cosine 返回 a 和 b 的余弦相似度；维度不一致或存在零向量时
// 返回 0。
func cosine(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / math.Sqrt(na) / math.Sqrt(nb)
}

// vectorCandidates 暴力扫描 embeddings 表，按可用 CPU
// 分片，找出与 query 余弦距离最近的 k 个 chunk。vault 的
// 规模（≤1 万 chunk）让扫描更简单也足够快；ANN 索引是后续
// 优化，不是 P2 阶段的需求。余弦非正（与查询正交或相反）的
// chunk 不是候选。
func (x *Index) vectorCandidates(ctx context.Context, query []float32, k int) ([]candidate, error) {
	rows, err := x.db.QueryContext(ctx,
		`SELECT e.file, e.idx, c.heading_path, c.text, e.vector
		 FROM embeddings e JOIN chunks c ON c.file = e.file AND c.idx = e.idx`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type record struct {
		candidate
		vector []float32
	}
	var records []record
	for rows.Next() {
		var r record
		var pathJSON string
		var blob []byte
		if err := rows.Scan(&r.hit.Path, &r.hit.Idx, &pathJSON, &r.hit.Text, &blob); err != nil {
			return nil, err
		}
		if err := decodeHeadingPath(pathJSON, &r.hit); err != nil {
			return nil, err
		}
		r.vector = decodeVector(blob)
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(records) == 0 || len(query) == 0 {
		return nil, nil
	}

	workers := runtime.NumCPU()
	if workers > len(records) {
		workers = len(records)
	}
	shard := (len(records) + workers - 1) / workers
	// 向上取整除法可能让靠后的 worker 分到空分片
	// （len(records)=25、workers=20、shard=2 时第 13 号 worker
	// 就越界了）；丢弃它们，让每个 worker 都持有非空区间。
	if w := (len(records) + shard - 1) / shard; w < workers {
		workers = w
	}
	tops := make([][]candidate, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		lo, hi := w*shard, (w+1)*shard
		if hi > len(records) {
			hi = len(records)
		}
		wg.Add(1)
		go func(w, lo, hi int) {
			defer wg.Done()
			var top []candidate
			for _, r := range records[lo:hi] {
				score := cosine(query, r.vector)
				if score <= 0 {
					continue
				}
				top = insertTop(top, candidate{hit: r.hit, score: score}, k)
			}
			tops[w] = top
		}(w, lo, hi)
	}
	wg.Wait()

	var merged []candidate
	for _, top := range tops {
		for _, cand := range top {
			merged = insertTop(merged, cand, k)
		}
	}
	return merged, nil
}

// insertTop 保留分数最高的 k 个候选，按分数降序（最优在
// 前）；溢出时丢弃尾部最弱的。path/idx 决胜规则保证跨分片
// 顺序确定。
func insertTop(top []candidate, cand candidate, k int) []candidate {
	pos := len(top)
	for pos > 0 {
		prev := top[pos-1]
		before := prev.score < cand.score ||
			(prev.score == cand.score && (prev.hit.Path > cand.hit.Path ||
				(prev.hit.Path == cand.hit.Path && prev.hit.Idx > cand.hit.Idx)))
		if !before {
			break
		}
		pos--
	}
	top = append(top, candidate{})
	copy(top[pos+1:], top[pos:])
	top[pos] = cand
	if len(top) > k {
		top = top[:k]
	}
	return top
}

// minMaxNormalize 缩放分数，使最优候选为 1、最差为 0。只有
// 一个候选、或所有候选共享同一个正分数时，都归一化为 1；全零
// 的一组保持为零。
func minMaxNormalize(cands []candidate) {
	if len(cands) == 0 {
		return
	}
	lo, hi := cands[0].score, cands[0].score
	for _, c := range cands[1:] {
		lo = math.Min(lo, c.score)
		hi = math.Max(hi, c.score)
	}
	switch {
	case hi <= 0:
		return
	case hi == lo:
		for i := range cands {
			cands[i].score = 1
		}
	default:
		for i := range cands {
			cands[i].score = (cands[i].score - lo) / (hi - lo)
		}
	}
}
