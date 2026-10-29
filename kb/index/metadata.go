package index

import (
	"database/sql"
	"strconv"
)

// EmbeddingChanged 报告期望的 provider/model 指纹或维度是否与
// 当前索引不一致。没有元数据的旧向量索引也必须重建。
func (x *Index) EmbeddingChanged(fingerprint string, dimension int) (bool, error) {
	var stored, storedDim string
	err := x.db.QueryRow(`SELECT value FROM metadata WHERE key = 'embedding_fingerprint'`).Scan(&stored)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}
	_ = x.db.QueryRow(`SELECT value FROM metadata WHERE key = 'embedding_dimension'`).Scan(&storedDim)
	var count int
	if err := x.db.QueryRow(`SELECT COUNT(*) FROM embeddings`).Scan(&count); err != nil {
		return false, err
	}
	if count > 0 && stored == "" {
		return true, nil
	}
	if stored != fingerprint {
		return true, nil
	}
	if dimension > 0 && storedDim != "" && storedDim != "0" && storedDim != strconv.Itoa(dimension) {
		return true, nil
	}
	return false, nil
}

// SetEmbeddingFingerprint 记录当前向量空间；维度 0 表示尚未确定。
func (x *Index) SetEmbeddingFingerprint(fingerprint string, dimension int) error {
	tx, err := x.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for key, value := range map[string]string{
		"embedding_fingerprint": fingerprint,
		"embedding_dimension":   strconv.Itoa(dimension),
	} {
		if _, err := tx.Exec(`INSERT INTO metadata(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ResetEmbedding 清空全部派生索引并记录新的向量空间，调用方随后
// 从 vault 事实源执行全量重建。
func (x *Index) ResetEmbedding(fingerprint string, dimension int) error {
	if err := x.Rebuild(nil); err != nil {
		return err
	}
	return x.SetEmbeddingFingerprint(fingerprint, dimension)
}
