// Package embed 是 kb 插件的嵌入向量能力：Embedder 定义，
// 外加一个 OpenAI 兼容 provider 和一个用于测试的
// 确定性 fake。
package embed

import (
	"context"
	"fmt"
)

// Embedder 将文本转成稠密向量。实现必须恰好返回
// 每个输入文本一个向量、保持输入顺序，
// 且跨调用向量维度固定。
type Embedder interface {
	// Embed 嵌入文本；空输入直接嵌入为
	// 空结果，不发起任何 provider 调用。
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}

// Fingerprint 返回 embedder 的 provider/model 身份；未显式实现的
// 自定义 embedder 至少按具体类型隔离，维度由索引另行校验。
func Fingerprint(e Embedder) string {
	if fp, ok := e.(interface{ Fingerprint() string }); ok {
		return fp.Fingerprint()
	}
	return fmt.Sprintf("%T", e)
}
