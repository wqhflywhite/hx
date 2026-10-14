// Package embed 是 kb 插件的嵌入向量能力：Embedder 定义，
// 外加一个 OpenAI 兼容 provider 和一个用于测试的
// 确定性 fake。
package embed

import "context"

// Embedder 将文本转成稠密向量。实现必须恰好返回
// 每个输入文本一个向量、保持输入顺序，
// 且跨调用向量维度固定。
type Embedder interface {
	// Embed 嵌入文本；空输入直接嵌入为
	// 空结果，不发起任何 provider 调用。
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}
