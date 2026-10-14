package embed_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"hx/kb/embed"
)

// TestOpenAIWireFormat 钉住请求形态（POST {base}/embeddings、Bearer
// 鉴权、model+input 请求体）以及按 index 重排响应行。
func TestOpenAIWireFormat(t *testing.T) {
	var gotAuth, gotPath string
	var gotBody struct {
		Model string   `json:"model"`
		Input []string `json:"input"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("bad request body: %v", err)
		}
		// 每个输入一行，乱序（逆序）返回；输入 i 得到 [i+1, i+1]。
		var rows []string
		for i := len(gotBody.Input) - 1; i >= 0; i-- {
			rows = append(rows, fmt.Sprintf(`{"index":%d,"embedding":[%d,%d]}`, i, i+1, i+1))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[` + strings.Join(rows, ",") + `]}`))
	}))
	defer srv.Close()

	p, err := embed.New(embed.Config{BaseURL: srv.URL + "/", APIKey: "k", Model: "m"}, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	vecs, err := p.Embed(context.Background(), []string{"first", "second"})
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/embeddings" || gotAuth != "Bearer k" {
		t.Fatalf("request = %s %q; want POST /embeddings with Bearer k", gotPath, gotAuth)
	}
	if gotBody.Model != "m" || len(gotBody.Input) != 2 || gotBody.Input[0] != "first" {
		t.Fatalf("body = %+v; want model m with the two inputs in order", gotBody)
	}
	if len(vecs) != 2 || vecs[0][0] != 1 || vecs[1][0] != 2 {
		t.Fatalf("vectors = %v; want input order restored by index", vecs)
	}

	// 无 API key 即不带 Authorization 头（本地服务）。
	p, err = embed.New(embed.Config{BaseURL: srv.URL, Model: "m"}, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	gotAuth = "unset"
	if _, err := p.Embed(context.Background(), []string{"x"}); err != nil {
		t.Fatal(err)
	}
	if gotAuth != "" {
		t.Fatalf("keyless request sent Authorization %q", gotAuth)
	}
}

// TestOpenAIHTTPError 验证非 200 状态连同 body 摘要一起浮现。
func TestOpenAIHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "model not found", http.StatusInternalServerError)
	}))
	defer srv.Close()
	p, err := embed.New(embed.Config{BaseURL: srv.URL, Model: "m"}, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Embed(context.Background(), []string{"x"})
	if err == nil || !strings.Contains(err.Error(), "HTTP 500") || !strings.Contains(err.Error(), "model not found") {
		t.Fatalf("err = %v; want HTTP 500 with the body digest", err)
	}
}

// TestOpenAIEnvFallback 验证空 Config 从 HX_EMBED_* 环境变量解析。
func TestOpenAIEnvFallback(t *testing.T) {
	t.Setenv("HX_EMBED_BASE_URL", "http://127.0.0.1:1")
	t.Setenv("HX_EMBED_API_KEY", "k")
	t.Setenv("HX_EMBED_MODEL", "m")
	if _, err := embed.New(embed.Config{}, nil); err != nil {
		t.Fatalf("env fallback failed: %v", err)
	}
}

// TestOpenAIMissingFields 验证必填字段在配置与
// 环境变量都缺失时响亮报错。
func TestOpenAIMissingFields(t *testing.T) {
	t.Setenv("HX_EMBED_BASE_URL", "")
	t.Setenv("HX_EMBED_MODEL", "")
	_, err := embed.New(embed.Config{}, nil)
	if err == nil || !strings.Contains(err.Error(), "base_url") || !strings.Contains(err.Error(), "model") {
		t.Fatalf("err = %v; want missing base_url and model", err)
	}
}

func cosine(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	return dot / math.Sqrt(na) / math.Sqrt(nb)
}

// TestFake 钉住确定性契约：相同输入得到相同的
// 单位范数向量，同义词坍缩到同一方向，
// 无关文本保持接近正交。
func TestFake(t *testing.T) {
	f := embed.Fake{Dim: 64, Synonyms: map[string]string{"车辆": "car", "汽车": "car"}}
	vecs, err := f.Embed(context.Background(), []string{"车辆", "汽车", "香蕉"})
	if err != nil {
		t.Fatal(err)
	}
	if len(vecs) != 3 {
		t.Fatalf("got %d vectors; want 3", len(vecs))
	}
	for i, v := range vecs {
		if len(v) != 64 {
			t.Fatalf("vector %d dim = %d; want 64", i, len(v))
		}
		if n := math.Sqrt(cosine(v, v)); math.Abs(n-1) > 1e-5 {
			t.Fatalf("vector %d norm = %v; want 1", i, n)
		}
	}
	again, err := f.Embed(context.Background(), []string{"车辆"})
	if err != nil {
		t.Fatal(err)
	}
	if cosine(vecs[0], again[0]) != 1 {
		t.Fatal("same input embedded differently across calls")
	}
	if c := cosine(vecs[0], vecs[1]); c < 0.999 {
		t.Fatalf("synonym cosine = %v; want ~1", c)
	}
	if c := math.Abs(cosine(vecs[0], vecs[2])); c > 0.5 {
		t.Fatalf("unrelated cosine = %v; want near 0", c)
	}
}

// TestRealAPI 打一遍真实的 OpenAI 兼容端点；
// 没有 HX_EMBED_* 环境变量时自动跳过。
func TestRealAPI(t *testing.T) {
	if os.Getenv("HX_EMBED_BASE_URL") == "" || os.Getenv("HX_EMBED_MODEL") == "" {
		t.Skip("HX_EMBED_BASE_URL / HX_EMBED_MODEL not set")
	}
	p, err := embed.New(embed.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	vecs, err := p.Embed(context.Background(), []string{"hello"})
	if err != nil {
		t.Fatal(err)
	}
	if len(vecs) != 1 || len(vecs[0]) == 0 {
		t.Fatalf("vectors = %v; want one non-empty vector", vecs)
	}
}
