package prompt_test

import (
	"testing"

	"hx/core/fiber"
	"hx/prompt"
)

func textSection(name string, order int, text string) prompt.Section {
	return prompt.Section{
		Name:   name,
		Order:  order,
		Render: func(*fiber.Context) (string, error) { return text, nil },
	}
}

// TestAssembleDeterministicOrder 钉住拼接的全序语义：同 Order 的
// 分区按 Name 决出先后，不同实例、不同注册顺序下输出逐字节一致
// （map 遍历顺序与不稳定排序都不得影响结果）。
func TestAssembleDeterministicOrder(t *testing.T) {
	secs := []prompt.Section{
		textSection("charlie", 1, "C"),
		textSection("alpha", 1, "A"),
		textSection("bravo", 1, "B"),
		textSection("intro", 0, "I"),
		textSection("outro", 2, "O"),
	}
	// (Order, Name) 全序的精确期望：intro(0) → alpha → bravo →
	// charlie(1) → outro(2)，以 "\n\n" 拼接。
	want := "I\n\nA\n\nB\n\nC\n\nO"

	// 正序、逆序、交错三种注册顺序；每种都新建实例组装，且重复
	// 多轮以覆盖 map 遍历的随机性。
	registrations := [][]int{
		{0, 1, 2, 3, 4},
		{4, 3, 2, 1, 0},
		{2, 0, 4, 1, 3},
	}
	for round := 0; round < 50; round++ {
		for _, order := range registrations {
			p := prompt.New()
			for _, i := range order {
				d := p.Register(secs[i])
				defer d()
			}
			root := fiber.New()
			text, _, err := p.Assemble(root.Context())
			root.Dispose()
			if err != nil {
				t.Fatal(err)
			}
			if text != want {
				t.Fatalf("round %d order %v: text = %q; want %q", round, order, text, want)
			}
		}
	}
}
