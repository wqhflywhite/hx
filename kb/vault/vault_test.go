package vault_test

import (
	"reflect"
	"testing"

	"hx/kb/vault"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name string
		data string
		fm   map[string]any
		want []vault.Chunk
	}{
		{
			name: "no frontmatter",
			data: "plain note\nsecond line\n",
			fm:   nil,
			want: []vault.Chunk{
				{Path: nil, Start: 1, End: 3, Text: "plain note\nsecond line\n"},
			},
		},
		{
			name: "frontmatter offsets chunk line numbers",
			data: "---\ntags: [go, kb]\ntitle: 笔记\n---\n# 实验方法\nbody\n",
			fm:   map[string]any{"tags": []any{"go", "kb"}, "title": "笔记"},
			want: []vault.Chunk{
				{Path: []string{"实验方法"}, Start: 5, End: 7, Text: "# 实验方法\nbody\n"},
			},
		},
		{
			name: "unterminated frontmatter stays body",
			data: "---\ntags: [go]\n# 标题\n",
			fm:   nil,
			want: []vault.Chunk{
				{Path: nil, Start: 1, End: 2, Text: "---\ntags: [go]"},
				{Path: []string{"标题"}, Start: 3, End: 4, Text: "# 标题\n"},
			},
		},
		{
			name: "nested headings stack and pop",
			data: "# A\na\n## B\nb\n### C\nc\n## D\nd\n# E\ne\n",
			want: []vault.Chunk{
				{Path: []string{"A"}, Start: 1, End: 2, Text: "# A\na"},
				{Path: []string{"A", "B"}, Start: 3, End: 4, Text: "## B\nb"},
				{Path: []string{"A", "B", "C"}, Start: 5, End: 6, Text: "### C\nc"},
				{Path: []string{"A", "D"}, Start: 7, End: 8, Text: "## D\nd"},
				{Path: []string{"E"}, Start: 9, End: 11, Text: "# E\ne\n"},
			},
		},
		{
			name: "preamble before first heading",
			data: "intro\n\n# H\nbody\n",
			want: []vault.Chunk{
				{Path: nil, Start: 1, End: 2, Text: "intro\n"},
				{Path: []string{"H"}, Start: 3, End: 5, Text: "# H\nbody\n"},
			},
		},
		{
			name: "heading needs a space after the fence",
			data: "#notheading\n# real\n",
			want: []vault.Chunk{
				{Path: nil, Start: 1, End: 1, Text: "#notheading"},
				{Path: []string{"real"}, Start: 2, End: 3, Text: "# real\n"},
			},
		},
		{
			name: "crlf normalizes",
			data: "# H\r\nbody\r\n",
			want: []vault.Chunk{
				{Path: []string{"H"}, Start: 1, End: 3, Text: "# H\nbody\n"},
			},
		},
		{
			name: "whitespace-only sections dropped",
			data: "\n\n# H\n\n\n",
			want: []vault.Chunk{
				{Path: []string{"H"}, Start: 3, End: 6, Text: "# H\n\n\n"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc, err := vault.Parse([]byte(tt.data))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(doc.Frontmatter, tt.fm) {
				t.Fatalf("frontmatter = %#v; want %#v", doc.Frontmatter, tt.fm)
			}
			if !reflect.DeepEqual(doc.Chunks, tt.want) {
				t.Fatalf("chunks =\n%#v\nwant\n%#v", doc.Chunks, tt.want)
			}
		})
	}
}

func TestParseMalformedFrontmatter(t *testing.T) {
	if _, err := vault.Parse([]byte("---\n: bad: yaml: [\n---\nbody\n")); err == nil {
		t.Fatal("malformed frontmatter accepted")
	}
}

func TestWikilinks(t *testing.T) {
	tests := []struct {
		name  string
		data  string
		links []string
	}{
		{"plain target", "see [[目标]]\n", []string{"目标"}},
		{"alias stripped", "see [[目标|别名]]\n", []string{"目标"}},
		{"anchor stripped", "see [[目标#锚点]] and [[目标#锚点|别名]]\n", []string{"目标"}},
		{"alias with anchor", "[[a#b|c]]\n", []string{"a"}},
		{"dedup in order", "[[b]] [[a]] [[b]]\n", []string{"b", "a"}},
		{"multiple per line", "[[x]] and [[y|why]]\n", []string{"x", "y"}},
		{"empty target skipped", "[[|alias]] [[ # ]] [[z]]\n", []string{"z"}},
		{"none", "no links here\n", nil},
		{"unclosed ignored", "[[not a link\n", nil},
		{"links in frontmatter ignored", "---\nref: [[fm-link]]\n---\n[[body-link]]\n", []string{"body-link"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc, err := vault.Parse([]byte(tt.data))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(doc.Links, tt.links) {
				t.Fatalf("links = %v; want %v", doc.Links, tt.links)
			}
		})
	}
}
