// Package hxconfig 加载 hx.yml 并挂载声明的插件栈。
package hxconfig

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"gopkg.in/yaml.v3"
)

// Entry 是配置中的一个插件条目。
type Entry struct {
	ID       string         `yaml:"id"`
	Name     string         `yaml:"name"`
	Config   map[string]any `yaml:"config"`
	Disabled bool           `yaml:"disabled"`
}

// File 是解析后的 hx.yml：插件列表加上可选的 patch 列表。
type File struct {
	Plugins []Entry   `yaml:"plugins"`
	Patch   []Entry   `yaml:"patch"`
	Pinned  *[]string `yaml:"pinned"`
}

// defaultPinned 在 hx.yml 未声明 pinned 列表时，把会话日志和 llm 注册表
// 排除在可热重挂层之外：hx 没有持久化 HMR 机制，会话中途重挂会话日志
// 会把它丢失。
var defaultPinned = []string{"session", "llm"}

// IsPinned 报告 name 是否在配置热重挂后存活。pinned 列表由 hx.yml
// 声明；显式的空列表表示什么都不 pinned。
func (f *File) IsPinned(name string) bool {
	for _, p := range f.PinnedSet() {
		if p == name {
			return true
		}
	}
	return false
}

// PinnedSet 返回生效的 pinned 名单：未声明时用默认名单，排序后返回，
// 便于做顺序无关的集合比较。
func (f *File) PinnedSet() []string {
	pinned := defaultPinned
	if f.Pinned != nil {
		pinned = *f.Pinned
	}
	out := slices.Clone(pinned)
	slices.Sort(out)
	return out
}

// Find 解析配置文件路径：先 --config 标志，再 ./hx.yml，再
// ~/.hx/hx.yml。第二个返回值报告文件是否存在；显式指定的标志路径
// 不存在时是错误，而不是未命中。
func Find(flagPath string) (string, bool, error) {
	if flagPath != "" {
		if _, err := os.Stat(flagPath); err != nil {
			return "", false, fmt.Errorf("hxconfig: %w", err)
		}
		return flagPath, true, nil
	}
	candidates := []string{"hx.yml"}
	if home, err := os.UserHomeDir(); err == nil {
		candidates = append(candidates, filepath.Join(home, ".hx", "hx.yml"))
	}
	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path, true, nil
		}
	}
	return "", false, nil
}

// Load 解析 path，展开配置值里的 ${ENV} 引用，并应用 patch 列表。
// 遇到未知字段、缺 id 的 patch 条目、缺 name 的插入条目、以及缺 name 的
// 生效条目时都会直接报错。
func Load(path string) (*File, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f File
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("hxconfig: parse %s: %w", path, err)
	}
	for _, e := range f.Plugins {
		expand(e.Config)
	}
	for _, e := range f.Patch {
		expand(e.Config)
	}
	if err := f.applyPatch(); err != nil {
		return nil, err
	}
	for _, e := range f.Plugins {
		if e.ID == "" {
			return nil, fmt.Errorf("hxconfig: entry missing id")
		}
		if !e.Disabled && e.Name == "" {
			return nil, fmt.Errorf("hxconfig: entry %q missing name", e.ID)
		}
	}
	return &f, nil
}

// applyPatch 把 patch 列表叠进插件列表：id 匹配的 patch 条目整体替换
// 该条目的 config（不做合并）；id 不匹配时插入一个新条目，此时它和其他
// 条目一样需要有 name。
func (f *File) applyPatch() error {
	for _, p := range f.Patch {
		if p.ID == "" {
			return fmt.Errorf("hxconfig: patch entry missing id")
		}
		found := false
		for i, e := range f.Plugins {
			if e.ID == p.ID {
				f.Plugins[i].Config = p.Config
				found = true
				break
			}
		}
		if !found {
			f.Plugins = append(f.Plugins, Entry{ID: p.ID, Name: p.Name, Config: p.Config, Disabled: p.Disabled})
		}
	}
	return nil
}

// Entries 返回生效的插件列表，跳过 disabled 的条目。
func (f *File) Entries() []Entry {
	var out []Entry
	for _, e := range f.Plugins {
		if !e.Disabled {
			out = append(out, e)
		}
	}
	return out
}

// expand 对配置树里的每个字符串应用 ${ENV} 替换。
func expand(v map[string]any) {
	for k, val := range v {
		switch val := val.(type) {
		case string:
			v[k] = os.ExpandEnv(val)
		case map[string]any:
			expand(val)
		case []any:
			for i, item := range val {
				if s, ok := item.(string); ok {
					val[i] = os.ExpandEnv(s)
				} else if m, ok := item.(map[string]any); ok {
					expand(m)
				}
			}
		}
	}
}
