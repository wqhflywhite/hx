package llm_test

import (
	"context"
	"iter"
	"testing"

	"hx/llm"
)

type stubAdapter struct{ name string }

func (a *stubAdapter) Stream(context.Context, *llm.Request) iter.Seq[llm.Chunk] { return nil }

func requireAdapter(t *testing.T, reg *llm.Registry, name string, want *stubAdapter) {
	t.Helper()
	got, ok := reg.Adapter(name)
	if !ok || got != want {
		t.Fatalf("Adapter(%q) = %v, %v; want %v, true", name, got, ok, want)
	}
}

func TestSetAdapterReplacesAndRollsBack(t *testing.T) {
	reg := llm.NewRegistry()
	v1 := &stubAdapter{name: "v1"}
	v2 := &stubAdapter{name: "v2"}
	reg.SetAdapter("provider", v1)
	rollback := reg.SetAdapter("provider", v2)

	requireAdapter(t, reg, "provider", v2)
	requireAdapter(t, reg, "", v2)
	if err := rollback(); err != nil {
		t.Fatal(err)
	}
	requireAdapter(t, reg, "provider", v1)
	requireAdapter(t, reg, "", v1)
}

func TestSetAdapterOldDisposerDoesNotDeleteReplacement(t *testing.T) {
	reg := llm.NewRegistry()
	v1 := &stubAdapter{name: "v1"}
	v2 := &stubAdapter{name: "v2"}
	v3 := &stubAdapter{name: "v3"}
	disposeV1 := reg.SetAdapter("provider", v1)
	disposeV2 := reg.SetAdapter("provider", v2)

	if err := disposeV1(); err != nil {
		t.Fatal(err)
	}
	requireAdapter(t, reg, "provider", v2)

	reg.SetAdapter("provider", v3)
	if err := disposeV2(); err != nil {
		t.Fatal(err)
	}
	requireAdapter(t, reg, "provider", v3)
}

func TestSetAdapterFirstRegistrationDisposer(t *testing.T) {
	reg := llm.NewRegistry()
	v1 := &stubAdapter{name: "v1"}
	dispose := reg.SetAdapter("provider", v1)
	if err := dispose(); err != nil {
		t.Fatal(err)
	}
	if got, ok := reg.Adapter("provider"); ok {
		t.Fatalf("Adapter after dispose = %v; want absent", got)
	}

	v2 := &stubAdapter{name: "v2"}
	reg.SetAdapter("provider", v2)
	requireAdapter(t, reg, "provider", v2)
}

func TestSetAdapterReplacementKeepsDefaultAndOrder(t *testing.T) {
	reg := llm.NewRegistry()
	firstV1 := &stubAdapter{name: "first-v1"}
	secondV1 := &stubAdapter{name: "second-v1"}
	secondV2 := &stubAdapter{name: "second-v2"}
	reg.SetAdapter("first", firstV1)
	reg.SetAdapter("second", secondV1)
	restoreDefault := reg.SetDefault("second")

	reg.SetAdapter("second", secondV2)
	requireAdapter(t, reg, "", secondV2)
	if err := restoreDefault(); err != nil {
		t.Fatal(err)
	}
	// 显式默认恢复为空后，隐式默认仍是最早注册的 first。
	requireAdapter(t, reg, "", firstV1)
}
