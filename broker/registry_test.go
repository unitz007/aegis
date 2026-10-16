package broker

import "testing"

func okSpec(name string) Spec {
	return Spec{Name: name, Envs: []string{"demo"}, Fields: []string{"key", "secret"}, Identity: []string{"key"},
		New: func(string, Credentials) (Port, error) { return nil, nil }}
}

func TestRegistry_RegisterAndFind(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(okSpec(" Capital ")); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Get("CAPITAL"); !ok {
		t.Error("lookup should ignore case and spaces")
	}
	if _, ok := r.Get("oanda"); ok {
		t.Error("a broker that was never registered must not be found")
	}
	if got := r.Names(); len(got) != 1 || got[0] != "capital" {
		t.Errorf("names = %v", got)
	}
}

func TestRegistry_RefusesBadSpecs(t *testing.T) {
	r := NewRegistry()
	r.Register(okSpec("capital")) //nolint:errcheck
	cases := map[string]Spec{
		"duplicate":        okSpec("capital"),
		"no name":          func() Spec { s := okSpec(""); return s }(),
		"no environments":  func() Spec { s := okSpec("a"); s.Envs = nil; return s }(),
		"no credentials":   func() Spec { s := okSpec("b"); s.Fields = nil; return s }(),
		"no identity":      func() Spec { s := okSpec("c"); s.Identity = nil; return s }(),
		"identity unknown": func() Spec { s := okSpec("d"); s.Identity = []string{"nope"}; return s }(),
		"cannot connect":   func() Spec { s := okSpec("e"); s.New = nil; return s }(),
	}
	for name, s := range cases {
		if err := r.Register(s); err == nil {
			t.Errorf("%s: expected a refusal", name)
		}
	}
}

func TestSpec_HasEnv(t *testing.T) {
	s := okSpec("x")
	if !s.HasEnv("demo") || s.HasEnv("live") {
		t.Error("HasEnv wrong")
	}
}
