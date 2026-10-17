package broker

import "testing"

func okSpec(name string) Spec {
	return Spec{Name: name, Envs: []string{"demo"}, Fields: []Field{{Name: "key"}, {Name: "secret", Secret: true}}, Identity: []string{"key"},
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

func TestRegistry_DefaultsLabelsAndRefusesBadFields(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(okSpec("capital")); err != nil {
		t.Fatal(err)
	}
	spec, _ := r.Get("capital")
	if spec.Label != "capital" || spec.Fields[0].Label != "key" {
		t.Errorf("labels should default to the names: %+v", spec)
	}
	for name, s := range map[string]Spec{
		"unnamed field":  func() Spec { s := okSpec("a"); s.Fields = []Field{{Name: " "}}; return s }(),
		"repeated field": func() Spec { s := okSpec("b"); s.Fields = []Field{{Name: "key"}, {Name: "key"}}; return s }(),
	} {
		if err := r.Register(s); err == nil {
			t.Errorf("%s: expected a refusal", name)
		}
	}
	if got := spec.FieldNames(); len(got) != 2 || got[0] != "key" || got[1] != "secret" {
		t.Errorf("FieldNames = %v", got)
	}
}

func TestRegistry_ListDescribesBrokersAndHonoursTheEnvFilter(t *testing.T) {
	r := NewRegistry()
	two := okSpec("capital")
	two.Label = "Capital.com"
	two.Envs = []string{"demo", "live"}
	r.Register(two) //nolint:errcheck
	liveOnly := okSpec("zeta")
	liveOnly.Envs = []string{"live"}
	r.Register(liveOnly) //nolint:errcheck

	all := r.List(nil)
	if len(all) != 2 || all[0].Name != "capital" || all[1].Name != "zeta" {
		t.Fatalf("both brokers, sorted: %+v", all)
	}
	if all[0].Label != "Capital.com" || len(all[0].Fields) != 2 || !all[0].Fields[1].Secret {
		t.Errorf("label and fields should come through: %+v", all[0])
	}
	noLive := r.List(func(env string) bool { return env != "live" })
	if len(noLive) != 1 || noLive[0].Name != "capital" || len(noLive[0].Envs) != 1 || noLive[0].Envs[0] != "demo" {
		t.Errorf("with live off: only capital/demo, a live-only broker must disappear: %+v", noLive)
	}
	// What List hands out must not let a caller change the registry.
	all[0].Fields[0].Name = "tampered"
	if again, _ := r.Get("capital"); again.Fields[0].Name != "key" {
		t.Error("List must return a copy")
	}
}

func TestRegistry_EveryBrokerHasAWayOfConnecting(t *testing.T) {
	r := NewRegistry()
	r.Register(okSpec("a")) //nolint:errcheck
	spec, _ := r.Get("a")
	if spec.Strategy != StrategyFields {
		t.Errorf("the default way of connecting is typing in details, got %q", spec.Strategy)
	}
	odd := okSpec("b")
	odd.Strategy = "carrier-pigeon"
	if err := r.Register(odd); err == nil {
		t.Error("a way of connecting nobody can handle must be refused at registration")
	}
	if list := r.List(nil); len(list) != 1 || list[0].Strategy != StrategyFields {
		t.Errorf("the list should say how each broker connects: %+v", list)
	}
}
