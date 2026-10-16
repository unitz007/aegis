package broker

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Credentials are the login details for one account, as the broker names them
// (for example api_key, email, password).
type Credentials map[string]string

// Spec describes a broker aegis is willing to trade on: what to call it, which
// environments it has, what a login needs and how to connect.
type Spec struct {
	// Name is what callers send to choose the broker, lower case ("capital").
	Name string
	// Envs lists the environments the broker has, such as "demo" and "live".
	Envs []string
	// Fields lists every credential a login needs. A request that is missing one,
	// or carries one that isn't listed, is refused.
	Fields []string
	// Identity lists the Fields that say WHICH account this is. The rest (for
	// example a password) can change without it becoming a different account.
	Identity []string
	// New connects to the broker for one account.
	New func(env string, c Credentials) (Port, error)
}

// Registry is the set of brokers aegis will trade on. A broker that isn't
// registered can't be asked for, whatever the request says.
type Registry struct {
	mu    sync.RWMutex
	specs map[string]Spec
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{specs: make(map[string]Spec)} }

// Register adds a broker. It refuses an incomplete or duplicate spec.
func (r *Registry) Register(s Spec) error {
	s.Name = strings.ToLower(strings.TrimSpace(s.Name))
	switch {
	case s.Name == "":
		return fmt.Errorf("broker: a registered broker needs a name")
	case len(s.Envs) == 0:
		return fmt.Errorf("broker %q: needs at least one environment", s.Name)
	case len(s.Fields) == 0:
		return fmt.Errorf("broker %q: needs the credentials a login takes", s.Name)
	case len(s.Identity) == 0:
		return fmt.Errorf("broker %q: needs the credentials that identify an account", s.Name)
	case s.New == nil:
		return fmt.Errorf("broker %q: needs a way to connect", s.Name)
	}
	fields := map[string]bool{}
	for _, f := range s.Fields {
		fields[f] = true
	}
	for _, id := range s.Identity {
		if !fields[id] {
			return fmt.Errorf("broker %q: identity field %q is not one of its credentials", s.Name, id)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.specs[s.Name]; dup {
		return fmt.Errorf("broker %q is already registered", s.Name)
	}
	r.specs[s.Name] = s
	return nil
}

// Get finds a registered broker by name (case-insensitive).
func (r *Registry) Get(name string) (Spec, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.specs[strings.ToLower(strings.TrimSpace(name))]
	return s, ok
}

// Names lists the registered brokers, sorted.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.specs))
	for n := range r.specs {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// HasEnv reports whether the broker has the given environment.
func (s Spec) HasEnv(env string) bool {
	for _, e := range s.Envs {
		if e == env {
			return true
		}
	}
	return false
}
