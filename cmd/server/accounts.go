package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/unitz007/aegis"
	"github.com/unitz007/aegis/broker"
	"github.com/unitz007/aegis/broker/capital"
)

// accountSpec is the broker account a signed request wants to trade on. The
// caller (e.g. closed-kael) holds each user's credentials and sends them with
// the request; aegis keeps them in memory only, never on disk or in logs.
type accountSpec struct {
	Provider string `json:"provider"` // "capital"
	Env      string `json:"env"`      // "demo" | "live"
	APIKey   string `json:"api_key"`
	Email    string `json:"email"`
	Password string `json:"password"`
}

type accountEntry struct {
	exec     *aegis.Executor
	passHash [sha256.Size]byte
	used     time.Time
}

// accounts resolves per-request broker accounts into ready Executors. It keeps
// one adapter per account so Capital's session (login) is reused across trades.
type accounts struct {
	template  aegis.Executor // risk %, gates and hooks shared by every account
	allowLive bool           // master switch: live accounts are refused unless set
	maxSize   int
	newBroker func(s accountSpec) broker.Port // overridable in tests

	mu    sync.Mutex
	cache map[string]*accountEntry
}

func newAccounts(template aegis.Executor, allowLive bool) *accounts {
	return &accounts{
		template:  template,
		allowLive: allowLive,
		maxSize:   200,
		newBroker: func(s accountSpec) broker.Port { return capital.New(s.Env, s.APIKey, s.Email, s.Password) },
		cache:     make(map[string]*accountEntry),
	}
}

type execCtxKey struct{}

// executorFrom returns the per-account Executor placed in ctx by resolve.
func executorFrom(ctx context.Context) (*aegis.Executor, bool) {
	e, ok := ctx.Value(execCtxKey{}).(*aegis.Executor)
	return e, ok
}

// resolve implements webhook.AccountResolver. Errors are returned to the caller,
// so they never contain credentials.
func (a *accounts) resolve(ctx context.Context, raw json.RawMessage) (context.Context, string, error) {
	var s accountSpec
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, "", fmt.Errorf("account must be a JSON object")
	}
	s.Provider = strings.ToLower(strings.TrimSpace(s.Provider))
	s.Env = strings.ToLower(strings.TrimSpace(s.Env))
	s.APIKey, s.Email = strings.TrimSpace(s.APIKey), strings.TrimSpace(s.Email)
	if s.Provider != "capital" {
		return nil, "", fmt.Errorf("unsupported provider (only capital)")
	}
	if s.Env != "demo" && s.Env != "live" {
		return nil, "", fmt.Errorf("env must be demo or live")
	}
	if s.APIKey == "" || s.Email == "" || s.Password == "" {
		return nil, "", fmt.Errorf("api_key, email and password are required")
	}
	if s.Env == "live" && !a.allowLive {
		return nil, "", fmt.Errorf("live accounts are disabled on this server")
	}

	sum := sha256.Sum256([]byte(s.Provider + "|" + s.Env + "|" + s.APIKey + "|" + s.Email))
	key := hex.EncodeToString(sum[:8])
	passHash := sha256.Sum256([]byte(s.Password))

	a.mu.Lock()
	defer a.mu.Unlock()
	e := a.cache[key]
	if e == nil || e.passHash != passHash {
		if e == nil && len(a.cache) >= a.maxSize {
			a.evictOldestLocked()
		}
		exec := a.template // copy: shared settings, this account's broker
		exec.Broker = a.newBroker(s)
		exec.BrokerName = "capital:" + s.Env
		e = &accountEntry{exec: &exec, passHash: passHash}
		a.cache[key] = e
	}
	e.used = time.Now()
	return context.WithValue(ctx, execCtxKey{}, e.exec), key, nil
}

func (a *accounts) evictOldestLocked() {
	var oldestKey string
	var oldest time.Time
	for k, e := range a.cache {
		if oldestKey == "" || e.used.Before(oldest) {
			oldestKey, oldest = k, e.used
		}
	}
	delete(a.cache, oldestKey)
}

// check implements webhook.AccountCheckFunc: it proves the credentials work by
// reading the account, and never places an order.
func (a *accounts) check(ctx context.Context) (any, error) {
	e, ok := executorFrom(ctx)
	if !ok {
		return nil, fmt.Errorf("no account in context")
	}
	acct, err := e.Broker.Account(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"currency": acct.Currency,
		"balance":  acct.Balance,
		"env":      strings.TrimPrefix(e.BrokerName, "capital:"),
	}, nil
}
