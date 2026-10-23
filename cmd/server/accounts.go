package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/unitz007/aegis"
	"github.com/unitz007/aegis/broker"
	"github.com/unitz007/aegis/sealed"
	"github.com/unitz007/aegis/transport/webhook"
)

// accountSpec is the broker account a signed request wants to trade on.
//
// The sender names the broker, which must be one aegis has registered, and the
// environment. The login details travel sealed to aegis's public key, so they
// are never readable in the request body, in logs or by anything in between.
// aegis opens them here, in memory only, and never stores or logs them.
type accountSpec struct {
	Broker      string `json:"broker"`      // a registered broker, e.g. "capital"
	Env         string `json:"env"`         // one of that broker's environments, e.g. "demo" | "live"
	Credentials string `json:"credentials"` // base64 of the login details sealed to aegis's public key
}

// sealContext ties a sealed login to the broker and environment it was sealed
// for, so demo details can never be replayed against live, or one broker's
// against another.
func sealContext(brokerName, env string) []byte {
	return []byte("aegis/account/v1|" + brokerName + "|" + env)
}

type accountEntry struct {
	exec     *aegis.Executor
	credHash [sha256.Size]byte
	used     time.Time
}

// accounts resolves per-request broker accounts into ready Executors. It keeps
// one connection per account so the broker session (login) is reused across trades.
type accounts struct {
	template  aegis.Executor // gates and hooks shared by every account
	registry  *broker.Registry
	private   []byte // aegis's private key, for opening sealed logins
	allowLive bool   // master switch: live accounts are refused unless set
	maxSize   int

	mu    sync.Mutex
	cache map[string]*accountEntry
}

func newAccounts(template aegis.Executor, registry *broker.Registry, private []byte, allowLive bool) *accounts {
	return &accounts{
		template:  template,
		registry:  registry,
		private:   private,
		allowLive: allowLive,
		maxSize:   200,
		cache:     make(map[string]*accountEntry),
	}
}

type execCtxKey struct{}

// executorFrom returns the per-account Executor placed in ctx by resolve.
func executorFrom(ctx context.Context) (*aegis.Executor, bool) {
	e, ok := ctx.Value(execCtxKey{}).(*aegis.Executor)
	return e, ok
}

// resolve implements webhook.AccountResolver. Errors go back to the caller, so
// they never contain credentials.
func (a *accounts) resolve(ctx context.Context, raw json.RawMessage) (context.Context, string, error) {
	var s accountSpec
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields() // a stray plaintext field (api_key, password…) is refused, not ignored
	if err := dec.Decode(&s); err != nil {
		return nil, "", fmt.Errorf("account must be {broker, env, credentials}")
	}
	s.Broker = strings.ToLower(strings.TrimSpace(s.Broker))
	s.Env = strings.ToLower(strings.TrimSpace(s.Env))

	spec, ok := a.registry.Get(s.Broker)
	if !ok {
		return nil, "", fmt.Errorf("broker is not registered (available: %s)", strings.Join(a.registry.Names(), ", "))
	}
	if !spec.HasEnv(s.Env) {
		return nil, "", fmt.Errorf("env must be one of: %s", strings.Join(spec.Envs, ", "))
	}
	if s.Env == "live" && !a.allowLive {
		return nil, "", fmt.Errorf("live accounts are disabled on this server")
	}
	if s.Credentials == "" {
		return nil, "", fmt.Errorf("credentials are required (sealed to aegis's public key)")
	}

	blob, err := base64.StdEncoding.DecodeString(s.Credentials)
	if err != nil {
		return nil, "", fmt.Errorf("credentials must be base64")
	}
	plain, err := sealed.Open(a.private, sealContext(s.Broker, s.Env), blob)
	if err != nil {
		return nil, "", fmt.Errorf("credentials could not be opened (sealed for another key, broker or env?)")
	}
	creds, err := parseCredentials(spec, plain)
	if err != nil {
		return nil, "", err
	}

	// The key names the account without containing any secret. It also namespaces
	// duplicate-signal protection, so two accounts never collide on a signal id.
	key := accountKey(spec, s.Env, creds)
	credHash := hashCredentials(creds)

	a.mu.Lock()
	defer a.mu.Unlock()
	e := a.cache[key]
	if e == nil || e.credHash != credHash {
		if e == nil && len(a.cache) >= a.maxSize {
			a.evictOldestLocked()
		}
		port, err := spec.New(s.Env, creds)
		if err != nil {
			return nil, "", fmt.Errorf("could not connect to %s", spec.Name)
		}
		exec := a.template // copy: shared gates, this account's broker
		exec.Broker = port
		exec.BrokerName = spec.Name + ":" + s.Env
		e = &accountEntry{exec: &exec, credHash: credHash}
		a.cache[key] = e
	}
	e.used = time.Now()
	return context.WithValue(ctx, execCtxKey{}, e.exec), key, nil
}

// parseCredentials reads the opened details and holds them to what the broker
// asks for: every field present, nothing extra.
func parseCredentials(spec broker.Spec, plain []byte) (broker.Credentials, error) {
	var creds broker.Credentials
	dec := json.NewDecoder(bytes.NewReader(plain))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&creds); err != nil {
		return nil, fmt.Errorf("credentials must be a JSON object of the broker's login fields")
	}
	allowed := map[string]bool{}
	for _, f := range spec.FieldNames() {
		allowed[f] = true
		if strings.TrimSpace(creds[f]) == "" {
			return nil, fmt.Errorf("credentials need: %s", strings.Join(spec.FieldNames(), ", "))
		}
	}
	for f := range creds {
		if !allowed[f] {
			return nil, fmt.Errorf("credentials need: %s", strings.Join(spec.FieldNames(), ", "))
		}
		creds[f] = strings.TrimSpace(creds[f])
	}
	return creds, nil
}

func accountKey(spec broker.Spec, env string, creds broker.Credentials) string {
	parts := []string{spec.Name, env}
	for _, f := range spec.Identity {
		parts = append(parts, creds[f])
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:8])
}

// hashCredentials covers every field, so a changed password rebuilds the
// connection while the account (and its key) stays the same.
func hashCredentials(creds broker.Credentials) [sha256.Size]byte {
	names := make([]string, 0, len(creds))
	for k := range creds {
		names = append(names, k)
	}
	sort.Strings(names)
	h := sha256.New()
	for _, k := range names {
		fmt.Fprintf(h, "%s=%d:%s;", k, len(creds[k]), creds[k])
	}
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
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

// status implements webhook.AccountCheckFunc for /account/status: what is open on the account right
// now, read from the broker. It never places, changes or cancels anything, and it decides nothing:
// it only reports what the broker says.
func (a *accounts) status(ctx context.Context) (any, error) {
	e, ok := executorFrom(ctx)
	if !ok {
		return nil, fmt.Errorf("no account in context")
	}
	acct, err := e.Broker.Account(ctx)
	if err != nil {
		return nil, err
	}
	trades, err := e.Broker.OpenTrades(ctx)
	if err != nil {
		return nil, err
	}
	orders, err := e.Broker.OpenOrders(ctx)
	if err != nil {
		return nil, err
	}

	openTrades := make([]map[string]any, 0, len(trades))
	for _, t := range trades {
		openTrades = append(openTrades, map[string]any{
			"symbol": t.Symbol, "direction": directionOf(t.Units), "units": absInt(t.Units),
			"open_price": t.OpenPrice, "current_price": t.CurrentPrice, "unrealized_pnl": t.UnrealizedPnL,
			"stop_loss": t.StopLoss, "take_profit": t.TakeProfit, "opened_at": t.OpenedAt,
		})
	}
	pending := make([]map[string]any, 0, len(orders))
	for _, o := range orders {
		dir := strings.ToUpper(o.Direction)
		if dir != "BUY" && dir != "SELL" {
			dir = directionOf(o.Units)
		}
		pending = append(pending, map[string]any{
			"symbol": o.Symbol, "direction": dir, "units": absInt(o.Units), "order_type": strings.ToLower(o.OrderType),
			"level": o.Level, "stop_loss": o.StopLoss, "take_profit": o.TakeProfit, "created_at": o.CreatedAt,
		})
	}
	return map[string]any{
		"env":              e.BrokerName[strings.Index(e.BrokerName, ":")+1:],
		"currency":         acct.Currency,
		"balance":          acct.Balance,
		"equity":           acct.NAV,
		"unrealized_pnl":   acct.UnrealizedPnL,
		"margin_used":      acct.MarginUsed,
		"margin_available": acct.MarginAvailable,
		"open_trades":      openTrades,
		"pending_orders":   pending,
	}, nil
}

// maxMarketSymbols is how many symbols one /market/info read may ask about.
const maxMarketSymbols = 5

// market implements webhook.AccountCheckFunc for /market/info: for each symbol asked about, the broker's live bid and
// ask and its smallest and largest deal size, so a sender can work out a size within what the broker accepts. It reads
// only and decides nothing. A symbol the broker can't answer for says so on its own, without failing the others; a
// price of 0 means the broker gives none (the paper broker), and min/max of 0 mean it has no rules for the symbol.
func (a *accounts) market(ctx context.Context) (any, error) {
	e, ok := executorFrom(ctx)
	if !ok {
		return nil, fmt.Errorf("no account in context")
	}
	var req struct {
		Symbols []string `json:"symbols"`
	}
	_ = json.Unmarshal(webhook.AccountRequestBody(ctx), &req)
	if len(req.Symbols) > maxMarketSymbols {
		req.Symbols = req.Symbols[:maxMarketSymbols]
	}
	rules, hasRules := e.Broker.(broker.DealingRulesProvider)
	markets := map[string]any{}
	for _, raw := range req.Symbols {
		sym := strings.ToUpper(strings.TrimSpace(raw))
		if sym == "" {
			continue
		}
		m := map[string]any{}
		if bid, ask, err := e.Broker.LiveQuote(ctx, sym); err != nil {
			m["error"] = "no price for this symbol"
		} else {
			m["bid"], m["ask"] = bid, ask
		}
		if hasRules {
			if r, err := rules.MarketDealingRules(ctx, sym); err == nil {
				m["min_size"], m["max_size"] = r.MinDealSize, r.MaxDealSize
			}
		}
		markets[sym] = m
	}
	return map[string]any{"markets": markets}, nil
}

// directionOf says whether a signed size is a long (BUY) or a short (SELL) position.
func directionOf(units int) string {
	if units < 0 {
		return "SELL"
	}
	return "BUY"
}

func absInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
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
		"env":      e.BrokerName[strings.Index(e.BrokerName, ":")+1:],
	}, nil
}
