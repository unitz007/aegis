// Package ctrader trades on any cTrader broker (IC Markets, Pepperstone, FxPro, …) through Spotware's cTrader Open API,
// with each trader's own OAuth access to their accounts (the platform registers one app; nobody hands over a password).
//
// The Open API is JSON messages over a WebSocket on port 5036 ({"clientMsgId", "payloadType", "payload"}), the message
// ids and fields of Spotware's protobuf definitions (github.com/spotware/openapi-proto-messages). One connection per
// environment serves every account: the application authorises once, and each account is authorised on it with its
// trader's access token. Volumes are in hundredths of a unit; quoted prices in 1/100000.
package ctrader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/unitz007/aegis/broker"
)

// Hosts are the Open API's JSON endpoints by environment (a demo account is reached only on demo, a live one on live).
var Hosts = map[string]string{
	"demo": "wss://demo.ctraderapi.com:5036",
	"live": "wss://live.ctraderapi.com:5036",
}

// Message ids (ProtoOAPayloadType, ProtoPayloadType).
const (
	tErrorRes            = 50
	tHeartbeat           = 51
	tAppAuthReq          = 2100
	tAppAuthRes          = 2101
	tAccountAuthReq      = 2102
	tAccountAuthRes      = 2103
	tNewOrderReq         = 2106
	tCancelOrderReq      = 2108
	tAmendOrderReq       = 2109
	tAmendPositionReq    = 2110
	tClosePositionReq    = 2111
	tAssetListReq        = 2112
	tAssetListRes        = 2113
	tSymbolsListReq      = 2114
	tSymbolsListRes      = 2115
	tSymbolByIDReq       = 2116
	tSymbolByIDRes       = 2117
	tTraderReq           = 2121
	tTraderRes           = 2122
	tReconcileReq        = 2124
	tReconcileRes        = 2125
	tExecutionEvent      = 2126
	tSubscribeSpotsReq   = 2127
	tSubscribeSpotsRes   = 2128
	tSpotEvent           = 2131
	tOrderErrorEvent     = 2132
	tOAErrorRes          = 2142
	tTokenInvalidated    = 2147
	tClientDisconnect    = 2148
	tAccountsByTokenReq  = 2149
	tAccountsByTokenRes  = 2150
	tAccountDisconnected = 2164
	tRefreshTokenReq     = 2173
	tRefreshTokenRes     = 2174
	tUnrealizedPnLReq    = 2187
	tUnrealizedPnLRes    = 2188
)

// message is one Open API JSON message.
type message struct {
	ClientMsgID string          `json:"clientMsgId,omitempty"`
	PayloadType int             `json:"payloadType"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

// fields is a payload read as JSON values (numbers kept exact).
type fields map[string]any

func (m message) fields() fields {
	var f fields
	d := json.NewDecoder(strings.NewReader(string(m.Payload)))
	d.UseNumber()
	_ = d.Decode(&f)
	if f == nil {
		f = fields{}
	}
	return f
}

// APIError is the Open API's "no": its error code (ProtoErrorCode / ProtoOAErrorCode by name) and description.
type APIError struct {
	Code        string
	Description string
}

func (e *APIError) Error() string {
	if e.Description != "" {
		return "ctrader: " + e.Code + ": " + e.Description
	}
	return "ctrader: " + e.Code
}

// errAuth codes mean the account's access is gone: its token must be refreshed or the trader must sign in again.
var authCodes = map[string]bool{"OA_AUTH_TOKEN_EXPIRED": true, "CH_ACCESS_TOKEN_INVALID": true, "ACCOUNT_NOT_AUTHORIZED": true,
	"CH_CTID_TRADER_ACCOUNT_NOT_FOUND": true, "RET_NO_SUCH_LOGIN": true, "RET_ACCOUNT_DISABLED": true}

// ErrAccess is an account whose access was refused or revoked (the caller refreshes the token or asks to sign in again).
var ErrAccess = fmt.Errorf("ctrader: %w", broker.ErrAccessGone)

func errorOf(m message) error {
	f := m.fields()
	code := str(f["errorCode"])
	if code == "" {
		code = "UNKNOWN_ERROR"
	}
	e := &APIError{Code: code, Description: str(f["description"])}
	if authCodes[code] {
		return fmt.Errorf("%w: %v", ErrAccess, e)
	}
	return e
}

// conn is one connection to an environment, shared by every account on it.
type conn struct {
	url, clientID, secret string
	dialer                *websocket.Dialer

	mu      sync.Mutex
	ws      *websocket.Conn
	gen     int                     // bumped on every new connection
	authed  map[int64]string        // accounts authorised on this connection, with the token they were authorised with
	pending map[string]chan message // requests waiting for their answers, by clientMsgId
	quotes  map[[2]int64]quote      // last bid and ask by account and symbol
	subbed  map[[2]int64]bool       // spot subscriptions on this connection
	waiters map[[2]int64][]chan struct{}

	wmu sync.Mutex // one writer at a time
	seq atomic.Int64
}

type quote struct {
	bid, ask float64
	at       time.Time
}

var (
	poolMu sync.Mutex
	pool   = map[string]*conn{}
)

// connFor is the shared connection to an environment for an application.
func connFor(url, clientID, secret string) *conn {
	poolMu.Lock()
	defer poolMu.Unlock()
	k := url + "\x00" + clientID
	if c := pool[k]; c != nil {
		c.secret = secret
		return c
	}
	c := &conn{url: url, clientID: clientID, secret: secret, dialer: websocket.DefaultDialer}
	pool[k] = c
	return c
}

// ensure connects and authorises the application, when not connected.
func (c *conn) ensure(ctx context.Context) error {
	c.mu.Lock()
	if c.ws != nil {
		c.mu.Unlock()
		return nil
	}
	ws, _, err := c.dialer.DialContext(ctx, c.url, nil)
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("ctrader: connecting: %w", err)
	}
	c.ws, c.gen = ws, c.gen+1
	c.authed, c.pending = map[int64]string{}, map[string]chan message{}
	c.quotes, c.subbed, c.waiters = map[[2]int64]quote{}, map[[2]int64]bool{}, map[[2]int64][]chan struct{}{}
	gen := c.gen
	c.mu.Unlock()
	go c.read(ws, gen)
	go c.beat(ws, gen)
	if _, err := c.ask(ctx, tAppAuthReq, map[string]any{"clientId": c.clientID, "clientSecret": c.secret}, tAppAuthRes); err != nil {
		c.drop(ws, gen)
		return fmt.Errorf("ctrader: the application wasn't authorised: %w", err)
	}
	return nil
}

// drop forgets a connection that failed: whatever was waiting on it is told.
func (c *conn) drop(ws *websocket.Conn, gen int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gen != gen || c.ws == nil {
		return
	}
	_ = ws.Close()
	c.ws = nil
	for id, ch := range c.pending {
		close(ch)
		delete(c.pending, id)
	}
	for k, ws := range c.waiters {
		for _, w := range ws {
			close(w)
		}
		delete(c.waiters, k)
	}
}

// beat keeps the connection open: the Open API closes one idle for 30 seconds.
func (c *conn) beat(ws *websocket.Conn, gen int) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for range t.C {
		c.mu.Lock()
		live := c.gen == gen && c.ws != nil
		c.mu.Unlock()
		if !live {
			return
		}
		if err := c.write(ws, message{PayloadType: tHeartbeat}); err != nil {
			c.drop(ws, gen)
			return
		}
	}
}

func (c *conn) write(ws *websocket.Conn, m message) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_ = ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return ws.WriteJSON(m)
}

// read hands each message to whoever waits for it: an answer by its clientMsgId (an execution of an order also by the
// clientOrderId it was placed with), a price to the quotes.
func (c *conn) read(ws *websocket.Conn, gen int) {
	defer c.drop(ws, gen)
	for {
		var m message
		if err := ws.ReadJSON(&m); err != nil {
			return
		}
		switch m.PayloadType {
		case tHeartbeat:
			continue
		case tSpotEvent:
			c.spot(m.fields())
			continue
		case tClientDisconnect:
			log.Printf("ctrader: the Open API closed the connection: %s", str(m.fields()["reason"]))
			return
		case tTokenInvalidated, tAccountDisconnected:
			f := m.fields()
			c.mu.Lock()
			for _, id := range ints(f["ctidTraderAccountIds"]) {
				delete(c.authed, id)
			}
			if id := i64(f["ctidTraderAccountId"]); id != 0 {
				delete(c.authed, id)
			}
			c.mu.Unlock()
			continue
		}
		id := m.ClientMsgID
		if id == "" && m.PayloadType == tExecutionEvent {
			if o, ok := m.fields()["order"].(map[string]any); ok {
				id = str(o["clientOrderId"])
			}
		}
		if id == "" {
			continue
		}
		c.mu.Lock()
		ch := c.pending[id]
		c.mu.Unlock()
		if ch != nil {
			select {
			case ch <- m:
			default: // nobody reading any more
			}
		}
	}
}

func (c *conn) spot(f fields) {
	acct, sym := i64(f["ctidTraderAccountId"]), i64(f["symbolId"])
	k := [2]int64{acct, sym}
	c.mu.Lock()
	defer c.mu.Unlock()
	q := c.quotes[k]
	if b := i64(f["bid"]); b > 0 {
		q.bid = float64(b) / 1e5
	}
	if a := i64(f["ask"]); a > 0 {
		q.ask = float64(a) / 1e5
	}
	q.at = time.Now()
	c.quotes[k] = q
	if q.bid > 0 && q.ask > 0 {
		for _, w := range c.waiters[k] {
			close(w)
		}
		delete(c.waiters, k)
	}
}

// send writes a request and returns the channel its answers arrive on.
func (c *conn) send(ctx context.Context, typ int, payload any) (string, chan message, func(), error) {
	if err := c.ensure(ctx); err != nil {
		return "", nil, nil, err
	}
	id := "k" + strconv.FormatInt(c.seq.Add(1), 36)
	if p, ok := payload.(map[string]any); ok && typ == tNewOrderReq {
		p["clientOrderId"] = id // its executions find their way back by it too
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", nil, nil, err
	}
	ch := make(chan message, 16)
	c.mu.Lock()
	ws := c.ws
	if ws == nil {
		c.mu.Unlock()
		return "", nil, nil, errors.New("ctrader: not connected")
	}
	gen := c.gen
	c.pending[id] = ch
	c.mu.Unlock()
	done := func() {
		c.mu.Lock()
		if c.pending[id] == ch {
			delete(c.pending, id)
		}
		c.mu.Unlock()
	}
	if err := c.write(ws, message{ClientMsgID: id, PayloadType: typ, Payload: raw}); err != nil {
		done()
		c.drop(ws, gen)
		return "", nil, nil, fmt.Errorf("ctrader: sending: %w", err)
	}
	return id, ch, done, nil
}

// ask sends a request and waits for its answer of the type wanted (or the Open API's error).
func (c *conn) ask(ctx context.Context, typ int, payload any, want int) (fields, error) {
	_, ch, done, err := c.send(ctx, typ, payload)
	if err != nil {
		return nil, err
	}
	defer done()
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				return nil, errors.New("ctrader: the connection closed before an answer")
			}
			switch m.PayloadType {
			case tErrorRes, tOAErrorRes, tOrderErrorEvent:
				return nil, errorOf(m)
			case want:
				return m.fields(), nil
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("ctrader: no answer: %w", ctx.Err())
		}
	}
}

// authorise authorises an account on the connection with its trader's token (once per connection and token).
func (c *conn) authorise(ctx context.Context, account int64, token string) error {
	if err := c.ensure(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	ok := c.authed[account] == token
	c.mu.Unlock()
	if ok {
		return nil
	}
	if _, err := c.ask(ctx, tAccountAuthReq, map[string]any{"ctidTraderAccountId": account, "accessToken": token}, tAccountAuthRes); err != nil {
		var e *APIError
		if !(errors.As(err, &e) && e.Code == "ALREADY_LOGGED_IN") {
			return err
		}
	}
	c.mu.Lock()
	c.authed[account] = token
	c.mu.Unlock()
	return nil
}

// ── reading JSON values (numbers may come as numbers or strings, enums as numbers or names) ──

func str(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	}
	return ""
}

func i64(v any) int64 {
	switch x := v.(type) {
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return n
		}
		f, _ := x.Float64()
		return int64(f)
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		return n
	case float64:
		return int64(x)
	}
	return 0
}

func f64(v any) float64 {
	switch x := v.(type) {
	case json.Number:
		f, _ := x.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f
	case float64:
		return x
	}
	return 0
}

func ints(v any) []int64 {
	list, _ := v.([]any)
	out := make([]int64, 0, len(list))
	for _, x := range list {
		out = append(out, i64(x))
	}
	return out
}

func objs(v any) []map[string]any {
	list, _ := v.([]any)
	out := make([]map[string]any, 0, len(list))
	for _, x := range list {
		if m, ok := x.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func obj(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		m = map[string]any{}
	}
	return m
}

// enum reads an enum sent as its number or its name.
func enum(v any, names map[string]int) int {
	if s, ok := v.(string); ok {
		if n, ok := names[strings.ToUpper(strings.TrimSpace(s))]; ok {
			return n
		}
	}
	return int(i64(v))
}

var (
	sideNames      = map[string]int{"BUY": 1, "SELL": 2}
	orderTypeNames = map[string]int{"MARKET": 1, "LIMIT": 2, "STOP": 3, "STOP_LOSS_TAKE_PROFIT": 4, "MARKET_RANGE": 5, "STOP_LIMIT": 6}
	execNames      = map[string]int{"ORDER_ACCEPTED": 2, "ORDER_FILLED": 3, "ORDER_REPLACED": 4, "ORDER_CANCELLED": 5, "ORDER_EXPIRED": 6,
		"ORDER_REJECTED": 7, "ORDER_CANCEL_REJECTED": 8, "ORDER_PARTIAL_FILL": 11}
	tradingModeNames = map[string]int{"ENABLED": 0, "DISABLED_WITHOUT_PENDINGS_EXECUTION": 1, "DISABLED_WITH_PENDINGS_EXECUTION": 2, "CLOSE_ONLY_MODE": 3}
)

const (
	sideBuy  = 1
	sideSell = 2

	orderMarket = 1
	orderLimit  = 2
	orderStop   = 3

	execAccepted     = 2
	execFilled       = 3
	execReplaced     = 4
	execCancelled    = 5
	execExpired      = 6
	execRejected     = 7
	execCancelReject = 8
	execPartial      = 11
)
