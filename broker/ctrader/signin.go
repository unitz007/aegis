package ctrader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Signing in: a trader is sent to cTrader's own page (AuthorizeURL), grants the application access to their accounts,
// and comes back with a code that becomes their tokens (ExchangeCode). Their accounts are listed by the token
// (Accounts); the access token lasts about 30 days and is renewed with the refresh token, which doesn't run out
// (Refresh). Each person's password is only ever typed on cTrader's page.

// AuthorizeBase is cTrader's page where a trader grants an application access; TokenURL where a code becomes tokens.
var (
	AuthorizeBase = "https://id.ctrader.com/my/settings/openapi/grantingaccess/"
	TokenURL      = "https://openapi.ctrader.com/apps/token"
)

// AuthorizeURL is the page a trader signs in on: scope "trading" lets the application trade, "accounts" only look.
func AuthorizeURL(clientID, redirectURI, scope, state string) string {
	q := url.Values{"client_id": {clientID}, "redirect_uri": {redirectURI}, "scope": {scope}, "product": {"web"}}
	if state != "" {
		q.Set("state", state)
	}
	return AuthorizeBase + "?" + q.Encode()
}

// Token is a trader's access: the access token, the refresh token that renews it, and when the access runs out.
type Token struct {
	Access, Refresh string
	Expires         time.Time
}

// ExchangeCode turns the code cTrader sent back (good for a minute) into the trader's tokens.
func ExchangeCode(ctx context.Context, clientID, clientSecret, code, redirectURI string) (Token, error) {
	q := url.Values{"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {redirectURI},
		"client_id": {clientID}, "client_secret": {clientSecret}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, TokenURL+"?"+q.Encode(), nil)
	if err != nil {
		return Token{}, err
	}
	req.Header.Set("Accept", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return Token{}, fmt.Errorf("ctrader: signing in: %w", err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	var body struct {
		AccessToken  string  `json:"accessToken"`
		RefreshToken string  `json:"refreshToken"`
		ExpiresIn    float64 `json:"expiresIn"`
		ErrorCode    *string `json:"errorCode"`
		Description  *string `json:"description"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return Token{}, fmt.Errorf("ctrader: signing in: HTTP %d", res.StatusCode)
	}
	if body.ErrorCode != nil && *body.ErrorCode != "" || body.AccessToken == "" {
		why := "no access was given"
		if body.ErrorCode != nil && *body.ErrorCode != "" {
			why = *body.ErrorCode
		}
		if body.Description != nil && *body.Description != "" {
			why += ": " + *body.Description
		}
		return Token{}, fmt.Errorf("ctrader: signing in: %s", why)
	}
	return Token{Access: body.AccessToken, Refresh: body.RefreshToken, Expires: expiry(body.ExpiresIn)}, nil
}

func expiry(seconds float64) time.Time {
	if seconds <= 0 {
		seconds = 2628000 // cTrader's own default, about 30 days
	}
	return time.Now().Add(time.Duration(seconds) * time.Second).UTC()
}

// Account is one of a trader's cTrader accounts their access reaches.
type Account struct {
	ID     string // ctidTraderAccountId
	Live   bool
	Login  string // the number the trader knows it by at their broker
	Broker string // their broker's short name
}

// Accounts are the accounts a trader's access reaches, asked on env's connection (either serves the list).
func Accounts(ctx context.Context, env, clientID, clientSecret, accessToken string) ([]Account, bool, error) {
	u, ok := Hosts[env]
	if !ok {
		return nil, false, fmt.Errorf("ctrader: no %q environment", env)
	}
	c := connFor(u, clientID, clientSecret)
	f, err := c.ask(ctx, tAccountsByTokenReq, map[string]any{"accessToken": accessToken}, tAccountsByTokenRes)
	if err != nil {
		return nil, false, err
	}
	trading := enum(f["permissionScope"], map[string]int{"SCOPE_VIEW": 0, "SCOPE_TRADE": 1}) == 1
	var out []Account
	for _, a := range objs(f["ctidTraderAccount"]) {
		live, _ := a["isLive"].(bool)
		if s, ok := a["isLive"].(string); ok {
			live = strings.EqualFold(s, "true")
		}
		out = append(out, Account{ID: strconv.FormatInt(i64(a["ctidTraderAccountId"]), 10), Live: live,
			Login: strconv.FormatInt(i64(a["traderLogin"]), 10), Broker: str(a["brokerTitleShort"])})
	}
	return out, trading, nil
}

// Refresh renews a trader's access with their refresh token (asked on env's connection).
func Refresh(ctx context.Context, env, clientID, clientSecret, refreshToken string) (Token, error) {
	u, ok := Hosts[env]
	if !ok {
		return Token{}, fmt.Errorf("ctrader: no %q environment", env)
	}
	f, err := connFor(u, clientID, clientSecret).ask(ctx, tRefreshTokenReq, map[string]any{"refreshToken": refreshToken}, tRefreshTokenRes)
	if err != nil {
		return Token{}, err
	}
	t := Token{Access: str(f["accessToken"]), Refresh: str(f["refreshToken"]), Expires: expiry(f64(f["expiresIn"]))}
	if t.Access == "" {
		return Token{}, fmt.Errorf("%w: no new access was given", ErrAccess)
	}
	if t.Refresh == "" {
		t.Refresh = refreshToken
	}
	return t, nil
}
