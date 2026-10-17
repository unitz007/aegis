package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/unitz007/aegis/broker"
	"github.com/unitz007/aegis/broker/capital"
	"github.com/unitz007/aegis/sealed"
)

// brokerRegistry is the list of brokers aegis will trade on. A request can only
// name a broker that is listed here; adding a broker means adding it here.
func brokerRegistry() (*broker.Registry, error) {
	r := broker.NewRegistry()
	if err := r.Register(broker.Spec{
		Name:  "capital",
		Label: "Capital.com",
		Envs:  []string{"demo", "live"},
		Fields: []broker.Field{
			{Name: "api_key", Label: "API key", Secret: true, Help: "Create one in Capital.com under Settings → API integrations."},
			{Name: "email", Label: "Account email"},
			{Name: "password", Label: "Password", Secret: true},
		},
		Identity: []string{"api_key", "email"},
		New: func(env string, c broker.Credentials) (broker.Port, error) {
			return capital.New(env, c["api_key"], c["email"], c["password"]), nil
		},
	}); err != nil {
		return nil, err
	}
	return r, nil
}

// loadSealKey reads aegis's private key (base64) from its setting. An empty
// value means per-request accounts are switched off.
func loadSealKey(raw string) (private, public []byte, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil, nil
	}
	private, err = base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("ACCOUNT_SEAL_KEY must be base64")
	}
	public, err = sealed.PublicKey(private)
	if err != nil {
		return nil, nil, fmt.Errorf("ACCOUNT_SEAL_KEY is not a valid key")
	}
	return private, public, nil
}

// keyHandler publishes the public key senders seal broker logins to. It is
// public on purpose: it can only be used to encrypt, never to read.
func keyHandler(public []byte) http.Handler {
	sum := sha256.Sum256(public)
	body, _ := json.Marshal(map[string]string{
		"key_id":     hex.EncodeToString(sum[:4]),
		"algorithm":  "x25519-hkdf-sha256-aes256gcm",
		"public_key": base64.StdEncoding.EncodeToString(public),
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "GET required", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
}

// brokersHandler publishes which brokers can be used and what a login for each
// needs, so a connect screen can be built from it instead of hard-coding any one
// broker. Public on purpose: it describes the options, never an account. An
// environment that is switched off on this server (live, unless allowed) is not
// listed.
func brokersHandler(reg *broker.Registry, allowLive bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "GET required", http.StatusMethodNotAllowed)
			return
		}
		list := reg.List(func(env string) bool { return env != "live" || allowLive })
		if list == nil {
			list = []broker.Info{}
		}
		body, _ := json.Marshal(map[string]any{"brokers": list})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
}
