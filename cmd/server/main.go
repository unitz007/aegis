// Command server is the aegis standalone live trade-execution service.
//
// It wires a Capital.com or paper broker, an Executor, a Gateway, and two
// HTTP transports (webhook + MCP Streamable-HTTP) onto a standard http.ServeMux.
//
// Environment variables:
//
//	PORT                   listen port (default 8080)
//	CAPITAL_ENV            "demo" | "live" (default "demo")
//	CAPITAL_API_KEY        Capital.com API key   — required when BROKER_MODE=capital
//	CAPITAL_EMAIL          Capital.com account email — required when BROKER_MODE=capital
//	CAPITAL_PASSWORD       Capital.com account password — required when BROKER_MODE=capital
//	WEBHOOK_SIGNING_SECRET HMAC-SHA256 secret for the webhook transport
//	MCP_AUTH_TOKEN         Bearer token for the MCP transport
//	BROKER_MODE            "paper" | "capital" (DEFAULT "paper" — live-money master switch)
//	ALLOWED_SOURCES        comma-separated source tags accepted by the Gateway (default "finos")
//	MIN_RR                 minimum reward:risk floor (default 2.0)
//	RISK_PCT               risk fraction of account per trade (default 0.01 = 1%)
//	MIN_CONFIDENCE         minimum LLM confidence for non-algo pairs (default 45)
//
// Safety invariants:
//   - BROKER_MODE defaults to "paper" — live money requires an explicit opt-in.
//   - If BROKER_MODE=capital but any credential is empty → log.Fatal (fail-closed;
//     never silently fall back to paper).
//   - A loud "LIVE MONEY MODE" log is emitted when BROKER_MODE=capital + CAPITAL_ENV=live.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/unitz007/aegis"
	"github.com/unitz007/aegis/broker"
	"github.com/unitz007/aegis/broker/capital"
	"github.com/unitz007/aegis/broker/paper"
	aegismcp "github.com/unitz007/aegis/transport/mcp"
	"github.com/unitz007/aegis/transport/webhook"
)

func main() {
	// ── Environment ──────────────────────────────────────────────────────────────
	port := getenvOrDefault("PORT", "8080")
	capitalEnv := getenvOrDefault("CAPITAL_ENV", "demo")
	capitalAPIKey := os.Getenv("CAPITAL_API_KEY")
	capitalEmail := os.Getenv("CAPITAL_EMAIL")
	capitalPassword := os.Getenv("CAPITAL_PASSWORD")
	webhookSecret := os.Getenv("WEBHOOK_SIGNING_SECRET")
	mcpAuthToken := os.Getenv("MCP_AUTH_TOKEN")
	brokerMode := strings.ToLower(getenvOrDefault("BROKER_MODE", "paper"))
	allowedSourcesRaw := getenvOrDefault("ALLOWED_SOURCES", "finos")
	minRRStr := getenvOrDefault("MIN_RR", "2.0")
	riskPctStr := getenvOrDefault("RISK_PCT", "0.01")
	minConfidenceStr := getenvOrDefault("MIN_CONFIDENCE", "45")

	minRR, err := strconv.ParseFloat(minRRStr, 64)
	if err != nil || minRR <= 0 {
		log.Fatalf("server: invalid MIN_RR=%q (must be a positive float)", minRRStr)
	}
	riskPct, err := strconv.ParseFloat(riskPctStr, 64)
	if err != nil || riskPct <= 0 {
		log.Fatalf("server: invalid RISK_PCT=%q (must be a positive float)", riskPctStr)
	}
	minConfidence, err := strconv.Atoi(minConfidenceStr)
	if err != nil || minConfidence < 0 {
		log.Fatalf("server: invalid MIN_CONFIDENCE=%q (must be a non-negative integer)", minConfidenceStr)
	}

	allowedSources := splitNonEmpty(allowedSourcesRaw)
	if len(allowedSources) == 0 {
		log.Fatal("server: ALLOWED_SOURCES must contain at least one source tag")
	}

	// ── Startup banner (never log secrets) ───────────────────────────────────────
	log.Printf("server: starting aegis-exec")
	log.Printf("server: broker_mode=%s capital_env=%s allowed_sources=%v min_rr=%.2f risk_pct=%.4f min_confidence=%d",
		brokerMode, capitalEnv, allowedSources, minRR, riskPct, minConfidence)

	// ── Broker selection — fail-closed ───────────────────────────────────────────
	var brokerImpl broker.Port
	var brokerName string

	switch brokerMode {
	case "paper":
		brokerImpl = paper.New(100_000, nil)
		brokerName = "paper"
		log.Printf("server: broker=paper (simulation mode — no real orders)")

	case "capital":
		// FAIL-CLOSED: all three credentials must be present.
		if capitalAPIKey == "" || capitalEmail == "" || capitalPassword == "" {
			log.Fatal("server: BROKER_MODE=capital requires CAPITAL_API_KEY, CAPITAL_EMAIL, and CAPITAL_PASSWORD — refusing to start with missing credentials")
		}
		brokerImpl = capital.New(capitalEnv, capitalAPIKey, capitalEmail, capitalPassword)
		brokerName = "capital"
		if strings.ToLower(capitalEnv) == "live" {
			log.Printf("server: !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
			log.Printf("server: !!  LIVE MONEY MODE — CAPITAL.COM LIVE   !!")
			log.Printf("server: !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
		} else {
			log.Printf("server: broker=capital env=%s", capitalEnv)
		}

	default:
		log.Fatalf("server: unknown BROKER_MODE=%q — must be 'paper' or 'capital'", brokerMode)
	}

	// ── Executor ──────────────────────────────────────────────────────────────────
	exec := &aegis.Executor{
		Broker:        brokerImpl,
		BrokerName:    brokerName,
		RiskPct:       riskPct,
		MinConfidence: minConfidence,
		SessionGate:   StockAISessionGate,
		Journal:       nil, // finOS owns the journal in remote mode
		// IsAlgoPair uses aegis.DefaultIsAlgoPair (forex = algo, crypto/stocks = ai).
		// QuoteConverter uses aegis.DefaultQuoteConverter (correct for USD-leg pairs;
		// static fallback table for cross pairs).
	}

	// ── Gateway ───────────────────────────────────────────────────────────────────
	placeFunc := func(ctx context.Context, intent aegis.OrderIntent, decisionID string) (string, bool, error) {
		return exec.ExecuteWithResult(ctx, intent, decisionID)
	}

	gw, err := aegis.NewGateway(placeFunc, aegis.GatewayConfig{
		AllowedSources:     allowedSources,
		MinRR:              minRR,
		SymbolClassifier:   aegis.ClassifySymbolDefault,
		ExecutabilityCheck: aegis.DefaultExecutability,
	})
	if err != nil {
		log.Fatalf("server: NewGateway: %v", err)
	}

	// ── HTTP transports ───────────────────────────────────────────────────────────
	mux := http.NewServeMux()

	// /signal/trade — HMAC-signed webhook POST
	whHandler := webhook.Handler(
		webhookSecret,
		gw,
		webhook.WithSourceTag(allowedSources[0]),
		webhook.WithDefaultStrategy("smc_engine"),
		webhook.WithTimestampTolerance(60*time.Second),
	)
	mux.Handle("/signal/trade", whHandler)

	// /mcp/ — MCP Streamable-HTTP bearer-auth
	mcpHandler := aegismcp.Handler(
		mcpAuthToken,
		gw,
		log.Default(),
		aegismcp.WithSourceTag(allowedSources[0]),
		aegismcp.WithDefaultStrategy("smc_engine"),
	)
	mux.Handle("/mcp/", mcpHandler)

	// /healthz — lightweight liveness probe (no broker call, no auth required)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"mode":        "ok",
			"broker_mode": brokerMode,
		})
	})

	// ── HTTP server with graceful shutdown ────────────────────────────────────────
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("server: listening on :%s", port)
		if listenErr := srv.ListenAndServe(); listenErr != nil && listenErr != http.ErrServerClosed {
			log.Fatalf("server: ListenAndServe: %v", listenErr)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Printf("server: shutdown signal received — draining connections")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if shutdownErr := srv.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Printf("server: shutdown error: %v", shutdownErr)
	}
	log.Printf("server: stopped")
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func getenvOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// splitNonEmpty splits s by commas and trims whitespace, dropping empty tokens.
func splitNonEmpty(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
