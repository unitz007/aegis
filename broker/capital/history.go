package capital

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/unitz007/aegis/broker"
)

// maxHistory is as far back as Capital.com's history goes in one read.
const maxHistory = 24 * time.Hour

// History reads what Capital.com recorded on the account since a time (a day at most): GET /api/v1/history/activity for
// what each deal did and why (its source: SL, TP, USER, CLOSE_OUT…), and GET /api/v1/history/transactions for what each
// closed trade made or lost. Only the documented fields are read; the first activities a server sees are logged whole,
// so what Capital.com sends beyond them can be learned.
func (a *Adapter) History(ctx context.Context, since time.Time) (broker.History, error) {
	if err := a.ensureSession(ctx); err != nil {
		return broker.History{}, err
	}
	period := time.Since(since)
	if period > maxHistory {
		period = maxHistory
	}
	secs := int(math.Ceil(period.Seconds()))
	if secs < 60 {
		secs = 60
	}
	q := url.Values{"lastPeriod": {strconv.Itoa(secs)}, "detailed": {"true"}}

	var acts struct {
		Activities []map[string]any `json:"activities"`
	}
	if err := a.get(ctx, "/api/v1/history/activity?"+q.Encode(), &acts); err != nil {
		return broker.History{}, fmt.Errorf("capital activity: %w", err)
	}
	logActivitiesOnce(acts.Activities)
	var out broker.History
	for _, raw := range acts.Activities {
		out.Activities = append(out.Activities, broker.Activity{
			DealID: text(raw["dealId"]),
			Symbol: fromCapitalEpic(text(raw["epic"])),
			Kind:   activityKind(text(raw["type"])),
			Status: strings.ToLower(text(raw["status"])),
			By:     activityBy(text(raw["source"])),
			At:     capitalTime(text(raw["dateUTC"])),
		})
	}

	var txs struct {
		Transactions []struct {
			DateUTC         string `json:"dateUtc"`
			InstrumentName  string `json:"instrumentName"`
			TransactionType string `json:"transactionType"`
			Note            string `json:"note"`
			Reference       string `json:"reference"`
			Size            any    `json:"size"`
			Currency        string `json:"currency"`
		} `json:"transactions"`
	}
	tq := url.Values{"lastPeriod": {strconv.Itoa(secs)}, "type": {"TRADE"}}
	if err := a.get(ctx, "/api/v1/history/transactions?"+tq.Encode(), &txs); err != nil {
		// What each made is a detail; what happened is still worth having.
		log.Printf("capital: transactions: %v", err)
		return out, nil
	}
	for _, t := range txs.Transactions {
		if !strings.EqualFold(t.TransactionType, "TRADE") {
			continue
		}
		amount, err := strconv.ParseFloat(strings.TrimSpace(text(t.Size)), 64)
		if err != nil {
			continue
		}
		out.Results = append(out.Results, broker.TradeResult{
			Symbol: fromCapitalEpic(capitalEpic(t.InstrumentName)), Amount: amount, Currency: cleanCurrencyCode(t.Currency),
			Note: t.Note, Reference: t.Reference, At: capitalTime(t.DateUTC),
		})
	}
	return out, nil
}

// activityKind is what an activity's type says it happened to.
func activityKind(t string) string {
	switch strings.ToUpper(t) {
	case "POSITION":
		return "position"
	case "WORKING_ORDER":
		return "order"
	case "EDIT_STOP_AND_LIMIT":
		return "levels"
	}
	return "other"
}

// activityBy is what an activity's source says made it happen.
func activityBy(s string) string {
	switch strings.ToUpper(s) {
	case "SL":
		return "stop_loss"
	case "TP":
		return "take_profit"
	case "USER":
		return "you"
	case "CLOSE_OUT":
		return "close_out"
	case "DEALER", "SYSTEM":
		return "broker"
	}
	return ""
}

// capitalTime reads Capital.com's UTC times, with or without milliseconds; zero when it can't.
func capitalTime(s string) time.Time {
	for _, f := range []string{"2006-01-02T15:04:05.000", "2006-01-02T15:04:05", time.RFC3339Nano} {
		if t, err := time.Parse(f, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func text(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case nil:
		return ""
	}
	return fmt.Sprint(v)
}

var logged struct {
	sync.Mutex
	n int
}

// logActivitiesOnce logs the first few activities this server reads, whole: Capital.com doesn't document what its
// detailed history holds (the price a deal closed at, what it touched), and this is how it will be learned. Activities
// hold no login details.
func logActivitiesOnce(acts []map[string]any) {
	logged.Lock()
	defer logged.Unlock()
	for _, a := range acts {
		if logged.n >= 6 {
			return
		}
		logged.n++
		log.Printf("capital: activity as sent: %v", a)
	}
}
