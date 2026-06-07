// Package webhook provides the HMAC-SHA256 webhook HTTP handler for the aegis Gateway.
//
// Handler returns a POST /signal/trade handler. Fail-closed: returns 503 when
// secret is empty or gateway is nil. Timestamp replay window defaults to 60
// seconds and is configurable via WithTimestampTolerance.
//
// HTTP status mapping:
//   - ReasonInFlight  → 409
//   - Any other rejection → 400
//   - Accepted → 200
//
// This package is a placeholder — implementation arrives in a later task.
package webhook
