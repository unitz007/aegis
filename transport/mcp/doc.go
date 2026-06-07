// Package mcp provides the MCP Streamable-HTTP handler for the aegis Gateway.
//
// Handler builds an authenticated HTTP handler that exposes a place_trade MCP
// tool. bearer-auth constant-time check runs before any tool dispatch.
// Fail-closed: returns 503 when authToken is empty or gateway is nil.
//
// This package is a placeholder — implementation arrives in a later task.
package mcp
