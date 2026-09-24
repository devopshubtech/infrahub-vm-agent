// Hand-duplicated twin of backend/internal/services/vm_agent_protocol.go
// -- this codebase's established convention for agent binaries (see
// docker-agent/protocol.go, k8s-agent's own copy) is no shared Go module
// between backend and agent, so this must be kept in sync by hand. Must
// stay in sync with the backend's copy.
package main

import "encoding/json"

type CommandType string

const (
	CmdFetchLogsSince CommandType = "fetch_logs_since"
	CmdStreamLogs     CommandType = "stream_logs"
	CmdStopStream     CommandType = "stop_stream"
	// CmdPing answers "is this agent actually alive right now" -- unlike
	// metrics (pushed on the agent's own interval, so "connected" alone
	// can be up to a full interval stale) this is a synchronous
	// request/response round trip, backing the UI's "Test Connection"
	// button. Answered with PingResult.
	CmdPing CommandType = "ping"
)

type Command struct {
	ID    string      `json:"id"`
	Type  CommandType `json:"type"`
	Since string      `json:"since,omitempty"`
}

type MessageType string

const (
	MsgResult      MessageType = "result"
	MsgLogLine     MessageType = "log_line"
	MsgDone        MessageType = "done"
	MsgError       MessageType = "error"
	MsgMetricsPush MessageType = "metrics_push"
)

type Message struct {
	ID      string          `json:"id"`
	Type    MessageType     `json:"type"`
	Data    json.RawMessage `json:"data,omitempty"`
	Line    string          `json:"line,omitempty"`
	Message string          `json:"message,omitempty"`
}

// PingResult is what "ping" returns -- deliberately just a liveness
// marker, no version string: unlike the Docker/K8s agents, this binary
// has no build-time version identifier anywhere to report.
type PingResult struct {
	Pong bool `json:"pong"`
}

// MetricsPushData is metrics_push's payload -- one point-in-time sample.
// Field set matches the backend's VMAgentMetricsPushData exactly (see
// that type's doc comment for why CPU%/network rates are already deltas
// by the time they're sent: this agent computes them itself from its own
// in-memory previous-sample state, see metrics.go).
type MetricsPushData struct {
	CPUPercent         *float64 `json:"cpu_percent,omitempty"`
	CPUCores           *int32   `json:"cpu_cores,omitempty"`
	MemoryUsedBytes    *int64   `json:"memory_used_bytes,omitempty"`
	MemoryTotalBytes   *int64   `json:"memory_total_bytes,omitempty"`
	SwapUsedBytes      *int64   `json:"swap_used_bytes,omitempty"`
	SwapTotalBytes     *int64   `json:"swap_total_bytes,omitempty"`
	Load1m             *float64 `json:"load_1m,omitempty"`
	Load5m             *float64 `json:"load_5m,omitempty"`
	Load15m            *float64 `json:"load_15m,omitempty"`
	UptimeSeconds      *int64   `json:"uptime_seconds,omitempty"`
	StorageUsedBytes   *int64   `json:"storage_used_bytes,omitempty"`
	StorageTotalBytes  *int64   `json:"storage_total_bytes,omitempty"`
	NetworkRxRateBytes *int64   `json:"network_rx_rate_bytes,omitempty"`
	NetworkTxRateBytes *int64   `json:"network_tx_rate_bytes,omitempty"`
	ProcessCount       *int32   `json:"process_count,omitempty"`
}
