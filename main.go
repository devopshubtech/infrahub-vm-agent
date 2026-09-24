// Command vm-agent is InfraHub's push-based VM Agent: installed once (by
// InfraHub itself, over the same SSH connection already used to manage
// the VM -- see backend/internal/services/vm_agent_install.go) on a VM
// you want live agent-pushed metrics/logs for, given a backend URL and a
// bearer token, it dials OUT to InfraHub over a WebSocket -- mirrors
// /docker-agent and /k8s-agent's connect/reconnect architecture exactly
// (see docker-agent/main.go for the full backoff/liveness rationale).
//
// Unlike those two, this agent also PUSHES: on its own interval (default
// 15s, INFRAHUB_METRICS_INTERVAL overridable), unprompted, it sends a
// metrics_push message the backend never asked for -- see protocol.go's
// MsgMetricsPush and backend/internal/services/vm_agent_hub.go's dispatch
// for how the backend routes that. Logs remain purely request/response
// (fetch_logs_since/stream_logs), identical in shape to the Docker
// agent's own log commands.
//
// This is additive, alongside the backend's existing SSH-based monitoring
// scheduler (services.VMMonitoringService), never a replacement: a VM
// with no agent installed keeps working exactly as before; a VM with the
// agent installed additionally gets this live, agent-pushed series, kept
// in a fully separate table (vm_agent_metric_snapshots) so the two can
// never interfere with each other.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const defaultMetricsIntervalSeconds = 15

func main() {
	backendURL := os.Getenv("INFRAHUB_BACKEND_URL")
	token := os.Getenv("INFRAHUB_AGENT_TOKEN")
	if backendURL == "" || token == "" {
		log.Fatal("INFRAHUB_BACKEND_URL and INFRAHUB_AGENT_TOKEN must both be set")
	}
	metricsInterval := defaultMetricsIntervalSeconds * time.Second
	if v := os.Getenv("INFRAHUB_METRICS_INTERVAL"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			metricsInterval = time.Duration(secs) * time.Second
		}
	}
	log.Printf("starting vm-agent: backend=%s token=provided metrics_interval=%s", backendURL, metricsInterval)

	collector := newMetricsCollector()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	const maxBackoff = 30 * time.Second
	backoff := time.Second
	for ctx.Err() == nil {
		connectedAt := time.Now()
		if err := runOnce(ctx, backendURL, token, metricsInterval, collector); err != nil {
			log.Printf("connection ended: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
		if time.Since(connectedAt) > maxBackoff {
			backoff = time.Second
		}
		log.Printf("reconnecting in %s", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
}

// pongWait/pingPeriod: same proven values docker-agent/k8s-agent use --
// see docker-agent/main.go's doc comment for the full derivation.
const pongWait = 90 * time.Second
const pingPeriod = 15 * time.Second

func runOnce(ctx context.Context, backendURL, token string, metricsInterval time.Duration, collector *metricsCollector) error {
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	log.Printf("connecting to InfraHub at %s", backendURL)
	conn, _, err := dialer.DialContext(ctx, backendURL, http.Header{"Authorization": {"Bearer " + token}})
	if err != nil {
		return err
	}
	defer conn.Close()
	log.Println("connected to InfraHub")

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-connCtx.Done()
		_ = conn.Close()
	}()

	var writeMu sync.Mutex
	var activeStreams sync.Map // command ID -> context.CancelFunc, for in-flight stream_logs commands

	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	go pingLoop(connCtx, conn, &writeMu, cancel)
	go metricsLoop(connCtx, conn, &writeMu, metricsInterval, collector)

	for {
		var cmd Command
		if err := conn.ReadJSON(&cmd); err != nil {
			activeStreams.Range(func(_, v any) bool {
				v.(context.CancelFunc)()
				return true
			})
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		go handleCommand(connCtx, conn, &writeMu, cmd, &activeStreams)
	}
}

func pingLoop(ctx context.Context, conn *websocket.Conn, writeMu *sync.Mutex, cancel context.CancelFunc) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			writeMu.Lock()
			err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second))
			writeMu.Unlock()
			if err != nil {
				cancel()
				return
			}
		}
	}
}

// metricsLoop is this agent's one real departure from the Docker/K8s
// agents' pattern: it never waits to be asked. Every interval, it
// collects one sample and pushes it unprompted with an empty ID -- see
// protocol.go's MetricsPushData and the backend's matching
// VMAgentMsgMetricsPush handling.
func metricsLoop(ctx context.Context, conn *websocket.Conn, writeMu *sync.Mutex, interval time.Duration, collector *metricsCollector) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			data := collector.Collect()
			log.Printf("metrics push: %s", summarizeMetrics(data))
			raw, err := json.Marshal(data)
			if err != nil {
				log.Printf("encode metrics sample: %v", err)
				continue
			}
			writeMu.Lock()
			err = conn.WriteJSON(Message{Type: MsgMetricsPush, Data: raw})
			writeMu.Unlock()
			if err != nil {
				log.Printf("push metrics: %v", err)
				return
			}
		}
	}
}

func handleCommand(ctx context.Context, conn *websocket.Conn, writeMu *sync.Mutex, cmd Command, activeStreams *sync.Map) {
	switch cmd.Type {
	case CmdPing:
		sendResult(conn, writeMu, cmd.ID, PingResult{Pong: true}, nil)

	case CmdFetchLogsSince:
		var since time.Time
		if cmd.Since != "" {
			since, _ = time.Parse(time.RFC3339Nano, cmd.Since)
		}
		output, err := FetchLogsSince(since)
		if err != nil {
			log.Printf("fetch_logs_since failed: %v", err)
		}
		sendResult(conn, writeMu, cmd.ID, map[string]string{"output": output}, err)

	case CmdStreamLogs:
		streamCtx, cancel := context.WithCancel(ctx)
		activeStreams.Store(cmd.ID, cancel)
		defer func() {
			activeStreams.Delete(cmd.ID)
			cancel()
		}()
		err := StreamLogs(streamCtx, func(line string) {
			writeMu.Lock()
			_ = conn.WriteJSON(Message{ID: cmd.ID, Type: MsgLogLine, Line: line})
			writeMu.Unlock()
		})
		writeMu.Lock()
		if err != nil && streamCtx.Err() == nil {
			_ = conn.WriteJSON(Message{ID: cmd.ID, Type: MsgError, Message: "log stream ended: " + err.Error()})
		} else {
			_ = conn.WriteJSON(Message{ID: cmd.ID, Type: MsgDone})
		}
		writeMu.Unlock()
		if err != nil && streamCtx.Err() == nil {
			log.Printf("stream_logs error: %v", err)
		}

	case CmdStopStream:
		if v, ok := activeStreams.Load(cmd.ID); ok {
			v.(context.CancelFunc)()
			activeStreams.Delete(cmd.ID)
			log.Printf("log stream stopped by request: command %s", cmd.ID)
		}

	default:
		log.Printf("received unknown command %q -- agent may need upgrading", cmd.Type)
		writeMu.Lock()
		_ = conn.WriteJSON(Message{ID: cmd.ID, Type: MsgError, Message: "unknown command \"" + string(cmd.Type) + "\" -- agent may need upgrading"})
		writeMu.Unlock()
	}
}

func sendResult(conn *websocket.Conn, writeMu *sync.Mutex, id string, data any, err error) {
	writeMu.Lock()
	defer writeMu.Unlock()
	if err != nil {
		_ = conn.WriteJSON(Message{ID: id, Type: MsgError, Message: err.Error()})
		return
	}
	raw, marshalErr := json.Marshal(data)
	if marshalErr != nil {
		_ = conn.WriteJSON(Message{ID: id, Type: MsgError, Message: "internal error encoding response"})
		return
	}
	_ = conn.WriteJSON(Message{ID: id, Type: MsgResult, Data: raw})
}
