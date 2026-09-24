//go:build !linux

// logs_other.go stands in for logs_linux.go on every platform this agent
// runs natively on (Windows, macOS) -- there is no journald/syslog
// equivalent this agent reads today. A native Windows agent could read
// the Event Log (via golang.org/x/sys/windows/svc/eventlog or similar)
// and macOS has its own unified logging (os_log/`log show`), but neither
// is implemented yet -- deliberately out of scope for the initial
// cross-platform metrics push (see the plan this shipped under). Metrics
// collection is entirely unaffected: FetchLogsSince/StreamLogs are the
// only two calls that reach this file.
package main

import (
	"context"
	"errors"
	"time"
)

// errLogsNotSupportedOnPlatform is surfaced to the backend verbatim (see
// main.go's handleCommand, which forwards err.Error() as the command's
// error message) -- the same "never fabricate logs" discipline
// vm_agent_logs.go already applies server-side for other platform gaps.
var errLogsNotSupportedOnPlatform = errors.New("OS-level log collection is not yet supported on this agent for this platform")

func FetchLogsSince(_ time.Time) (string, error) {
	return "", errLogsNotSupportedOnPlatform
}

func StreamLogs(_ context.Context, _ func(line string)) error {
	return errLogsNotSupportedOnPlatform
}
