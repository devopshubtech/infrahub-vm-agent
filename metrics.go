// metrics.go holds the OS-agnostic orchestration shared by every
// platform: the delta-tracking state (metricsCollector), the top-level
// Collect() that calls out to whichever platform file (metrics_linux.go/
// metrics_windows.go/metrics_darwin.go, selected by Go build tags) is
// compiled in, and the shared host-identity lookup (hostname/OS/kernel
// version), which is genuinely cross-platform via gopsutil's host
// package and so doesn't need a per-OS implementation the way the
// numeric stats below it do.
//
// Every platform file exposes the exact same function signatures
// (readCPU/readMemory/readLoadAvg/readUptime/readRootStorage/
// readNetwork/readProcessCount), so this file never branches on OS
// itself -- the Go build system picks the right implementation at
// compile time.
package main

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/host"
)

type cpuSample struct {
	idle, total uint64
}

type netSample struct {
	rxBytes, txBytes uint64
	at                time.Time
}

// metricsCollector holds the previous-sample state a delta-based reading
// (CPU%, network rate) needs -- one per running agent process, never
// persisted, lost (harmlessly) on restart.
type metricsCollector struct {
	prevCPU *cpuSample
	prevNet *netSample
}

func newMetricsCollector() *metricsCollector {
	return &metricsCollector{}
}

// hostIdentity caches gopsutil's host.Info() -- hostname/OS/kernel
// version essentially never change while this process is running, so
// there's no reason to re-read it every collection cycle the way the
// numeric stats are. Looked up once, lazily, on the first Collect call.
var (
	hostIdentityOnce sync.Once
	hostIdentity     struct {
		hostname, os, osVersion, kernelVersion string
		ok                                     bool
	}
)

func lookupHostIdentity() {
	info, err := host.Info()
	if err != nil {
		log.Printf("host identity unavailable: %v", err)
		return
	}
	hostIdentity.hostname = info.Hostname
	hostIdentity.os = info.OS
	hostIdentity.osVersion = strings.TrimSpace(info.Platform + " " + info.PlatformVersion)
	hostIdentity.kernelVersion = info.KernelVersion
	hostIdentity.ok = true
}

// Collect takes one full sample. Every field is a pointer, left nil (never
// a fabricated zero) when it couldn't be read -- e.g. CPU%/network rate on
// the very first call, before any previous sample exists to delta against.
// totalReadings is the number of independent readings Collect attempts
// each pass -- used only to size its "N/total readings failed" summary
// log line below.
const totalReadings = 7

func (c *metricsCollector) Collect() MetricsPushData {
	var data MetricsPushData
	var failures []string

	hostIdentityOnce.Do(lookupHostIdentity)
	if hostIdentity.ok {
		data.Hostname = &hostIdentity.hostname
		data.OS = &hostIdentity.os
		data.OSVersion = &hostIdentity.osVersion
		data.KernelVersion = &hostIdentity.kernelVersion
	}

	if cpuPct, cores, err := c.readCPU(); err == nil {
		data.CPUPercent = cpuPct
		data.CPUCores = cores
	} else {
		failures = append(failures, fmt.Sprintf("cpu: %v", err))
	}
	if used, total, swapUsed, swapTotal, err := readMemory(); err == nil {
		data.MemoryUsedBytes, data.MemoryTotalBytes = &used, &total
		data.SwapUsedBytes, data.SwapTotalBytes = &swapUsed, &swapTotal
	} else {
		failures = append(failures, fmt.Sprintf("memory: %v", err))
	}
	if l1, l5, l15, err := readLoadAvg(); err == nil {
		data.Load1m, data.Load5m, data.Load15m = &l1, &l5, &l15
	} else {
		failures = append(failures, fmt.Sprintf("loadavg: %v", err))
	}
	if uptime, err := readUptime(); err == nil {
		data.UptimeSeconds = &uptime
	} else {
		failures = append(failures, fmt.Sprintf("uptime: %v", err))
	}
	if used, total, err := readRootStorage(); err == nil {
		data.StorageUsedBytes, data.StorageTotalBytes = &used, &total
	} else {
		failures = append(failures, fmt.Sprintf("storage: %v", err))
	}
	if rxRate, txRate, err := c.readNetwork(); err == nil {
		data.NetworkRxRateBytes, data.NetworkTxRateBytes = rxRate, txRate
	} else {
		failures = append(failures, fmt.Sprintf("network: %v", err))
	}
	if count, err := readProcessCount(); err == nil {
		data.ProcessCount = &count
	} else {
		failures = append(failures, fmt.Sprintf("process count: %v", err))
	}

	// One aggregated line covers every reading that failed this pass (e.g.
	// a permission error on a bind-mounted /proc path) -- surfaced right
	// where it happens rather than only as a downstream missing field,
	// without spamming a separate line per metric every interval.
	if len(failures) > 0 {
		log.Printf("metrics collection: %d/%d readings failed: %s", len(failures), totalReadings, strings.Join(failures, "; "))
	}

	return data
}

// summarizeMetrics renders one collected sample as a compact,
// human-readable line for this agent's own stdout -- e.g. "cpu=12.3%
// mem=340.0MiB/2048.0MiB disk=45.1GiB/100.0GiB load1=0.42". Any field that
// couldn't be read is simply omitted, matching Collect's own "never a
// fabricated value" discipline; never sent over the wire.
func summarizeMetrics(d MetricsPushData) string {
	var b strings.Builder
	add := func(format string, args ...any) {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, format, args...)
	}
	if d.CPUPercent != nil {
		add("cpu=%.1f%%", *d.CPUPercent)
	}
	if d.MemoryUsedBytes != nil && d.MemoryTotalBytes != nil {
		add("mem=%s/%s", formatBytes(*d.MemoryUsedBytes), formatBytes(*d.MemoryTotalBytes))
	}
	if d.StorageUsedBytes != nil && d.StorageTotalBytes != nil {
		add("disk=%s/%s", formatBytes(*d.StorageUsedBytes), formatBytes(*d.StorageTotalBytes))
	}
	if d.Load1m != nil {
		add("load1=%.2f", *d.Load1m)
	}
	if b.Len() == 0 {
		return "no metrics available this pass"
	}
	return b.String()
}

// formatBytes renders a byte count as a compact IEC-unit string (e.g.
// "340.0MiB"), for summarizeMetrics' stdout logging only.
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
