//go:build windows

// metrics_windows.go reads real host metrics via gopsutil -- this agent
// runs as a plain native Windows process (never inside a Docker Desktop
// container, which always virtualizes through WSL2 and would report that
// VM's own resource allocation instead of the physical machine's -- see
// the VM Agent's top-level doc comment / the backend's VMAgentRunCommand
// for why the Linux build takes the container route and this one
// deliberately doesn't).
package main

import (
	"errors"
	"os"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
	psnet "github.com/shirou/gopsutil/v4/net"
	"github.com/shirou/gopsutil/v4/process"
)

var errNoNetworkInterfaces = errors.New("no network interfaces reported")

// readCPU uses cpu.Percent's own "since the last call" mode (interval=0)
// instead of reimplementing jiffie-style delta tracking -- gopsutil
// caches the previous sample internally for exactly this use.
func (c *metricsCollector) readCPU() (*float64, *int32, error) {
	pcts, err := cpu.Percent(0, false)
	if err != nil {
		return nil, nil, err
	}
	cores, err := cpu.Counts(true)
	if err != nil {
		return nil, nil, err
	}
	coreCount := int32(cores)
	if len(pcts) == 0 {
		// First call: gopsutil has no prior sample yet to delta against,
		// same "no percentage yet, but cores are known" case the Linux
		// implementation returns on its own first call.
		return nil, &coreCount, nil
	}
	return &pcts[0], &coreCount, nil
}

func readMemory() (usedBytes, totalBytes, swapUsedBytes, swapTotalBytes int64, err error) {
	vm, err := mem.VirtualMemory()
	if err != nil {
		return 0, 0, 0, 0, err
	}
	swap, err := mem.SwapMemory()
	if err != nil {
		// Real memory numbers are still useful even if swap/pagefile
		// stats couldn't be read -- report zero swap rather than
		// failing the whole reading.
		return int64(vm.Used), int64(vm.Total), 0, 0, nil
	}
	return int64(vm.Used), int64(vm.Total), int64(swap.Used), int64(swap.Total), nil
}

// readLoadAvg: Windows has no native "load average" concept the way
// Unix does -- gopsutil approximates one from the processor queue length,
// which is unreliable enough on Windows that a failure here is expected
// and not logged as noisily as the other readings; nil (never fabricated)
// is exactly the right outcome when it's unavailable.
func readLoadAvg() (load1, load5, load15 float64, err error) {
	avg, err := load.Avg()
	if err != nil {
		return 0, 0, 0, err
	}
	return avg.Load1, avg.Load5, avg.Load15, nil
}

func readUptime() (int64, error) {
	seconds, err := host.Uptime()
	if err != nil {
		return 0, err
	}
	return int64(seconds), nil
}

// readRootStorage reports the system drive (the real physical/virtual
// disk Windows itself is installed on, e.g. C:\), not the container/VM
// view a Docker-based agent would be stuck with.
func readRootStorage() (usedBytes, totalBytes int64, err error) {
	drive := os.Getenv("SystemDrive")
	if drive == "" {
		drive = "C:"
	}
	usage, err := disk.Usage(drive + `\`)
	if err != nil {
		return 0, 0, err
	}
	return int64(usage.Used), int64(usage.Total), nil
}

// readNetwork sums bytes sent/received across every interface and
// returns the rate since the previous call, the same delta-against-
// in-memory-previous-sample approach metrics_linux.go uses against
// /proc/net/dev.
func (c *metricsCollector) readNetwork() (*int64, *int64, error) {
	counters, err := psnet.IOCounters(false)
	if err != nil || len(counters) == 0 {
		if err == nil {
			err = errNoNetworkInterfaces
		}
		return nil, nil, err
	}
	total := counters[0]

	now := time.Now()
	sample := &netSample{rxBytes: total.BytesRecv, txBytes: total.BytesSent, at: now}
	prev := c.prevNet
	c.prevNet = sample
	if prev == nil || total.BytesRecv < prev.rxBytes || total.BytesSent < prev.txBytes {
		return nil, nil, nil // first sample, or a counter reset
	}
	elapsed := now.Sub(prev.at).Seconds()
	if elapsed <= 0 {
		return nil, nil, nil
	}
	rxRate := int64(float64(total.BytesRecv-prev.rxBytes) / elapsed)
	txRate := int64(float64(total.BytesSent-prev.txBytes) / elapsed)
	return &rxRate, &txRate, nil
}

func readProcessCount() (int32, error) {
	pids, err := process.Pids()
	if err != nil {
		return 0, err
	}
	return int32(len(pids)), nil
}
