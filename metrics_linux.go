//go:build linux

// metrics_linux.go reads host metrics from /proc and the host's root
// filesystem, both bind-mounted read-only into this container at
// `docker run` time (see backend/internal/services/vm_agent_install.go's
// VMAgentRunCommand: -v /proc:/host/proc:ro -v /:/host/root:ro,rslave).
// No --pid=host is needed -- procfs's global counters (stat/meminfo/
// loadavg/net/dev) are host-real the moment /proc itself is bind-mounted,
// the same technique node_exporter-style host agents use; only
// *per-process* /proc/<pid> data would need --pid=host, and this agent
// collects none of that.
//
// CPU% and the two network rates are computed here, as deltas against an
// in-memory previous sample -- unlike the backend's own SSH-based
// VMMonitoringService (which persists raw jiffie counters between polls
// because each poll is a fresh, stateless SSH command), this agent is a
// long-running process with its own continuous state, so there is nothing
// for the backend to delta against; every pushed sample is already final.
package main

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	hostProc = "/host/proc"
	hostRoot = "/host/root"
)

// readCPU parses the aggregate "cpu " line of /proc/stat (8 jiffie
// counters: user, nice, system, idle, iowait, irq, softirq, steal) and
// returns the busy percentage since the previous call. cores counts the
// per-core "cpuN" lines that follow.
func (c *metricsCollector) readCPU() (*float64, *int32, error) {
	f, err := os.Open(hostProc + "/stat")
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	var cores int32
	var sample cpuSample
	haveTotal := false

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "cpu") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if fields[0] == "cpu" {
			var sum uint64
			for _, v := range fields[1:] {
				n, err := strconv.ParseUint(v, 10, 64)
				if err != nil {
					continue
				}
				sum += n
			}
			var idle uint64
			if len(fields) > 4 {
				idle, _ = strconv.ParseUint(fields[4], 10, 64)
			}
			sample = cpuSample{idle: idle, total: sum}
			haveTotal = true
			continue
		}
		// "cpu0", "cpu1", ... -- one line per logical core.
		cores++
	}
	if !haveTotal {
		return nil, nil, os.ErrInvalid
	}

	prev := c.prevCPU
	c.prevCPU = &sample
	if prev == nil || sample.total <= prev.total {
		return nil, &cores, nil // first sample, or a counter reset: no delta yet
	}
	totalDelta := sample.total - prev.total
	idleDelta := sample.idle - prev.idle
	if idleDelta > totalDelta {
		idleDelta = totalDelta
	}
	pct := 100 * (1 - float64(idleDelta)/float64(totalDelta))
	return &pct, &cores, nil
}

func readMemory() (usedBytes, totalBytes, swapUsedBytes, swapTotalBytes int64, err error) {
	f, err := os.Open(hostProc + "/meminfo")
	if err != nil {
		return 0, 0, 0, 0, err
	}
	defer f.Close()

	var memTotal, memAvailable, swapTotal, swapFree int64
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		valueKB, parseErr := strconv.ParseInt(fields[1], 10, 64)
		if parseErr != nil {
			continue
		}
		switch strings.TrimSuffix(fields[0], ":") {
		case "MemTotal":
			memTotal = valueKB
		case "MemAvailable":
			memAvailable = valueKB
		case "SwapTotal":
			swapTotal = valueKB
		case "SwapFree":
			swapFree = valueKB
		}
	}
	return (memTotal - memAvailable) * 1024, memTotal * 1024, (swapTotal - swapFree) * 1024, swapTotal * 1024, nil
}

func readLoadAvg() (load1, load5, load15 float64, err error) {
	raw, err := os.ReadFile(hostProc + "/loadavg")
	if err != nil {
		return 0, 0, 0, err
	}
	fields := strings.Fields(string(raw))
	if len(fields) < 3 {
		return 0, 0, 0, os.ErrInvalid
	}
	load1, _ = strconv.ParseFloat(fields[0], 64)
	load5, _ = strconv.ParseFloat(fields[1], 64)
	load15, _ = strconv.ParseFloat(fields[2], 64)
	return load1, load5, load15, nil
}

func readUptime() (int64, error) {
	raw, err := os.ReadFile(hostProc + "/uptime")
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(string(raw))
	if len(fields) < 1 {
		return 0, os.ErrInvalid
	}
	seconds, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, err
	}
	return int64(seconds), nil
}

// readRootStorage statfs's the bind-mounted host root -- root filesystem
// only in v1, matching the plan's stated scope (the SSH-based path's
// per-mount-point vm_filesystems table is untouched).
func readRootStorage() (usedBytes, totalBytes int64, err error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(hostRoot, &stat); err != nil {
		return 0, 0, err
	}
	total := int64(stat.Blocks) * int64(stat.Bsize)
	free := int64(stat.Bfree) * int64(stat.Bsize)
	return total - free, total, nil
}

// readNetwork sums rx/tx bytes across every non-loopback interface in
// /proc/net/dev and returns the rate since the previous call.
func (c *metricsCollector) readNetwork() (*int64, *int64, error) {
	f, err := os.Open(hostProc + "/net/dev")
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	var rxTotal, txTotal uint64
	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		if lineNum <= 2 {
			continue // two header lines
		}
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		iface := strings.TrimSpace(parts[0])
		if iface == "lo" {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) < 9 {
			continue
		}
		rx, _ := strconv.ParseUint(fields[0], 10, 64)
		tx, _ := strconv.ParseUint(fields[8], 10, 64)
		rxTotal += rx
		txTotal += tx
	}

	now := time.Now()
	sample := &netSample{rxBytes: rxTotal, txBytes: txTotal, at: now}
	prev := c.prevNet
	c.prevNet = sample
	if prev == nil || rxTotal < prev.rxBytes || txTotal < prev.txBytes {
		return nil, nil, nil // first sample, or a counter reset (interface replaced/reset)
	}
	elapsed := now.Sub(prev.at).Seconds()
	if elapsed <= 0 {
		return nil, nil, nil
	}
	rxRate := int64(float64(rxTotal-prev.rxBytes) / elapsed)
	txRate := int64(float64(txTotal-prev.txBytes) / elapsed)
	return &rxRate, &txRate, nil
}

// readProcessCount counts numeric entries directly under /proc -- one per
// process, the same convention `ps`/procfs-reading tools use.
func readProcessCount() (int32, error) {
	entries, err := os.ReadDir(hostProc)
	if err != nil {
		return 0, err
	}
	var count int32
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if _, err := strconv.Atoi(e.Name()); err == nil {
			count++
		}
	}
	return count, nil
}
