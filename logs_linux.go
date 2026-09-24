//go:build linux

// logs_linux.go reads the VM's OS-level logs: journald primary, a
// plain-text tail fallback only if the journal can't be opened or is
// empty. journald is the only guaranteed-present log store on
// current-generation minimal cloud images (many no longer ship rsyslog
// by default), so it's the primary source; this agent can't shell out to
// `journalctl` the way a full OS could (this image is intentionally
// minimal, no shell/coreutils -- matching this codebase's other agents'
// distroless discipline as closely as cgo+libsystemd0 allows, see
// Dockerfile), so it reads the journal directly via sdjournal instead.
//
// The host's journal directories are bind-mounted read-only at fixed
// container paths by VMAgentRunCommand (backend/internal/services/
// vm_agent_install.go): /host/var/log/journal (persistent logging, if
// enabled) and /host/run/log/journal (volatile, tmpfs-backed -- present
// even when persistent logging isn't). Both are tried, persistent first.
// Windows/macOS builds have no equivalent (see logs_other.go) -- this
// agent only ever ships as a Linux Docker image, never natively for those
// platforms, so cgo/libsystemd0 here is never a cross-compilation problem.
package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/coreos/go-systemd/v22/sdjournal"
)

const (
	journalPersistentDir = "/host/var/log/journal"
	journalVolatileDir   = "/host/run/log/journal"
)

var plainTextLogCandidates = []string{"/host/var/log/syslog", "/host/var/log/messages"}

// openJournal tries the persistent journal directory, then the volatile
// one, returning the first that opens successfully.
func openJournal() (*sdjournal.Journal, error) {
	if j, err := sdjournal.NewJournalFromDir(journalPersistentDir); err == nil {
		return j, nil
	}
	return sdjournal.NewJournalFromDir(journalVolatileDir)
}

// formatEntry renders one journal entry as "<RFC3339Nano> <message>" --
// the same shape ParseTimestampedLogLines (backend/internal/services/
// log_parse.go) already expects from the Docker agent's own
// fetch_logs_since output, so the backend's classification/parsing is
// reused unmodified for this agent's logs too.
func formatEntry(entry *sdjournal.JournalEntry) string {
	ts := time.UnixMicro(int64(entry.RealtimeTimestamp)).UTC()
	msg := entry.Fields["MESSAGE"]
	unit := entry.Fields["_SYSTEMD_UNIT"]
	if unit != "" {
		return fmt.Sprintf("%s %s: %s", ts.Format(time.RFC3339Nano), unit, msg)
	}
	return fmt.Sprintf("%s %s", ts.Format(time.RFC3339Nano), msg)
}

// FetchLogsSince returns every journal entry newer than since, newline-
// joined. Bounded at maxFetchLines so one call can never return an
// unbounded amount of data for a chatty VM.
const maxFetchLines = 5000

func FetchLogsSince(since time.Time) (string, error) {
	j, err := openJournal()
	if err != nil {
		log.Printf("fetch_logs_since: systemd journal unavailable (%v), falling back to plain-text logs", err)
		return fetchPlainTextLogs(since)
	}
	defer j.Close()

	if err := j.SeekRealtimeUsec(uint64(since.UnixMicro())); err != nil {
		log.Printf("fetch_logs_since: journal seek failed (%v), falling back to plain-text logs", err)
		return fetchPlainTextLogs(since)
	}

	var lines []string
	for len(lines) < maxFetchLines {
		n, err := j.Next()
		if err != nil || n == 0 {
			break
		}
		entry, err := j.GetEntry()
		if err != nil {
			continue
		}
		lines = append(lines, formatEntry(entry))
	}
	if len(lines) == 0 {
		return fetchPlainTextLogs(since)
	}
	log.Printf("fetch_logs_since: %d lines from systemd journal", len(lines))
	return strings.Join(lines, "\n"), nil
}

// fetchPlainTextLogs is the fallback path -- best-effort only: it doesn't
// attempt to parse legacy syslog's year-less timestamp format, it just
// tails the file and stamps each line with the current time (still
// visible and searchable, just not individually time-filterable the way
// journal-sourced lines are).
func fetchPlainTextLogs(_ time.Time) (string, error) {
	for _, path := range plainTextLogCandidates {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
		if len(lines) > maxFetchLines {
			lines = lines[len(lines)-maxFetchLines:]
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		stamped := make([]string, 0, len(lines))
		for _, l := range lines {
			if l == "" {
				continue
			}
			stamped = append(stamped, now+" "+l)
		}
		log.Printf("fetch_logs_since: %d lines from plain-text log %s", len(stamped), path)
		return strings.Join(stamped, "\n"), nil
	}
	return "", fmt.Errorf("no journal and no plain-text log file found")
}

// StreamLogs follows new log entries as they arrive, calling onLine for
// each formatted line, until ctx is cancelled or the journal errors.
func StreamLogs(ctx context.Context, onLine func(line string)) error {
	j, err := openJournal()
	if err != nil {
		log.Printf("log stream: systemd journal unavailable (%v), falling back to plain-text tail", err)
		return streamPlainTextLogs(ctx, onLine)
	}
	defer j.Close()

	if err := j.SeekTail(); err != nil {
		return err
	}
	// SeekTail positions past the last entry; one Previous+Next pair is
	// the documented way to land exactly at the last entry so the
	// following Wait/Next loop picks up only genuinely new ones.
	_, _ = j.Previous()

	log.Printf("log stream started (systemd journal)")
	lines := 0
	defer func() { log.Printf("log stream ended (systemd journal): forwarded %d lines", lines) }()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		status := j.Wait(2 * time.Second)
		if status == sdjournal.SD_JOURNAL_NOP {
			continue
		}
		for {
			n, err := j.Next()
			if err != nil {
				return err
			}
			if n == 0 {
				break
			}
			entry, err := j.GetEntry()
			if err != nil {
				continue
			}
			lines++
			onLine(formatEntry(entry))
		}
	}
}

// streamPlainTextLogs is StreamLogs' fallback: a simple poll-and-tail
// loop over the plain-text file, re-reading it on an interval and
// emitting any lines beyond what was already sent.
func streamPlainTextLogs(ctx context.Context, onLine func(line string)) error {
	var path string
	for _, candidate := range plainTextLogCandidates {
		if _, err := os.Stat(candidate); err == nil {
			path = candidate
			break
		}
	}
	if path == "" {
		return fmt.Errorf("no journal and no plain-text log file found")
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	// Start from the end -- a live tail, not a replay of the whole file.
	if _, err := f.Seek(0, os.SEEK_END); err != nil {
		return err
	}
	reader := bufio.NewReader(f)

	log.Printf("log stream started (plain-text tail: %s)", path)
	lines := 0
	defer func() { log.Printf("log stream ended (plain-text tail): forwarded %d lines", lines) }()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			for {
				line, err := reader.ReadString('\n')
				if line != "" {
					lines++
					onLine(time.Now().UTC().Format(time.RFC3339Nano) + " " + strings.TrimRight(line, "\n"))
				}
				if err != nil {
					break
				}
			}
		}
	}
}
