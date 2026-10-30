package metrics

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// Host is the CPU, memory and disk of the machine pulse runs on. In a container
// /proc/stat and /proc/meminfo show the host (no lxcfs), and the root file system
// shows the disk of the container runtime.
type Host struct {
	// CPURatio is the busy share of all CPUs since the last sample, 0 to 1.
	// Null on the first sample.
	CPURatio         *float64 `json:"cpu_ratio"`
	MemoryUsedBytes  uint64   `json:"memory_used_bytes"`
	MemoryTotalBytes uint64   `json:"memory_total_bytes"`
	// DiskUsedBytes is total minus free blocks, as df shows it: blocks reserved for
	// root count as used.
	DiskUsedBytes  uint64 `json:"disk_used_bytes"`
	DiskTotalBytes uint64 `json:"disk_total_bytes"`
}

// HostSampler reads /proc and statfs. Safe for concurrent use.
type HostSampler struct {
	proc string
	disk string

	mu        sync.Mutex
	lastBusy  uint64
	lastTotal uint64
}

// NewHostSampler reads from the proc dir (normally /proc) and the disk of path.
func NewHostSampler(proc, path string) *HostSampler {
	return &HostSampler{proc: proc, disk: path}
}

// Sample returns the current numbers. It fails when /proc is not readable (not Linux).
func (h *HostSampler) Sample() (Host, error) {
	busy, total, err := readCPU(filepath.Join(h.proc, "stat"))
	if err != nil {
		return Host{}, err
	}
	memTotal, memAvail, err := readMem(filepath.Join(h.proc, "meminfo"))
	if err != nil {
		return Host{}, err
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(h.disk, &fs); err != nil {
		return Host{}, fmt.Errorf("metrics: statfs: %w", err)
	}
	bsize := uint64(fs.Bsize) //nolint:gosec // block size is positive
	out := Host{
		MemoryTotalBytes: memTotal,
		MemoryUsedBytes:  memTotal - min(memAvail, memTotal),
		DiskTotalBytes:   fs.Blocks * bsize,
		DiskUsedBytes:    (fs.Blocks - min(fs.Bfree, fs.Blocks)) * bsize,
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.lastTotal > 0 && total > h.lastTotal && busy >= h.lastBusy {
		r := float64(busy-h.lastBusy) / float64(total-h.lastTotal)
		r = min(max(r, 0), 1)
		out.CPURatio = &r
	}
	h.lastBusy, h.lastTotal = busy, total
	return out, nil
}

// readCPU reads the first "cpu" line of /proc/stat: busy and total jiffies.
// Idle is idle plus iowait. guest and guest_nice are already in user and nice.
func readCPU(path string) (busy, total uint64, err error) {
	raw, err := os.ReadFile(path) //nolint:gosec // path is the configured proc dir
	if err != nil {
		return 0, 0, fmt.Errorf("metrics: cpu: %w", err)
	}
	line, _, _ := bytes.Cut(raw, []byte("\n"))
	f := strings.Fields(string(line))
	if len(f) < 5 || f[0] != "cpu" {
		return 0, 0, errors.New("metrics: cpu: no cpu line in stat")
	}
	var idle uint64
	for i, s := range f[1:min(len(f), 9)] { // user nice system idle iowait irq softirq steal
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("metrics: cpu: field %d: %w", i+1, err)
		}
		total += v
		if i == 3 || i == 4 {
			idle += v
		}
	}
	return total - idle, total, nil
}

// readMem reads MemTotal and MemAvailable of /proc/meminfo, in bytes.
func readMem(path string) (total, avail uint64, err error) {
	f, err := os.Open(path) //nolint:gosec // path is the configured proc dir
	if err != nil {
		return 0, 0, fmt.Errorf("metrics: memory: %w", err)
	}
	defer func() { _ = f.Close() }()
	var haveTotal, haveAvail bool
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		name, rest, ok := strings.Cut(sc.Text(), ":")
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		v, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			continue
		}
		switch name {
		case "MemTotal":
			total, haveTotal = v*1024, true
		case "MemAvailable":
			avail, haveAvail = v*1024, true
		}
	}
	if err := sc.Err(); err != nil {
		return 0, 0, fmt.Errorf("metrics: memory: %w", err)
	}
	if !haveTotal || !haveAvail {
		return 0, 0, errors.New("metrics: memory: MemTotal or MemAvailable missing")
	}
	return total, avail, nil
}
