//go:build linux

package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	defaultXrayBin    = "/usr/local/bin/xray"
	defaultConfigPath = "/app/config.json"
	defaultAssetDir   = "/usr/local/share/xray"
	defaultXrayMemMB  = 550

	defaultLoopbackXH = "127.0.0.1:18443"
	defaultLoopbackWS = "127.0.0.1:18444"
	defaultLoopbackTR = "127.0.0.1:18445"

	fastProbeWindow       = 1500 * time.Millisecond
	fastProbeInterval     = 25 * time.Millisecond
	slowProbeInterval     = 3 * time.Second
	degradedProbeInterval = 250 * time.Millisecond
	minProbeNowInterval   = 1 * time.Second
	probeDialTimeout      = 250 * time.Millisecond
	defaultStartTimeout   = 10 * time.Second
	defaultStopTimeout    = 8 * time.Second
	pipeReadBufferSize    = 64 * 1024

	stableWindow       = 60 * time.Second
	initialBackoff     = 80 * time.Millisecond
	maxBackoffInterval = 5 * time.Second
	backoffJitterFrac  = 0.20

	healthGenShift      = 8
	prSetChildSubreaper = 36
)

const (
	healthRunning uint64 = 1 << iota
	healthReady
	healthXH
	healthWS
	healthTR
)

var scannerBufPool = sync.Pool{
	New: func() any { return bufio.NewReaderSize(nil, pipeReadBufferSize) },
}

var pumpLogMu sync.Mutex
var pumpNewline = [1]byte{'\n'}

type SupervisorHealthSnapshot struct {
	Running     bool   `json:"running"`
	Ready       bool   `json:"ready"`
	Restarts    int32  `json:"restarts"`
	UptimeSec   int64  `json:"uptime_sec"`
	XHInbound   bool   `json:"xh_inbound_ok"`
	WSInbound   bool   `json:"ws_inbound_ok"`
	TRInbound   bool   `json:"tr_inbound_ok"`
	LastProbeAt string `json:"last_probe_at"`
	ChildPID    int    `json:"child_pid,omitempty"`
}

type childExit struct {
	pid    int
	status syscall.WaitStatus
	err    error
}

type processReaper struct {
	mu      sync.Mutex
	waiters map[int]chan childExit
}

func newProcessReaper() (*processReaper, error) {
	_, _, errno := syscall.RawSyscall6(
		syscall.SYS_PRCTL,
		uintptr(prSetChildSubreaper),
		1, 0, 0, 0, 0,
	)
	if errno != 0 {
		return nil, fmt.Errorf("enable PR_SET_CHILD_SUBREAPER: %w", errno)
	}

	r := &processReaper{
		waiters: make(map[int]chan childExit),
	}
	go r.reaperLoop()
	return r, nil
}

func (r *processReaper) reaperLoop() {
	for {
		var status syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &status, 0, nil)

		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.ECHILD) {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		r.mu.Lock()
		ch, ok := r.waiters[pid]
		if ok {
			delete(r.waiters, pid)
		}
		r.mu.Unlock()

		if ok && ch != nil {
			ch <- childExit{pid: pid, status: status}
			close(ch)
		}
	}
}

func (r *processReaper) StartProcess(name string, argv []string, attr *os.ProcAttr) (*os.Process, <-chan childExit, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	p, err := os.StartProcess(name, argv, attr)
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan childExit, 1)
	r.waiters[p.Pid] = ch
	return p, ch, nil
}

type Supervisor struct {
	binPath      string
	configPath   string
	assetDir     string
	memLimitMB   int
	maxProcs     int
	xhAddr       string
	wsAddr       string
	trAddr       string
	startTimeout time.Duration
	stopTimeout  time.Duration

	reaper *processReaper

	mu         sync.Mutex
	proc       *os.Process
	startedAt  time.Time
	lastUptime time.Duration
	runCancel  context.CancelFunc

	runStarted    atomic.Bool
	stopRequested atomic.Bool
	restarts      atomic.Int32
	stopGraceNS   atomic.Int64
	runReady      chan struct{}
	stopped       chan struct{}

	state       atomic.Uint64
	lastProbeNS atomic.Int64

	probeMu     sync.Mutex
	probeDialer *net.Dialer
}

func NewSupervisor() *Supervisor {
	reaper, err := newProcessReaper()
	if err != nil {
		log.Printf("[Supervisor] Warning: Subreaper initialization: %v", err)
	}

	bin := getEnv("BERMUDA_XRAY_BIN", defaultXrayBin)
	cfg := getEnv("BERMUDA_XRAY_CONFIG", defaultConfigPath)
	assets := getEnv("XRAY_LOCATION_ASSET", defaultAssetDir)

	dialer := &net.Dialer{
		Timeout:   probeDialTimeout,
		KeepAlive: -1,
		Control: func(network, address string, c syscall.RawConn) error {
			var controlErr error
			err := c.Control(func(fd uintptr) {
				controlErr = syscall.SetsockoptLinger(int(fd), syscall.SOL_SOCKET,
					syscall.SO_LINGER, &syscall.Linger{Onoff: 1, Linger: 0})
			})
			if err != nil {
				return err
			}
			return controlErr
		},
	}

	s := &Supervisor{
		binPath:      bin,
		configPath:   cfg,
		assetDir:     assets,
		memLimitMB:   getEnvInt("BERMUDA_XRAY_MEM_MB", defaultXrayMemMB),
		maxProcs:     getEnvInt("BERMUDA_XRAY_GOMAXPROCS", 0),
		xhAddr:       getEnv("BERMUDA_BACKEND_XH", defaultLoopbackXH),
		wsAddr:       getEnv("BERMUDA_BACKEND_WS", defaultLoopbackWS),
		trAddr:       getEnv("BERMUDA_BACKEND_TR", defaultLoopbackTR),
		startTimeout: defaultStartTimeout,
		stopTimeout:  defaultStopTimeout,
		reaper:       reaper,
		runReady:     make(chan struct{}),
		stopped:      make(chan struct{}),
		probeDialer:  dialer,
	}
	s.stopGraceNS.Store(int64(defaultStopTimeout))
	return s
}

func (s *Supervisor) IsRunning() bool             { return s.state.Load()&healthRunning != 0 }
func (s *Supervisor) IsReady() bool               { return s.state.Load()&healthReady != 0 }
func (s *Supervisor) Restarts() int32             { return s.restarts.Load() }
func (s *Supervisor) RunStarted() <-chan struct{} { return s.runReady }

func (s *Supervisor) childEnv() []string {
	repl := map[string]string{
		"XRAY_LOCATION_ASSET": s.assetDir,
		"GOMEMLIMIT":          fmt.Sprintf("%dMiB", s.memLimitMB),
		"GODEBUG":             mergeCSVEnv(os.Getenv("GODEBUG"), "madvdontneed=1"),
		"GOGC":                "100",
	}
	if s.maxProcs > 0 {
		repl["GOMAXPROCS"] = strconv.Itoa(s.maxProcs)
	}
	return replaceEnv(os.Environ(), repl)
}

func (s *Supervisor) Preflight() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return s.PreflightContext(ctx)
}

func (s *Supervisor) PreflightContext(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()

	if s.reaper == nil {
		return errors.New("reaper subsystem not initialized")
	}

	pipeR, pipeW, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create preflight pipe: %w", err)
	}
	defer pipeR.Close()

	attr := &os.ProcAttr{
		Dir: "/app",
		Env: replaceEnv(os.Environ(), map[string]string{"XRAY_LOCATION_ASSET": s.assetDir}),
		Files: []*os.File{nil, pipeW, pipeW},
		Sys: &syscall.SysProcAttr{
			Setpgid: true,
		},
	}

	args := []string{s.binPath, "run", "-test", "-c", s.configPath}
	proc, exitCh, err := s.reaper.StartProcess(s.binPath, args, attr)
	_ = pipeW.Close()
	if err != nil {
		return fmt.Errorf("start preflight validator: %w", err)
	}

	var outBuf bytes.Buffer
	readDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(&outBuf, pipeR)
		close(readDone)
	}()

	var exit childExit
	select {
	case exit = <-exitCh:
	case <-ctx.Done():
		_ = syscall.Kill(-proc.Pid, syscall.SIGKILL)
		exit = <-exitCh
		<-readDone
		_ = proc.Release()
		return fmt.Errorf("preflight check timed out: %w", ctx.Err())
	}

	<-readDone
	_ = proc.Release()

	if exit.status.ExitStatus() != 0 {
		return fmt.Errorf("xray preflight validation failed (code %d): %s",
			exit.status.ExitStatus(), strings.TrimSpace(outBuf.String()))
	}
	log.Printf("[Supervisor] Preflight validation passed for %s", s.configPath)
	return nil
}

func (s *Supervisor) Run(parent context.Context) error {
	s.mu.Lock()
	if !s.runStarted.CompareAndSwap(false, true) {
		s.mu.Unlock()
		return errors.New("supervisor Run may only be called once")
	}
	runCtx, cancel := context.WithCancel(parent)
	s.runCancel = cancel
	close(s.runReady)
	s.mu.Unlock()

	defer func() {
		cancel()
		s.mu.Lock()
		s.runCancel = nil
		s.mu.Unlock()
		close(s.stopped)
	}()

	backoff := initialBackoff
	for {
		if s.stopRequested.Load() || runCtx.Err() != nil {
			return nil
		}

		err := s.startAndWait(runCtx)
		if s.stopRequested.Load() || runCtx.Err() != nil {
			return nil
		}

		s.restarts.Add(1)
		s.mu.Lock()
		uptime := s.lastUptime
		s.mu.Unlock()
		if uptime >= stableWindow {
			backoff = initialBackoff
		}
		if err == nil {
			err = errors.New("Xray exited normally")
		}

		delay := jitteredBackoff(backoff)
		log.Printf("[Supervisor] Xray stopped after %s (err: %v); restart=%d in %s",
			uptime.Round(time.Millisecond), err, s.restarts.Load(), delay.Round(time.Millisecond))

		timer := time.NewTimer(delay)
		select {
		case <-runCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
		backoff *= 2
		if backoff > maxBackoffInterval {
			backoff = maxBackoffInterval
		}
	}
}

func jitteredBackoff(base time.Duration) time.Duration {
	if base <= 0 {
		return base
	}
	spread := float64(base) * backoffJitterFrac
	delta := (rand.Float64()*2 - 1) * spread
	result := time.Duration(float64(base) + delta)
	if result < 0 {
		return 0
	}
	return result
}

func (s *Supervisor) startAndWait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.lastUptime = 0
	s.mu.Unlock()

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create stdout pipe: %w", err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		_ = stdoutR.Close()
		_ = stdoutW.Close()
		return fmt.Errorf("create stderr pipe: %w", err)
	}

	attr := &os.ProcAttr{
		Dir:   "/app",
		Env:   s.childEnv(),
		Files: []*os.File{nil, stdoutW, stderrW},
		Sys: &syscall.SysProcAttr{
			Setpgid:   true,
			Pdeathsig: syscall.SIGKILL,
		},
	}

	args := []string{s.binPath, "run", "-c", s.configPath}
	proc, exitCh, err := s.reaper.StartProcess(s.binPath, args, attr)
	if err != nil {
		_ = stdoutR.Close()
		_ = stdoutW.Close()
		_ = stderrR.Close()
		_ = stderrW.Close()
		return fmt.Errorf("start Xray: %w", err)
	}

	_ = stdoutW.Close()
	_ = stderrW.Close()

	generation := s.startGeneration()
	s.mu.Lock()
	s.proc = proc
	s.startedAt = time.Now()
	s.lastUptime = 0
	s.mu.Unlock()
	pid := proc.Pid
	log.Printf("[Supervisor] Xray started pid=%d pgid=%d GOMEMLIMIT=%dMiB GOMAXPROCS=%d",
		pid, pid, s.memLimitMB, s.maxProcs)

	var pumpWG sync.WaitGroup
	pumpWG.Add(2)
	go func() { defer pumpWG.Done(); s.pumpPipe(stdoutR, "[Xray-Out]") }()
	go func() { defer pumpWG.Done(); s.pumpPipe(stderrR, "[Xray-Err]") }()
	pumpDone := make(chan struct{})
	go func() { pumpWG.Wait(); close(pumpDone) }()

	probeCtx, cancelProbes := context.WithCancel(ctx)
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		s.awaitReadiness(probeCtx, generation)
	}()

	var exit childExit
	select {
	case exit = <-exitCh:
		terminateProcessGroup(pid, 200*time.Millisecond)
	case <-ctx.Done():
		terminateProcessGroup(pid, s.terminationGrace())
		exit = <-exitCh
	}

	cancelProbes()
	<-probeDone
	s.endGeneration(generation)

	s.mu.Lock()
	s.lastUptime = time.Since(s.startedAt)
	if s.proc == proc {
		s.proc = nil
	}
	s.mu.Unlock()
	_ = proc.Release()

	select {
	case <-pumpDone:
	case <-time.After(1500 * time.Millisecond):
		_ = stdoutR.Close()
		_ = stderrR.Close()
	}
	_ = stdoutR.Close()
	_ = stderrR.Close()

	if ctx.Err() != nil || s.stopRequested.Load() {
		log.Printf("[Supervisor] Xray process group pid=%d stopped cleanly", pid)
		return nil
	}
	if exit.status.ExitStatus() != 0 {
		return fmt.Errorf("Xray pid=%d exited with status %d", pid, exit.status.ExitStatus())
	}
	return fmt.Errorf("Xray pid=%d exited normally", pid)
}

func (s *Supervisor) startGeneration() uint64 {
	for {
		old := s.state.Load()
		gen := (old >> healthGenShift) + 1
		next := (gen << healthGenShift) | healthRunning
		if s.state.CompareAndSwap(old, next) {
			return gen
		}
	}
}

func (s *Supervisor) endGeneration(generation uint64) {
	for {
		old := s.state.Load()
		if old>>healthGenShift != generation {
			return
		}
		next := (generation + 1) << healthGenShift
		if s.state.CompareAndSwap(old, next) {
			return
		}
	}
}

func (s *Supervisor) commitProbe(generation uint64, xh, ws, tr bool) bool {
	var flags uint64 = healthRunning
	if xh {
		flags |= healthXH
	}
	if ws {
		flags |= healthWS
	}
	if tr {
		flags |= healthTR
	}
	if xh && ws && tr {
		flags |= healthReady
	}
	for {
		old := s.state.Load()
		if old>>healthGenShift != generation || old&healthRunning == 0 {
			return false
		}
		if s.state.CompareAndSwap(old, (generation<<healthGenShift)|flags) {
			s.lastProbeNS.Store(time.Now().UnixNano())
			return true
		}
	}
}

func (s *Supervisor) pumpPipe(r io.Reader, prefix string) {
	reader := scannerBufPool.Get().(*bufio.Reader)
	reader.Reset(r)
	defer func() {
		reader.Reset(nil)
		scannerBufPool.Put(reader)
	}()

	for {
		fragment, err := reader.ReadSlice('\n')
		if len(fragment) != 0 {
			writePumpRecord(os.Stderr, prefix, fragment)
		}
		if err == nil || errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			return
		}
		writePumpRecord(os.Stderr, prefix, []byte("log pipe read error: "+err.Error()))
		return
	}
}

func writePumpRecord(dst *os.File, prefix string, data []byte) {
	pumpLogMu.Lock()
	defer pumpLogMu.Unlock()

	now := time.Now().UTC()
	var header [256]byte
	year := now.Year()
	header[0] = byte('0' + year/1000%10)
	header[1] = byte('0' + year/100%10)
	header[2] = byte('0' + year/10%10)
	header[3] = byte('0' + year%10)
	header[4] = '/'
	putTwoDigits(header[5:7], int(now.Month()))
	header[7] = '/'
	putTwoDigits(header[8:10], now.Day())
	header[10] = ' '
	putTwoDigits(header[11:13], now.Hour())
	header[13] = ':'
	putTwoDigits(header[14:16], now.Minute())
	header[16] = ':'
	putTwoDigits(header[17:19], now.Second())
	header[19] = ' '
	pos := 20 + copy(header[20:len(header)-1], prefix)
	header[pos] = ' '
	writeFileAll(dst, header[:pos+1])
	writeFileAll(dst, data)
	if len(data) == 0 || data[len(data)-1] != '\n' {
		writeFileAll(dst, pumpNewline[:])
	}
}

func putTwoDigits(dst []byte, value int) {
	dst[0] = byte('0' + value/10%10)
	dst[1] = byte('0' + value%10)
}

func writeFileAll(dst *os.File, data []byte) {
	for len(data) != 0 {
		n, err := dst.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil || n == 0 {
			return
		}
	}
}

func (s *Supervisor) awaitReadiness(ctx context.Context, generation uint64) {
	started := time.Now()
	deadline := started.Add(s.startTimeout)
	var warned bool
	nextDelay := time.Duration(0)

	for {
		state := s.state.Load()
		if state>>healthGenShift != generation || state&healthRunning == 0 {
			return
		}
		if nextDelay > 0 {
			timer := time.NewTimer(nextDelay)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
		}
		if ctx.Err() != nil {
			return
		}

		prior := s.state.Load()
		xh, ws, tr := s.probeAll(ctx, generation)
		isAllOk := xh && ws && tr

		if s.commitProbe(generation, xh, ws, tr) && isAllOk && prior&healthReady == 0 {
			log.Printf("[Supervisor] All loopback inbounds ready after %s", time.Since(started).Round(time.Millisecond))
		}
		if !warned && !isAllOk && time.Now().After(deadline) {
			warned = true
			log.Printf("[Supervisor] Readiness pending after %s; continuing background probes", s.startTimeout)
		}

		if time.Since(started) < fastProbeWindow {
			nextDelay = fastProbeInterval
		} else if isAllOk {
			nextDelay = slowProbeInterval
		} else {
			nextDelay = degradedProbeInterval
		}
	}
}

func (s *Supervisor) probeAll(ctx context.Context, generation uint64) (bool, bool, bool) {
	s.probeMu.Lock()
	defer s.probeMu.Unlock()
	state := s.state.Load()
	if ctx.Err() != nil || state>>healthGenShift != generation || state&healthRunning == 0 {
		return false, false, false
	}

	results := make(chan struct {
		index int
		ok    bool
	}, 3)
	addresses := [3]string{s.xhAddr, s.wsAddr, s.trAddr}
	for i, address := range addresses {
		go func(index int, addr string) {
			results <- struct {
				index int
				ok    bool
			}{index: index, ok: s.probeTCP(ctx, addr)}
		}(i, address)
	}
	var status [3]bool
	for range addresses {
		select {
		case result := <-results:
			status[result.index] = result.ok
		case <-ctx.Done():
			return false, false, false
		}
	}
	return status[0], status[1], status[2]
}

func (s *Supervisor) probeTCP(parent context.Context, addr string) bool {
	ctx, cancel := context.WithTimeout(parent, probeDialTimeout)
	defer cancel()
	conn, err := s.probeDialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func (s *Supervisor) ProbeNow() (bool, bool, bool) {
	state := s.state.Load()
	if state&healthRunning == 0 {
		return false, false, false
	}

	last := s.lastProbeNS.Load()
	if last != 0 && (time.Now().UnixNano()-last) < int64(minProbeNowInterval) {
		return state&healthXH != 0, state&healthWS != 0, state&healthTR != 0
	}

	generation := state >> healthGenShift
	xh, ws, tr := s.probeAll(context.Background(), generation)
	if !s.commitProbe(generation, xh, ws, tr) {
		return false, false, false
	}
	return xh, ws, tr
}

func (s *Supervisor) Snapshot() SupervisorHealthSnapshot {
	state := s.state.Load()
	s.mu.Lock()
	started := s.startedAt
	pid := 0
	if s.proc != nil {
		pid = s.proc.Pid
	}
	s.mu.Unlock()

	var uptime int64
	if state&healthRunning != 0 && !started.IsZero() {
		uptime = int64(time.Since(started).Seconds())
	}
	lastProbeAt := ""
	if probedAt := s.lastProbeNS.Load(); probedAt != 0 {
		lastProbeAt = time.Unix(0, probedAt).UTC().Format(time.RFC3339Nano)
	}
	return SupervisorHealthSnapshot{
		Running:     state&healthRunning != 0,
		Ready:       state&healthReady != 0,
		Restarts:    s.restarts.Load(),
		UptimeSec:   uptime,
		XHInbound:   state&healthXH != 0,
		WSInbound:   state&healthWS != 0,
		TRInbound:   state&healthTR != 0,
		LastProbeAt: lastProbeAt,
		ChildPID:    pid,
	}
}

func (s *Supervisor) Stop(grace time.Duration) {
	if grace <= 0 {
		grace = s.stopTimeout
	}
	s.stopGraceNS.Store(int64(grace))
	s.stopRequested.Store(true)
	s.mu.Lock()
	cancel := s.runCancel
	running := s.runStarted.Load()
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if running {
		select {
		case <-s.stopped:
		case <-time.After(grace + 3*time.Second):
			log.Printf("[Supervisor] Warning: Stop deadline exceeded (%s); unblocking caller", grace+3*time.Second)
		}
	}
}

func (s *Supervisor) terminationGrace() time.Duration {
	n := s.stopGraceNS.Load()
	if n <= 0 {
		return s.stopTimeout
	}
	return time.Duration(n)
}

func terminateProcessGroup(pgid int, grace time.Duration) {
	if pgid <= 1 {
		return
	}
	if err := syscall.Kill(-pgid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		log.Printf("[Supervisor] SIGTERM process group %d: %v", pgid, err)
	}
	deadline := time.Now().Add(grace)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := syscall.Kill(-pgid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if !time.Now().Before(deadline) {
			log.Printf("[Supervisor] Process group %d exceeded TERM grace; escalating to SIGKILL", pgid)
			if killErr := syscall.Kill(-pgid, syscall.SIGKILL); killErr != nil && !errors.Is(killErr, syscall.ESRCH) {
				log.Printf("[Supervisor] SIGKILL process group %d: %v", pgid, killErr)
			}
			return
		}
		<-ticker.C
	}
}

func replaceEnv(env []string, replacements map[string]string) []string {
	out := make([]string, 0, len(env)+len(replacements))
	seen := make(map[string]struct{}, len(replacements))
	for _, item := range env {
		key, _, ok := strings.Cut(item, "=")
		if !ok {
			out = append(out, item)
			continue
		}
		if value, replace := replacements[key]; replace {
			if _, already := seen[key]; !already {
				out = append(out, key+"="+value)
				seen[key] = struct{}{}
			}
			continue
		}
		out = append(out, item)
	}
	for key, value := range replacements {
		if _, ok := seen[key]; !ok {
			out = append(out, key+"="+value)
		}
	}
	return out
}

func mergeCSVEnv(existing, required string) string {
	key, _, _ := strings.Cut(required, "=")
	parts := strings.Split(existing, ",")
	kept := parts[:0]
	for _, part := range parts {
		part = strings.TrimSpace(part)
		partKey, _, _ := strings.Cut(part, "=")
		if partKey != key && part != "" {
			kept = append(kept, part)
		}
	}
	kept = append(kept, required)
	return strings.Join(kept, ",")
}

func getEnvInt(key string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func getEnv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
