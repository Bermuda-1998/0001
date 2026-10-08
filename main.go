package main

import (
	"context"
	"errors"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	defaultPort            = "8080"
	fallbackSelfMemMB      = 128
	fallbackXrayMemMB      = 550
	defaultGOMAXPROCS      = 2
	gatewayMemPercent      = 12
	xrayMemPercent         = 55
	httpDrainTimeout       = 10 * time.Second
	edgeKeepAlivePeriod    = 15 * time.Second
	drainPropagationWindow = 500 * time.Millisecond
	tcpUserTimeoutOpt      = 18
)

func cgroupMemoryLimit() (uint64, bool) {
	candidates := []string{
		"/sys/fs/cgroup/memory.max",
		"/sys/fs/cgroup/memory/memory.limit_in_bytes",
	}
	for _, p := range candidates {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		s := strings.TrimSpace(string(data))
		if s == "" || s == "max" {
			continue
		}
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil || n == 0 || n >= 1<<50 {
			continue
		}
		return n, true
	}
	return 0, false
}

func deriveMemoryBudget() (gwMB, xrayMB int) {
	gwMB, xrayMB = fallbackSelfMemMB, fallbackXrayMemMB
	limit, limited := cgroupMemoryLimit()
	if limited {
		totalMB := int(limit >> 20)
		gwMB = totalMB * gatewayMemPercent / 100
		xrayMB = totalMB * xrayMemPercent / 100

		allowedMB := totalMB * 67 / 100
		if gwMB < 32 {
			gwMB = 32
		}
		if xrayMB < 64 {
			xrayMB = 64
		}
		if total := gwMB + xrayMB; total > allowedMB && allowedMB >= 96 {
			gwMB = allowedMB * gatewayMemPercent / 67
			xrayMB = allowedMB * xrayMemPercent / 67
		}
	}

	gwMB = getEnvInt("BERMUDA_SELF_MEM_MB", gwMB)
	xrayMB = getEnvInt("BERMUDA_XRAY_MEM_MB", xrayMB)
	return gwMB, xrayMB
}

func applyMemoryCeiling(selfMB int) {
	debug.SetMemoryLimit(int64(selfMB) << 20)
	debug.SetGCPercent(100)
}

func cgroupCPUQuota() (float64, bool) {
	if data, err := os.ReadFile("/sys/fs/cgroup/cpu.max"); err == nil {
		f := strings.Fields(string(data))
		if len(f) == 2 && f[0] != "max" {
			q, e1 := strconv.ParseFloat(f[0], 64)
			p, e2 := strconv.ParseFloat(f[1], 64)
			if e1 == nil && e2 == nil && q > 0 && p > 0 {
				return q / p, true
			}
		}
		return 0, false
	}
	qb, e1 := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
	pb, e2 := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
	if e1 == nil && e2 == nil {
		q, e3 := strconv.ParseFloat(strings.TrimSpace(string(qb)), 64)
		p, e4 := strconv.ParseFloat(strings.TrimSpace(string(pb)), 64)
		if e3 == nil && e4 == nil && q > 0 && p > 0 {
			return q / p, true
		}
	}
	return 0, false
}

func applyGOMAXPROCS() int {
	n := getEnvInt("BERMUDA_GOMAXPROCS", 0)
	if n == 0 {
		if q, ok := cgroupCPUQuota(); ok {
			n = int(math.Ceil(q))
		}
	}
	if n <= 0 {
		n = defaultGOMAXPROCS
	}
	if cpus := runtime.NumCPU(); n > cpus {
		n = cpus
	}
	runtime.GOMAXPROCS(n)
	return n
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func teardown(srv *http.Server, gw *Gateway, sup *Supervisor, supCancel context.CancelFunc, graceful bool) {
	hardDeadline := time.Now().Add(23 * time.Second)

	gw.SetDraining()
	if graceful {
		log.Println("[Gateway] Stage 1/5: Health status flipped to 503 (traffic shedding)...")
		time.Sleep(drainPropagationWindow)
	}

	if graceful {
		log.Println("[Gateway] Stage 2/5: Draining HTTP server listeners and short-lived requests...")
		httpDeadline := minTime(time.Now().Add(5*time.Second), hardDeadline)
		httpCtx, cancelHTTP := context.WithDeadline(context.Background(), httpDeadline)
		if err := srv.Shutdown(httpCtx); err != nil {
			log.Printf("[Gateway] HTTP server drain timeout (%v); forcing listener closure", err)
			_ = srv.Close()
		} else {
			log.Println("[Gateway] HTTP server listener drained successfully")
		}
		cancelHTTP()

		log.Println("[Gateway] Stage 3/5: Waiting for active WebSocket/hijacked tunnels to conclude...")
		tunnelDeadline := minTime(hardDeadline.Add(-8*time.Second), time.Now().Add(httpDrainTimeout))
		if tunnelDeadline.After(time.Now()) {
			tunnelCtx, cancelTunnel := context.WithDeadline(context.Background(), tunnelDeadline)
			gw.WaitTunnels(tunnelCtx)
			cancelTunnel()
		}
	} else {
		_ = srv.Close()
	}

	log.Println("[Gateway] Stage 4/5: Force-closing remaining hijacked sockets and backend pools...")
	gw.CloseTunnels()
	gw.CloseIdleBackendConns()

	log.Println("[Gateway] Stage 5/5: Tearing down child Xray process group...")
	supCancel()
	remainingSupTime := time.Until(hardDeadline)
	if remainingSupTime <= 0 {
		remainingSupTime = 2 * time.Second
	}
	sup.Stop(remainingSupTime)
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	log.Println("[Gateway] Initializing BERMUDA Stealth Gateway NG...")

	gwMB, xrayMB := deriveMemoryBudget()
	applyMemoryCeiling(gwMB)
	procs := applyGOMAXPROCS()

	_ = os.Setenv("BERMUDA_XRAY_MEM_MB", strconv.Itoa(xrayMB))
	_ = os.Setenv("BERMUDA_XRAY_GOMAXPROCS", strconv.Itoa(procs))
	log.Printf("[Runtime] Dynamic memory ceiling: Gateway=%dMiB, Xray=%dMiB (GOGC=100) | GOMAXPROCS=%d",
		gwMB, xrayMB, procs)

	port := getEnv("PORT", defaultPort)

	sup := NewSupervisor()
	if err := sup.Preflight(); err != nil {
		log.Printf("[Gateway] Warning: Supervisor preflight issue: %v. Continuing to start...", err)
	}

	gw := NewGateway(sup)

	cfToken := strings.TrimSpace(getEnv("CLOUDFLARE_API_TOKEN", os.Getenv("BERMUDA_CF_API_TOKEN")))
	cfZoneID := strings.TrimSpace(getEnv("CLOUDFLARE_ZONE_ID", os.Getenv("BERMUDA_CF_ZONE_ID")))
	if cfToken != "" && cfZoneID != "" {
		cf := NewCloudflareClient(cfToken, cfZoneID)
		go cf.Tune(context.Background())
	} else {
		log.Println("[Cloudflare] Notice: CLOUDFLARE_API_TOKEN or CLOUDFLARE_ZONE_ID unset. Edge auto-tuning bypassed.")
	}

	sigCtx, stopSig := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSig()
	supCtx, supCancel := context.WithCancel(context.Background())

	supErrCh := make(chan error, 1)
	go func() {
		supErrCh <- sup.Run(supCtx)
	}()

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           gw.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      0,
		IdleTimeout:       16 * time.Minute,
		MaxHeaderBytes:    32 << 10,
		ConnState:         gw.TrackConnState,
	}

	lc := net.ListenConfig{
		KeepAliveConfig: net.KeepAliveConfig{
			Enable:   true,
			Idle:     edgeKeepAlivePeriod,
			Interval: edgeKeepAlivePeriod,
			Count:    4,
		},
		Control: func(network, address string, c syscall.RawConn) error {
			var controlErr error
			err := c.Control(func(fd uintptr) {
				if e := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpUserTimeoutOpt, 90000); e != nil {
					controlErr = e
				}
			})
			if err != nil {
				return err
			}
			return controlErr
		},
	}

	ln, err := lc.Listen(context.Background(), "tcp", srv.Addr)
	if err != nil {
		log.Fatalf("[Gateway] Fatal: Cannot bind listener on %s: %v", srv.Addr, err)
	}

	serverErrCh := make(chan error, 1)
	go func() {
		log.Printf("[Gateway] Edge listener active on :%s (PID %d, GOMAXPROCS=%d)", port, os.Getpid(), procs)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
	}()

	select {
	case err := <-serverErrCh:
		log.Printf("[Gateway] Fatal: HTTP server failure: %v", err)
		teardown(srv, gw, sup, supCancel, false)
		os.Exit(1)
	case err := <-supErrCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[Gateway] Fatal: Supervisor halted unexpectedly: %v", err)
		}
		teardown(srv, gw, sup, supCancel, false)
		os.Exit(1)
	case <-sigCtx.Done():
		log.Println("[Gateway] Termination signal intercepted. Commencing graceful teardown...")
	}

	teardown(srv, gw, sup, supCancel, true)
	log.Println("[Gateway] BERMUDA Stealth Gateway shutdown complete. Ports released cleanly. Exit 0.")
}
