package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultPathXH = "/api/v1/sync"
	defaultPathWS = "/api/v1/live"
	defaultPathTR = "/api/v1/gateway"

	defaultHealthPath = "/.well-known/hc-5b1e7c"

	openrestyServerToken = "openresty"

	openrestyIndexLastModified = "Fri, 06 Aug 2021 21:32:34 GMT"
	openrestyIndexETag         = "\"610daa72-449\""

	openresty50xLastModified = openrestyIndexLastModified
	openresty50xETag         = "\"610daa72-3d6\""

	defaultProxyBufferSize  = 64 * 1024
	defaultTransportBufSize = 64 * 1024
	bufferPoolCapacity      = 64
	halfCloseGrace          = 25 * time.Second
	backendDialTimeout      = 5 * time.Second
	backendHandshakeTimeout = 5 * time.Second
	tunnelDrainPollInterval = 50 * time.Millisecond
)

var openrestyModTime = time.Date(2021, time.August, 6, 21, 32, 34, 0, time.UTC)

const openrestyWelcomeHTML = "<!DOCTYPE html>\n" +
	"<html>\n" +
	"<head>\n" +
	"<meta content=\"text/html;charset=utf-8\" http-equiv=\"Content-Type\">\n" +
	"<meta content=\"utf-8\" http-equiv=\"encoding\">\n" +
	"<title>Welcome to OpenResty!</title>\n" +
	"<style>\n" +
	"    body {\n" +
	"        width: 35em;\n" +
	"        margin: 0 auto;\n" +
	"        font-family: Tahoma, Verdana, Arial, sans-serif;\n" +
	"    }\n" +
	"</style>\n" +
	"</head>\n" +
	"<body>\n" +
	"<h1>Welcome to OpenResty!</h1>\n" +
	"<p>If you see this page, the OpenResty web platform is successfully installed and\n" +
	"working. Further configuration is required.</p>\n" +
	"\n" +
	"<p>For online documentation and support please refer to our\n" +
	"<a href=\"https://openresty.org/\">openresty.org</a> site<br/>\n" +
	"Commercial support is available at\n" +
	"<a href=\"https://openresty.com/\">openresty.com</a>.</p>\n" +
	"<p>We have articles on troubleshooting issues like <a href=\"https://blog.openresty.com/en/lua-cpu-flame-graph/?src=wb\">high CPU usage</a> and\n" +
	"<a href=\"https://blog.openresty.com/en/how-or-alloc-mem/\">large memory usage</a> on <a href=\"https://blog.openresty.com/\">our official blog site</a>.\n" +
	"<p><em>Thank you for flying <a href=\"https://openresty.org/\">OpenResty</a>.</em></p>\n" +
	"</body>\n" +
	"</html>\n"

const openresty50xHTML = "<!DOCTYPE html>\n" +
	"<html>\n" +
	"<head>\n" +
	"<meta content=\"text/html;charset=utf-8\" http-equiv=\"Content-Type\">\n" +
	"<meta content=\"utf-8\" http-equiv=\"encoding\">\n" +
	"<title>Error</title>\n" +
	"<style>\n" +
	"    body {\n" +
	"        width: 35em;\n" +
	"        margin: 0 auto;\n" +
	"        font-family: Tahoma, Verdana, Arial, sans-serif;\n" +
	"    }\n" +
	"</style>\n" +
	"</head>\n" +
	"<body>\n" +
	"<h1>An error occurred.</h1>\n" +
	"<p>Sorry, the page you are looking for is currently unavailable.<br/>\n" +
	"Please try again later.</p>\n" +
	"<p>If you are the system administrator of this resource then you should check\n" +
	"the <a href=\"http://nginx.org/r/error_log\">error log</a> for details.</p>\n" +
	"<p>We have articles on troubleshooting issues like <a href=\"https://blog.openresty.com/en/lua-cpu-flame-graph/?src=wb\">high CPU usage</a> and\n" +
	"<a href=\"https://blog.openresty.com/en/how-or-alloc-mem/\">large memory usage</a> on <a href=\"https://blog.openresty.com/\">our official blog site</a>.\n" +
	"<p><em>Faithfully yours, <a href=\"https://openresty.org/\">OpenResty</a>.</em></p>\n" +
	"</body>\n" +
	"</html>\n"

const openresty404HTML = "<html>\r\n<head><title>404 Not Found</title></head>\r\n<body>\r\n<center><h1>404 Not Found</h1></center>\r\n<hr><center>openresty</center>\r\n</body>\r\n</html>\r\n"
const openresty405HTML = "<html>\r\n<head><title>405 Not Allowed</title></head>\r\n<body>\r\n<center><h1>405 Not Allowed</h1></center>\r\n<hr><center>openresty</center>\r\n</body>\r\n</html>\r\n"

type recycledBufferPool struct {
	size int
	ch   chan []byte
}

func newRecycledBufferPool(size int) *recycledBufferPool {
	return &recycledBufferPool{size: size, ch: make(chan []byte, bufferPoolCapacity)}
}

func (p *recycledBufferPool) Get() []byte {
	select {
	case b := <-p.ch:
		return b
	default:
		return make([]byte, p.size)
	}
}

func (p *recycledBufferPool) Put(b []byte) {
	if cap(b) != p.size {
		return
	}
	b = b[:cap(b)]
	select {
	case p.ch <- b:
	default:
	}
}

type HealthResponse struct {
	Status     string                   `json:"status"`
	Draining   bool                     `json:"draining"`
	Healthy    bool                     `json:"healthy"`
	UptimeSec  int64                    `json:"uptime_sec"`
	Supervisor SupervisorHealthSnapshot `json:"supervisor"`
	Telemetry  TelemetrySnapshot        `json:"telemetry"`
}

type TelemetrySnapshot struct {
	OpenConnections int64 `json:"open_connections"`
	ActiveTunnels   int64 `json:"active_tunnels"`
	TunnelsOpened   int64 `json:"tunnels_opened_total"`
	TotalRequests   int64 `json:"total_requests"`
}

type Gateway struct {
	sup *Supervisor

	pathXH, pathWS, pathTR          string
	backendXH, backendWS, backendTR string
	healthPath                      string
	adminToken                      string

	xhProxy *httputil.ReverseProxy
	tr      *http.Transport
	dialer  net.Dialer
	bufPool *recycledBufferPool

	draining    atomic.Bool
	openConns   atomic.Int64
	totalReq    atomic.Int64
	tunnelsLive atomic.Int64
	tunnelsHit  atomic.Int64

	tunnelsMu sync.Mutex
	tunnels   map[*net.TCPConn]struct{}

	startedAt time.Time
}

func NewGateway(sup *Supervisor) *Gateway {
	return newGateway(sup, defaultProxyBufferSize, defaultTransportBufSize)
}

func newGateway(sup *Supervisor, proxyBuf, transportBuf int) *Gateway {
	g := &Gateway{
		sup:        sup,
		pathXH:     getEnv("BERMUDA_PATH_XH", defaultPathXH),
		pathWS:     getEnv("BERMUDA_PATH_WS", defaultPathWS),
		pathTR:     getEnv("BERMUDA_PATH_TR", defaultPathTR),
		backendXH:  getEnv("BERMUDA_BACKEND_XH", defaultLoopbackXH),
		backendWS:  getEnv("BERMUDA_BACKEND_WS", defaultLoopbackWS),
		backendTR:  getEnv("BERMUDA_BACKEND_TR", defaultLoopbackTR),
		healthPath: getEnv("BERMUDA_HEALTH_PATH", defaultHealthPath),
		adminToken: strings.TrimSpace(getEnv("BERMUDA_ADMIN_TOKEN", "")),
		bufPool:    newRecycledBufferPool(proxyBuf),
		tunnels:    make(map[*net.TCPConn]struct{}),
		startedAt:  time.Now(),
		dialer:     net.Dialer{Timeout: backendDialTimeout, KeepAlive: -1},
	}
	g.tr = newLoopbackTransport(transportBuf)
	proxy, err := newBackendProxy(g.backendXH, g.tr, g.bufPool)
	if err != nil {
		log.Fatalf("[Gateway] Fatal: invalid XHTTP backend address %q: %v", g.backendXH, err)
	}
	g.xhProxy = proxy
	return g
}

func newLoopbackTransport(bufSize int) *http.Transport {
	bufSize = min(max(bufSize, 4<<10), 64<<10)
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: -1}
	return &http.Transport{
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          128,
		MaxIdleConnsPerHost:   64,
		MaxConnsPerHost:       128,
		IdleConnTimeout:       90 * time.Second,
		DisableCompression:    true,
		ReadBufferSize:        bufSize,
		WriteBufferSize:       bufSize,
		ResponseHeaderTimeout: 120 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

func newBackendProxy(targetAddr string, tr http.RoundTripper, bp httputil.BufferPool) (*httputil.ReverseProxy, error) {
	targetURL, err := url.Parse("http://" + targetAddr)
	if err != nil {
		return nil, err
	}
	if targetURL.Host == "" {
		return nil, errors.New("missing target host")
	}
	return &httputil.ReverseProxy{
		Transport:     tr,
		BufferPool:    bp,
		FlushInterval: -1,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = targetURL.Scheme
			pr.Out.URL.Host = targetURL.Host
			pr.Out.Host = pr.In.Host
			pr.SetXForwarded()
			pr.Out.Header.Set("X-Accel-Buffering", "no")
			if cfIP := pr.In.Header.Get("CF-Connecting-IP"); cfIP != "" {
				pr.Out.Header.Set("CF-Connecting-IP", cfIP)
			}
		},
		ModifyResponse: func(res *http.Response) error {
			switch res.StatusCode {
			case http.StatusBadRequest, http.StatusNotFound:
				rewriteTo404(res)
			case http.StatusMethodNotAllowed:
				rewriteTo405(res)
			default:
				res.Header.Set("X-Accel-Buffering", "no")
				res.Header.Set("Cache-Control", "no-cache, no-store, must-revalidate")
				res.Header.Set("Server", openrestyServerToken)
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if !errors.Is(err, context.Canceled) {
				log.Printf("[Proxy] Error: backend %s unreachable: %v", targetAddr, err)
			}
			if r.Method == http.MethodPost {
				camoOpenResty(w, r, http.StatusMethodNotAllowed, openresty405HTML, nil)
			} else {
				camoOpenResty(w, r, http.StatusBadGateway, openresty50xHTML, map[string]string{
					"Last-Modified": openresty50xLastModified,
					"ETag":          openresty50xETag,
				})
			}
		},
	}, nil
}

func rewriteTo404(res *http.Response) {
	if res.Body != nil {
		_ = res.Body.Close()
	}
	res.StatusCode = http.StatusNotFound
	res.Status = "404 Not Found"
	res.Header = http.Header{}
	res.Header.Set("Server", openrestyServerToken)
	res.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	res.Header.Set("Content-Type", "text/html")
	res.Header.Set("Content-Length", strconv.Itoa(len(openresty404HTML)))
	res.Body = io.NopCloser(strings.NewReader(openresty404HTML))
	res.ContentLength = int64(len(openresty404HTML))
	res.TransferEncoding = nil
}

func rewriteTo405(res *http.Response) {
	if res.Body != nil {
		_ = res.Body.Close()
	}
	res.StatusCode = http.StatusMethodNotAllowed
	res.Status = "405 Not Allowed"
	res.Header = http.Header{}
	res.Header.Set("Server", openrestyServerToken)
	res.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	res.Header.Set("Content-Type", "text/html")
	res.Header.Set("Content-Length", strconv.Itoa(len(openresty405HTML)))
	res.Body = io.NopCloser(strings.NewReader(openresty405HTML))
	res.ContentLength = int64(len(openresty405HTML))
	res.TransferEncoding = nil
}

func (g *Gateway) SetDraining()     { g.draining.Store(true) }
func (g *Gateway) IsDraining() bool { return g.draining.Load() }

func (g *Gateway) CloseIdleBackendConns() {
	if g.tr != nil {
		g.tr.CloseIdleConnections()
	}
}

func (g *Gateway) TrackConnState(_ net.Conn, state http.ConnState) {
	switch state {
	case http.StateNew:
		g.openConns.Add(1)
	case http.StateClosed, http.StateHijacked:
		g.openConns.Add(-1)
	}
}

func (g *Gateway) track(a, b *net.TCPConn) bool {
	g.tunnelsMu.Lock()
	defer g.tunnelsMu.Unlock()
	if g.draining.Load() {
		return false
	}
	g.tunnels[a] = struct{}{}
	g.tunnels[b] = struct{}{}
	g.tunnelsLive.Add(1)
	g.tunnelsHit.Add(1)
	return true
}

func (g *Gateway) untrack(a, b *net.TCPConn) {
	g.tunnelsMu.Lock()
	delete(g.tunnels, a)
	delete(g.tunnels, b)
	g.tunnelsLive.Add(-1)
	g.tunnelsMu.Unlock()
}

func (g *Gateway) WaitTunnels(ctx context.Context) {
	t := time.NewTicker(tunnelDrainPollInterval)
	defer t.Stop()
	for g.tunnelsLive.Load() > 0 {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (g *Gateway) CloseTunnels() {
	g.tunnelsMu.Lock()
	conns := make([]*net.TCPConn, 0, len(g.tunnels))
	for c := range g.tunnels {
		conns = append(conns, c)
	}
	g.tunnelsMu.Unlock()
	for _, c := range conns {
		_ = c.SetDeadline(time.Now())
		_ = c.Close()
	}
}

func matchBase(p, base string) bool {
	return p == base || strings.HasPrefix(p, base+"/")
}

func (g *Gateway) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.totalReq.Add(1)
		p := r.URL.Path
		switch {
		case p == g.healthPath:
			g.handleHealth(w, r)
		case matchBase(p, g.pathXH):
			if g.draining.Load() {
				camoOpenResty(w, r, http.StatusServiceUnavailable, openresty50xHTML, nil)
				return
			}
			if !g.validXHTTP(r) {
				g.camo404(w, r)
				return
			}
			if r.ProtoMajor == 1 {
				_ = http.NewResponseController(w).EnableFullDuplex()
			}
			g.xhProxy.ServeHTTP(w, r)
		case matchBase(p, g.pathWS):
			g.serveWS(w, r, g.backendWS)
		case matchBase(p, g.pathTR):
			g.serveWS(w, r, g.backendTR)
		default:
			g.serveCamouflage(w, r)
		}
	})
}

func (g *Gateway) validXHTTP(r *http.Request) bool {
	switch r.Method {
	case http.MethodPost:
		return true
	case http.MethodGet:
		return strings.Trim(strings.TrimPrefix(r.URL.Path, g.pathXH), "/") != ""
	}
	return false
}

func headerHasToken(h http.Header, key, token string) bool {
	for _, v := range h.Values(key) {
		for _, t := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(t), token) {
				return true
			}
		}
	}
	return false
}

func validWSUpgrade(r *http.Request) bool {
	if r.Method != http.MethodGet || r.ProtoMajor != 1 || r.ProtoMinor < 1 {
		return false
	}
	if !headerHasToken(r.Header, "Connection", "upgrade") || !headerHasToken(r.Header, "Upgrade", "websocket") {
		return false
	}
	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		return false
	}
	key, err := base64.StdEncoding.DecodeString(r.Header.Get("Sec-WebSocket-Key"))
	return err == nil && len(key) == 16
}

func (g *Gateway) serveWS(w http.ResponseWriter, r *http.Request, backend string) {
	if !validWSUpgrade(r) {
		g.camo404(w, r)
		return
	}
	if g.draining.Load() {
		camoOpenResty(w, r, http.StatusServiceUnavailable, openresty50xHTML, nil)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), backendDialTimeout)
	raw, err := g.dialer.DialContext(ctx, "tcp", backend)
	cancel()
	if err != nil {
		camoOpenResty(w, r, http.StatusBadGateway, openresty50xHTML, nil)
		return
	}
	bc, ok := raw.(*net.TCPConn)
	if !ok {
		_ = raw.Close()
		camoOpenResty(w, r, http.StatusBadGateway, openresty50xHTML, nil)
		return
	}

	_ = bc.SetDeadline(time.Now().Add(backendHandshakeTimeout))
	out := r.Clone(r.Context())
	out.RequestURI = ""
	out.Body = http.NoBody
	out.ContentLength = 0
	if _, has := out.Header["User-Agent"]; !has {
		out.Header["User-Agent"] = []string{""}
	}
	if cfIP := r.Header.Get("CF-Connecting-IP"); cfIP != "" {
		out.Header.Set("CF-Connecting-IP", cfIP)
	}
	if proto := r.Header.Get("Sec-WebSocket-Protocol"); proto != "" {
		out.Header.Set("Sec-WebSocket-Protocol", proto)
	}
	out.Header.Set("X-Accel-Buffering", "no")

	if err := out.Write(bc); err != nil {
		_ = bc.Close()
		camoOpenResty(w, r, http.StatusBadGateway, openresty50xHTML, nil)
		return
	}
	br := bufio.NewReaderSize(bc, 4096)
	resp, err := http.ReadResponse(br, out)
	if err != nil || resp.StatusCode != http.StatusSwitchingProtocols {
		_ = bc.Close()
		g.camo404(w, r)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		_ = bc.Close()
		camoOpenResty(w, r, http.StatusBadGateway, openresty50xHTML, nil)
		return
	}
	cc, brw, err := hj.Hijack()
	if err != nil {
		_ = bc.Close()
		return
	}
	ct, ok := cc.(*net.TCPConn)
	if !ok {
		_ = cc.Close()
		_ = bc.Close()
		return
	}

	if !g.track(ct, bc) {
		_ = ct.Close()
		_ = bc.Close()
		return
	}
	defer g.untrack(ct, bc)
	defer ct.Close()
	defer bc.Close()

	deadline := time.Now().Add(backendHandshakeTimeout)
	_ = ct.SetDeadline(deadline)
	_ = bc.SetDeadline(deadline)

	var head bytes.Buffer
	head.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
	_ = resp.Header.Write(&head)
	head.WriteString("\r\n")
	if n := br.Buffered(); n > 0 {
		pending := make([]byte, n)
		if _, err := io.ReadFull(br, pending); err == nil {
			head.Write(pending)
		}
	}
	if _, err := ct.Write(head.Bytes()); err != nil {
		return
	}

	_ = brw.Writer.Flush()
	if n := brw.Reader.Buffered(); n > 0 {
		if _, err := io.CopyN(bc, brw.Reader, int64(n)); err != nil {
			return
		}
	}

	_ = ct.SetDeadline(time.Time{})
	_ = bc.SetDeadline(time.Time{})

	g.relay(ct, bc)
}

func (g *Gateway) relay(client, backend *net.TCPConn) {
	done := make(chan struct{}, 2)
	go pipe(backend, client, done)
	go pipe(client, backend, done)

	<-done

	drainDeadline := time.Now().Add(halfCloseGrace)
	_ = client.SetDeadline(drainDeadline)
	_ = backend.SetDeadline(drainDeadline)

	<-done
}

func pipe(dst, src *net.TCPConn, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()
	_, err := io.Copy(dst, src)
	if err != nil {
		_ = dst.Close()
		_ = src.Close()
		return
	}
	_ = dst.CloseWrite()
}

func (g *Gateway) camo404(w http.ResponseWriter, r *http.Request) {
	camoOpenResty(w, r, http.StatusNotFound, openresty404HTML, nil)
}

func (g *Gateway) serveCamouflage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		camoOpenResty(w, r, http.StatusMethodNotAllowed, openresty405HTML, nil)
		return
	}

	if r.URL.Path == "/index.html/" || r.URL.Path == "/50x.html/" {
		g.camo404(w, r)
		return
	}

	clean := r.URL.Path
	switch {
	case clean == "/" || clean == "/index.html":
		g.serveStatic(w, r, "index.html", openrestyIndexETag, []byte(openrestyWelcomeHTML))
	case clean == "/50x.html":
		g.serveStatic(w, r, "50x.html", openresty50xETag, []byte(openresty50xHTML))
	default:
		g.camo404(w, r)
	}
}

func (g *Gateway) serveStatic(w http.ResponseWriter, r *http.Request, name, etag string, content []byte) {
	h := w.Header()
	h["Server"] = []string{openrestyServerToken}
	h["ETag"] = []string{etag}
	h["Content-Type"] = []string{"text/html"}

	if shouldCloseConnection(r) {
		h["Connection"] = []string{"close"}
	} else {
		h.Del("Connection")
	}

	http.ServeContent(w, r, name, openrestyModTime, bytes.NewReader(content))
}

func (g *Gateway) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		g.camo404(w, r)
		return
	}

	snap := g.sup.Snapshot()
	snap.ChildPID = 0
	draining := g.draining.Load()
	healthy := !draining && snap.Running && snap.Ready

	tok := r.Header.Get("X-Bermuda-Token")
	if g.adminToken != "" && subtle.ConstantTimeCompare([]byte(tok), []byte(g.adminToken)) == 1 {
		status := "ok"
		code := http.StatusOK
		switch {
		case draining:
			status, code = "draining", http.StatusServiceUnavailable
		case !snap.Running:
			status, code = "down", http.StatusServiceUnavailable
		case !snap.Ready:
			status, code = "starting", http.StatusServiceUnavailable
		}

		h := w.Header()
		h["Server"] = []string{openrestyServerToken}
		h["Content-Type"] = []string{"application/json; charset=utf-8"}
		h["Cache-Control"] = []string{"no-store, no-cache, must-revalidate"}
		w.WriteHeader(code)
		if r.Method == http.MethodHead {
			return
		}
		_ = json.NewEncoder(w).Encode(HealthResponse{
			Status:     status,
			Draining:   draining,
			Healthy:    healthy,
			UptimeSec:  int64(time.Since(g.startedAt).Seconds()),
			Supervisor: snap,
			Telemetry: TelemetrySnapshot{
				OpenConnections: g.openConns.Load(),
				ActiveTunnels:   g.tunnelsLive.Load(),
				TunnelsOpened:   g.tunnelsHit.Load(),
				TotalRequests:   g.totalReq.Load(),
			},
		})
		return
	}

	if healthy {
		g.serveStatic(w, r, "index.html", openrestyIndexETag, []byte(openrestyWelcomeHTML))
	} else {
		camoOpenResty(w, r, http.StatusServiceUnavailable, openresty50xHTML, map[string]string{
			"ETag": openresty50xETag,
		})
	}
}

func camoOpenResty(w http.ResponseWriter, r *http.Request, code int, body string, extra map[string]string) {
	h := w.Header()
	h["Server"] = []string{openrestyServerToken}
	h["Date"] = []string{time.Now().UTC().Format(http.TimeFormat)}
	h["Content-Type"] = []string{"text/html"}
	h["Content-Length"] = []string{strconv.Itoa(len(body))}

	if shouldCloseConnection(r) {
		h["Connection"] = []string{"close"}
	} else {
		h.Del("Connection")
	}

	for k, v := range extra {
		if k == "ETag" {
			h["ETag"] = []string{v}
		} else {
			h[k] = []string{v}
		}
	}

	w.WriteHeader(code)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = io.WriteString(w, body)
}

func shouldCloseConnection(r *http.Request) bool {
	return r.Close || headerHasToken(r.Header, "Connection", "close")
}
