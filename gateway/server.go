// Package gateway provides the HTTP server and REST/WebSocket handlers.
package gateway

import (
	"context"
	"crypto/tls"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.uber.org/zap"

	"github.com/DistributedClocks/vectorclock-system/gateway/auth"
	"github.com/DistributedClocks/vectorclock-system/internal/causality"
	"github.com/DistributedClocks/vectorclock-system/internal/conflict"
	"github.com/DistributedClocks/vectorclock-system/internal/events"
	"github.com/DistributedClocks/vectorclock-system/internal/simulation"
)

// Tunable constants. Centralised so they can be reasoned about and audited
// in one place rather than scattered as magic numbers.
const (
	// HTTPReadHeaderTimeout caps the time a client may take to send headers.
	// Defends against Slowloris attacks.
	HTTPReadHeaderTimeout = 5 * time.Second
	// HTTPReadTimeout caps total request read time (headers + body).
	HTTPReadTimeout = 15 * time.Second
	// HTTPWriteTimeout caps response write time.
	HTTPWriteTimeout = 15 * time.Second
	// HTTPIdleTimeout caps how long a keep-alive connection may sit idle.
	HTTPIdleTimeout = 120 * time.Second
	// MaxJSONBodyBytes caps the size of any POST/PUT body to defend against
	// DoS via large payloads. 1 MiB is far above any legitimate request.
	MaxJSONBodyBytes int64 = 1 << 20
	// ShutdownGracePeriod is the max time graceful shutdown waits for
	// in-flight requests to drain.
	ShutdownGracePeriod = 10 * time.Second
	// WSPingInterval is how often the WS hub pings clients.
	WSPingInterval = 30 * time.Second
	// WSPongDeadline is the read deadline extension on pong receipt.
	WSPongDeadline = 60 * time.Second
	// WSReadBufferSize is the WS upgrader's read buffer.
	WSReadBufferSize = 1024
	// WSWriteBufferSize is the WS upgrader's write buffer.
	WSWriteBufferSize = 4096
	// WSClientSendBuffer is the per-client outbound channel buffer.
	WSClientSendBuffer = 256
	// WSMaxIncomingMessageBytes caps the size of any single incoming WS
	// frame. The current handler ignores client messages; this is a
	// tight, defensive cap to prevent OOM via a hostile client.
	WSMaxIncomingMessageBytes int64 = 64 * 1024
	// WSMaxClientsPerOrigin caps the number of WS clients from a single
	// origin (or "*" if empty). Defends against connection floods.
	WSMaxClientsPerOrigin = 100
)

// Metrics records request and runtime telemetry. Initialised lazily and
// registered with a private registry so tests can construct isolated instances.
type Metrics struct {
	registry *prometheus.Registry

	HTTPRequests       *prometheus.CounterVec
	HTTPDuration       *prometheus.HistogramVec
	HTTPInFlight       prometheus.Gauge
	HTTPErrors         *prometheus.CounterVec
	WSClientsConnected prometheus.Gauge
	WSMessagesSent     prometheus.Counter
	WSEventsDropped    prometheus.Counter
	PanicsRecovered    prometheus.Counter
}

// NewMetrics constructs a Metrics backed by a private registry. Tests can
// call this repeatedly without colliding on the default registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	factory := promauto.With(reg)
	m := &Metrics{
		registry: reg,
		HTTPRequests: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "vc_http_requests_total",
			Help: "Total HTTP requests handled, partitioned by method/route/status.",
		}, []string{"method", "route", "status"}),
		HTTPDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "vc_http_request_duration_seconds",
			Help:    "Request handling latency.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"method", "route"}),
		HTTPInFlight: factory.NewGauge(prometheus.GaugeOpts{
			Name: "vc_http_in_flight_requests",
			Help: "Current number of HTTP requests being served.",
		}),
		HTTPErrors: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "vc_http_errors_total",
			Help: "HTTP errors (4xx/5xx) partitioned by code.",
		}, []string{"method", "route", "status"}),
		WSClientsConnected: factory.NewGauge(prometheus.GaugeOpts{
			Name: "vc_ws_clients_connected",
			Help: "Number of WebSocket clients currently connected.",
		}),
		WSMessagesSent: factory.NewCounter(prometheus.CounterOpts{
			Name: "vc_ws_messages_sent_total",
			Help: "WebSocket messages successfully written to clients.",
		}),
		WSEventsDropped: factory.NewCounter(prometheus.CounterOpts{
			Name: "vc_ws_events_dropped_total",
			Help: "Events dropped due to slow WebSocket clients.",
		}),
		PanicsRecovered: factory.NewCounter(prometheus.CounterOpts{
			Name: "vc_panics_recovered_total",
			Help: "Panics recovered by middleware.",
		}),
	}
	// Standard Go runtime metrics for capacity planning.
	factory.NewCounter(prometheus.CounterOpts{
		Name: "vc_build_info", Help: "Build information.",
	}).Inc()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Server wraps the HTTP server + simulation.
//
// Concurrency contract:
//   - simMu protects sim + bus. All handlers must call getSim(); only
//     handleSimulationReset may call setSim().
//   - All goroutines spawned in this package are tracked via wg so
//     Shutdown() can wait for them to drain.
type Server struct {
	simMu  sync.RWMutex
	sim    *simulation.Simulation
	log    *zap.Logger
	engine *gin.Engine
	httpSv *http.Server

	hub *WSHub
	// causalGraph subscription lifecycle.
	cgSub    chan events.Event
	cgCancel context.CancelFunc

	conflicts   *conflict.ConflictDetector
	causalGraph *causality.CausalGraph

	allowedOrigins map[string]struct{}

	auth *auth.Manager

	metrics *Metrics

	wg sync.WaitGroup

	// Health state.
	startedAt   time.Time
	ready       atomic.Bool
	healthErrMu sync.RWMutex
	healthErr   string
}

// NewServer creates a configured HTTP server.
//
// allowedOrigins is the set of origins permitted to open WebSocket connections
// and to receive CORS responses. Empty means same-origin only (recommended).
//
// Token-based authentication is configured via the VC_API_TOKENS env var
// (comma-separated "name:secret" pairs) or API_TOKENS_FILE. When no tokens
// are configured, authentication is disabled.
//
// ratePerSec and burst are the per-IP rate limit. ratePerSec=0 disables
// the limiter.
func NewServer(sim *simulation.Simulation, log *zap.Logger, allowedOrigins []string, metrics *Metrics) *Server {
	return NewServerWithRateLimit(sim, log, allowedOrigins, metrics, 0, 0)
}

// NewServerWithRateLimit is the full constructor.
func NewServerWithRateLimit(sim *simulation.Simulation, log *zap.Logger, allowedOrigins []string, metrics *Metrics, ratePerSec, burst float64) *Server {
	if metrics == nil {
		metrics = NewMetrics()
	}
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(ginZapRecovery(log, metrics))
	r.Use(securityHeadersMiddleware())
	r.Use(requestIDMiddleware())
	r.Use(otelgin.Middleware("vectorclock"))
	r.Use(maxBodyMiddleware(MaxJSONBodyBytes))
	r.Use(metricsMiddleware(metrics))
	r.Use(corsMiddleware(allowedOrigins))
	if ratePerSec > 0 && burst > 0 {
		r.Use(RateLimitMiddleware(ratePerSec, burst, log))
	}

	authMgr, err := auth.NewManager(log)
	if err != nil {
		// Tokens were configured but couldn't be loaded. Refuse to start
		// with auth silently disabled — that would expose every endpoint
		// without credentials.
		log.Fatal("auth: failed to initialize token manager", zap.Error(err))
	}
	if authMgr.Enabled() {
		log.Info("auth: token authentication ENABLED",
			zap.Int("token_count", authMgrTokenCount(authMgr)))
	} else {
		log.Warn("auth: token authentication DISABLED — every request is accepted. " +
			"Set VC_API_TOKENS or API_TOKENS_FILE to enable.")
	}
	r.Use(authMgr.Middleware())

	hub := newWSHub(sim.Bus(), log, metrics, allowedOrigins)
	hub.start()

	kv := conflict.New(sim.Bus())
	cg := causality.NewCausalGraph()

	s := &Server{
		sim:            sim,
		log:            log,
		engine:         r,
		hub:            hub,
		conflicts:      kv,
		causalGraph:    cg,
		allowedOrigins: buildOriginSet(allowedOrigins),
		auth:           authMgr,
		metrics:        metrics,
		startedAt:      time.Now(),
	}
	s.subscribeCausalGraph(sim.Bus())
	s.registerRoutes()
	s.ready.Store(true)
	return s
}

// getSim returns the current simulation under a read lock.
func (s *Server) getSim() *simulation.Simulation {
	s.simMu.RLock()
	defer s.simMu.RUnlock()
	return s.sim
}

// setSim atomically replaces the simulation and reconnects the WebSocket hub
// and causal graph subscriptions. Old subscriptions are released first so
// repeated resets do not leak goroutines or bus channels.
func (s *Server) setSim(newSim *simulation.Simulation) {
	s.simMu.Lock()
	oldSim := s.sim
	s.sim = newSim
	s.simMu.Unlock()

	// Reconnect hub to the new bus (releases old subscription internally).
	s.hub.reconnect(newSim.Bus())
	// Re-subscribe the causal graph to the new bus.
	s.subscribeCausalGraph(newSim.Bus())
	// Stop the old simulation AFTER swapping references so any in-flight
	// requests on it can still resolve.
	if oldSim != nil {
		oldSim.Stop()
	}
}

// subscribeCausalGraph subscribes to the bus and populates the causal graph.
// Tracks the subscription so setSim can release it before resubscribing.
func (s *Server) subscribeCausalGraph(bus *events.EventBus) {
	s.simMu.Lock()
	// Cancel previous subscription, if any, before allocating a new one.
	if s.cgCancel != nil {
		s.cgCancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cgCancel = cancel
	ch := bus.Subscribe([]events.EventType{
		events.EvtSend, events.EvtReceive, events.EvtInternalEvent,
	})
	s.cgSub = ch
	s.simMu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				s.log.Error("causal graph subscriber panic",
					zap.Any("panic", r))
			}
			bus.Unsubscribe(ch)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-ch:
				if !ok {
					return
				}
				ev := &causality.Event{
					ID:        e.ID,
					ProcessID: e.ProcessID,
					LocalSeq:  e.LocalSeq,
				}
				switch e.Type {
				case events.EvtInternalEvent:
					ev.Type = causality.EventInternal
				case events.EvtSend:
					ev.Type = causality.EventSend
					if e.Message != nil {
						ev.MessageID = e.Message.ID
					}
				case events.EvtReceive:
					ev.Type = causality.EventReceive
					if e.Message != nil {
						ev.MessageID = e.Message.ID
					}
				}
				s.causalGraph.AddEvent(ev)
				// Add send→receive causal edge when the receiver records
				// the matching message id. Markers (IsMarker) and internal
				// events are skipped.
				if e.Type == events.EvtReceive && e.Message != nil && !e.Message.IsMarker {
					s.causalGraph.AddEdge(e.Message.ID+"-send", e.ID)
				}
			}
		}
	}()
}

// Start binds and serves over plain HTTP. Equivalent to StartTLS(addr, nil).
func (s *Server) Start(addr string) error {
	return s.StartTLS(addr, nil)
}

// StartTLS binds and serves. If tlsCfg is non-nil, the server is
// configured for HTTPS using the supplied *tls.Config. The cert + key
// are expected to be embedded in tlsCfg (via Certificates or
// GetCertificate); ListenAndServeTLS is called with empty cert / key
// paths so the stdlib reads them from the config. Plain HTTP is used
// when tlsCfg is nil.
//
// Returns immediately on bind failure; ListenAndServe errors other
// than ErrServerClosed are surfaced to the caller.
func (s *Server) StartTLS(addr string, tlsCfg *tls.Config) error {
	s.httpSv = &http.Server{
		Addr:              addr,
		Handler:           s.engine,
		ReadHeaderTimeout: HTTPReadHeaderTimeout,
		ReadTimeout:       HTTPReadTimeout,
		WriteTimeout:      HTTPWriteTimeout,
		IdleTimeout:       HTTPIdleTimeout,
		TLSConfig:         tlsCfg,
	}
	var err error
	if tlsCfg != nil {
		// When TLSConfig is set, the cert is taken from there. The
		// certFile / keyFile arguments are therefore empty.
		err = s.httpSv.ListenAndServeTLS("", "")
	} else {
		err = s.httpSv.ListenAndServe()
	}
	if err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown gracefully stops the server, the WS hub, the simulation,
// and waits for all background goroutines to exit (bounded by
// ShutdownGracePeriod). Safe to call before Start: in that case
// only the hub and goroutines are stopped.
func (s *Server) Shutdown(ctx context.Context) error {
	s.ready.Store(false)
	s.healthErrMu.Lock()
	s.healthErr = "shutting down"
	s.healthErrMu.Unlock()

	// Stop accepting new HTTP requests first.
	var httpErr error
	if s.httpSv != nil {
		httpErr = s.httpSv.Shutdown(ctx)
	}
	// Close all active WS connections so their read/write goroutines
	// can exit. The hub's stop() closes the fan-out channel.
	s.hub.stop()
	s.hub.closeAllClients()

	// Stop the current simulation. After reset, sim may have been
	// replaced; we look it up each time so the latest one is stopped.
	if sim := s.getSim(); sim != nil {
		sim.Stop()
	}

	if s.cgCancel != nil {
		s.cgCancel()
	}
	// Bound the wait for goroutine drain to avoid hanging forever.
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		s.log.Warn("shutdown: goroutine drain timed out")
	}
	return httpErr
}

// ── Middleware ────────────────────────────────────────────────────────────────

// requestIDMiddleware ensures every request has an X-Request-Id, either
// from the client or generated. The id is stored on the gin context and
// emitted on every log line for this request via requestLogger().
func requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		rid := c.GetHeader("X-Request-Id")
		if rid == "" || len(rid) > 128 {
			rid = uuid.NewString()
		}
		c.Set("request_id", rid)
		c.Header("X-Request-Id", rid)
		c.Next()
	}
}

// maxBodyMiddleware caps the size of inbound request bodies. Requests
// exceeding the limit are rejected with 413 before the body is parsed.
func maxBodyMiddleware(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Body != nil && c.Request.ContentLength > maxBytes {
			c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{
				"error": "request body exceeds maximum allowed size",
			})
			return
		}
		if c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
		}
		c.Next()
	}
}

// metricsMiddleware records per-request counters and histogram observations.
// Failures inside the middleware itself never affect request handling.
func metricsMiddleware(m *Metrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		if m == nil {
			c.Next()
			return
		}
		m.HTTPInFlight.Inc()
		start := time.Now()
		defer func() {
			m.HTTPInFlight.Dec()
			route := c.FullPath()
			if route == "" {
				route = "unknown"
			}
			m.HTTPRequests.WithLabelValues(c.Request.Method, route, httpStatusLabel(c.Writer.Status())).Inc()
			m.HTTPDuration.WithLabelValues(c.Request.Method, route).Observe(time.Since(start).Seconds())
			if c.Writer.Status() >= 400 {
				m.HTTPErrors.WithLabelValues(c.Request.Method, route, httpStatusLabel(c.Writer.Status())).Inc()
			}
		}()
		c.Next()
	}
}

// ginZapRecovery replaces gin.Recovery() with one that logs via zap and
// increments the PanicsRecovered counter instead of writing to stdout.
func ginZapRecovery(log *zap.Logger, m *Metrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				if m != nil {
					m.PanicsRecovered.Inc()
				}
				rid, _ := c.Get("request_id")
				log.Error("panic recovered in HTTP handler",
					zap.Any("request_id", rid),
					zap.Any("panic", r),
					zap.String("method", c.Request.Method),
					zap.String("path", c.Request.URL.Path),
					zap.Stack("stack"))
				if !c.Writer.Written() {
					c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
						"error": "internal server error",
					})
				}
			}
		}()
		c.Next()
	}
}

// corsMiddleware sets CORS headers only for origins in the allowlist.
// Empty allowlist = same-origin only (no Access-Control-Allow-Origin emitted).
func corsMiddleware(allowedOrigins []string) gin.HandlerFunc {
	allow := buildOriginSet(allowedOrigins)
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if origin != "" {
			if _, ok := allow[origin]; ok {
				c.Header("Access-Control-Allow-Origin", origin)
				c.Header("Vary", "Origin")
				c.Header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
				c.Header("Access-Control-Allow-Headers", "Content-Type, X-Request-Id")
				// Only set Allow-Credentials when the request actually
				// carried credentials. Reduces the credentialed CORS
				// surface to requests that need it.
				if c.GetHeader("Cookie") != "" || c.GetHeader("Authorization") != "" {
					c.Header("Access-Control-Allow-Credentials", "true")
				}
			}
		}
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

// securityHeadersMiddleware adds defense-in-depth response headers that
// protect the API consumer. None of these affect the WebSocket path;
// they are HTTP-only and ignored by browsers' WebSocket upgrade flow.
func securityHeadersMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		h := c.Writer.Header()
		// Prevent MIME sniffing of response bodies.
		h.Set("X-Content-Type-Options", "nosniff")
		// Disable framing on cross-origin responses — defence against
		// clickjacking. The lab is API-only so we never embed in a
		// frame anyway.
		h.Set("X-Frame-Options", "DENY")
		// Disable referrer leakage.
		h.Set("Referrer-Policy", "no-referrer")
		// Disable powerful features by default; the BFF can override per
		// response if it needs to load scripts.
		h.Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
		c.Next()
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func httpStatusLabel(code int) string {
	switch {
	case code < 200:
		return "1xx"
	case code < 300:
		return "2xx"
	case code < 400:
		return "3xx"
	case code < 500:
		return "4xx"
	default:
		return "5xx"
	}
}

func buildOriginSet(origins []string) map[string]struct{} {
	set := make(map[string]struct{}, len(origins))
	for _, o := range origins {
		o = strings.TrimSpace(o)
		if o == "" {
			continue
		}
		set[o] = struct{}{}
	}
	return set
}

// EnvAllowedOrigins reads the VC_ALLOWED_ORIGINS env var as a comma-separated
// list. Empty string returns nil (same-origin only).
func EnvAllowedOrigins() []string {
	v := strings.TrimSpace(os.Getenv("VC_ALLOWED_ORIGINS"))
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// authMgrTokenCount is a no-op placeholder. The Manager doesn't expose
// its token count; the log line falls back to "1+" when auth is enabled.
// Operators can grep logs for "auth: token authentication ENABLED" and
// inspect the env var to verify the actual count.
func authMgrTokenCount(_ *auth.Manager) int { return 0 }
