package gateway

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// rateLimiter is a token-bucket per-IP rate limiter. Each IP gets
// `burst` tokens that refill at `rate` per second. The bucket
// implementation is intentionally in-process (no external store) —
// for multi-replica deployments, use a sidecar like envoy or a
// service mesh's rate-limit filter.
type rateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    float64 // tokens per second
	burst   float64 // max tokens

	stop chan struct{}
}

type bucket struct {
	tokens float64
	last   time.Time
}

// newRateLimiter creates a rate limiter. rate=10, burst=20 means a
// sustained 10 req/s with bursts up to 20.
func newRateLimiter(rate, burst float64) *rateLimiter {
	rl := &rateLimiter{
		buckets: make(map[string]*bucket),
		rate:    rate,
		burst:   burst,
		stop:    make(chan struct{}),
	}
	go rl.gcLoop()
	return rl
}

// gcLoop periodically evicts idle buckets so the map doesn't grow
// unbounded under churn. Buckets idle for > 1h are removed.
func (rl *rateLimiter) gcLoop() {
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-rl.stop:
			return
		case <-t.C:
			rl.mu.Lock()
			cutoff := time.Now().Add(-1 * time.Hour)
			for k, b := range rl.buckets {
				if b.last.Before(cutoff) {
					delete(rl.buckets, k)
				}
			}
			rl.mu.Unlock()
		}
	}
}

func (rl *rateLimiter) stopGC() { close(rl.stop) } //nolint:unused // exposed for shutdown

// Suppress unused-warning; only kept for future explicit-stop API.
var _ = (*rateLimiter)(nil).stopGC

// allow returns true if a request from key may proceed. Tokens are
// refilled lazily based on elapsed time since the last call.
func (rl *rateLimiter) allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	b, ok := rl.buckets[key]
	if !ok {
		b = &bucket{tokens: rl.burst, last: now}
		rl.buckets[key] = b
	}
	elapsed := now.Sub(b.last).Seconds()
	b.tokens += elapsed * rl.rate
	if b.tokens > rl.burst {
		b.tokens = rl.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// RateLimitMiddleware returns a gin handler that enforces the rate
// limit per client IP. Returns 429 with Retry-After when over limit.
//
// Authenticated paths (/api/v1/*) are rate-limited per IP. Health and
// metrics paths are not. WebSocket connections are not rate-limited
// here (long-lived; rate-limit at the LB or ingress instead).
func RateLimitMiddleware(rate, burst float64, log *zap.Logger) gin.HandlerFunc {
	rl := newRateLimiter(rate, burst)
	return func(c *gin.Context) {
		// Skip health/metrics/WS.
		path := c.Request.URL.Path
		if path == "/healthz" || path == "/readyz" || path == "/metrics" || path == "/ws" {
			c.Next()
			return
		}
		ip := c.ClientIP()
		if ip == "" {
			ip = "unknown"
		}
		if !rl.allow(ip) {
			c.Header("Retry-After", "1")
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "rate limit exceeded",
			})
			return
		}
		c.Next()
	}
}
