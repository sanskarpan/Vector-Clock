package gateway

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func TestRateLimiter_AllowsUnderBurst(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(RateLimitMiddleware(10, 20, zap.NewNop()))
	r.GET("/test", func(c *gin.Context) { c.String(200, "ok") })

	for i := 0; i < 20; i++ {
		req := httptest.NewRequest("GET", "/test", nil)
		req.RemoteAddr = "192.0.2.1:1234"
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("request %d: expected 200, got %d", i, rec.Code)
		}
	}
}

func TestRateLimiter_BlocksOverBurst(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(RateLimitMiddleware(1, 5, zap.NewNop())) // 1 req/s, burst 5
	r.GET("/test", func(c *gin.Context) { c.String(200, "ok") })

	// Burst of 5 should pass; the 6th and beyond should 429.
	allowed := 0
	denied := 0
	for i := 0; i < 10; i++ {
		req := httptest.NewRequest("GET", "/test", nil)
		req.RemoteAddr = "192.0.2.2:1234"
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if rec.Code == 200 {
			allowed++
		} else if rec.Code == 429 {
			denied++
		}
	}
	if allowed != 5 {
		t.Errorf("expected 5 allowed, got %d", allowed)
	}
	if denied != 5 {
		t.Errorf("expected 5 denied, got %d", denied)
	}
}

func TestRateLimiter_PerIPIsolation(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(RateLimitMiddleware(1, 1, zap.NewNop())) // 1 req/s, burst 1
	r.GET("/test", func(c *gin.Context) { c.String(200, "ok") })

	// IP A: 1 request should pass, 2nd should 429.
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/test", nil)
		req.RemoteAddr = "10.0.0.1:1234"
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if i == 0 && rec.Code != 200 {
			t.Errorf("IP A first req: expected 200, got %d", rec.Code)
		}
		if i == 1 && rec.Code != 429 {
			t.Errorf("IP A second req: expected 429, got %d", rec.Code)
		}
	}
	// IP B: 1 request should pass (different bucket).
	req := httptest.NewRequest("GET", "/test", nil)
	req.RemoteAddr = "10.0.0.2:1234"
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Errorf("IP B first req: expected 200, got %d", rec.Code)
	}
}

func TestRateLimiter_BypassesHealthAndMetrics(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(RateLimitMiddleware(0.1, 1, zap.NewNop())) // very low
	for _, p := range []string{"/healthz", "/readyz", "/metrics", "/ws"} {
		// 10 requests each — would all 429 if not bypassed.
		for i := 0; i < 10; i++ {
			req := httptest.NewRequest("GET", p, nil)
			req.RemoteAddr = "10.0.0.1:1234"
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code == 429 {
				t.Errorf("%s should not be rate limited (req %d)", p, i)
			}
		}
	}
}

func TestRateLimiter_Refill(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(RateLimitMiddleware(20, 1, zap.NewNop())) // 20/s, burst 1
	r.GET("/test", func(c *gin.Context) { c.String(200, "ok") })

	// Burst 1, then sleep 100ms, expect another request to pass.
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/test", nil)
		req.RemoteAddr = "10.0.0.5:1234"
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if i == 0 && rec.Code != 200 {
			t.Fatalf("first req: expected 200, got %d", rec.Code)
		}
		if i == 1 && rec.Code != 429 {
			t.Fatalf("second immediate req: expected 429, got %d", rec.Code)
		}
	}
	// Wait for refill.
	time.Sleep(200 * time.Millisecond)
	req := httptest.NewRequest("GET", "/test", nil)
	req.RemoteAddr = "10.0.0.5:1234"
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Errorf("after refill: expected 200, got %d", rec.Code)
	}
}

func TestRateLimiter_HighConcurrency(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(RateLimitMiddleware(1000, 100, zap.NewNop()))
	r.GET("/test", func(c *gin.Context) { c.String(200, "ok") })

	const N = 200
	var wg sync.WaitGroup
	var allowed atomic.Int64
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("GET", "/test", nil)
			req.RemoteAddr = "10.0.0.10:1234"
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code == 200 {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	// We allow burst (100) + some refill over the test duration.
	// At 1000/s, even 1ms = 1 token, and the test takes longer than
	// 1ms for 200 goroutines, so 100-200 allowed is reasonable.
	got := allowed.Load()
	if got < 50 || got > 200 {
		t.Errorf("expected 50-200 allowed under concurrent load, got %d", got)
	}
}

// Suppress unused warnings from race-detector for unused types.
var _ = http.StatusTooManyRequests
