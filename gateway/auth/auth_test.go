package auth_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/DistributedClocks/vectorclock-system/gateway/auth"
)

func TestNewManager_DisabledByDefault(t *testing.T) {
	// Unset env so we get a disabled manager.
	t.Setenv("VC_API_TOKENS", "")
	t.Setenv("API_TOKENS_FILE", "")
	m, err := auth.NewManager(zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if m.Enabled() {
		t.Fatal("manager should be disabled when no tokens configured")
	}
}

func TestNewManager_EnabledFromEnv(t *testing.T) {
	t.Setenv("VC_API_TOKENS", "admin:s3cret,bot:b0ttoken")
	t.Setenv("API_TOKENS_FILE", "")
	m, err := auth.NewManager(zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if !m.Enabled() {
		t.Fatal("manager should be enabled with 2 tokens")
	}
	if !m.Validate("admin", "s3cret") {
		t.Error("admin token should validate")
	}
	if m.Validate("admin", "wrong") {
		t.Error("admin with wrong secret should not validate")
	}
	if m.Validate("ghost", "anything") {
		t.Error("nonexistent token should not validate")
	}
}

func TestNewManager_FromFile(t *testing.T) {
	t.Setenv("VC_API_TOKENS", "")
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "tokens.json")
	data := `{"tokens": [{"name":"alpha","secret":"a1"},{"name":"beta","secret":"b2"}]}`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("API_TOKENS_FILE", path)
	m, err := auth.NewManager(zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if !m.Enabled() {
		t.Fatal("manager should be enabled from file")
	}
	if !m.Validate("alpha", "a1") {
		t.Error("alpha should validate")
	}
	if !m.Validate("beta", "b2") {
		t.Error("beta should validate")
	}
}

func TestNewManager_RejectsMalformedEntries(t *testing.T) {
	// After hardening, malformed entries cause NewManager to error so the
	// server refuses to start with auth silently disabled.
	t.Setenv("VC_API_TOKENS", "good:a,no-colon,also:bad:colon,:empty,trailing:")
	if _, err := auth.NewManager(zap.NewNop()); err == nil {
		t.Fatal("expected error for malformed tokens")
	}
}

func TestMiddleware_AllowsWhenDisabled(t *testing.T) {
	t.Setenv("VC_API_TOKENS", "")
	t.Setenv("API_TOKENS_FILE", "")
	m, err := auth.NewManager(zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	r.Use(m.Middleware())
	r.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})
	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestMiddleware_RejectsWithoutHeader(t *testing.T) {
	t.Setenv("VC_API_TOKENS", "admin:s3cret")
	m, err := auth.NewManager(zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	r.Use(m.Middleware())
	r.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})
	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestMiddleware_RejectsBadSecret(t *testing.T) {
	t.Setenv("VC_API_TOKENS", "admin:s3cret")
	m, err := auth.NewManager(zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	r.Use(m.Middleware())
	r.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "admin:wrong")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestMiddleware_AcceptsValidToken(t *testing.T) {
	t.Setenv("VC_API_TOKENS", "admin:s3cret")
	m, err := auth.NewManager(zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	r.Use(m.Middleware())
	var seenToken string
	r.GET("/test", func(c *gin.Context) {
		seenToken = c.GetString("auth.token")
		c.String(http.StatusOK, "ok")
	})
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer admin:s3cret")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	if seenToken != "admin" {
		t.Errorf("expected token=admin, got %q", seenToken)
	}
}

func TestMiddleware_AllowsHealthAndMetrics(t *testing.T) {
	t.Setenv("VC_API_TOKENS", "admin:s3cret")
	m, err := auth.NewManager(zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	r.Use(m.Middleware())
	for _, p := range []string{"/healthz", "/readyz", "/metrics"} {
		req := httptest.NewRequest("GET", p, nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if rec.Code == http.StatusUnauthorized {
			t.Errorf("%s should not require auth, got 401", p)
		}
	}
}

func TestReadTokensFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tokens.txt")
	content := "# comment line\nadmin:s3cret\n\nbot:b0t\n"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	tokens, err := auth.ReadTokensFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2 {
		t.Fatalf("expected 2 tokens, got %d", len(tokens))
	}
	if tokens[0].Name != "admin" || tokens[0].Secret != "s3cret" {
		t.Errorf("first token wrong: %+v", tokens[0])
	}
}

// TestJSONRoundtrip verifies the JSON shape operators can edit.
func TestJSONRoundtrip(t *testing.T) {
	tokens := []auth.Token{{Name: "a", Secret: "1"}, {Name: "b", Secret: "2"}}
	data, _ := json.Marshal(struct {
		Tokens []auth.Token `json:"tokens"`
	}{tokens})
	if !contains(string(data), `"name":"a"`) {
		t.Errorf("JSON missing name field: %s", data)
	}
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
