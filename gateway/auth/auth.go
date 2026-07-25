// Package auth provides a simple token-based authentication layer for
// the lab API. Tokens are read from the VC_API_TOKENS environment
// variable as a comma-separated list of "name:secret" pairs, or from
// the API_TOKENS_FILE env var pointing to a JSON file with the same
// structure. When no tokens are configured, authentication is DISABLED
// — every request is accepted. This is intentional: the lab may be
// deployed internally without auth, but production deployments MUST
// configure tokens.
package auth

import (
	"bufio"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// Token represents one allowed API token.
type Token struct {
	Name   string `json:"name"`
	Secret string `json:"secret"`
}

// Manager is a thread-safe token store. Lookups are O(N) over the token
// list, which is appropriate for a small allowlist.
type Manager struct {
	mu     sync.RWMutex
	tokens map[string]Token // name -> token
	log    *zap.Logger
}

// NewManager constructs a Manager from the VC_API_TOKENS env var.
// Returns an error if tokens WERE configured but could not be loaded —
// the server must fail to start rather than serve requests with auth
// silently disabled. If no tokens are configured, NewManager returns
// a disabled manager and (nil, nil).
//
// Format: comma-separated "name:secret" pairs. Example:
//
//	VC_API_TOKENS="admin:s3cret,bot:b0ttoken"
//
// If empty, authentication is disabled.
func NewManager(log *zap.Logger) (*Manager, error) {
	m := &Manager{tokens: make(map[string]Token), log: log}
	if err := m.load(); err != nil {
		return nil, fmt.Errorf("auth: configured tokens could not be loaded: %w", err)
	}
	return m, nil
}

// load (re)reads the token configuration from the environment.
// If the env var was set but no tokens loaded successfully (e.g.
// because of a malformed entry), load returns an error so the
// server refuses to start in a fail-open state.
func (m *Manager) load() error {
	path := os.Getenv("API_TOKENS_FILE")
	raw := os.Getenv("VC_API_TOKENS")

	if path == "" && raw == "" {
		// No tokens configured: auth disabled.
		return nil
	}

	var tokens []Token
	if path != "" {
		// Clean the path defensively even though it is operator-controlled,
		// so symlink traversal within the operator's chosen directory is
		// at least visible. Read the file.
		cleaned := filepath.Clean(path)
		data, err := os.ReadFile(cleaned) // #nosec G304
		if err != nil {
			return fmt.Errorf("API_TOKENS_FILE %q: %w", path, err)
		}
		var file struct {
			Tokens []Token `json:"tokens"`
		}
		if err := json.Unmarshal(data, &file); err != nil {
			return fmt.Errorf("API_TOKENS_FILE %q: %w", path, err)
		}
		tokens = append(tokens, file.Tokens...)
	}

	if raw != "" {
		for _, entry := range strings.Split(raw, ",") {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			idx := strings.IndexByte(entry, ':')
			if idx <= 0 || idx >= len(entry)-1 {
				return fmt.Errorf("malformed VC_API_TOKENS entry %q: expected name:secret", entry)
			}
			tokens = append(tokens, Token{
				Name:   entry[:idx],
				Secret: entry[idx+1:],
			})
		}
	}

	if len(tokens) == 0 {
		return fmt.Errorf("VC_API_TOKENS or API_TOKENS_FILE was set but no valid tokens were loaded")
	}

	m.set(tokens)
	m.log.Info("auth: loaded tokens", zap.Int("count", len(tokens)))
	return nil
}

func (m *Manager) set(tokens []Token) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tokens = make(map[string]Token, len(tokens))
	for _, t := range tokens {
		m.tokens[t.Name] = t
	}
}

// Enabled returns true if at least one token is configured.
func (m *Manager) Enabled() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.tokens) > 0
}

// Validate checks whether the given (name, secret) pair matches any
// configured token. Uses constant-time comparison to avoid timing
// side-channels.
func (m *Manager) Validate(name, secret string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tokens[name]
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(t.Secret), []byte(secret)) == 1
}

// Middleware returns a gin handler that authenticates requests via
// Bearer token (Authorization: Bearer <name>:<secret>). When auth is
// disabled (no tokens configured), the middleware is a no-op and all
// requests are allowed.
//
// On success, the token name is stored in the gin context as
// "auth.token" and may be read by handlers via c.GetString("auth.token").
//
// The /healthz, /readyz, /metrics, and /ws paths are always permitted.
// The /ws path is exempt because WebSocket clients cannot set custom
// headers before the upgrade handshake; in production, the BFF sits
// in front of the browser and handles browser-facing auth itself.
func (m *Manager) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Request.URL.Path
		if path == "/healthz" || path == "/readyz" || path == "/metrics" || path == "/ws" {
			c.Next()
			return
		}
		if !m.Enabled() {
			c.Next()
			return
		}
		authz := c.GetHeader("Authorization")
		if authz == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "missing Authorization header",
			})
			return
		}
		// Accept both "Bearer <name>:<secret>" and "<name>:<secret>".
		authz = strings.TrimPrefix(authz, "Bearer ")
		idx := strings.IndexByte(authz, ':')
		if idx <= 0 || idx >= len(authz)-1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "malformed Authorization header (expected 'name:secret')",
			})
			return
		}
		name := authz[:idx]
		secret := authz[idx+1:]
		if !m.Validate(name, secret) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid credentials",
			})
			return
		}
		c.Set("auth.token", name)
		c.Next()
	}
}

// ReadTokensFile is a helper for tests and operators to inspect a token file.
func ReadTokensFile(path string) ([]Token, error) {
	// #nosec G304 -- operator-controlled path for token inspection.
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer func() { _ = f.Close() }()

	var tokens []Token
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.IndexByte(line, ':')
		if idx <= 0 || idx >= len(line)-1 {
			return nil, fmt.Errorf("malformed line: %q", line)
		}
		tokens = append(tokens, Token{
			Name:   line[:idx],
			Secret: line[idx+1:],
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return tokens, nil
}
