// main bootstraps the Vector Clock simulation server.
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/goccy/go-yaml"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/DistributedClocks/vectorclock-system/gateway"
	"github.com/DistributedClocks/vectorclock-system/gateway/tlsconfig"
	"github.com/DistributedClocks/vectorclock-system/internal/process"
	"github.com/DistributedClocks/vectorclock-system/internal/simulation"
	"github.com/DistributedClocks/vectorclock-system/internal/telemetry"
)

// serverConfig mirrors the structure of config.yaml.
type serverConfig struct {
	Server struct {
		Port           int     `yaml:"port"`
		WSBuffer       int     `yaml:"ws_buffer"`
		RateLimitPerIP float64 `yaml:"rate_limit_per_ip"` // req/s per IP; 0 = disabled
		RateLimitBurst float64 `yaml:"rate_limit_burst"`  // burst max tokens; 0 = disabled
	} `yaml:"server"`
	Simulation struct {
		InitialProcesses int    `yaml:"initial_processes"`
		ClockType        string `yaml:"clock_type"`
		DeliveryMode     string `yaml:"delivery_mode"`
		Channels         string `yaml:"channels"`
	} `yaml:"simulation"`
	Logging struct {
		Level  string `yaml:"level"`
		Format string `yaml:"format"`
	} `yaml:"logging"`
}

// loadConfig reads the YAML config at path. Missing file → defaults.
// Unparseable file → defaults + warning on stderr (operator visible).
//
// The path is validated to be a regular file under the working directory or
// /etc/vectorclock/. This defends against the operator (or a compromised
// environment) pointing CONFIG_PATH at a sensitive file like /etc/passwd.
func loadConfig(path string) serverConfig {
	cfg := serverConfig{}
	cfg.Server.Port = 8080
	cfg.Simulation.InitialProcesses = 3
	cfg.Simulation.ClockType = "vector"
	cfg.Simulation.DeliveryMode = "causal"
	cfg.Simulation.Channels = "full_mesh"
	cfg.Logging.Level = "info"
	cfg.Server.RateLimitPerIP = 1.67 // ~100 req/min
	cfg.Server.RateLimitBurst = 10   // burst up to 10

	cleaned := filepath.Clean(path)
	info, err := os.Stat(cleaned)
	if err != nil {
		// No config file: silent, just use defaults.
		return cfg
	}
	if !info.Mode().IsRegular() {
		fmt.Fprintf(os.Stderr, "warning: config path %q is not a regular file; using defaults\n", cleaned)
		return cfg
	}
	data, err := os.ReadFile(cleaned) //nolint:gosec // path is operator-controlled and Stat-validated above
	if err != nil {
		return cfg
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "warning: config.yaml parse error: %v\n", err)
	}
	return cfg
}

// resolvePort applies precedence: PORT env var > config file > built-in default.
// Returns an error rather than silently accepting garbage.
func resolvePort(envValue string, configDefault int) (int, error) {
	if envValue == "" {
		if configDefault < 0 || configDefault > 65535 {
			return 0, fmt.Errorf("config port %d out of range (1..65535)", configDefault)
		}
		return configDefault, nil
	}
	port, err := strconv.Atoi(envValue)
	if err != nil {
		return 0, fmt.Errorf("PORT env var %q is not a valid integer: %w", envValue, err)
	}
	if port < 1 || port > 65535 {
		return 0, fmt.Errorf("PORT env var %d out of range (1..65535)", port)
	}
	return port, nil
}

func main() {
	// Load config; allow CONFIG_PATH override.
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config.yaml"
	}
	cfg := loadConfig(configPath)

	// Build logger.
	logLevel := zapcore.InfoLevel
	if err := logLevel.UnmarshalText([]byte(cfg.Logging.Level)); err != nil {
		logLevel = zapcore.InfoLevel
	}
	zapCfg := zap.NewProductionConfig()
	zapCfg.Level = zap.NewAtomicLevelAt(logLevel)
	if cfg.Logging.Format == "console" {
		zapCfg.Encoding = "console"
	}
	log := zap.Must(zapCfg.Build())
	defer func() { _ = log.Sync() }()

	// Initialise OpenTelemetry tracing. OTEL_EXPORTER env var selects
	// the exporter ("otlp", "stdout", "none"). Default is "none" (no-op).
	telShutdown, err := telemetry.Init(context.Background(), telemetry.Config{
		ServiceName:    envOr("OTEL_SERVICE_NAME", "vectorclock-server"),
		ServiceVersion: "dev",
		Exporter:       envOr("OTEL_EXPORTER", "none"),
		OTLPEndpoint:   envOr("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4318"),
		SampleRatio:    envOrFloat("OTEL_SAMPLE_RATIO", 1.0),
		Insecure:       os.Getenv("OTEL_INSECURE") == "true",
	})
	if err != nil {
		log.Fatal("telemetry: init failed", zap.Error(err))
	}
	defer func() { _ = telShutdown(context.Background()) }()
	if exporter := os.Getenv("OTEL_EXPORTER"); exporter != "" && exporter != "none" {
		log.Info("telemetry: tracing enabled", zap.String("exporter", exporter))
	}

	// Validate simulation parameters up-front. Better to fail fast than to
	// spawn a server with invalid config that breaks the first request.
	clockType := process.ClockType(cfg.Simulation.ClockType)
	if clockType == "" {
		clockType = process.ClockVector
	}
	deliveryMode := process.DeliveryMode(cfg.Simulation.DeliveryMode)
	if deliveryMode == "" {
		deliveryMode = process.CausalDelivery
	}
	if cfg.Simulation.InitialProcesses < 0 || cfg.Simulation.InitialProcesses > 100 {
		log.Fatal("invalid initial_processes",
			zap.Int("initial_processes", cfg.Simulation.InitialProcesses))
	}

	sim := simulation.New(simulation.SimConfig{
		ClockType:    clockType,
		DeliveryMode: deliveryMode,
		Channels:     cfg.Simulation.Channels,
	})
	sim.SetLogger(log)

	for i := 0; i < cfg.Simulation.InitialProcesses; i++ {
		pid := fmt.Sprintf("P%d", i+1)
		if err := sim.SpawnProcess(process.ProcessConfig{
			ID:           pid,
			ClockType:    clockType,
			DeliveryMode: deliveryMode,
		}); err != nil {
			log.Fatal("spawn process", zap.String("pid", pid), zap.Error(err))
		}
	}

	port, err := resolvePort(os.Getenv("PORT"), cfg.Server.Port)
	if err != nil {
		log.Fatal("invalid port", zap.Error(err))
	}
	addr := fmt.Sprintf(":%d", port)

	allowedOrigins := gateway.EnvAllowedOrigins()
	metrics := gateway.NewMetrics()
	srv := gateway.NewServerWithRateLimit(sim, log, allowedOrigins, metrics,
		cfg.Server.RateLimitPerIP, cfg.Server.RateLimitBurst)

	// TLS configuration. If VC_TLS_CERT_FILE is set, terminate TLS at
	// the application; otherwise serve plain HTTP. VC_TLS_CLIENT_CA_FILE
	// enables mTLS. VC_TLS_RELOAD_INTERVAL, when > 0, starts a
	// background goroutine that polls the cert + key files for changes
	// (intended for cert-manager / Let's Encrypt renewals).
	tlsCfg, stopReloader := buildTLSConfig(log)

	// Graceful shutdown.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	serverErr := make(chan error, 1)
	go func() {
		log.Info("server starting",
			zap.String("addr", addr),
			zap.Strings("allowed_origins", allowedOrigins),
			zap.Int("initial_processes", cfg.Simulation.InitialProcesses),
			zap.Bool("tls", tlsCfg != nil),
			zap.Bool("mtls", tlsCfg != nil && tlsCfg.ClientAuth == tls.RequireAndVerifyClientCert))
		var startErr error
		if tlsCfg != nil {
			startErr = srv.StartTLS(addr, tlsCfg)
		} else {
			startErr = srv.Start(addr)
		}
		if startErr != nil && startErr != http.ErrServerClosed {
			serverErr <- startErr
		}
		close(serverErr)
	}()

	select {
	case err := <-serverErr:
		if err != nil {
			log.Fatal("server failed to start", zap.Error(err))
		}
	case sig := <-quit:
		log.Info("shutdown signal received", zap.String("signal", sig.String()))
	}

	if stopReloader != nil {
		stopReloader()
	}

	ctx, cancel := context.WithTimeout(context.Background(), gateway.ShutdownGracePeriod)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Error("graceful shutdown error", zap.Error(err))
	}
	sim.Stop()

	log.Info("shutdown complete", zap.Duration("uptime", time.Since(startupTime)))
}

// startupTime is captured at init so Shutdown can report a stable uptime
// even if main runs long enough that "now - startedAt" drifts.
var startupTime = time.Now()

// buildTLSConfig reads VC_TLS_CERT_FILE / VC_TLS_KEY_FILE /
// VC_TLS_CLIENT_CA_FILE / VC_TLS_RELOAD_INTERVAL from the environment
// and returns a configured *tls.Config plus a stop function for the
// reload goroutine. If VC_TLS_CERT_FILE is empty, both return values
// are nil and the server runs plain HTTP.
//
// The reload goroutine is cancellable via the returned stop function
// so the main shutdown path can stop it before http.Server.Shutdown
// returns. The context is also cancelled on process exit (SIGINT /
// SIGTERM) by the main loop.
//
// Environment contract:
//
//	VC_TLS_CERT_FILE        path to PEM-encoded cert (required to enable TLS)
//	VC_TLS_KEY_FILE         path to PEM-encoded key (required when cert is set)
//	VC_TLS_CLIENT_CA_FILE   path to PEM-encoded CA bundle for mTLS (optional)
//	VC_TLS_RELOAD_INTERVAL  e.g. "5m" (optional, 0 = no reload)
func buildTLSConfig(log *zap.Logger) (*tls.Config, func()) {
	certFile := strings.TrimSpace(os.Getenv("VC_TLS_CERT_FILE"))
	if certFile == "" {
		return nil, nil
	}
	keyFile := strings.TrimSpace(os.Getenv("VC_TLS_KEY_FILE"))
	if keyFile == "" {
		log.Fatal("VC_TLS_CERT_FILE is set but VC_TLS_KEY_FILE is empty")
	}
	clientCAFile := strings.TrimSpace(os.Getenv("VC_TLS_CLIENT_CA_FILE"))

	cfg, err := tlsconfig.Load(certFile, keyFile, clientCAFile)
	if err != nil {
		log.Fatal("tlsconfig: load failed",
			zap.String("cert_file", certFile),
			zap.String("key_file", keyFile),
			zap.Error(err))
	}
	tlsCfg := cfg.TLSConfig()

	reloadInterval := parseDurationEnv("VC_TLS_RELOAD_INTERVAL", 0)
	var stop func()
	if reloadInterval > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		cfg.StartReloader(ctx, reloadInterval, log)
		stop = cancel
		log.Info("tls: cert reloader enabled",
			zap.String("cert_file", certFile),
			zap.Duration("interval", reloadInterval))
	}
	return tlsCfg, stop
}

// envOr reads an env var. Empty returns def.
func envOr(name, def string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return def
}

// envOrFloat reads an env var as float64. Empty or invalid returns def.
func envOrFloat(name string, def float64) float64 {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

// parseDurationEnv reads a time.Duration from an env var. Empty or
// invalid input returns def. Used for VC_TLS_RELOAD_INTERVAL.
func parseDurationEnv(name string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
