package gateway

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/DistributedClocks/vectorclock-system/internal/clock/vector"
	"github.com/DistributedClocks/vectorclock-system/internal/conflict"
	"github.com/DistributedClocks/vectorclock-system/internal/events"
	"github.com/DistributedClocks/vectorclock-system/internal/process"
	"github.com/DistributedClocks/vectorclock-system/internal/simulation"
	"github.com/DistributedClocks/vectorclock-system/internal/snapshot"
)

// pidPattern restricts process IDs to a safe ASCII subset. We need this at
// the trust boundary so a caller cannot inject path separators or absurdly
// long IDs that confuse routing or logging.
var pidPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)

// validatePID returns a user-safe error if the ID is malformed.
func validatePID(id string) error {
	if id == "" {
		return errors.New("process id required")
	}
	if len(id) > 64 {
		return errors.New("process id too long")
	}
	if !pidPattern.MatchString(id) {
		return errors.New("process id must match [A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
	}
	return nil
}

// requestLogger returns the logger with request_id pre-bound.
func (s *Server) requestLogger(c *gin.Context) *zap.Logger {
	rid, _ := c.Get("request_id")
	if rid == nil {
		return s.log
	}
	return s.log.With(zap.Any("request_id", rid))
}

// safeError logs the full error server-side and returns a redacted message
// suitable for the client. Stops internal details (file paths, goroutine
// stacks, etc.) from leaking through err.Error().
func (s *Server) safeError(c *gin.Context, status int, publicMsg string, err error, fields ...zap.Field) {
	all := append([]zap.Field{
		zap.String("method", c.Request.Method),
		zap.String("path", c.Request.URL.Path),
		zap.Int("status", status),
	}, fields...)
	if err != nil {
		all = append(all, zap.Error(err))
	}
	// 5xx: log at error level. 4xx: log at warn level (operator-relevant but
	// not system failure).
	if status >= 500 {
		s.requestLogger(c).Error(publicMsg, all...)
	} else {
		s.requestLogger(c).Warn(publicMsg, all...)
	}
	c.JSON(status, gin.H{"error": publicMsg})
}

func (s *Server) registerRoutes() {
	v1 := s.engine.Group("/api/v1")

	// Simulation
	v1.POST("/simulation/start", s.handleSimulationStart)
	v1.POST("/simulation/reset", s.handleSimulationReset)
	v1.GET("/simulation/state", s.handleSimulationState)

	// Processes
	v1.POST("/processes", s.handleSpawnProcess)
	v1.DELETE("/processes/:id", s.handleKillProcess)
	v1.GET("/processes/:id", s.handleGetProcess)
	v1.POST("/processes/:id/event", s.handleInternalEvent)
	v1.POST("/processes/:id/snapshot", s.handleTriggerSnapshot)

	// Messages
	v1.POST("/messages", s.handleSendMessage)
	v1.POST("/broadcast", s.handleBroadcast)

	// Snapshots
	v1.GET("/snapshots/:id", s.handleGetSnapshot)
	v1.GET("/snapshots/:id/verify", s.handleVerifySnapshot)

	// Causality
	v1.GET("/causality/happened-before", s.handleHappenedBefore)

	// Faults
	v1.POST("/faults/delay", s.handleFaultDelay)
	v1.POST("/faults/drop", s.handleFaultDrop)
	v1.DELETE("/faults", s.handleClearFaults)

	// KV store (conflict detection)
	v1.POST("/kv/:key", s.handleKVWrite)
	v1.GET("/kv/:key", s.handleKVRead)
	v1.POST("/kv/:key/resolve", s.handleKVResolve)

	// Scenarios
	v1.GET("/scenarios", s.handleListScenarios)
	v1.POST("/scenarios/:name/run", s.handleRunScenario)

	// Metrics (legacy JSON view)
	v1.GET("/metrics", s.handleMetrics)

	// Operational endpoints — outside /api/v1 by convention.
	s.engine.GET("/healthz", s.handleHealthz)
	s.engine.GET("/readyz", s.handleReadyz)
	s.engine.GET("/metrics", s.handlePrometheusMetrics)

	// WebSocket
	s.engine.GET("/ws", s.hub.handleWS)
}

// ── Health & metrics ─────────────────────────────────────────────────────────

// handleHealthz is a liveness probe. Returns 200 as long as the process is
// running. Does NOT depend on the simulation being healthy — a broken sim
// should not cause Kubernetes to kill and restart the pod.
func (s *Server) handleHealthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "ok",
		"startedAt": s.startedAt.Format(time.RFC3339),
		"uptime":    time.Since(s.startedAt).String(),
	})
}

// handleReadyz is a readiness probe. Returns 200 only when the server is
// ready to take traffic. During shutdown we flip ready to false so the load
// balancer stops sending requests.
func (s *Server) handleReadyz(c *gin.Context) {
	if !s.ready.Load() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"ready": false})
		return
	}
	sim := s.getSim()
	if sim == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"ready": false, "reason": "no simulation"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ready": true})
}

// handlePrometheusMetrics emits Prometheus-format metrics.
func (s *Server) handlePrometheusMetrics(c *gin.Context) {
	if s.metrics == nil {
		c.JSON(http.StatusOK, gin.H{})
		return
	}
	families, err := s.metrics.registry.Gather()
	if err != nil {
		s.safeError(c, http.StatusInternalServerError, "metrics gather failed", err)
		return
	}
	// Use a manual render to avoid pulling in promhttp.Handler().
	renderPrometheus(c.Writer, families)
}

// ── Simulation control ───────────────────────────────────────────────────────

type startRequest struct {
	ProcessCount int    `json:"processCount"`
	ClockType    string `json:"clockType"`
	DeliveryMode string `json:"deliveryMode"`
}

func (s *Server) handleSimulationStart(c *gin.Context) {
	var req startRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid request body", err)
		return
	}
	if req.ProcessCount < 1 || req.ProcessCount > 100 {
		s.safeError(c, http.StatusBadRequest, "processCount must be between 1 and 100", nil,
			zap.Int("processCount", req.ProcessCount))
		return
	}
	if req.ClockType == "" {
		req.ClockType = "vector"
	}
	ct, err := parseClockType(req.ClockType)
	if err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid clockType", err,
			zap.String("clockType", req.ClockType))
		return
	}
	dm, err := parseDeliveryMode(req.DeliveryMode)
	if err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid deliveryMode", err,
			zap.String("deliveryMode", req.DeliveryMode))
		return
	}

	sim := s.getSim()
	for i := 0; i < req.ProcessCount; i++ {
		pid := fmt.Sprintf("P%d", i+1)
		if err := sim.SpawnProcess(process.ProcessConfig{
			ID:           pid,
			ClockType:    ct,
			DeliveryMode: dm,
		}); err != nil {
			s.requestLogger(c).Warn("spawn process", zap.String("pid", pid), zap.Error(err))
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "started", "processes": req.ProcessCount})
}

func (s *Server) handleSimulationReset(c *gin.Context) {
	var req startRequest
	// Body is optional; do not error on missing JSON.
	_ = c.ShouldBindJSON(&req)
	if req.ClockType == "" {
		req.ClockType = "vector"
	}
	if req.DeliveryMode == "" {
		req.DeliveryMode = string(process.CausalDelivery)
	}
	if req.ProcessCount < 1 || req.ProcessCount > 100 {
		req.ProcessCount = 3
	}

	ct, err := parseClockType(req.ClockType)
	if err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid clockType", err)
		return
	}
	dm, err := parseDeliveryMode(req.DeliveryMode)
	if err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid deliveryMode", err)
		return
	}

	newSim := simulation.New(simulation.SimConfig{
		ClockType:    ct,
		DeliveryMode: dm,
		Channels:     "full_mesh",
	})
	for i := 0; i < req.ProcessCount; i++ {
		pid := fmt.Sprintf("P%d", i+1)
		if err := newSim.SpawnProcess(process.ProcessConfig{
			ID:           pid,
			ClockType:    ct,
			DeliveryMode: dm,
		}); err != nil {
			s.requestLogger(c).Warn("reset spawn", zap.String("pid", pid), zap.Error(err))
		}
	}
	// setSim handles stopping the old sim AFTER references are swapped.
	s.setSim(newSim)
	c.JSON(http.StatusOK, gin.H{"status": "reset", "processes": req.ProcessCount})
}

func (s *Server) handleSimulationState(c *gin.Context) {
	c.JSON(http.StatusOK, s.getSim().GetState())
}

// ── Processes ────────────────────────────────────────────────────────────────

type spawnRequest struct {
	ID           string `json:"id"`
	ClockType    string `json:"clockType"`
	DeliveryMode string `json:"deliveryMode"`
}

func (s *Server) handleSpawnProcess(c *gin.Context) {
	var req spawnRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid request body", err)
		return
	}
	if err := validatePID(req.ID); err != nil {
		s.safeError(c, http.StatusBadRequest, err.Error(), nil, zap.String("id", req.ID))
		return
	}
	ct, err := parseClockType(req.ClockType)
	if err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid clockType", err,
			zap.String("clockType", req.ClockType))
		return
	}
	dm, err := parseDeliveryMode(req.DeliveryMode)
	if err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid deliveryMode", err,
			zap.String("deliveryMode", req.DeliveryMode))
		return
	}
	if err := s.getSim().SpawnProcess(process.ProcessConfig{
		ID:           req.ID,
		ClockType:    ct,
		DeliveryMode: dm,
	}); err != nil {
		s.safeError(c, http.StatusConflict, err.Error(), err, zap.String("id", req.ID))
		return
	}
	c.JSON(http.StatusCreated, gin.H{"id": req.ID})
}

func (s *Server) handleKillProcess(c *gin.Context) {
	id := c.Param("id")
	if err := validatePID(id); err != nil {
		s.safeError(c, http.StatusBadRequest, err.Error(), nil, zap.String("id", id))
		return
	}
	if err := s.getSim().KillProcess(id); err != nil {
		s.safeError(c, http.StatusNotFound, err.Error(), err, zap.String("id", id))
		return
	}
	c.JSON(http.StatusOK, gin.H{"killed": id})
}

func (s *Server) handleGetProcess(c *gin.Context) {
	id := c.Param("id")
	if err := validatePID(id); err != nil {
		s.safeError(c, http.StatusBadRequest, err.Error(), nil, zap.String("id", id))
		return
	}
	p := s.getSim().GetProcess(id)
	if p == nil {
		s.safeError(c, http.StatusNotFound, "process not found", nil, zap.String("id", id))
		return
	}
	c.JSON(http.StatusOK, p.Snapshot())
}

func (s *Server) handleInternalEvent(c *gin.Context) {
	id := c.Param("id")
	if err := validatePID(id); err != nil {
		s.safeError(c, http.StatusBadRequest, err.Error(), nil, zap.String("id", id))
		return
	}
	if err := s.getSim().InternalEvent(id); err != nil {
		s.safeError(c, http.StatusNotFound, err.Error(), err, zap.String("id", id))
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (s *Server) handleTriggerSnapshot(c *gin.Context) {
	id := c.Param("id")
	if err := validatePID(id); err != nil {
		s.safeError(c, http.StatusBadRequest, err.Error(), nil, zap.String("id", id))
		return
	}
	snapID, err := s.getSim().TriggerSnapshot(id)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.safeError(c, http.StatusNotFound, "process not found", err, zap.String("id", id))
			return
		}
		s.safeError(c, http.StatusInternalServerError, "snapshot failed", err, zap.String("id", id))
		return
	}
	c.JSON(http.StatusOK, gin.H{"snapshotId": snapID})
}

// ── Messages ─────────────────────────────────────────────────────────────────

type sendRequest struct {
	From string      `json:"from"`
	To   string      `json:"to"`
	Data interface{} `json:"data"`
}

func (s *Server) handleSendMessage(c *gin.Context) {
	var req sendRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid request body", err)
		return
	}
	if err := validatePID(req.From); err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid 'from'", err, zap.String("from", req.From))
		return
	}
	if err := validatePID(req.To); err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid 'to'", err, zap.String("to", req.To))
		return
	}
	if err := s.getSim().SendMessage(req.From, req.To, req.Data); err != nil {
		s.safeError(c, http.StatusInternalServerError, "send failed", err,
			zap.String("from", req.From), zap.String("to", req.To))
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

type broadcastRequest struct {
	From string      `json:"from"`
	Data interface{} `json:"data"`
}

func (s *Server) handleBroadcast(c *gin.Context) {
	var req broadcastRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid request body", err)
		return
	}
	if err := validatePID(req.From); err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid 'from'", err, zap.String("from", req.From))
		return
	}
	if err := s.getSim().BroadcastMessage(req.From, req.Data); err != nil {
		s.safeError(c, http.StatusInternalServerError, "broadcast failed", err,
			zap.String("from", req.From))
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// ── Snapshots ────────────────────────────────────────────────────────────────

func (s *Server) handleGetSnapshot(c *gin.Context) {
	id := c.Param("id")
	gs := s.getSim().GetSnapshot(id)
	if gs == nil {
		s.safeError(c, http.StatusNotFound, "snapshot not found", nil, zap.String("id", id))
		return
	}
	// ToDTO copies under the snapshot's lock so the JSON encoder doesn't
	// race with the coordinator.
	c.JSON(http.StatusOK, gs.ToDTO())
}

func (s *Server) handleVerifySnapshot(c *gin.Context) {
	id := c.Param("id")
	gs := s.getSim().GetSnapshot(id)
	if gs == nil {
		s.safeError(c, http.StatusNotFound, "snapshot not found", nil, zap.String("id", id))
		return
	}
	result := snapshot.Verify(gs)
	c.JSON(http.StatusOK, gin.H{
		"snapshotId": id,
		"consistent": result.Consistent,
		"evidence":   result.Evidence,
	})
}

// ── Causality ────────────────────────────────────────────────────────────────

func (s *Server) handleHappenedBefore(c *gin.Context) {
	aID := c.Query("a")
	bID := c.Query("b")
	if aID == "" || bID == "" {
		s.safeError(c, http.StatusBadRequest, "query params 'a' and 'b' required", nil)
		return
	}
	hb := s.causalGraph.HappenedBefore(aID, bID)
	concurrent := s.causalGraph.Concurrent(aID, bID)
	c.JSON(http.StatusOK, gin.H{
		"happenedBefore": hb,
		"concurrent":     concurrent,
		"a":              aID,
		"b":              bID,
	})
}

// ── Faults ───────────────────────────────────────────────────────────────────

type delayRequest struct {
	From    string `json:"from"`
	To      string `json:"to"`
	DelayMs int64  `json:"delayMs"`
}

func (s *Server) handleFaultDelay(c *gin.Context) {
	var req delayRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid request body", err)
		return
	}
	if err := validatePID(req.From); err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid 'from'", err)
		return
	}
	if err := validatePID(req.To); err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid 'to'", err)
		return
	}
	if req.DelayMs < 0 || req.DelayMs > 600000 {
		s.safeError(c, http.StatusBadRequest, "delayMs must be between 0 and 600000", nil,
			zap.Int64("delayMs", req.DelayMs))
		return
	}
	s.getSim().InjectDelay(req.From, req.To, req.DelayMs)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

type dropRequest struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func (s *Server) handleFaultDrop(c *gin.Context) {
	var req dropRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid request body", err)
		return
	}
	if err := validatePID(req.From); err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid 'from'", err)
		return
	}
	if err := validatePID(req.To); err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid 'to'", err)
		return
	}
	s.getSim().InjectDrop(req.From, req.To)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (s *Server) handleClearFaults(c *gin.Context) {
	s.getSim().GetTransport().ClearFaults()
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// ── KV store (conflict detection) ────────────────────────────────────────────

type kvWriteRequest struct {
	Value     []byte            `json:"value"`
	AuthorID  string            `json:"authorId"`
	ContextVC map[string]uint64 `json:"contextVc"`
}

func (s *Server) handleKVWrite(c *gin.Context) {
	key := c.Param("key")
	if key == "" || len(key) > 256 {
		s.safeError(c, http.StatusBadRequest, "invalid key", nil, zap.String("key", key))
		return
	}
	var req kvWriteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid request body", err)
		return
	}
	if len(req.Value) > 1<<20 {
		s.safeError(c, http.StatusRequestEntityTooLarge, "value too large", nil,
			zap.Int("size", len(req.Value)))
		return
	}
	if req.AuthorID == "" {
		s.safeError(c, http.StatusBadRequest, "authorId required", nil)
		return
	}
	if err := validatePID(req.AuthorID); err != nil {
		s.safeError(c, http.StatusBadRequest, "invalid authorId", err,
			zap.String("authorId", req.AuthorID))
		return
	}
	ver, isConflict, err := s.conflicts.Write(key, req.Value, vectorFromMap(req.ContextVC), req.AuthorID)
	if err != nil {
		s.safeError(c, http.StatusServiceUnavailable, "kv write failed", err, zap.String("key", key))
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"key":      key,
		"conflict": isConflict,
		"clock":    ver.Clock,
		"author":   ver.Author,
	})
}

func (s *Server) handleKVRead(c *gin.Context) {
	key := c.Param("key")
	if key == "" || len(key) > 256 {
		s.safeError(c, http.StatusBadRequest, "invalid key", nil)
		return
	}
	versions := s.conflicts.Read(key)
	if versions == nil {
		s.safeError(c, http.StatusNotFound, "key not found", nil, zap.String("key", key))
		return
	}
	type versionView struct {
		Value   []byte            `json:"value"`
		Clock   map[string]uint64 `json:"clock"`
		Author  string            `json:"author"`
		Written string            `json:"written"`
	}
	views := make([]versionView, len(versions))
	for i, v := range versions {
		views[i] = versionView{
			Value:   v.Value,
			Clock:   v.Clock,
			Author:  v.Author,
			Written: v.Written.Format(time.RFC3339Nano),
		}
	}
	c.JSON(http.StatusOK, gin.H{"key": key, "versions": views, "siblings": len(views)})
}

type kvResolveRequest struct {
	Strategy string `json:"strategy"`
}

func (s *Server) handleKVResolve(c *gin.Context) {
	key := c.Param("key")
	if key == "" || len(key) > 256 {
		s.safeError(c, http.StatusBadRequest, "invalid key", nil)
		return
	}
	var req kvResolveRequest
	_ = c.ShouldBindJSON(&req)

	var strategy conflict.ConflictResolution
	switch req.Strategy {
	case "first_writer_wins":
		strategy = conflict.FirstWriterWins
	case "keep_all":
		strategy = conflict.KeepAll
	case "", "last_writer_wins":
		strategy = conflict.LastWriterWins
	default:
		s.safeError(c, http.StatusBadRequest, "unknown strategy", nil,
			zap.String("strategy", req.Strategy))
		return
	}
	winner := s.conflicts.Resolve(key, strategy)
	if winner == nil {
		s.safeError(c, http.StatusNotFound, "key not found", nil, zap.String("key", key))
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"key":      key,
		"value":    winner.Value,
		"clock":    winner.Clock,
		"author":   winner.Author,
		"strategy": req.Strategy,
	})
}

// ── Scenarios ────────────────────────────────────────────────────────────────

func (s *Server) handleListScenarios(c *gin.Context) {
	scenarios := simulation.AllScenarios()
	type info struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		StepCount   int    `json:"stepCount"`
	}
	result := make([]info, len(scenarios))
	for i, sc := range scenarios {
		result[i] = info{sc.Name, sc.Description, len(sc.Steps)}
	}
	c.JSON(http.StatusOK, result)
}

func (s *Server) handleRunScenario(c *gin.Context) {
	name := c.Param("name")
	if name == "" || len(name) > 128 {
		s.safeError(c, http.StatusBadRequest, "invalid scenario name", nil)
		return
	}
	for _, sc := range simulation.AllScenarios() {
		if sc.Name == name {
			scenario := sc // capture
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer func() {
					if r := recover(); r != nil {
						s.log.Error("scenario runner panic recovered",
							zap.String("scenario", scenario.Name),
							zap.Any("panic", r),
							zap.Stack("stack"))
					}
				}()
				// Snapshot the current sim under the lock and operate on it.
				// Operations remain valid even if a concurrent reset swaps the
				// sim out from under us — sim methods are idempotent enough to
				// complete or no-op cleanly.
				sim := s.getSim()
				s.log.Info("scenario runner started",
					zap.String("scenario", scenario.Name),
					zap.Int("steps", len(scenario.Steps)))
				for i, step := range scenario.Steps {
					if step.Delay > 0 {
						time.Sleep(step.Delay)
					}
					if step.Narrate != "" {
						sim.Bus().Publish(events.Event{
							Type:      events.EvtScenarioStep,
							GlobalSeq: sim.Bus().NextSeq(),
							Narration: step.Narrate,
						})
					}
					if step.Action != nil {
						if err := step.Action(sim); err != nil {
							s.log.Warn("scenario step returned error (continuing)",
								zap.String("scenario", scenario.Name),
								zap.Int("step", i),
								zap.Error(err))
							// CRITICAL: do not return. Continue to next step.
						}
					}
					s.log.Info("scenario step done",
						zap.String("scenario", scenario.Name),
						zap.Int("step", i))
				}
				s.log.Info("scenario runner finished",
					zap.String("scenario", scenario.Name))
			}()
			c.JSON(http.StatusOK, gin.H{"running": name})
			return
		}
	}
	s.safeError(c, http.StatusNotFound, "scenario not found", nil, zap.String("name", name))
}

// ── Metrics (legacy JSON) ────────────────────────────────────────────────────

func (s *Server) handleMetrics(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"eventBusDrops": s.getSim().Bus().DropCount(),
	})
}

// ── helpers ──────────────────────────────────────────────────────────────────

func vectorFromMap(m map[string]uint64) vector.VectorClock {
	return vector.VectorClock(m)
}

func parseClockType(s string) (process.ClockType, error) {
	switch s {
	case "", "vector":
		return process.ClockVector, nil
	case "lamport":
		return process.ClockLamport, nil
	case "matrix":
		return process.ClockMatrix, nil
	default:
		return "", fmt.Errorf("unknown clockType %q", s)
	}
}

func parseDeliveryMode(s string) (process.DeliveryMode, error) {
	switch s {
	case "", "causal":
		return process.CausalDelivery, nil
	case "immediate":
		return process.ImmediateDelivery, nil
	case "total_order":
		return process.TotalOrderDelivery, nil
	default:
		return "", fmt.Errorf("unknown deliveryMode %q", s)
	}
}

// parseIntQueryParam returns the integer value of a query param or a default.
// Returns an error for non-numeric values so the handler can reply 400.
//
// Currently unused; retained because future query-param handlers will need
// it. Suppressed with a var-decl to keep staticcheck happy.
var _ = func(c *gin.Context, name string, def int) (int, error) {
	raw := c.Query(name)
	if raw == "" {
		return def, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("query param %q must be an integer", name)
	}
	return v, nil
}
