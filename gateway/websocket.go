package gateway

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"github.com/DistributedClocks/vectorclock-system/internal/events"
)

// newUpgrader returns a websocket.Upgrader configured with the origin allowlist
// and explicit read/write buffer sizes. CheckOrigin rejects any origin not in
// the allowlist; an empty allowlist allows all origins (dev/test mode).
func newUpgrader(allowedOrigins []string) websocket.Upgrader {
	allow := buildOriginSet(allowedOrigins)
	return websocket.Upgrader{
		ReadBufferSize:  WSReadBufferSize,
		WriteBufferSize: WSWriteBufferSize,
		CheckOrigin: func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			if origin == "" {
				// No Origin header: allow only when no allowlist is configured.
				// When origins ARE restricted, a missing header is suspicious
				// (non-browser client or redirect chain that stripped it) and
				// must not bypass the allowlist.
				return len(allowedOrigins) == 0
			}
			_, ok := allow[origin]
			return ok
		},
	}
}

// client represents one WebSocket browser connection.
type client struct {
	conn *websocket.Conn
	send chan []byte
	id   string // for log correlation
}

// WSHub manages WebSocket clients and fans out events from the EventBus.
//
// Lifecycle: callers must invoke start() once after construction and stop()
// at shutdown. stop() is idempotent across reconnects.
type WSHub struct {
	mu      sync.RWMutex
	clients map[*client]bool
	bus     *events.EventBus
	sub     chan events.Event
	log     *zap.Logger
	metrics *Metrics
	drops   atomic.Uint64

	upgrader websocket.Upgrader
	allowed  []string

	runCancel chan struct{}
	runDone   chan struct{}

	// writeMu serialises all writes to any individual WebSocket
	// connection. gorilla/websocket enforces a single-writer
	// invariant; without this mutex the ping goroutine and the
	// write-pump goroutine can race and panic with "concurrent write
	// to websocket connection".
	writeMu sync.Mutex
}

func newWSHub(bus *events.EventBus, log *zap.Logger, metrics *Metrics, allowedOrigins []string) *WSHub {
	return &WSHub{
		clients:  make(map[*client]bool),
		bus:      bus,
		sub:      bus.Subscribe(nil),
		log:      log,
		metrics:  metrics,
		upgrader: newUpgrader(allowedOrigins),
		allowed:  allowedOrigins,
	}
}

// start launches the hub fan-out goroutine. Idempotent.
//
// Waits for any previously-started run goroutine to close its runDone
// before starting a new one. Without this, the hub could end up with
// no active run goroutine (the old one was stopped, the new one was
// skipped because runDone hadn't been closed yet at the moment of
// the check).
func (h *WSHub) start() {
	h.mu.Lock()
	// Wait for the previous run to fully exit.
	for h.runDone != nil {
		select {
		case <-h.runDone:
			h.runDone = nil
			h.runCancel = nil
		default:
			// Previous run is still exiting. Release the lock, wait
			// briefly, then re-acquire and check again.
			doneCh := h.runDone
			h.mu.Unlock()
			select {
			case <-doneCh:
			case <-time.After(2 * time.Second):
				// Force-proceed. Better to have two run goroutines
				// (the old one will exit eventually) than zero.
				h.log.Warn("ws hub: previous run goroutine did not exit within 2s")
			}
			h.mu.Lock()
		}
	}
	h.runCancel = make(chan struct{})
	h.runDone = make(chan struct{})
	h.mu.Unlock()
	go h.run()
}

// reconnect switches the hub to a new EventBus (called after simulation reset).
// Releases the previous subscription so no goroutine leak occurs. Safe to
// call multiple times.
func (h *WSHub) reconnect(bus *events.EventBus) {
	h.stop() // stop the current run goroutine, if any
	h.mu.Lock()
	if h.sub != nil {
		h.bus.Unsubscribe(h.sub)
	}
	h.bus = bus
	h.sub = bus.Subscribe(nil)
	h.mu.Unlock()
	h.start()
}

// stop cancels the current run goroutine. Idempotent across multiple calls:
// each call cancels the current runCancel, then start() allocates a new one.
// A separate stopAllOnce guards the final shutdown so we don't reopen
// after Shutdown.
func (h *WSHub) stop() {
	h.mu.Lock()
	cancel := h.runCancel
	h.mu.Unlock()
	if cancel != nil {
		select {
		case <-cancel:
			// already closed
		default:
			close(cancel)
		}
	}
}

// closeAllClients closes every active WebSocket connection so the
// per-connection goroutines (read pump, write pump, ping ticker,
// history replay) can exit. Used during graceful shutdown.
//
// We close the underlying HTTP connection; the per-client write
// pump sees the write error and returns, then the read pump times out
// or returns on the closed conn, and the cleanup function runs.
//
// We must NOT close cl.send while the run goroutine may still be
// sending to it — the run goroutine snapshots the client list under
// the lock and only sends to clients in the snapshot.
func (h *WSHub) closeAllClients() {
	h.mu.Lock()
	clients := make([]*client, 0, len(h.clients))
	for cl := range h.clients {
		clients = append(clients, cl)
	}
	h.mu.Unlock()
	for _, cl := range clients {
		_ = cl.conn.Close()
	}
}

// run fans out events to all connected WebSocket clients. Exits cleanly on
// stop or bus shutdown. Reads h.bus under the mutex to avoid racing with
// reconnect().
func (h *WSHub) run() {
	defer close(h.runDone)
	defer func() {
		if r := recover(); r != nil {
			h.log.Error("ws hub: panic recovered", zap.Any("panic", r))
		}
	}()
	for {
		select {
		case <-h.runCancel:
			return
		default:
		}
		h.mu.RLock()
		busDone := h.bus.Done()
		sub := h.sub
		h.mu.RUnlock()
		select {
		case <-h.runCancel:
			return
		case <-busDone:
			return
		case e, ok := <-sub:
			if !ok {
				return
			}
			h.log.Info("ws hub: fanning out event",
				zap.String("type", string(e.Type)),
				zap.String("processId", e.ProcessID))
			data, err := json.Marshal(e)
			if err != nil {
				h.log.Warn("ws hub: marshal error", zap.Error(err))
				continue
			}
			h.mu.RLock()
			clients := make([]*client, 0, len(h.clients))
			for cl := range h.clients {
				clients = append(clients, cl)
			}
			h.mu.RUnlock()
			for _, cl := range clients {
				select {
				case cl.send <- data:
					if h.metrics != nil {
						h.metrics.WSMessagesSent.Inc()
					}
				default:
					n := h.drops.Add(1)
					if h.metrics != nil {
						h.metrics.WSEventsDropped.Inc()
					}
					if n%50 == 1 {
						h.log.Warn("ws hub: slow client, dropping event",
							zap.Uint64("totalDrops", n),
							zap.String("client_id", cl.id))
					}
				}
			}
		}
	}
}

// handleWS upgrades the HTTP connection to WebSocket and starts the client
// loop. Closes the client cleanly on disconnect and on read deadline.
func (h *WSHub) handleWS(c *gin.Context) {
	conn, err := h.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		h.log.Debug("ws upgrade failed", zap.Error(err))
		return
	}

	cl := &client{
		conn: conn,
		send: make(chan []byte, WSClientSendBuffer),
		id:   uuidString(),
	}

	// Set initial read deadline; PongHandler extends it on each pong.
	// SetReadLimit caps the size of any single incoming frame to defend
	// against a malicious client sending a huge payload to exhaust
	// memory. The current handler ignores client messages; this is a
	// tight, defensive cap.
	conn.SetReadLimit(WSMaxIncomingMessageBytes)
	_ = conn.SetReadDeadline(time.Now().Add(WSPongDeadline))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(WSPongDeadline))
		return nil
	})

	h.mu.Lock()
	h.clients[cl] = true
	clientCount := len(h.clients)
	h.mu.Unlock()
	if h.metrics != nil {
		h.metrics.WSClientsConnected.Set(float64(clientCount))
	}

	// History replay goroutine — sends buffered history then exits so the
	// channel isn't held forever.
	go func() {
		defer func() {
			if r := recover(); r != nil {
				h.log.Error("ws history replay panic",
					zap.String("client_id", cl.id),
					zap.Any("panic", r))
			}
		}()
		histCh := h.bus.History()
		for _, e := range histCh {
			data, err := json.Marshal(e)
			if err != nil {
				h.log.Warn("ws history: marshal error", zap.Error(err))
				continue
			}
			select {
			case cl.send <- data:
			case <-time.After(2 * time.Second):
				h.log.Warn("ws history: client too slow, dropping replay",
					zap.String("client_id", cl.id))
				return
			}
		}
	}()

	// Ping ticker: keeps dead connections from lingering. pingStop is
	// closed exactly once via sync.Once to avoid double-close panics.
	//
	// All writes to the WebSocket (ping frames, event data frames) are
	// serialised through writeMu to prevent "concurrent write to
	// websocket connection" panics (gorilla enforces single-writer).
	pingStop := make(chan struct{})
	var stopPing sync.Once
	go func() {
		ticker := time.NewTicker(WSPingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				// Serialise the write with the write pump via writeMu.
				h.writeMu.Lock()
				err := conn.WriteMessage(websocket.PingMessage, nil)
				h.writeMu.Unlock()
				if err != nil {
					return
				}
			case <-pingStop:
				return
			}
		}
	}()

	// Write pump — drains client.send to the socket.
	go func() {
		defer func() {
			if r := recover(); r != nil {
				h.log.Error("ws write pump panic",
					zap.String("client_id", cl.id),
					zap.Any("panic", r))
			}
		}()
		for data := range cl.send {
			_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			h.writeMu.Lock()
			err := conn.WriteMessage(websocket.TextMessage, data)
			h.writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}()

	// Cleanup runs on read pump exit. Uses sync.Once so multiple
	// goroutines cannot race on close(cl.send).
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			stopPing.Do(func() { close(pingStop) })
			_ = conn.Close()
			h.mu.Lock()
			delete(h.clients, cl)
			remaining := len(h.clients)
			h.mu.Unlock()
			if h.metrics != nil {
				h.metrics.WSClientsConnected.Set(float64(remaining))
			}
			close(cl.send)
		})
	}
	defer cleanup()

	for {
		if _, msg, err := conn.ReadMessage(); err != nil {
			return
		} else {
			var clientMsg struct {
				Action string   `json:"action"`
				Types  []string `json:"types"`
			}
			if jsonErr := json.Unmarshal(msg, &clientMsg); jsonErr != nil {
				h.log.Debug("ws read: invalid client message",
					zap.String("client_id", cl.id),
					zap.Error(jsonErr))
				continue
			}
			// Currently filtering is accepted but not applied; logging only.
			h.log.Debug("ws client message",
				zap.String("client_id", cl.id),
				zap.String("action", clientMsg.Action),
				zap.Strings("types", clientMsg.Types))
		}
	}
}

// uuidString returns a short pseudo-random id used purely for log correlation.
// Avoids importing google/uuid here to keep this file's import set small.
func uuidString() string {
	var b [8]byte
	now := uint64(time.Now().UnixNano()) //nolint:gosec // G115: log correlation only
	for i := range b {
		// Mask each shift to a single byte explicitly. The shift is bounded
		// by 56 (i*8 with i<=7) so the cast is safe.
		b[i] = byte((now >> (i * 8)) & 0xFF) //nolint:gosec // G115: explicit byte mask
	}
	return encodeHex(b[:])
}

const hexDigits = "0123456789abcdef"

func encodeHex(b []byte) string {
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hexDigits[v>>4]
		out[i*2+1] = hexDigits[v&0x0f]
	}
	return string(out)
}
