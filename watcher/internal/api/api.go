// Package api serves the watcher's events: a JSON list, a live stream (server-sent events), a health
// check and Prometheus metrics.
package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/otawfik/filings-lake/watcher/internal/edgar"
	"github.com/otawfik/filings-lake/watcher/internal/hub"
	"github.com/otawfik/filings-lake/watcher/internal/metrics"
	"github.com/otawfik/filings-lake/watcher/internal/store"
)

// Server holds what the handlers read. Set the exported fields before calling Handler.
type Server struct {
	Store *store.Store
	Hub   *hub.Hub
	Reg   *metrics.Registry
	// Status reports the poller's last poll without errors and its last error, if the latest poll failed.
	Status     func() (lastOK time.Time, lastErr string)
	StaleAfter time.Duration // /healthz fails when the last good poll is older than this
	Heartbeat  time.Duration // a comment line is sent on idle streams this often, to keep proxies from closing them
	Now        func() time.Time

	requests *metrics.Counter
}

const (
	defaultLimit = 100
	maxLimit     = 1000
)

var kinds = map[string]bool{edgar.KindItem402: true, edgar.KindItem401: true, edgar.KindNT10K: true}

// Handler registers the server's metrics and returns its routes.
func (s *Server) Handler() http.Handler {
	if s.Now == nil {
		s.Now = time.Now
	}
	if s.Heartbeat <= 0 {
		s.Heartbeat = 15 * time.Second
	}
	if s.StaleAfter <= 0 {
		s.StaleAfter = 5 * time.Minute
	}
	s.requests = s.Reg.Counter("watcher_http_requests_total", "Requests served by the API, by route and status.", "route", "code")
	s.Reg.GaugeFunc("watcher_stream_clients", "Clients connected to the event stream.", func() float64 { return float64(s.Hub.Subscribers()) })
	s.Reg.CounterFunc("watcher_stream_clients_cut_off_total", "Stream clients disconnected for falling a whole buffer behind.", func() float64 { return float64(s.Hub.Dropped()) })

	mux := http.NewServeMux()
	route := func(pattern string, h http.HandlerFunc) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
			h(rec, r)
			s.requests.Inc(pattern, strconv.Itoa(rec.code))
		})
	}
	route("GET /{$}", s.index)
	route("GET /healthz", s.health)
	route("GET /events", s.events)
	route("GET /events/stream", s.stream)
	route("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		s.Reg.Write(w)
	})
	return mux
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the real writer to flush the stream.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

func badRequest(w http.ResponseWriter, format string, a ...any) {
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf(format, a...)})
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "filings-lake watcher",
		"kinds":   []string{edgar.KindItem402, edgar.KindItem401, edgar.KindNT10K},
		"endpoints": map[string]string{
			"/events":        "stored events, newest first. ?kind= ?limit= (1-1000, default 100) ?after=<id> (events after that id, oldest first)",
			"/events/stream": "server-sent events as they are found. ?kind= ; resumes from the Last-Event-ID header or ?after=<id>",
			"/healthz":       "200 while the feed is being polled without errors, 503 otherwise",
			"/metrics":       "Prometheus text format",
		},
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	lastOK, lastErr := s.Status()
	counts, lastID := s.Store.Counts()
	body := map[string]any{"events": counts, "last_event_id": lastID}
	status, code := "ok", http.StatusOK
	switch {
	case lastOK.IsZero() && lastErr == "":
		status, code = "starting", http.StatusServiceUnavailable
	case lastOK.IsZero() || s.Now().Sub(lastOK) > s.StaleAfter:
		status, code = "stale", http.StatusServiceUnavailable
	case lastErr != "":
		status = "degraded" // the latest poll failed, but a recent one worked: still serving fresh data
	}
	body["status"] = status
	if !lastOK.IsZero() {
		body["last_successful_poll"] = lastOK.UTC().Format(time.RFC3339)
	}
	if lastErr != "" {
		body["last_error"] = lastErr
	}
	writeJSON(w, code, body)
}

// query reads ?kind= and ?after= (and, for the stream, the Last-Event-ID header).
func query(w http.ResponseWriter, r *http.Request) (q store.Query, hasAfter, ok bool) {
	v := r.URL.Query()
	if k := v.Get("kind"); k != "" {
		if !kinds[k] {
			badRequest(w, "unknown kind %q: use item_4_02, item_4_01 or nt_10k", k)
			return q, false, false
		}
		q.Kind = k
	}
	after := v.Get("after")
	if after == "" {
		after = r.Header.Get("Last-Event-ID")
	}
	if after != "" {
		id, err := strconv.ParseInt(after, 10, 64)
		if err != nil || id < 0 {
			badRequest(w, "after must be an event id, got %q", after)
			return q, false, false
		}
		q.AfterID, hasAfter = id, true
	}
	return q, hasAfter, true
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	q, hasAfter, ok := query(w, r)
	if !ok {
		return
	}
	q.Limit = defaultLimit
	if l := r.URL.Query().Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 || n > maxLimit {
			badRequest(w, "limit must be between 1 and %d, got %q", maxLimit, l)
			return
		}
		q.Limit = n
	}
	q.Newest = !hasAfter // a client paging forward from an id wants the next ones in order
	_, lastID := s.Store.Counts()
	writeJSON(w, http.StatusOK, map[string]any{"events": s.Store.List(q), "last_event_id": lastID})
}

// stream sends stored events after the client's last id, then new ones as they are found.
func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	q, _, ok := query(w, r)
	if !ok {
		return
	}
	rc := http.NewResponseController(w)
	rc.SetWriteDeadline(time.Time{}) // the server's write timeout must not end a stream
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")

	// Subscribe before reading the store, so an event stored in between is in one or both, never
	// neither; the id check below drops the repeat.
	sub := s.Hub.Subscribe()
	defer s.Hub.Unsubscribe(sub)

	sent := q.AfterID
	send := func(ev store.Event) error {
		if ev.ID <= sent || (q.Kind != "" && ev.Kind != q.Kind) {
			return nil
		}
		sent = ev.ID
		data, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", ev.ID, ev.Kind, data)
		return err
	}
	if r.URL.Query().Get("after") != "" || r.Header.Get("Last-Event-ID") != "" {
		for _, ev := range s.Store.List(q) {
			if send(ev) != nil {
				return
			}
		}
	}
	fmt.Fprint(w, ": connected\n\n")
	if rc.Flush() != nil {
		return
	}

	beat := time.NewTicker(s.Heartbeat)
	defer beat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev, open := <-sub.C:
			if !open {
				return // cut off for falling behind: the client reconnects with its last id and catches up
			}
			if send(ev) != nil || rc.Flush() != nil {
				return
			}
		case <-beat.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil || rc.Flush() != nil {
				return
			}
		}
	}
}
