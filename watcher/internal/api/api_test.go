package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/otawfik/filings-lake/watcher/internal/edgar"
	"github.com/otawfik/filings-lake/watcher/internal/hub"
	"github.com/otawfik/filings-lake/watcher/internal/metrics"
	"github.com/otawfik/filings-lake/watcher/internal/store"
)

type rig struct {
	t     *testing.T
	store *store.Store
	hub   *hub.Hub
	srv   *Server
	url   string

	mu      sync.Mutex
	lastOK  time.Time
	lastErr string
	now     time.Time
}

func newRig(t *testing.T) *rig {
	st, err := store.Open("", 1000)
	if err != nil {
		t.Fatal(err)
	}
	r := &rig{t: t, store: st, hub: hub.New(16), now: time.Date(2024, 5, 2, 21, 0, 0, 0, time.UTC)}
	r.srv = &Server{Store: st, Hub: r.hub, Reg: metrics.New(), StaleAfter: 5 * time.Minute, Heartbeat: time.Hour,
		Status: func() (time.Time, string) { r.mu.Lock(); defer r.mu.Unlock(); return r.lastOK, r.lastErr },
		Now:    func() time.Time { r.mu.Lock(); defer r.mu.Unlock(); return r.now }}
	return r
}

func (r *rig) start() {
	ts := httptest.NewServer(r.srv.Handler())
	r.t.Cleanup(ts.Close)
	r.url = ts.URL
}

// found stores and publishes an event the way the poller does.
func (r *rig) found(n int, kind string) store.Event {
	r.t.Helper()
	ev, added, err := r.store.Add(store.Event{Kind: kind, Accession: fmt.Sprintf("0001000001-24-%06d", n), Form: "8-K", Filed: "2024-05-02",
		Companies: []edgar.Company{{CIK: 1000001, Name: "EXAMPLE & SONS <INC>"}}, Source: "feed", SeenAt: time.Date(2024, 5, 2, 20, 31, 0, 0, time.UTC)})
	if err != nil || !added {
		r.t.Fatalf("add: %v %v", added, err)
	}
	r.hub.Publish(ev)
	return ev
}

func (r *rig) get(path string) (int, string) {
	r.t.Helper()
	resp, err := http.Get(r.url + path)
	if err != nil {
		r.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func (r *rig) eventIDs(path string) (ids []int64, lastID int64) {
	r.t.Helper()
	code, body := r.get(path)
	if code != 200 {
		r.t.Fatalf("GET %s: %d %s", path, code, body)
	}
	var out struct {
		Events []store.Event `json:"events"`
		LastID int64         `json:"last_event_id"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		r.t.Fatalf("GET %s: %v in %s", path, err, body)
	}
	for _, e := range out.Events {
		ids = append(ids, e.ID)
	}
	return ids, out.LastID
}

func TestEvents(t *testing.T) {
	r := newRig(t)
	r.start()
	if code, body := r.get("/events"); code != 200 || !strings.Contains(body, `"events": []`) {
		t.Errorf("no events yet: %d %s; want an empty list, not null", code, body)
	}
	for i := 1; i <= 6; i++ {
		r.found(i, []string{edgar.KindItem402, edgar.KindNT10K}[i%2])
	}
	for path, want := range map[string]string{
		"/events":                        "[6 5 4 3 2 1]",
		"/events?limit=2":                "[6 5]",
		"/events?kind=item_4_02":         "[6 4 2]",
		"/events?after=3":                "[4 5 6]",
		"/events?after=3&limit=2":        "[4 5]",
		"/events?after=1&kind=nt_10k":    "[3 5]",
		"/events?after=6":                "[]",
		"/events?kind=item_4_01&limit=1": "[]",
	} {
		ids, last := r.eventIDs(path)
		if fmt.Sprint(ids) != want || last != 6 {
			t.Errorf("GET %s: ids %v (last %d), want %s (last 6)", path, ids, last, want)
		}
	}
	for _, path := range []string{"/events?kind=fraud", "/events?limit=0", "/events?limit=1001", "/events?limit=x", "/events?after=-1", "/events?after=x", "/events/stream?kind=fraud"} {
		if code, body := r.get(path); code != 400 || !strings.Contains(body, `"error"`) {
			t.Errorf("GET %s: %d %s; want 400 with an error", path, code, body)
		}
	}
	if code, _ := r.get("/nope"); code != 404 {
		t.Errorf("GET /nope: %d", code)
	}
	resp, err := http.Post(r.url+"/events", "application/json", strings.NewReader("{}"))
	if err != nil || resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST /events: %v %v; want 405", resp.StatusCode, err)
	}
	if code, body := r.get("/"); code != 200 || !strings.Contains(body, "/events/stream") {
		t.Errorf("GET /: %d %s", code, body)
	}
	// The company name comes back as data, with its markup characters escaped by the JSON encoder.
	if _, body := r.get("/events?limit=1"); !strings.Contains(body, `EXAMPLE \u0026 SONS \u003cINC\u003e`) {
		t.Errorf("name not escaped: %s", body)
	}
}

func TestHealth(t *testing.T) {
	r := newRig(t)
	r.start()
	set := func(ok time.Time, msg string) { r.mu.Lock(); r.lastOK, r.lastErr = ok, msg; r.mu.Unlock() }
	check := func(wantCode int, wantStatus string) {
		t.Helper()
		code, body := r.get("/healthz")
		if code != wantCode || !strings.Contains(body, `"status": "`+wantStatus+`"`) {
			t.Errorf("healthz = %d %s; want %d %s", code, body, wantCode, wantStatus)
		}
	}
	check(503, "starting")
	set(time.Time{}, "8-K feed: HTTP 403")
	check(503, "stale")
	set(r.now.Add(-time.Minute), "")
	check(200, "ok")
	set(r.now.Add(-time.Minute), "8-K feed: HTTP 503")
	check(200, "degraded")
	set(r.now.Add(-6*time.Minute), "8-K feed: HTTP 503")
	check(503, "stale")
	r.found(1, edgar.KindItem402)
	if _, body := r.get("/healthz"); !strings.Contains(body, `"item_4_02": 1`) || !strings.Contains(body, `"last_event_id": 1`) || !strings.Contains(body, "HTTP 503") {
		t.Errorf("healthz body: %s", body)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	r := newRig(t)
	r.start()
	r.get("/events")
	r.get("/events?kind=fraud")
	code, body := r.get("/metrics")
	for _, want := range []string{
		`watcher_http_requests_total{route="GET /events",code="200"} 1`,
		`watcher_http_requests_total{route="GET /events",code="400"} 1`,
		"# TYPE watcher_stream_clients gauge\nwatcher_stream_clients 0\n",
		"# TYPE watcher_stream_clients_cut_off_total counter\nwatcher_stream_clients_cut_off_total 0\n",
	} {
		if code != 200 || !strings.Contains(body, want) {
			t.Errorf("metrics (%d) lack %q:\n%s", code, want, body)
		}
	}
}

// stream opens the event stream and returns a function that reads the next message (the lines up
// to a blank one), failing the test if none arrives in time.
func (r *rig) stream(path string, header http.Header) (next func() string, cancel func()) {
	r.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, r.url+path, nil)
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		r.t.Fatal(err)
	}
	if ct := resp.Header.Get("Content-Type"); resp.StatusCode != 200 || ct != "text/event-stream" {
		r.t.Fatalf("stream: %d %s", resp.StatusCode, ct)
	}
	msgs := make(chan string, 64)
	go func() {
		defer resp.Body.Close()
		defer close(msgs)
		sc := bufio.NewScanner(resp.Body)
		var lines []string
		for sc.Scan() {
			if sc.Text() != "" {
				lines = append(lines, sc.Text())
				continue
			}
			msgs <- strings.Join(lines, "\n")
			lines = nil
		}
	}()
	r.t.Cleanup(cancel)
	return func() string {
		r.t.Helper()
		select {
		case m, ok := <-msgs:
			if !ok {
				r.t.Fatal("stream closed")
			}
			return m
		case <-time.After(5 * time.Second):
			r.t.Fatal("no message on the stream within 5s")
			return ""
		}
	}, cancel
}

func TestStreamCatchesUpThenFollows(t *testing.T) {
	r := newRig(t)
	r.start()
	r.found(1, edgar.KindItem402)
	r.found(2, edgar.KindNT10K)
	r.found(3, edgar.KindItem402)

	next, _ := r.stream("/events/stream", http.Header{"Last-Event-ID": {"1"}})
	for _, want := range []string{"id: 2\nevent: nt_10k\ndata: {", "id: 3\nevent: item_4_02\ndata: {"} {
		if got := next(); !strings.HasPrefix(got, want) {
			t.Fatalf("catch-up message = %q, want it to start %q", got, want)
		}
	}
	if got := next(); got != ": connected" {
		t.Fatalf("got %q, want the connected comment after the catch-up", got)
	}
	r.found(4, edgar.KindItem401)
	got := next()
	if !strings.HasPrefix(got, "id: 4\nevent: item_4_01\ndata: ") {
		t.Fatalf("live message = %q", got)
	}
	var ev store.Event
	if err := json.Unmarshal([]byte(got[strings.Index(got, "data: ")+6:]), &ev); err != nil || ev.ID != 4 || ev.Accession != "0001000001-24-000004" {
		t.Errorf("data line is not the event: %v %+v", err, ev)
	}
}

func TestStreamWithoutAnIDSendsOnlyNewEventsAndFiltersByKind(t *testing.T) {
	r := newRig(t)
	r.start()
	r.found(1, edgar.KindItem402)
	next, cancel := r.stream("/events/stream?kind=item_4_02", nil)
	if got := next(); got != ": connected" {
		t.Fatalf("first message = %q; an event from before the connection was replayed", got)
	}
	r.found(2, edgar.KindNT10K)
	r.found(3, edgar.KindItem402)
	if got := next(); !strings.HasPrefix(got, "id: 3\n") {
		t.Errorf("got %q, want event 3 (event 2 is another kind)", got)
	}
	if r.hub.Subscribers() != 1 {
		t.Errorf("%d subscribers while one client is connected", r.hub.Subscribers())
	}
	cancel()
	for i := 0; r.hub.Subscribers() != 0; i++ {
		if i > 200 {
			t.Fatal("the subscription was not released after the client went away")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStreamHeartbeat(t *testing.T) {
	r := newRig(t)
	r.srv.Heartbeat = 20 * time.Millisecond
	r.start()
	next, _ := r.stream("/events/stream?after=0", nil)
	if got := next(); got != ": connected" {
		t.Fatalf("got %q", got)
	}
	if got := next(); got != ": ping" {
		t.Errorf("got %q, want a ping on an idle stream", got)
	}
}
