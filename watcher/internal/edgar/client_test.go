package edgar

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testUA = "filings-lake tests (no network)"

func testClient(t *testing.T, h http.HandlerFunc, mod func(*Config)) (*Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	cfg := Config{UserAgent: testUA, BaseURL: srv.URL, RPS: MaxRPS, BaseBackoff: time.Millisecond, ThrottleBackoff: 2 * time.Millisecond}
	if mod != nil {
		mod(&cfg)
	}
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c, srv
}

func TestNewClientRefusesToRunAnonymously(t *testing.T) {
	for _, ua := range []string{"", "   ", "Your Name your@email", "your name you@example.com"} {
		if _, err := NewClient(Config{UserAgent: ua}); err == nil || !strings.Contains(err.Error(), "SEC_USER_AGENT") {
			t.Errorf("User-Agent %q: err = %v, want one naming SEC_USER_AGENT", ua, err)
		}
	}
	if _, err := NewClient(Config{UserAgent: testUA, RPS: 11}); err == nil {
		t.Error("a rate over the SEC's limit was accepted")
	}
	c, err := NewClient(Config{UserAgent: testUA})
	if err != nil {
		t.Fatal(err)
	}
	if c.cfg.RPS != DefaultRPS || c.cfg.BaseURL != DefaultBaseURL {
		t.Errorf("defaults: %v %q", c.cfg.RPS, c.cfg.BaseURL)
	}
}

func TestGetSendsTheUserAgentAndReturnsTheBody(t *testing.T) {
	var gotUA, gotPath string
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotUA, gotPath = r.Header.Get("User-Agent"), r.URL.Path
		w.Write([]byte("0123456789"))
	}, nil)
	body, err := c.Get(context.Background(), "/Archives/x.txt", 4)
	if err != nil {
		t.Fatal(err)
	}
	if gotUA != testUA || gotPath != "/Archives/x.txt" {
		t.Errorf("request had User-Agent %q, path %q", gotUA, gotPath)
	}
	if string(body) != "0123" {
		t.Errorf("body = %q, want it cut at 4 bytes", body)
	}
}

func TestGetRetriesServerErrorsThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	var mu sync.Mutex
	var seen []string
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok"))
	}, func(cfg *Config) {
		cfg.Observe = func(status int, willRetry bool) {
			mu.Lock()
			defer mu.Unlock()
			seen = append(seen, http.StatusText(status)+map[bool]string{true: "+retry", false: ""}[willRetry])
		}
	})
	body, err := c.Get(context.Background(), "/x", 100)
	if err != nil || string(body) != "ok" {
		t.Fatalf("body %q, err %v", body, err)
	}
	if got := strings.Join(seen, ","); got != "Service Unavailable+retry,Service Unavailable+retry,OK" {
		t.Errorf("observed %s", got)
	}
}

func TestGetDoesNotRetryNotFound(t *testing.T) {
	var calls atomic.Int32
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.NotFound(w, r)
	}, nil)
	_, err := c.Get(context.Background(), "/missing", 100)
	var se *StatusError
	if !errors.As(err, &se) || se.Status != 404 {
		t.Fatalf("err = %v, want a 404 StatusError", err)
	}
	if calls.Load() != 1 {
		t.Errorf("%d requests for a 404, want 1", calls.Load())
	}
}

func TestGetGivesUpAfterMaxAttempts(t *testing.T) {
	var calls atomic.Int32
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}, func(cfg *Config) { cfg.MaxAttempts = 3 })
	_, err := c.Get(context.Background(), "/x", 100)
	var se *StatusError
	if !errors.As(err, &se) || se.Status != 502 || !strings.Contains(err.Error(), "gave up after 3 attempts") {
		t.Fatalf("err = %v", err)
	}
	if calls.Load() != 3 {
		t.Errorf("%d requests, want 3", calls.Load())
	}
}

// EDGAR answers 403 (and sometimes 429) to a client over the rate limit. The wait after one must not
// fall below the throttle floor, however the jitter lands.
func TestThrottleResponsesBackOffAtLeastTheFloor(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusTooManyRequests} {
		var calls atomic.Int32
		var first, second time.Time
		c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
			if calls.Add(1) == 1 {
				first = time.Now()
				w.WriteHeader(status)
				return
			}
			second = time.Now()
			w.Write([]byte("ok"))
		}, func(cfg *Config) { cfg.ThrottleBackoff = 300 * time.Millisecond })
		if _, err := c.Get(context.Background(), "/x", 100); err != nil {
			t.Fatal(err)
		}
		if gap := second.Sub(first); gap < 300*time.Millisecond {
			t.Errorf("after a %d the retry came %v later, want at least 300ms", status, gap)
		}
	}
}

func TestBackoffBounds(t *testing.T) {
	c, err := NewClient(Config{UserAgent: testUA, BaseBackoff: 100 * time.Millisecond, MaxBackoff: time.Second, ThrottleBackoff: 400 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 70; attempt++ { // far enough for the shift to overflow
		ceil := min(100*time.Millisecond<<min(attempt, 20), time.Second)
		for i := 0; i < 50; i++ {
			if d := c.backoff(attempt, false, 0); d < 0 || d > ceil {
				t.Fatalf("attempt %d: backoff %v outside [0, %v]", attempt, d, ceil)
			}
			floor := min(400*time.Millisecond<<min(attempt, 20), time.Second)
			if d := c.backoff(attempt, true, 0); d < floor || d > floor+ceil {
				t.Fatalf("attempt %d: throttled backoff %v outside [%v, %v]", attempt, d, floor, floor+ceil)
			}
		}
	}
	if d := c.backoff(0, false, 5*time.Second); d != 5*time.Second {
		t.Errorf("Retry-After of 5s gave %v", d)
	}
}

func TestGetStopsWhenTheContextEnds(t *testing.T) {
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}, func(cfg *Config) { cfg.BaseBackoff, cfg.MaxAttempts = time.Hour, 5 })
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.Get(ctx, "/x", 100)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want the context's", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("took %v to notice the context had ended", time.Since(start))
	}
}

// Twelve goroutines sharing a 10/s limiter must not get their requests out faster than 10 a second.
func TestLimiterSpacesConcurrentCallers(t *testing.T) {
	l := NewLimiter(10)
	start := time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var at []time.Duration
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.Wait(context.Background()); err != nil {
				t.Error(err)
			}
			mu.Lock()
			at = append(at, time.Since(start))
			mu.Unlock()
		}()
	}
	wg.Wait()
	if total := time.Since(start); total < 1050*time.Millisecond {
		t.Errorf("12 calls at 10/s finished in %v, want at least 1.1s", total)
	}
	early := 0
	for _, d := range at {
		if d < 950*time.Millisecond {
			early++
		}
	}
	if early > 10 {
		t.Errorf("%d calls got through in the first 0.95s, the limit is 10 a second", early)
	}
}

func TestURLs(t *testing.T) {
	c, err := NewClient(Config{UserAgent: testUA})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := c.FormIndexURL("20240502"), "https://www.sec.gov/Archives/edgar/daily-index/2024/QTR2/form.20240502.idx"; got != want {
		t.Errorf("FormIndexURL = %s", got)
	}
	if got := c.FormIndexURL("20241231"); !strings.Contains(got, "/QTR4/") {
		t.Errorf("December is in %s", got)
	}
	if got, want := c.SubmissionURL(1000001, "0001000001-24-000069"), "https://www.sec.gov/Archives/edgar/data/1000001/0001000001-24-000069.txt"; got != want {
		t.Errorf("SubmissionURL = %s", got)
	}
	got := c.LatestURL("NT 10-K", 100, 100)
	for _, part := range []string{"action=getcurrent", "type=NT+10-K", "start=100", "count=100", "output=atom"} {
		if !strings.Contains(got, part) {
			t.Errorf("LatestURL %s lacks %s", got, part)
		}
	}
}

func TestSmallReadsAskForARange(t *testing.T) {
	var ranges []string
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		ranges = append(ranges, r.Header.Get("Range"))
		if r.Header.Get("Range") != "" {
			w.WriteHeader(http.StatusPartialContent)
		}
		w.Write([]byte("0123456789"))
	}, nil)
	small, err := c.Get(context.Background(), "/x", 4)
	if err != nil || string(small) != "0123" {
		t.Fatalf("ranged read: %q %v", small, err)
	}
	if _, err := c.Get(context.Background(), "/x", 10<<20); err != nil {
		t.Fatal(err)
	}
	if len(ranges) != 2 || ranges[0] != "bytes=0-3" || ranges[1] != "" {
		t.Errorf("Range headers sent: %q", ranges)
	}
}
