package edgar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The SEC's fair-access policy: identify yourself in the User-Agent and stay under 10 requests a
// second. The watcher's default is 2 a second, and it refuses to start without a User-Agent.
const (
	DefaultBaseURL = "https://www.sec.gov"
	DefaultRPS     = 2.0
	MaxRPS         = 10.0
	placeholderUA  = "Your Name you@example.com"
	rangeBelow     = 64 << 10 // reads capped at or under this size send a Range header
	HeaderBytes    = 32 << 10 // enough for the SGML header of a filing with many co-registrants
)

// Config holds everything the client needs. Zero values get safe defaults.
type Config struct {
	UserAgent       string        // required; read from SEC_USER_AGENT by the caller, never hardcoded
	BaseURL         string        // DefaultBaseURL unless a test or replay points it elsewhere
	RPS             float64       // requests per second across all goroutines; capped at MaxRPS
	MaxAttempts     int           // tries per request, first one included
	BaseBackoff     time.Duration // first retry waits a random time up to this, doubling each attempt
	MaxBackoff      time.Duration
	ThrottleBackoff time.Duration // floor for the wait after a 429 or 403: EDGAR is telling us to slow down
	HTTP            *http.Client
	// Observe is called once per HTTP response or transport failure (status 0), for metrics.
	Observe func(status int, willRetry bool)
}

// Client fetches from EDGAR politely: one shared rate limit, retries with jittered backoff.
type Client struct {
	cfg Config
	lim *Limiter
}

// NewClient validates the configuration. It fails when the User-Agent is missing or is still the
// placeholder from the documentation, because an anonymous client breaks the SEC's access rules.
func NewClient(cfg Config) (*Client, error) {
	cfg.UserAgent = strings.TrimSpace(cfg.UserAgent)
	if cfg.UserAgent == "" {
		return nil, errors.New("SEC_USER_AGENT is not set: the SEC requires a User-Agent that identifies you (name and contact email)")
	}
	if cfg.UserAgent == placeholderUA {
		return nil, errors.New("SEC_USER_AGENT is still the placeholder from the docs: set your own name and contact email")
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.RPS <= 0 {
		cfg.RPS = DefaultRPS
	}
	if cfg.RPS > MaxRPS {
		return nil, fmt.Errorf("rate %.1f/s is over the SEC's limit of %.0f/s", cfg.RPS, MaxRPS)
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 500 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 2 * time.Minute
	}
	if cfg.ThrottleBackoff <= 0 {
		cfg.ThrottleBackoff = 30 * time.Second
	}
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{cfg: cfg, lim: NewLimiter(cfg.RPS)}, nil
}

// StatusError is a response the client will not retry.
type StatusError struct {
	URL    string
	Status int
}

func (e *StatusError) Error() string { return fmt.Sprintf("GET %s: HTTP %d", e.URL, e.Status) }

// Get fetches a URL (absolute, or a path under BaseURL) and returns at most maxBytes of the body.
// It retries transport errors, 5xx, 429 and 403 (EDGAR answers 403 when a client is over the rate
// limit), and gives up at once on anything else or when ctx ends.
func (c *Client) Get(ctx context.Context, rawURL string, maxBytes int64) ([]byte, error) {
	if !strings.HasPrefix(rawURL, "http://") && !strings.HasPrefix(rawURL, "https://") {
		rawURL = c.cfg.BaseURL + "/" + strings.TrimLeft(rawURL, "/")
	}
	var lastErr error
	for attempt := 0; attempt < c.cfg.MaxAttempts; attempt++ {
		if err := c.lim.Wait(ctx); err != nil {
			return nil, err
		}
		body, status, retryAfter, err := c.once(ctx, rawURL, maxBytes)
		if err == nil && status == http.StatusOK {
			c.observe(status, false)
			return body, nil
		}
		throttled := status == http.StatusTooManyRequests || status == http.StatusForbidden
		retryable := err != nil || throttled || status >= 500
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		last := attempt == c.cfg.MaxAttempts-1
		c.observe(status, retryable && !last)
		if err != nil {
			lastErr = fmt.Errorf("GET %s: %w", rawURL, err)
		} else {
			lastErr = &StatusError{URL: rawURL, Status: status}
		}
		if !retryable {
			return nil, lastErr
		}
		if last {
			break
		}
		if err := sleep(ctx, c.backoff(attempt, throttled, retryAfter)); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("gave up after %d attempts: %w", c.cfg.MaxAttempts, lastErr)
}

func (c *Client) once(ctx context.Context, u string, maxBytes int64) (body []byte, status int, retryAfter time.Duration, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, 0, 0, err
	}
	req.Header.Set("User-Agent", c.cfg.UserAgent)
	req.Header.Set("Accept-Encoding", "identity")
	if maxBytes <= rangeBelow {
		// A header read wants the first few kilobytes of a file that can run to megabytes. Ask for
		// just that; a server that ignores Range answers 200 and the body is cut at maxBytes anyway.
		req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", maxBytes-1))
	}
	resp, err := c.cfg.HTTP.Do(req)
	if err != nil {
		return nil, 0, 0, err
	}
	defer resp.Body.Close()
	if s, perr := strconv.Atoi(resp.Header.Get("Retry-After")); perr == nil && s > 0 {
		retryAfter = time.Duration(s) * time.Second
	}
	if resp.StatusCode == http.StatusPartialContent {
		resp.StatusCode = http.StatusOK
	}
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096)) // let the connection be reused
		return nil, resp.StatusCode, retryAfter, nil
	}
	body, err = io.ReadAll(io.LimitReader(resp.Body, maxBytes))
	return body, resp.StatusCode, retryAfter, err
}

// backoff is exponential with full jitter (a random wait in [0, base·2^attempt]), so a fleet of
// clients that failed together does not retry together. After a throttle response the wait never
// goes below ThrottleBackoff·2^attempt, nor below what Retry-After asked for.
func (c *Client) backoff(attempt int, throttled bool, retryAfter time.Duration) time.Duration {
	ceil := doubled(c.cfg.BaseBackoff, attempt, c.cfg.MaxBackoff)
	d := time.Duration(rand.Int64N(int64(ceil) + 1))
	if throttled {
		d += doubled(c.cfg.ThrottleBackoff, attempt, c.cfg.MaxBackoff)
	}
	if retryAfter > d {
		d = retryAfter
	}
	return d
}

// doubled is d·2^n capped at max, without the overflow a plain shift has for large n.
func doubled(d time.Duration, n int, max time.Duration) time.Duration {
	for ; n > 0 && d < max; n-- {
		d *= 2
	}
	return min(d, max)
}

func (c *Client) observe(status int, willRetry bool) {
	if c.cfg.Observe != nil {
		c.cfg.Observe(status, willRetry)
	}
}

// LatestURL is one page of the latest-filings Atom feed for a form type ("8-K" also returns 8-K/A).
func (c *Client) LatestURL(form string, start, count int) string {
	q := url.Values{"action": {"getcurrent"}, "type": {form}, "company": {""}, "dateb": {""}, "owner": {"include"},
		"start": {strconv.Itoa(start)}, "count": {strconv.Itoa(count)}, "output": {"atom"}}
	return c.cfg.BaseURL + "/cgi-bin/browse-edgar?" + q.Encode()
}

// FormIndexURL is the daily form index for a date given as YYYYMMDD.
func (c *Client) FormIndexURL(yyyymmdd string) string {
	y, m := yyyymmdd[:4], yyyymmdd[4:6]
	qtr := (int(m[0]-'0')*10+int(m[1]-'0')-1)/3 + 1
	return fmt.Sprintf("%s/Archives/edgar/daily-index/%s/QTR%d/form.%s.idx", c.cfg.BaseURL, y, qtr, yyyymmdd)
}

// SubmissionURL is a filing's full-submission text file, whose first lines are the SGML header.
func (c *Client) SubmissionURL(cik int64, accession string) string {
	return fmt.Sprintf("%s/Archives/edgar/data/%d/%s.txt", c.cfg.BaseURL, cik, accession)
}

// Limiter spaces calls evenly: each Wait reserves the next free slot, 1/rps after the previous one.
// Even spacing (no bursts) is the point: the SEC counts requests per second.
type Limiter struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
}

func NewLimiter(rps float64) *Limiter {
	return &Limiter{interval: time.Duration(float64(time.Second) / rps)}
}

// Wait blocks until the caller's slot comes up or ctx ends. A cancelled wait still uses its slot,
// which errs on the side of fewer requests.
func (l *Limiter) Wait(ctx context.Context) error {
	l.mu.Lock()
	now := time.Now()
	if l.next.Before(now) {
		l.next = now
	}
	wait := l.next.Sub(now)
	l.next = l.next.Add(l.interval)
	l.mu.Unlock()
	return sleep(ctx, wait)
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
