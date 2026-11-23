// Package ingest turns EDGAR's feeds into stored events: the poller follows the latest-filings feed,
// and the replay reads past days from the daily index to check the watcher against a known list.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/otawfik/filings-lake/watcher/internal/edgar"
	"github.com/otawfik/filings-lake/watcher/internal/hub"
	"github.com/otawfik/filings-lake/watcher/internal/metrics"
	"github.com/otawfik/filings-lake/watcher/internal/store"
)

// Fetcher is the part of the EDGAR client the poller uses; tests put a fake in its place.
type Fetcher interface {
	Get(ctx context.Context, url string, maxBytes int64) ([]byte, error)
	LatestURL(form string, start, count int) string
	FormIndexURL(yyyymmdd string) string
	SubmissionURL(cik int64, accession string) string
}

// Forms are the feed's form filters the watcher follows. "8-K" also returns 8-K/A, and "NT 10-K"
// also returns NT 10-K/A.
var Forms = []string{"8-K", "NT 10-K"}

const maxFeedBytes = 4 << 20

// Poller follows the latest-filings feed.
type Poller struct {
	Fetch Fetcher
	Store *store.Store
	Hub   *hub.Hub // may be nil
	Log   *slog.Logger
	Now   func() time.Time

	PageSize int // entries per feed page (EDGAR allows up to 100)
	MaxPages int // pages read per form and poll before giving up on reaching known ground
	// HeaderFallback reads a filing's SGML header when an 8-K's feed entry lists no items, so an
	// entry without item lines costs one extra request instead of a missed event.
	HeaderFallback bool

	m *pollMetrics

	mu      sync.Mutex
	known   map[string]struct{} // accessions fully handled in an earlier poll
	order   []string            // the same, oldest first, to bound the set
	lastOK  time.Time
	lastErr string
}

type pollMetrics struct {
	polls, events, entries, skipped, headers *metrics.Counter
	lastPoll, lastOK, duration, lag          *metrics.Gauge
}

// NewPoller fills in defaults and registers the poller's metrics.
func NewPoller(f Fetcher, s *store.Store, h *hub.Hub, reg *metrics.Registry, log *slog.Logger) *Poller {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Poller{Fetch: f, Store: s, Hub: h, Log: log, Now: time.Now, PageSize: 100, MaxPages: 5, HeaderFallback: true,
		known: map[string]struct{}{},
		m: &pollMetrics{
			polls:    reg.Counter("watcher_polls_total", "Polls of the latest-filings feed, by result.", "result"),
			events:   reg.Counter("watcher_events_total", "New events stored, by kind.", "kind"),
			entries:  reg.Counter("watcher_feed_entries_total", "Feed entries read, by form filter.", "form"),
			skipped:  reg.Counter("watcher_feed_entries_skipped_total", "Feed entries that could not be parsed."),
			headers:  reg.Counter("watcher_header_fetches_total", "Filing headers read because the feed listed no items, by result.", "result"),
			lastPoll: reg.Gauge("watcher_last_poll_timestamp_seconds", "When the last poll finished, as Unix time."),
			lastOK:   reg.Gauge("watcher_last_success_timestamp_seconds", "When the last poll without errors finished, as Unix time."),
			duration: reg.Gauge("watcher_last_poll_duration_seconds", "How long the last poll took."),
			lag:      reg.Gauge("watcher_last_event_lag_seconds", "Time from EDGAR accepting the newest event's filing to the watcher storing it."),
		}}
}

// Status is what the health endpoint reports.
func (p *Poller) Status() (lastOK time.Time, lastErr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastOK, p.lastErr
}

// Run polls at once and then every interval until ctx ends. A failed poll is logged and retried at
// the next tick; the client has already backed off within the poll.
func (p *Poller) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if n, err := p.PollOnce(ctx); err != nil && ctx.Err() == nil {
			p.Log.Error("poll failed", "err", err, "new_events", n)
		} else if n > 0 {
			p.Log.Info("poll", "new_events", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// PollOnce reads every form's feed once and returns how many new events it stored. An error on one
// form does not stop the others; the errors are joined.
func (p *Poller) PollOnce(ctx context.Context) (newEvents int, err error) {
	start := p.Now()
	var errs []error
	for _, form := range Forms {
		n, ferr := p.pollForm(ctx, form)
		newEvents += n
		if ferr != nil {
			errs = append(errs, fmt.Errorf("%s feed: %w", form, ferr))
		}
	}
	err = errors.Join(errs...)
	end := p.Now()
	p.m.duration.Set(end.Sub(start).Seconds())
	p.m.lastPoll.Set(float64(end.Unix()))
	p.mu.Lock()
	if err == nil {
		p.lastOK, p.lastErr = end, ""
	} else {
		p.lastErr = err.Error()
	}
	p.mu.Unlock()
	if err == nil {
		p.m.lastOK.Set(float64(end.Unix()))
		p.m.polls.Inc("ok")
	} else {
		p.m.polls.Inc("error")
	}
	return newEvents, err
}

// pollForm reads the feed newest first, a page at a time, until it reaches a filing an earlier poll
// already handled, a short page, or MaxPages. It then handles what it collected oldest first, so
// event IDs run in the order EDGAR accepted the filings, and a joint filing whose entries fell on
// two pages is merged into one.
func (p *Poller) pollForm(ctx context.Context, form string) (newEvents int, err error) {
	p.mu.Lock()
	first := len(p.known) == 0 // nothing is known yet, so there is no known ground to reach
	p.mu.Unlock()

	var fresh []edgar.Filing
	caughtUp := false
	for page := 0; page < p.MaxPages && !caughtUp; page++ {
		body, err := p.Fetch.Get(ctx, p.Fetch.LatestURL(form, page*p.PageSize, p.PageSize), maxFeedBytes)
		if err != nil {
			return 0, err
		}
		entries, skipped, err := edgar.ParseAtom(body)
		if err != nil {
			return 0, err
		}
		p.m.entries.Add(float64(len(entries)), form)
		p.m.skipped.Add(float64(skipped))
		for _, f := range entries {
			if p.isKnown(f.Accession) {
				caughtUp = true
				continue
			}
			fresh = append(fresh, f)
		}
		if len(entries)+skipped < p.PageSize {
			caughtUp = true // the end of the feed
		}
	}
	if !caughtUp && !first {
		p.Log.Warn("feed moved more than the pages read in one poll; older filings may have been missed", "form", form, "pages", p.MaxPages)
	}

	fresh = edgar.Merge(fresh)
	var handled []string
	var firstErr error
	for i := len(fresh) - 1; i >= 0; i-- {
		n, err := p.handle(ctx, fresh[i])
		newEvents += n
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue // not marked known, so the next poll tries this filing again
		}
		handled = append(handled, fresh[i].Accession)
	}
	p.markKnown(handled)
	return newEvents, firstErr
}

// handle stores the events one filing declares.
func (p *Poller) handle(ctx context.Context, f edgar.Filing) (newEvents int, err error) {
	source := "feed"
	if p.HeaderFallback && len(f.Items) == 0 && is8K(f.Form) && len(f.Companies) > 0 {
		body, err := p.Fetch.Get(ctx, p.Fetch.SubmissionURL(f.Companies[0].CIK, f.Accession), edgar.HeaderBytes)
		if err != nil {
			p.m.headers.Inc("error")
			return 0, fmt.Errorf("header of %s: %w", f.Accession, err)
		}
		h, ok := edgar.ParseHeader(body)
		if !ok || h.Accession != f.Accession {
			p.m.headers.Inc("unreadable")
			return 0, fmt.Errorf("header of %s: not a submission header", f.Accession)
		}
		p.m.headers.Inc("ok")
		f.Items, source = h.Items, "header"
	}
	return p.record(f, source)
}

func (p *Poller) record(f edgar.Filing, source string) (newEvents int, err error) {
	for _, kind := range edgar.Detect(f) {
		ev := store.Event{Kind: kind, Accession: f.Accession, Form: f.Form, Filed: f.Filed, Items: f.Items,
			Companies: f.Companies, URL: f.URL, Source: source, SeenAt: p.Now().UTC()}
		if !f.Updated.IsZero() {
			t := f.Updated.UTC()
			ev.Accepted = &t
		}
		stored, added, err := p.Store.Add(ev)
		if err != nil {
			return newEvents, fmt.Errorf("storing %s: %w", f.Accession, err)
		}
		if !added {
			continue
		}
		newEvents++
		p.m.events.Inc(kind)
		if stored.Accepted != nil {
			p.m.lag.Set(stored.SeenAt.Sub(*stored.Accepted).Seconds())
		}
		if p.Hub != nil {
			p.Hub.Publish(stored)
		}
		p.Log.Info("event", "id", stored.ID, "kind", kind, "accession", f.Accession, "form", f.Form, "company", firstName(f))
	}
	return newEvents, nil
}

func is8K(form string) bool { return form == "8-K" || form == "8-K/A" }

func firstName(f edgar.Filing) string {
	if len(f.Companies) == 0 {
		return ""
	}
	return f.Companies[0].Name
}

const maxKnown = 20000 // several weeks of 8-Ks and NT 10-Ks

func (p *Poller) isKnown(accession string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.known[accession]
	return ok
}

func (p *Poller) markKnown(accessions []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, a := range accessions {
		if _, ok := p.known[a]; !ok {
			p.known[a] = struct{}{}
			p.order = append(p.order, a)
		}
	}
	for len(p.order) > maxKnown {
		delete(p.known, p.order[0])
		p.order = p.order[1:]
	}
}
