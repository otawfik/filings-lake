package ingest

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/otawfik/filings-lake/watcher/internal/edgar"
	"github.com/otawfik/filings-lake/watcher/internal/edgartest"
	"github.com/otawfik/filings-lake/watcher/internal/hub"
	"github.com/otawfik/filings-lake/watcher/internal/metrics"
	"github.com/otawfik/filings-lake/watcher/internal/store"
)

func filing(n int, form string, items ...string) edgar.Filing {
	return edgar.Filing{Accession: fmt.Sprintf("0001%06d-24-%06d", n%1000000, n), Form: form, Filed: "2024-05-02", Items: items,
		Companies: []edgar.Company{{CIK: int64(1000000 + n), Name: fmt.Sprintf("COMPANY %d INC", n)}},
		Updated:   time.Date(2024, 5, 2, 20, 30, 0, 0, time.UTC)}
}

type rig struct {
	srv   *edgartest.Server
	store *store.Store
	hub   *hub.Hub
	reg   *metrics.Registry
	p     *Poller
}

func newRig(t *testing.T, path string) *rig {
	t.Helper()
	s, err := store.Open(path, 100000)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	r := &rig{srv: edgartest.New(), store: s, hub: hub.New(1024), reg: metrics.New()}
	r.p = NewPoller(r.srv.Direct(), s, r.hub, r.reg, nil)
	r.p.Now = func() time.Time { return time.Date(2024, 5, 2, 20, 31, 0, 0, time.UTC) }
	return r
}

func (r *rig) poll(t *testing.T) int {
	t.Helper()
	n, err := r.p.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	return n
}

func kinds(evs []store.Event) string {
	var out []string
	for _, e := range evs {
		out = append(out, e.Kind+":"+e.Accession[len(e.Accession)-3:])
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

func TestPollStoresEachEventOnceAndPublishesIt(t *testing.T) {
	r := newRig(t, "")
	sub := r.hub.Subscribe()
	r.srv.Publish(
		filing(1, "8-K", "2.02", "9.01"),
		filing(2, "8-K", "4.02", "9.01"),
		filing(3, "8-K/A", "4.01"),
		filing(4, "8-K", "4.01", "4.02"),
		filing(5, "NT 10-K"),
		filing(6, "NT 10-K/A"),
	)
	if n := r.poll(t); n != 6 {
		t.Errorf("first poll stored %d events, want 6", n)
	}
	got := r.store.List(store.Query{})
	if want := "item_4_01:003 item_4_01:004 item_4_02:002 item_4_02:004 nt_10k:005 nt_10k:006"; kinds(got) != want {
		t.Errorf("events = %s\n      want %s", kinds(got), want)
	}
	for i := 0; i < 6; i++ {
		select {
		case ev := <-sub.C:
			if ev.ID == 0 || ev.Source != "feed" || ev.Accepted == nil || len(ev.Companies) != 1 {
				t.Errorf("published event is incomplete: %+v", ev)
			}
		default:
			t.Fatalf("only %d events were published", i)
		}
	}
	if n := r.poll(t); n != 0 {
		t.Errorf("a second poll of the same feed stored %d events", n)
	}
	if got := r.reg.Value("watcher_events_total", edgar.KindItem402); got != 2 {
		t.Errorf("watcher_events_total{item_4_02} = %v, want 2", got)
	}
	if got := r.reg.Value("watcher_last_event_lag_seconds"); got != 60 {
		t.Errorf("lag = %vs, want 60 (accepted 20:30, seen 20:31)", got)
	}
	if ok, msg := r.p.Status(); ok.IsZero() || msg != "" {
		t.Errorf("status after good polls: %v %q", ok, msg)
	}
}

func TestJointFilingIsOneEventWithEveryFiler(t *testing.T) {
	r := newRig(t, "")
	f := filing(7, "8-K", "4.02")
	f.Companies = append(f.Companies, edgar.Company{CIK: 2000007, Name: "COMPANY 7 FINANCE LLC"})
	r.srv.Publish(f)
	if n := r.poll(t); n != 1 {
		t.Fatalf("stored %d events for one joint filing, want 1", n)
	}
	if ev := r.store.List(store.Query{})[0]; len(ev.Companies) != 2 || ev.Companies[1].CIK != 2000007 {
		t.Errorf("companies = %+v", ev.Companies)
	}
}

// If a feed entry carries no item lines, the filing's header is read instead: once, and not again
// on later polls.
func TestHeaderFallbackWhenTheFeedListsNoItems(t *testing.T) {
	r := newRig(t, "")
	r.srv.OmitFeedItems = true
	r.srv.Publish(filing(1, "8-K", "2.02"), filing(2, "8-K", "4.02", "9.01"), filing(3, "NT 10-K"))
	if n := r.poll(t); n != 2 {
		t.Errorf("stored %d events, want 2", n)
	}
	for _, ev := range r.store.List(store.Query{Kind: edgar.KindItem402}) {
		if ev.Source != "header" || fmt.Sprint(ev.Items) != "[4.02]" {
			t.Errorf("event from a header: source %q items %v", ev.Source, ev.Items)
		}
	}
	if got := r.srv.Requests("header"); got != 2 {
		t.Errorf("%d header requests, want 2 (one per 8-K, none for the NT 10-K)", got)
	}
	r.poll(t)
	if got := r.srv.Requests("header"); got != 2 {
		t.Errorf("headers were fetched again on the next poll: %d requests", got)
	}

	r.p.HeaderFallback = false
	r.srv.Publish(filing(4, "8-K", "4.02"))
	if n := r.poll(t); n != 0 || r.srv.Requests("header") != 2 {
		t.Errorf("with the fallback off: %d events, %d header requests", n, r.srv.Requests("header"))
	}
}

func TestAFailedHeaderIsRetriedOnTheNextPoll(t *testing.T) {
	r := newRig(t, "")
	r.srv.OmitFeedItems = true
	r.srv.Publish(filing(1, "8-K", "4.02"))
	r.srv.Publish(filing(2, "8-K", "4.01"))
	// The 8-K feed is served, then the first header request fails.
	r.p.Fetch = &failNth{Fetcher: r.p.Fetch, n: 2, status: http.StatusServiceUnavailable}
	n, err := r.p.PollOnce(context.Background())
	if err == nil || n != 1 {
		t.Fatalf("poll with one failing header: %d events, err %v; want 1 event and an error", n, err)
	}
	if _, msg := r.p.Status(); !strings.Contains(msg, "header of") {
		t.Errorf("status = %q", msg)
	}
	if got := r.reg.Value("watcher_polls_total", "error"); got != 1 {
		t.Errorf("error polls = %v", got)
	}
	if n := r.poll(t); n != 1 {
		t.Errorf("the next poll stored %d events, want the 1 that failed before", n)
	}
	if n := len(r.store.List(store.Query{})); n != 2 {
		t.Errorf("%d events in all, want 2", n)
	}
}

type failNth struct {
	Fetcher
	n, calls, status int
}

func (f *failNth) Get(ctx context.Context, url string, max int64) ([]byte, error) {
	f.calls++
	if f.calls == f.n {
		return nil, &edgar.StatusError{URL: url, Status: f.status}
	}
	return f.Fetcher.Get(ctx, url, max)
}

func TestPollReadsPagesOnlyUntilKnownGround(t *testing.T) {
	r := newRig(t, "")
	var batch []edgar.Filing
	for i := 1; i <= 250; i++ {
		batch = append(batch, filing(i, "8-K", "4.02"))
	}
	r.srv.PublishBulk(batch)
	if n := r.poll(t); n != 250 {
		t.Errorf("first poll over three pages stored %d events, want 250", n)
	}
	if got := r.srv.Requests("feed"); got != 3+1 {
		t.Errorf("first poll made %d feed requests, want 3 for 8-K and 1 for NT 10-K", got)
	}
	for i := 251; i <= 280; i++ {
		r.srv.Publish(filing(i, "8-K", "4.02"))
	}
	before := r.srv.Requests("feed")
	if n := r.poll(t); n != 30 {
		t.Errorf("second poll stored %d events, want 30", n)
	}
	if got := r.srv.Requests("feed") - before; got != 2 {
		t.Errorf("second poll made %d feed requests, want 2 (one page per form)", got)
	}
	// 130 new filings: two pages are needed to get back to known ground.
	batch = batch[:0]
	for i := 281; i <= 410; i++ {
		batch = append(batch, filing(i, "8-K", "4.02"))
	}
	r.srv.PublishBulk(batch)
	if n := r.poll(t); n != 130 {
		t.Errorf("third poll stored %d events, want 130", n)
	}
}

func TestRestartDoesNotRepeatEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	r := newRig(t, path)
	r.srv.Publish(filing(1, "8-K", "4.02"), filing(2, "NT 10-K"))
	r.poll(t)
	r.store.Close()

	s2, err := store.Open(path, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	p2 := NewPoller(r.srv.Direct(), s2, nil, metrics.New(), nil)
	r.srv.Publish(filing(3, "8-K", "4.01"))
	n, err := p2.PollOnce(context.Background())
	if err != nil || n != 1 {
		t.Errorf("after a restart the poll stored %d events (err %v), want only the new one", n, err)
	}
	if evs := s2.List(store.Query{}); len(evs) != 3 || evs[2].ID != 3 {
		t.Errorf("events after restart: %s", kinds(evs))
	}
}

func TestOneFormFailingDoesNotStopTheOther(t *testing.T) {
	r := newRig(t, "")
	r.srv.Publish(filing(1, "8-K", "4.02"), filing(2, "NT 10-K"))
	r.srv.FailNext(http.StatusServiceUnavailable) // the 8-K feed request
	n, err := r.p.PollOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "8-K feed") || n != 1 {
		t.Errorf("got %d events, err %v; want the NT 10-K and an error naming the 8-K feed", n, err)
	}
	if n := r.poll(t); n != 1 {
		t.Errorf("next poll stored %d, want the 8-K event", n)
	}
}

// End to end over real sockets: the real client, with its rate limit and retries, against the
// stand-in server answering 503 and 403 before it serves.
func TestPollThroughTheRealClientWithRetries(t *testing.T) {
	r := newRig(t, "")
	ts := newHTTPServer(t, r.srv)
	var retried int
	c, err := edgar.NewClient(edgar.Config{UserAgent: "ingest tests (no network)", BaseURL: ts, RPS: edgar.MaxRPS,
		BaseBackoff: time.Millisecond, ThrottleBackoff: 5 * time.Millisecond,
		Observe: func(status int, willRetry bool) {
			if willRetry {
				retried++
			}
		}})
	if err != nil {
		t.Fatal(err)
	}
	r.p.Fetch = c
	r.srv.Publish(filing(1, "8-K", "4.02"), filing(2, "NT 10-K"))
	r.srv.FailNext(http.StatusServiceUnavailable, http.StatusForbidden)
	if n := r.poll(t); n != 2 {
		t.Errorf("stored %d events, want 2", n)
	}
	if retried != 2 || r.srv.Requests("failed") != 2 {
		t.Errorf("%d retries observed, %d failures served; want 2 and 2", retried, r.srv.Requests("failed"))
	}
}
