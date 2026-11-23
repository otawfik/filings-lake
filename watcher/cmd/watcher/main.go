// Command watcher follows SEC EDGAR's latest-filings feed and serves the filings that are 8-K Item
// 4.02 (non-reliance on issued financial statements), 8-K Item 4.01 (auditor change) or NT 10-K
// (late annual report) as a JSON API and a live stream.
//
//	SEC_USER_AGENT="Your Name your@email" watcher serve
//	watcher serve -demo                                  # no network: made-up filings, to see it run
//	SEC_USER_AGENT=… watcher replay -from 2026-11-02 -to 2026-11-06
//	SEC_USER_AGENT=… watcher replay -sample 200
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/otawfik/filings-lake/watcher/internal/api"
	"github.com/otawfik/filings-lake/watcher/internal/edgar"
	"github.com/otawfik/filings-lake/watcher/internal/edgartest"
	"github.com/otawfik/filings-lake/watcher/internal/hub"
	"github.com/otawfik/filings-lake/watcher/internal/ingest"
	"github.com/otawfik/filings-lake/watcher/internal/metrics"
	"github.com/otawfik/filings-lake/watcher/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "watcher:", err)
		os.Exit(1)
	}
}

const usage = `usage:
  watcher serve  [-addr :8080] [-interval 60s] [-rps 2] [-data ./watcher-data] [-demo]
  watcher replay [-csv data/item_4_02.csv] (-from YYYY-MM-DD -to YYYY-MM-DD | -sample N [-seed S]) [-rps 2]

serve and replay read the SEC contact from the SEC_USER_AGENT environment variable ("Your Name your@email").
serve -demo needs neither that nor a network.`

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("no command\n" + usage)
	}
	switch args[0] {
	case "serve":
		return serve(ctx, args[1:], getenv, stderr)
	case "replay":
		return replay(ctx, args[1:], getenv, stdout, stderr)
	case "-h", "-help", "--help", "help":
		fmt.Fprintln(stdout, usage)
		return nil
	}
	return fmt.Errorf("unknown command %q\n%s", args[0], usage)
}

// client builds the EDGAR client. The contact comes from the environment and nowhere else.
func client(getenv func(string) string, baseURL string, rps float64, reg *metrics.Registry) (*edgar.Client, error) {
	cfg := edgar.Config{UserAgent: getenv("SEC_USER_AGENT"), BaseURL: baseURL, RPS: rps}
	if reg != nil {
		requests := reg.Counter("watcher_edgar_requests_total", "Responses from EDGAR by status; 0 is a transport error.", "status")
		retries := reg.Counter("watcher_edgar_retries_total", "Requests to EDGAR that were retried.")
		cfg.Observe = func(status int, willRetry bool) {
			requests.Inc(strconv.Itoa(status))
			if willRetry {
				retries.Inc()
			}
		}
	}
	return edgar.NewClient(cfg)
}

func serve(ctx context.Context, args []string, getenv func(string) string, stderr io.Writer) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", ":8080", "address the API listens on")
	interval := fs.Duration("interval", time.Minute, "time between polls of the feed")
	rps := fs.Float64("rps", edgar.DefaultRPS, "most requests per second to EDGAR (the SEC allows 10)")
	data := fs.String("data", "watcher-data", "directory for the event log")
	keep := fs.Int("keep", 5000, "events kept in memory for the API")
	baseURL := fs.String("base-url", "", "EDGAR base URL, for testing against a stand-in (default https://www.sec.gov)")
	noHeaders := fs.Bool("no-header-fallback", false, "do not read a filing's header when its feed entry lists no items")
	demo := fs.Bool("demo", false, "run against made-up filings, in memory, with no network and no event log")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *interval < 10*time.Second && !*demo && *baseURL == "" {
		return errors.New("-interval under 10s is more than the feed needs and unkind to EDGAR")
	}
	log := slog.New(slog.NewTextHandler(stderr, nil))
	reg := metrics.New()

	var fetch ingest.Fetcher
	logPath := filepath.Join(*data, "events.jsonl")
	if *demo {
		fetch, logPath = demoFeed(ctx, *interval), ""
		log.Warn("demo mode: the filings are made up and nothing is read from sec.gov")
	} else {
		c, err := client(getenv, *baseURL, *rps, reg)
		if err != nil {
			return err
		}
		fetch = c
	}
	st, err := store.Open(logPath, *keep)
	if err != nil {
		return err
	}
	defer st.Close()
	h := hub.New(256)
	p := ingest.NewPoller(fetch, st, h, reg, log)
	p.HeaderFallback = !*noHeaders

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	srvCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	srv := &http.Server{
		Handler:           (&api.Server{Store: st, Hub: h, Reg: reg, Status: p.Status, StaleAfter: 3**interval + time.Minute}).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      30 * time.Second, // the stream handler lifts this for itself
		IdleTimeout:       2 * time.Minute,
		BaseContext:       func(net.Listener) context.Context { return srvCtx }, // so open streams end at shutdown
	}
	counts, lastID := st.Counts()
	log.Info("listening", "addr", ln.Addr().String(), "interval", interval.String(), "events_on_disk", lastID, "by_kind", fmt.Sprint(counts))

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(ctx, *interval)
	}()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down")
	cancel()
	shutCtx, cancelShut := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShut()
	srv.Shutdown(shutCtx)
	<-done // the poller has stopped writing before the store is closed
	return nil
}

// demoFeed is an in-process stand-in for EDGAR that "accepts" a made-up filing every few seconds.
func demoFeed(ctx context.Context, interval time.Duration) ingest.Fetcher {
	srv := edgartest.New()
	shapes := []struct {
		form  string
		items []string
	}{{"8-K", []string{"2.02", "9.01"}}, {"8-K", []string{"4.02", "9.01"}}, {"8-K", []string{"8.01"}}, {"NT 10-K", nil},
		{"8-K", []string{"4.01"}}, {"8-K", []string{"2.02"}}, {"8-K/A", []string{"4.01", "4.02"}}}
	publish := func(n int) {
		s := shapes[n%len(shapes)]
		now := time.Now().UTC()
		srv.Publish(edgar.Filing{Accession: fmt.Sprintf("0009999999-%02d-%06d", now.Year()%100, n+1), Form: s.form, Items: s.items,
			Filed: now.Format("2006-01-02"), Updated: now, Companies: []edgar.Company{{CIK: int64(9990000 + n), Name: fmt.Sprintf("DEMO COMPANY %d (NOT A REAL FILING)", n+1)}}})
	}
	for n := 0; n < 5; n++ {
		publish(n)
	}
	go func() {
		t := time.NewTicker(max(interval/2, time.Second))
		defer t.Stop()
		for n := 5; ; n++ {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				publish(n)
			}
		}
	}()
	return srv.Direct()
}

func replay(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(stderr)
	csvPath := fs.String("csv", "data/item_4_02.csv", "the reference list of Item 4.02 filings")
	from := fs.String("from", "", "first day to read from the daily index (YYYY-MM-DD)")
	to := fs.String("to", "", "last day (YYYY-MM-DD)")
	sample := fs.Int("sample", 0, "instead of days: read the headers of this many filings picked at random from the list")
	seed := fs.Uint64("seed", 1, "seed for -sample")
	rps := fs.Float64("rps", edgar.DefaultRPS, "most requests per second to EDGAR")
	baseURL := fs.String("base-url", "", "EDGAR base URL, for testing against a stand-in")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if (*sample > 0) == (*from != "" || *to != "") {
		return errors.New("replay wants either -from and -to, or -sample\n" + usage)
	}
	ref, err := ingest.LoadReference(*csvPath)
	if err != nil {
		return err
	}
	c, err := client(getenv, *baseURL, *rps, nil)
	if err != nil {
		return err
	}
	var result any
	if *sample > 0 {
		fmt.Fprintf(stderr, "reading %d headers at up to %g requests a second\n", min(*sample, len(ref)), *rps)
		result, err = ingest.ReplaySample(ctx, c, ref, *sample, *seed)
	} else {
		a, aerr := time.Parse("2006-01-02", *from)
		b, berr := time.Parse("2006-01-02", *to)
		if aerr != nil || berr != nil || b.Before(a) {
			return errors.New("-from and -to must be dates (YYYY-MM-DD), in order")
		}
		result, err = ingest.ReplayDays(ctx, c, ref, a, b, func(day time.Time, r ingest.DayResult) {
			fmt.Fprintf(stderr, "%s: index %v, %d 8-Ks read, %d events, %d unreadable\n", day.Format("2006-01-02"), r.HasIndex, r.Filings8K, len(r.Found), len(r.Unreadable))
		})
	}
	if err != nil {
		return err
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}
