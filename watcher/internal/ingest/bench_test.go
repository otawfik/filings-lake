package ingest

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/otawfik/filings-lake/watcher/internal/edgar"
	"github.com/otawfik/filings-lake/watcher/internal/edgartest"
	"github.com/otawfik/filings-lake/watcher/internal/hub"
	"github.com/otawfik/filings-lake/watcher/internal/metrics"
	"github.com/otawfik/filings-lake/watcher/internal/store"
)

// page is a full feed page: 100 entries, one 8-K in ten declaring Item 4.02.
func page(from int) []edgar.Filing {
	fs := make([]edgar.Filing, 100)
	for i := range fs {
		items := []string{"2.02", "9.01"}
		if i%10 == 0 {
			items = []string{"4.02", "9.01"}
		}
		fs[i] = filing(from+i, "8-K", items...)
	}
	return fs
}

func feedPage(b *testing.B) []byte {
	srv := edgartest.New()
	srv.PublishBulk(page(1))
	d := srv.Direct()
	body, err := d.Get(context.Background(), d.LatestURL("8-K", 0, 100), 1<<22)
	if err != nil {
		b.Fatal(err)
	}
	return body
}

// Parsing one full page of the feed (100 entries, about 70 KB of Atom).
func BenchmarkParseAtomPage(b *testing.B) {
	body := feedPage(b)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for b.Loop() {
		if fs, _, err := edgar.ParseAtom(body); err != nil || len(fs) != 100 {
			b.Fatal(len(fs), err)
		}
	}
}

// Parsing, merging and applying the rules to one page: everything between the bytes and the store.
func BenchmarkDetectPage(b *testing.B) {
	body := feedPage(b)
	b.ReportAllocs()
	for b.Loop() {
		fs, _, _ := edgar.ParseAtom(body)
		n := 0
		for _, f := range edgar.Merge(fs) {
			n += len(edgar.Detect(f))
		}
		if n != 10 {
			b.Fatal(n)
		}
	}
}

func BenchmarkParseHeader(b *testing.B) {
	body := []byte(edgartest.Header(filing(1, "8-K", "4.01", "4.02", "9.01")))
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := edgar.ParseHeader(body); !ok {
			b.Fatal("not parsed")
		}
	}
}

// One poll that finds a page of 100 new 8-Ks, 10 of them events, against the in-process stand-in
// (no sockets, no rate limit). The time includes the stand-in rendering the two feed pages the poll
// asks for, so it is an upper bound on the watcher's own work. With "disk", each of the 10 events
// is appended to the log and synced before the poll moves on.
func BenchmarkPollNewPage(b *testing.B) {
	for _, where := range []string{"memory", "disk"} {
		b.Run(where, func(b *testing.B) {
			path := ""
			if where == "disk" {
				path = filepath.Join(b.TempDir(), "events.jsonl")
			}
			st, err := store.Open(path, 5000)
			if err != nil {
				b.Fatal(err)
			}
			defer st.Close()
			srv := edgartest.New()
			p := NewPoller(srv.Direct(), st, hub.New(64), metrics.New(), nil)
			n := 1
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				srv.PublishBulk(page(n))
				n += 100
				b.StartTimer()
				if got, err := p.PollOnce(context.Background()); err != nil || got != 10 {
					b.Fatal(got, err)
				}
			}
		})
	}
}

// One poll that finds nothing new: the steady state between filings.
func BenchmarkPollNothingNew(b *testing.B) {
	st, _ := store.Open("", 5000)
	srv := edgartest.New()
	srv.PublishBulk(page(1))
	p := NewPoller(srv.Direct(), st, nil, metrics.New(), nil)
	if _, err := p.PollOnce(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if got, err := p.PollOnce(context.Background()); err != nil || got != 0 {
			b.Fatal(got, err)
		}
	}
}

// Publishing one event to 1,000 connected stream clients. The benchmark drains the clients itself,
// off the clock, every 64 publishes (their buffer size), so none is ever cut off and every publish
// timed is a real delivery to all 1,000.
func BenchmarkHubPublish1000(b *testing.B) {
	const clients, buffer = 1000, 64
	h := hub.New(buffer)
	subs := make([]*hub.Sub, clients)
	for i := range subs {
		subs[i] = h.Subscribe()
	}
	ev := store.Event{ID: 1, Kind: edgar.KindItem402, Accession: fmt.Sprintf("0001000001-24-%06d", 1)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%buffer == 0 && i > 0 {
			b.StopTimer()
			for _, s := range subs {
				for len(s.C) > 0 {
					<-s.C
				}
			}
			b.StartTimer()
		}
		h.Publish(ev)
	}
	b.StopTimer()
	if h.Subscribers() != clients || h.Dropped() != 0 {
		b.Fatalf("%d subscribers left, %d cut off: the benchmark did not deliver to all of them", h.Subscribers(), h.Dropped())
	}
}
