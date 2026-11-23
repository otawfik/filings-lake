package ingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/otawfik/filings-lake/watcher/internal/edgar"
	"github.com/otawfik/filings-lake/watcher/internal/edgartest"
	"github.com/otawfik/filings-lake/watcher/internal/store"
)

const referenceCSV = "../../../data/item_4_02.csv"

func on(f edgar.Filing, day string) edgar.Filing {
	f.Filed = day
	f.Updated = time.Time{}
	return f
}

func date(s string) time.Time {
	d, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return d
}

func TestLoadReference(t *testing.T) {
	rows, err := LoadReference(referenceCSV)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) < 4000 || rows[0] != (RefRow{"0000950134-09-000030", "2009-01-02", 775351}) {
		t.Errorf("%d rows, first %+v", len(rows), rows[0])
	}
	seen := map[string]bool{}
	for _, r := range rows {
		if seen[r.Accession] || len(r.Filed) != 10 {
			t.Fatalf("bad or repeated row %+v", r)
		}
		seen[r.Accession] = true
	}

	dir := t.TempDir()
	for name, body := range map[string]string{
		"nocol.csv": "filed,accession\n2024-05-02,0001000001-24-000001\n",
		"bad.csv":   "filed,adsh,cik\n2024-05-02,not-an-accession,1\n",
	} {
		p := filepath.Join(dir, name)
		os.WriteFile(p, []byte(body), 0o644)
		if _, err := LoadReference(p); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// Three days of filings read back from the daily index: a Thursday, a Friday, and (after a weekend)
// a Monday with no index. The reference list is made to disagree in both directions.
func TestReplayDaysMeasuresAgreementBothWays(t *testing.T) {
	srv := edgartest.New()
	joint := on(filing(4, "8-K", "4.01", "4.02"), "2024-05-02")
	joint.Companies = append(joint.Companies, edgar.Company{CIK: 2000004, Name: "COMPANY 4 FINANCE LLC"})
	srv.Publish(
		on(filing(1, "8-K", "2.02", "9.01"), "2024-05-02"),
		on(filing(2, "8-K", "4.02", "9.01"), "2024-05-02"),
		on(filing(3, "10-K"), "2024-05-02"),
		joint,
		on(filing(5, "NT 10-K"), "2024-05-02"),
		on(filing(6, "8-K/A", "4.02"), "2024-05-03"),
		on(filing(7, "8-K", "8.01"), "2024-05-03"),
		on(filing(8, "8-K", "4.02"), "2024-05-10"), // outside the range
	)
	ref := []RefRow{
		{filing(2, "").Accession, "2024-05-02", 1000002},
		{filing(4, "").Accession, "2024-05-02", 1000004},
		{"0009999999-24-000001", "2024-05-03", 9999999},  // on the list, not on EDGAR's index
		{filing(8, "").Accession, "2024-05-10", 1000008}, // outside the range
		// filing 6 is missing from the list
	}
	var days []string
	a, err := ReplayDays(context.Background(), srv.Direct(), ref, date("2024-05-02"), date("2024-05-06"),
		func(d time.Time, r DayResult) {
			days = append(days, fmt.Sprintf("%s:%v:%d", d.Format("01-02"), r.HasIndex, r.Filings8K))
		})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(days, " "); got != "05-02:true:3 05-03:true:2 05-06:false:0" {
		t.Errorf("days = %s", got)
	}
	if a.DaysWithIndex != 2 || a.Filings8K != 5 || len(a.HeadersUnreadable) != 0 {
		t.Errorf("days %d, 8-Ks %d, unreadable %v", a.DaysWithIndex, a.Filings8K, a.HeadersUnreadable)
	}
	if a.Watcher != 3 || a.Reference != 3 || a.Both != 2 {
		t.Errorf("watcher %d, reference %d, both %d; want 3, 3, 2", a.Watcher, a.Reference, a.Both)
	}
	if fmt.Sprint(a.OnlyWatcher) != "["+filing(6, "").Accession+"]" || fmt.Sprint(a.OnlyReference) != "[0009999999-24-000001]" {
		t.Errorf("only watcher %v, only reference %v", a.OnlyWatcher, a.OnlyReference)
	}
	if *a.Recall != 2.0/3 || *a.Precision != 2.0/3 {
		t.Errorf("recall %v precision %v", *a.Recall, *a.Precision)
	}
	if got := srv.Requests("header"); got != 5 {
		t.Errorf("%d header requests, want 5: one per 8-K, joint filers counted once, none for other forms", got)
	}

	empty, err := ReplayDays(context.Background(), srv.Direct(), nil, date("2024-05-04"), date("2024-05-05"), nil)
	if err != nil || empty.Recall != nil || empty.Precision != nil {
		t.Errorf("a weekend: %+v %v; want no ratios, not 0/0", empty, err)
	}
}

func TestReplaySample(t *testing.T) {
	srv := edgartest.New()
	var ref []RefRow
	for i := 1; i <= 20; i++ {
		items := []string{"4.02", "9.01"}
		if i == 7 {
			items = []string{"8.01"} // on the list, but its header does not declare the item
		}
		f := filing(i, "8-K", items...)
		if i != 13 { // 13 is on the list and missing from the server
			srv.Publish(f)
		}
		ref = append(ref, RefRow{f.Accession, f.Filed, f.Companies[0].CIK})
	}
	res, err := ReplaySample(context.Background(), srv.Direct(), ref, 50, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Sampled != 20 || res.Detected != 18 || len(res.Missed) != 1 || len(res.Unreadable) != 1 {
		t.Errorf("%+v", res)
	}
	if *res.Recall != 18.0/19 {
		t.Errorf("recall = %v, want 18/19 (the unreadable one is left out of the denominator)", *res.Recall)
	}
	a, _ := ReplaySample(context.Background(), srv.Direct(), ref, 5, 42)
	b, _ := ReplaySample(context.Background(), srv.Direct(), ref, 5, 42)
	if a.Sampled != 5 || a.Detected != b.Detected || fmt.Sprint(a.Missed, a.Unreadable) != fmt.Sprint(b.Missed, b.Unreadable) {
		t.Errorf("the same seed gave different samples")
	}
}

// The whole reference list pushed through the live path: every filing in data/item_4_02.csv is
// published to the stand-in feed as an 8-K declaring Item 4.02, three ordinary 8-Ks and the odd
// NT 10-Q are mixed in after each, and the poller follows the feed in batches as it would over
// years of polling, restarting once in the middle. The Item 4.02 events stored must be exactly the
// list. This checks paging, merging, dedupe and persistence at the scale of the real list; it does
// not check the watcher against sec.gov, which only `watcher replay` can do.
func TestReferenceListThroughTheFeed(t *testing.T) {
	ref, err := LoadReference(referenceCSV)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "events.jsonl")
	r := newRig(t, path)
	noise, polls := 0, 0
	const batch = 60 // reference filings per poll: 240 feed entries, so each poll pages
	for at := 0; at < len(ref); at += batch {
		var fs []edgar.Filing
		for _, row := range ref[at:min(at+batch, len(ref))] {
			fs = append(fs, edgar.Filing{Accession: row.Accession, Form: "8-K", Filed: row.Filed, Items: []string{"4.02", "9.01"},
				Companies: []edgar.Company{{CIK: row.CIK, Name: "REFERENCE FILER"}}})
			for k := 0; k < 3; k++ {
				noise++
				form, items := "8-K", []string{"2.02", "9.01"}
				if noise%50 == 0 {
					form, items = "NT 10-Q", nil
				}
				fs = append(fs, edgar.Filing{Accession: fmt.Sprintf("0009%06d-99-%06d", noise/1000, noise%1000000), Form: form, Filed: row.Filed,
					Items: items, Companies: []edgar.Company{{CIK: int64(9000000 + noise), Name: "NOISE CO"}}})
			}
		}
		r.srv.PublishBulk(fs)
		r.poll(t)
		polls++
		if at == (len(ref)/2/batch)*batch { // restart half way: a new poller on the same log
			r.store.Close()
			s2, err := store.Open(path, 100000)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s2.Close() })
			r.store = s2
			r.p = NewPoller(r.srv.Direct(), s2, nil, r.reg2(), nil)
		}
	}
	want := map[string]bool{}
	for _, row := range ref {
		want[row.Accession] = true
	}
	got := map[string]bool{}
	for _, ev := range r.store.List(store.Query{}) {
		if ev.Kind != edgar.KindItem402 {
			t.Fatalf("an event of kind %s from a feed with no such filings: %s", ev.Kind, ev.Accession)
		}
		if got[ev.Accession] {
			t.Fatalf("%s stored twice", ev.Accession)
		}
		got[ev.Accession] = true
	}
	missing, extra := 0, 0
	for a := range want {
		if !got[a] {
			missing++
		}
	}
	for a := range got {
		if !want[a] {
			extra++
		}
	}
	t.Logf("reference filings %d, other filings mixed in %d, polls %d, feed requests %d: events stored %d, missing %d, extra %d",
		len(want), noise, polls, r.srv.Requests("feed"), len(got), missing, extra)
	if missing != 0 || extra != 0 || len(got) != len(want) {
		t.Errorf("the events stored are not the reference list: %d missing, %d extra", missing, extra)
	}
}
