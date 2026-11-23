package ingest

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/otawfik/filings-lake/watcher/internal/edgar"
)

// RefRow is one filing in the reference list (data/item_4_02.csv: filed, adsh, cik, …), which the
// Python side built from EDGAR full-text search. A joint filing has a row per filer; the first is kept.
type RefRow struct {
	Accession string
	Filed     string
	CIK       int64
}

// LoadReference reads the reference CSV by column name, one row per accession, in file order.
func LoadReference(path string) ([]RefRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	head, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	col := map[string]int{}
	for i, h := range head {
		col[h] = i
	}
	for _, want := range []string{"filed", "adsh", "cik"} {
		if _, ok := col[want]; !ok {
			return nil, fmt.Errorf("%s: no %q column", path, want)
		}
	}
	seen := map[string]bool{}
	var rows []RefRow
	for line := 2; ; line++ {
		rec, err := r.Read()
		if err == io.EOF {
			return rows, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		acc := rec[col["adsh"]]
		cik, cerr := strconv.ParseInt(rec[col["cik"]], 10, 64)
		if !edgar.ValidAccession(acc) || cerr != nil {
			return nil, fmt.Errorf("%s line %d: bad accession or CIK", path, line)
		}
		if !seen[acc] {
			seen[acc] = true
			rows = append(rows, RefRow{Accession: acc, Filed: rec[col["filed"]], CIK: cik})
		}
	}
}

// Found is an event read from a past day's index.
type Found struct {
	Kind   string
	Filing edgar.Filing
}

// DayResult is what one day's index held.
type DayResult struct {
	HasIndex   bool     // false on a day EDGAR published no index (a holiday)
	Filings8K  int      // 8-K and 8-K/A filings listed
	Unreadable []string // 8-Ks whose header could not be fetched or parsed
	Found      []Found
}

// Day reads one day's form index and finds the events in it, the same way the poller would have:
// an NT 10-K by its form, an 8-K by the items its header declares. The index has no items, so every
// 8-K costs one ranged request for its header.
func Day(ctx context.Context, f Fetcher, day time.Time) (DayResult, error) {
	var res DayResult
	body, err := f.Get(ctx, f.FormIndexURL(day.Format("20060102")), 64<<20)
	var se *edgar.StatusError
	if errors.As(err, &se) && se.Status == 404 {
		return res, nil
	}
	if err != nil {
		return res, err
	}
	res.HasIndex = true
	var filings []edgar.Filing
	paths := map[string]string{}
	for _, row := range edgar.ParseFormIndex(body) {
		if is8K(row.Form) || edgar.Detect(row.Filing) != nil {
			filings = append(filings, row.Filing)
			paths[row.Accession] = row.Path
		}
	}
	for _, fl := range edgar.Merge(filings) {
		if is8K(fl.Form) {
			res.Filings8K++
			hb, err := f.Get(ctx, "/Archives/"+paths[fl.Accession], edgar.HeaderBytes)
			if ctx.Err() != nil {
				return res, ctx.Err()
			}
			h, ok := edgar.ParseHeader(hb)
			if err != nil || !ok || h.Accession != fl.Accession {
				res.Unreadable = append(res.Unreadable, fl.Accession)
				continue
			}
			fl.Items = h.Items
		}
		for _, kind := range edgar.Detect(fl) {
			res.Found = append(res.Found, Found{Kind: kind, Filing: fl})
		}
	}
	return res, nil
}

// Agreement compares what the watcher finds for a kind over a date range with the reference list.
type Agreement struct {
	Kind              string   `json:"kind"`
	From              string   `json:"from"`
	To                string   `json:"to"`
	DaysWithIndex     int      `json:"days_with_index"`
	Filings8K         int      `json:"filings_8k_read"`
	HeadersUnreadable []string `json:"headers_unreadable"`
	Watcher           int      `json:"found_by_watcher"`
	Reference         int      `json:"in_reference"`
	Both              int      `json:"in_both"`
	OnlyWatcher       []string `json:"only_watcher"`
	OnlyReference     []string `json:"only_reference"`
	// Recall is the share of the reference list the watcher found; Precision the share of the
	// watcher's finds that are on the list. Both are null when their denominator is zero.
	Recall    *float64 `json:"recall"`
	Precision *float64 `json:"precision"`
}

// ReplayDays runs Day over [from, to], skipping weekends, and compares the Item 4.02 filings found
// with the reference rows filed in the same range. progress, if not nil, is called after each day.
func ReplayDays(ctx context.Context, f Fetcher, ref []RefRow, from, to time.Time, progress func(day time.Time, r DayResult)) (Agreement, error) {
	a := Agreement{Kind: edgar.KindItem402, From: from.Format("2006-01-02"), To: to.Format("2006-01-02"),
		HeadersUnreadable: []string{}, OnlyWatcher: []string{}, OnlyReference: []string{}}
	watcher := map[string]bool{}
	for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
		if wd := d.Weekday(); wd == time.Saturday || wd == time.Sunday {
			continue
		}
		r, err := Day(ctx, f, d)
		if err != nil {
			return a, fmt.Errorf("%s: %w", d.Format("2006-01-02"), err)
		}
		if r.HasIndex {
			a.DaysWithIndex++
		}
		a.Filings8K += r.Filings8K
		a.HeadersUnreadable = append(a.HeadersUnreadable, r.Unreadable...)
		for _, fd := range r.Found {
			if fd.Kind == a.Kind {
				watcher[fd.Filing.Accession] = true
			}
		}
		if progress != nil {
			progress(d, r)
		}
	}
	reference := map[string]bool{}
	for _, row := range ref {
		if row.Filed >= a.From && row.Filed <= a.To {
			reference[row.Accession] = true
		}
	}
	a.Watcher, a.Reference = len(watcher), len(reference)
	for acc := range watcher {
		if reference[acc] {
			a.Both++
		} else {
			a.OnlyWatcher = append(a.OnlyWatcher, acc)
		}
	}
	for acc := range reference {
		if !watcher[acc] {
			a.OnlyReference = append(a.OnlyReference, acc)
		}
	}
	sort.Strings(a.OnlyWatcher)
	sort.Strings(a.OnlyReference)
	a.Recall, a.Precision = ratio(a.Both, a.Reference), ratio(a.Both, a.Watcher)
	return a, nil
}

func ratio(n, d int) *float64 {
	if d == 0 {
		return nil
	}
	v := float64(n) / float64(d)
	return &v
}

// SampleResult is how the header rule did on a random sample of the reference list.
type SampleResult struct {
	Kind       string   `json:"kind"`
	Seed       uint64   `json:"seed"`
	Sampled    int      `json:"sampled"`
	Detected   int      `json:"detected"`
	Missed     []string `json:"missed"`     // header read, but the rule did not find the item
	Unreadable []string `json:"unreadable"` // header could not be fetched or parsed
	Recall     *float64 `json:"recall"`     // detected / (sampled - unreadable)
}

// ReplaySample reads the headers of n reference filings chosen at random and reports how many the
// watcher's header rule recognises as Item 4.02. It measures recall only: every filing in the sample
// is one the reference already holds to be an Item 4.02. One ranged request per filing.
func ReplaySample(ctx context.Context, f Fetcher, ref []RefRow, n int, seed uint64) (SampleResult, error) {
	res := SampleResult{Kind: edgar.KindItem402, Seed: seed, Missed: []string{}, Unreadable: []string{}}
	rng := rand.New(rand.NewPCG(seed, 0))
	pick := rng.Perm(len(ref))
	if n > len(pick) {
		n = len(pick)
	}
	for _, i := range pick[:n] {
		row := ref[i]
		body, err := f.Get(ctx, f.SubmissionURL(row.CIK, row.Accession), edgar.HeaderBytes)
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		res.Sampled++
		h, ok := edgar.ParseHeader(body)
		if err != nil || !ok || h.Accession != row.Accession {
			res.Unreadable = append(res.Unreadable, row.Accession)
			continue
		}
		hit := false
		for _, k := range edgar.Detect(h) {
			hit = hit || k == res.Kind
		}
		if hit {
			res.Detected++
		} else {
			res.Missed = append(res.Missed, row.Accession)
		}
	}
	res.Recall = ratio(res.Detected, res.Sampled-len(res.Unreadable))
	return res, nil
}
