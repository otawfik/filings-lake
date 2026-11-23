// Package edgar reads SEC EDGAR's public feeds: the "latest filings" Atom feed, the daily form index
// and the SGML header every filing carries. Parsing is separate from fetching so it can be tested and
// benchmarked without a network.
package edgar

import (
	"regexp"
	"sort"
	"strings"
	"time"
)

// Company is one filer named on a filing.
type Company struct {
	CIK  int64  `json:"cik"`
	Name string `json:"name"`
}

// Filing is what the watcher knows about one accession number before deciding whether it is an event.
type Filing struct {
	Accession string    `json:"accession"`
	Form      string    `json:"form"`
	Filed     string    `json:"filed"` // YYYY-MM-DD, EDGAR's filing date
	Items     []string  `json:"items,omitempty"`
	Companies []Company `json:"companies"`
	Updated   time.Time `json:"updated,omitempty"` // acceptance time, when the source carries one
	URL       string    `json:"url,omitempty"`
}

// Event kinds, named as the lake names them (lake/landing/events/<kind>.parquet).
const (
	KindItem402 = "item_4_02" // 8-K Item 4.02: non-reliance on previously issued financial statements
	KindItem401 = "item_4_01" // 8-K Item 4.01: change of certifying accountant
	KindNT10K   = "nt_10k"    // Form NT 10-K: the annual report will be late
)

// Detect returns the event kinds a filing declares, or nil if it is not an event. The form decides
// which rules apply: item numbers only mean something on an 8-K.
func Detect(f Filing) []string {
	switch strings.ToUpper(strings.TrimSpace(f.Form)) {
	case "NT 10-K", "NT 10-K/A":
		return []string{KindNT10K}
	case "8-K", "8-K/A":
		var kinds []string
		for _, it := range f.Items {
			switch it {
			case "4.01":
				kinds = appendOnce(kinds, KindItem401)
			case "4.02":
				kinds = appendOnce(kinds, KindItem402)
			}
		}
		sort.Strings(kinds)
		return kinds
	}
	return nil
}

func appendOnce(s []string, v string) []string {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(s, v)
}

var accessionRE = regexp.MustCompile(`^\d{10}-\d{2}-\d{6}$`)

// ValidAccession reports whether s has the shape of an accession number (0001234567-24-000123).
func ValidAccession(s string) bool { return accessionRE.MatchString(s) }

// Merge folds filings that share an accession number into one, keeping first-seen order. The Atom
// feed lists a filing once per filer, so a joint 8-K arrives as several entries.
func Merge(in []Filing) []Filing {
	at := make(map[string]int, len(in))
	out := make([]Filing, 0, len(in))
	for _, f := range in {
		i, ok := at[f.Accession]
		if !ok {
			at[f.Accession] = len(out)
			f.Companies = append([]Company(nil), f.Companies...)
			f.Items = append([]string(nil), f.Items...)
			out = append(out, f)
			continue
		}
		for _, c := range f.Companies {
			dup := false
			for _, have := range out[i].Companies {
				dup = dup || have.CIK == c.CIK
			}
			if !dup {
				out[i].Companies = append(out[i].Companies, c)
			}
		}
		for _, it := range f.Items {
			out[i].Items = appendOnce(out[i].Items, it)
		}
	}
	return out
}
