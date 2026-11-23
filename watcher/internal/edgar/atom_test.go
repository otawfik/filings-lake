package edgar

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func fixture(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestParseAtom8K(t *testing.T) {
	filings, skipped, err := ParseAtom(fixture(t, "latest_8k.atom"))
	if err != nil {
		t.Fatal(err)
	}
	if skipped != 1 {
		t.Errorf("skipped = %d, want 1 (the entry whose id carries no accession number)", skipped)
	}
	if len(filings) != 7 {
		t.Fatalf("got %d entries, want 7", len(filings))
	}

	f := filings[0]
	if f.Accession != "0001000001-24-000069" || f.Form != "8-K" || f.Filed != "2024-05-02" {
		t.Errorf("first entry: %+v", f)
	}
	if want := []string{"4.02", "9.01"}; !reflect.DeepEqual(f.Items, want) {
		t.Errorf("items = %v, want %v", f.Items, want)
	}
	if want := []Company{{1000001, "EXAMPLE HOLDINGS INC"}}; !reflect.DeepEqual(f.Companies, want) {
		t.Errorf("companies = %v, want %v", f.Companies, want)
	}
	if f.Updated.IsZero() || !strings.HasSuffix(f.URL, "0001000001-24-000069-index.htm") {
		t.Errorf("updated/url not read: %v %q", f.Updated, f.URL)
	}

	// The feed is ISO-8859-1: the accented name must come out as UTF-8.
	if got := filings[1].Companies[0].Name; got != "Société Exemple S.A." {
		t.Errorf("Latin-1 name = %q", got)
	}
	if filings[1].Form != "8-K/A" {
		t.Errorf("form = %q, want 8-K/A", filings[1].Form)
	}
	// A name with its own dash and parentheses.
	if got := filings[4].Companies[0]; got != (Company{1000005, "ACME (HOLDINGS) - WEST CO"}) {
		t.Errorf("company = %+v", got)
	}
	// A company called "ITEM 4.02 CAPITAL CORP" that declared only Item 8.01 is not an event.
	if got := filings[5].Items; !reflect.DeepEqual(got, []string{"8.01"}) {
		t.Errorf("items = %v, want [8.01]", got)
	}
	if filings[6].Items != nil {
		t.Errorf("an entry with no item lines has items %v", filings[6].Items)
	}

	// What the page amounts to once filers are merged and the rules applied.
	got := map[string][]string{}
	for _, f := range Merge(filings) {
		if k := Detect(f); k != nil {
			got[f.Accession] = k
		}
	}
	want := map[string][]string{
		"0001000001-24-000069": {KindItem402},
		"0001000002-24-000011": {KindItem401},
		"0001000005-24-000007": {KindItem401, KindItem402},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("events = %v, want %v", got, want)
	}
	if n := len(Merge(filings)); n != 6 {
		t.Errorf("%d filings after merging the joint filing, want 6", n)
	}
}

func TestParseAtomNT10K(t *testing.T) {
	filings, skipped, err := ParseAtom(fixture(t, "latest_nt10k.atom"))
	if err != nil || skipped != 0 || len(filings) != 3 {
		t.Fatalf("got %d entries, %d skipped, err %v", len(filings), skipped, err)
	}
	var kinds [][]string
	for _, f := range filings {
		kinds = append(kinds, Detect(f))
	}
	if want := [][]string{{KindNT10K}, {KindNT10K}, nil}; !reflect.DeepEqual(kinds, want) {
		t.Errorf("kinds = %v, want %v", kinds, want)
	}
}

func TestParseAtomEmptyAndBroken(t *testing.T) {
	if filings, skipped, err := ParseAtom(fixture(t, "latest_empty.atom")); err != nil || skipped != 0 || len(filings) != 0 {
		t.Errorf("empty feed: %d entries, %d skipped, err %v", len(filings), skipped, err)
	}
	for _, body := range []string{"", "<html><body>Request Rate Threshold Exceeded", `<?xml version="1.0" encoding="koi8-r"?><feed/>`} {
		if _, _, err := ParseAtom([]byte(body)); err == nil {
			t.Errorf("ParseAtom(%q) returned no error", body)
		}
	}
}

func TestSplitTitle(t *testing.T) {
	cases := []struct {
		title, form, name string
		cik               int64
		ok                bool
	}{
		{"8-K - EXAMPLE CORP (0001234567) (Filer)", "8-K", "EXAMPLE CORP", 1234567, true},
		{"NT 10-K - A - B (C) CO (0000000042) (Filer)", "NT 10-K", "A - B (C) CO", 42, true},
		{"4 - Doe John (0001999999) (Reporting)", "4", "Doe John", 1999999, true},
		{"8-K - NO ROLE CORP (0001234567)", "8-K", "NO ROLE CORP", 1234567, true},
		{"8-K - NO CIK CORP (Filer)", "", "", 0, false},
		{"no separator (0001234567) (Filer)", "", "", 0, false},
		{"", "", "", 0, false},
	}
	for _, c := range cases {
		form, name, cik, ok := splitTitle(c.title)
		if form != c.form || name != c.name || cik != c.cik || ok != c.ok {
			t.Errorf("splitTitle(%q) = %q, %q, %d, %v", c.title, form, name, cik, ok)
		}
	}
}

func FuzzParseAtom(f *testing.F) {
	for _, name := range []string{"latest_8k.atom", "latest_nt10k.atom", "latest_empty.atom"} {
		f.Add(fixture(f, name))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		filings, _, _ := ParseAtom(body) // must not panic, and must never hand back a bad accession
		for _, x := range filings {
			if !ValidAccession(x.Accession) || len(x.Companies) != 1 || x.Companies[0].CIK <= 0 {
				t.Fatalf("bad filing from fuzzed input: %+v", x)
			}
		}
	})
}
