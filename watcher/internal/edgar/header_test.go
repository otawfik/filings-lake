package edgar

import (
	"reflect"
	"testing"
)

func TestParseHeader(t *testing.T) {
	cases := []struct {
		file      string
		accession string
		form      string
		filed     string
		items     []string
		companies []Company
		kinds     []string
	}{
		{"header_402.txt", "0001000001-24-000069", "8-K", "2024-05-02", []string{"4.02"},
			[]Company{{1000001, "EXAMPLE HOLDINGS INC"}}, []string{KindItem402}},
		{"header_401_402_joint.txt", "0001000005-24-000007", "8-K/A", "2024-05-02", []string{"4.01", "4.02"},
			[]Company{{1000005, "ACME (HOLDINGS) - WEST CO"}, {1000012, "ACME FINANCE: WEST LLC"}}, []string{KindItem401, KindItem402}},
		// The body of this fixture mentions Item 4.02 and a certifying accountant after </SEC-HEADER>.
		{"header_202.txt", "0001000003-24-000040", "8-K", "2024-05-02", nil,
			[]Company{{1000003, "JOINT FILER PARENT CORP"}}, nil},
		{"header_nt10k.txt", "0001000009-24-000002", "NT 10-K", "2024-04-01", nil,
			[]Company{{1000009, "LATE FILER INC"}}, []string{KindNT10K}},
	}
	for _, c := range cases {
		f, ok := ParseHeader(fixture(t, c.file))
		if !ok {
			t.Errorf("%s: not parsed", c.file)
			continue
		}
		if f.Accession != c.accession || f.Form != c.form || f.Filed != c.filed {
			t.Errorf("%s: got %s %s %s", c.file, f.Accession, f.Form, f.Filed)
		}
		if !reflect.DeepEqual(f.Items, c.items) {
			t.Errorf("%s: items = %v, want %v", c.file, f.Items, c.items)
		}
		if !reflect.DeepEqual(f.Companies, c.companies) {
			t.Errorf("%s: companies = %v, want %v", c.file, f.Companies, c.companies)
		}
		if got := Detect(f); !reflect.DeepEqual(got, c.kinds) {
			t.Errorf("%s: kinds = %v, want %v", c.file, got, c.kinds)
		}
	}
}

func TestParseHeaderTruncatedOrNotAHeader(t *testing.T) {
	full := fixture(t, "header_402.txt")
	if f, ok := ParseHeader(full[:400]); !ok || f.Accession != "0001000001-24-000069" {
		t.Errorf("a header cut after the accession number should still give it: %+v %v", f, ok)
	}
	for _, body := range []string{"", "<html>Request Rate Threshold Exceeded</html>", "ACCESSION NUMBER:\t\tnot-a-number\n"} {
		if _, ok := ParseHeader([]byte(body)); ok {
			t.Errorf("ParseHeader(%q) reported ok", body)
		}
	}
}
