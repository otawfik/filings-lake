package edgar

import (
	"reflect"
	"testing"
)

func TestDetect(t *testing.T) {
	cases := []struct {
		name  string
		form  string
		items []string
		want  []string
	}{
		{"non-reliance", "8-K", []string{"4.02", "9.01"}, []string{KindItem402}},
		{"auditor change on an amendment", "8-K/A", []string{"4.01"}, []string{KindItem401}},
		{"both, sorted", "8-K", []string{"4.02", "4.01", "4.02"}, []string{KindItem401, KindItem402}},
		{"an ordinary 8-K", "8-K", []string{"2.02", "9.01"}, nil},
		{"an 8-K with no items", "8-K", nil, nil},
		{"late 10-K", "NT 10-K", nil, []string{KindNT10K}},
		{"late 10-K, amended, lower case", " nt 10-k/a ", nil, []string{KindNT10K}},
		{"a late 10-Q is not a late 10-K", "NT 10-Q", nil, nil},
		{"item numbers mean nothing off an 8-K", "10-K", []string{"4.02"}, nil},
	}
	for _, c := range cases {
		if got := Detect(Filing{Form: c.form, Items: c.items}); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: Detect(%q, %v) = %v, want %v", c.name, c.form, c.items, got, c.want)
		}
	}
}

func TestValidAccession(t *testing.T) {
	for s, want := range map[string]bool{
		"0001234567-24-000123":  true,
		"0001234567-24-00012":   false,
		"000123456724000123":    false,
		"0001234567-24-000123 ": false,
		"../../etc/passwd":      false,
		"":                      false,
	} {
		if got := ValidAccession(s); got != want {
			t.Errorf("ValidAccession(%q) = %v, want %v", s, got, want)
		}
	}
}

func TestMergeJoinsFilersOfOneFiling(t *testing.T) {
	a := Filing{Accession: "0001000003-24-000040", Form: "8-K", Items: []string{"2.02"}, Companies: []Company{{1000003, "PARENT"}}}
	b := Filing{Accession: "0001000003-24-000040", Form: "8-K", Items: []string{"2.02", "4.02"}, Companies: []Company{{1000004, "FINANCE"}}}
	c := Filing{Accession: "0001000001-24-000069", Form: "8-K", Companies: []Company{{1000001, "OTHER"}}}
	got := Merge([]Filing{a, c, b, a})
	if len(got) != 2 {
		t.Fatalf("got %d filings, want 2", len(got))
	}
	if got[0].Accession != a.Accession || got[1].Accession != c.Accession {
		t.Errorf("first-seen order lost: %s, %s", got[0].Accession, got[1].Accession)
	}
	if want := []Company{{1000003, "PARENT"}, {1000004, "FINANCE"}}; !reflect.DeepEqual(got[0].Companies, want) {
		t.Errorf("companies = %v, want %v", got[0].Companies, want)
	}
	if want := []string{"2.02", "4.02"}; !reflect.DeepEqual(got[0].Items, want) {
		t.Errorf("items = %v, want %v", got[0].Items, want)
	}
	if len(a.Companies) != 1 || len(a.Items) != 1 {
		t.Errorf("Merge changed its input: %+v", a)
	}
}
