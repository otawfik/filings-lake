package edgar

import "testing"

func TestParseFormIndex(t *testing.T) {
	rows := ParseFormIndex(fixture(t, "form.20240502.idx"))
	want := []struct {
		form, name, accession string
		cik                   int64
	}{
		{"10-K", "ANNUAL REPORT CO", "0001000020-24-000001", 1000020},
		{"8-K", "EXAMPLE HOLDINGS INC", "0001000001-24-000069", 1000001},
		{"8-K", "JOINT FILER PARENT CORP", "0001000003-24-000040", 1000003},
		{"8-K/A", "ACME (HOLDINGS) - WEST CO", "0001000005-24-000007", 1000005},
		{"NT 10-K", "LATE FILER INC", "0001000009-24-000002", 1000009},
		{"NT 10-Q", "QUARTERLY LATE CO", "0001000011-24-000008", 1000011},
		{"SC 13G/A", "A VERY LONG COMPANY NAME THAT RUNS RIGHT UP TO THE EDGE OF I", "0001000021-24-000004", 1000021},
	}
	if len(rows) != len(want) {
		t.Fatalf("got %d rows, want %d", len(rows), len(want))
	}
	for i, w := range want {
		r := rows[i]
		if r.Form != w.form || r.Companies[0].Name != w.name || r.Accession != w.accession || r.Companies[0].CIK != w.cik {
			t.Errorf("row %d = %q %q %s %d", i, r.Form, r.Companies[0].Name, r.Accession, r.Companies[0].CIK)
		}
		if r.Filed != "2024-05-02" || r.Path != "edgar/data/"+itoa(w.cik)+"/"+w.accession+".txt" {
			t.Errorf("row %d: filed %q path %q", i, r.Filed, r.Path)
		}
	}
	if got := ParseFormIndex([]byte("no header rule, so no rows\n8-K  X  1  20240502  edgar/data/1/0000000001-24-000001.txt\n")); got != nil {
		t.Errorf("rows before the rule line were read: %v", got)
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}
