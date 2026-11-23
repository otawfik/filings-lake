package edgar

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// The feed at /cgi-bin/browse-edgar?action=getcurrent&output=atom. One <entry> per filing and filer:
//
//	<title>8-K - EXAMPLE CORP (0001234567) (Filer)</title>
//	<summary type="html"> <b>Filed:</b> 2024-05-02 <b>AccNo:</b> 0001234567-24-000069 <b>Size:</b> 5 MB
//	<br>Item 4.02: Non-Reliance on Previously Issued Financial Statements ...
//	<category scheme="https://www.sec.gov/" label="form type" term="8-K"/>
//	<id>urn:tag:sec.gov,2008:accession-number=0001234567-24-000069</id>
type atomFeed struct {
	Entries []atomEntry `xml:"entry"`
}

type atomEntry struct {
	Title   string `xml:"title"`
	Summary string `xml:"summary"`
	Updated string `xml:"updated"`
	ID      string `xml:"id"`
	Link    struct {
		Href string `xml:"href,attr"`
	} `xml:"link"`
	Category struct {
		Term string `xml:"term,attr"`
	} `xml:"category"`
}

// ParseAtom reads one page of the latest-filings feed. Entries it cannot make sense of are skipped
// and counted, not fatal: one odd entry must not hide the rest of the page.
func ParseAtom(body []byte) (filings []Filing, skipped int, err error) {
	dec := xml.NewDecoder(bytes.NewReader(body))
	dec.CharsetReader = charsetReader // EDGAR serves this feed as ISO-8859-1
	var feed atomFeed
	if err := dec.Decode(&feed); err != nil {
		return nil, 0, fmt.Errorf("atom feed: %w", err)
	}
	filings = make([]Filing, 0, len(feed.Entries))
	for _, e := range feed.Entries {
		f, ok := e.filing()
		if !ok {
			skipped++
			continue
		}
		filings = append(filings, f)
	}
	return filings, skipped, nil
}

func (e atomEntry) filing() (Filing, bool) {
	const idPrefix = "accession-number="
	i := strings.LastIndex(e.ID, idPrefix)
	if i < 0 {
		return Filing{}, false
	}
	acc := strings.TrimSpace(e.ID[i+len(idPrefix):])
	if !ValidAccession(acc) {
		return Filing{}, false
	}
	form, name, cik, ok := splitTitle(e.Title)
	if !ok {
		return Filing{}, false
	}
	if t := strings.TrimSpace(e.Category.Term); t != "" {
		form = t
	}
	f := Filing{Accession: acc, Form: form, Companies: []Company{{CIK: cik, Name: name}}, URL: e.Link.Href}
	f.Filed, f.Items = splitSummary(e.Summary)
	if t, err := time.Parse(time.RFC3339, strings.TrimSpace(e.Updated)); err == nil {
		f.Updated = t
		if f.Filed == "" {
			f.Filed = t.Format("2006-01-02")
		}
	}
	return f, true
}

// splitTitle takes "8-K/A - EXAMPLE CORP (0001234567) (Filer)" apart. Company names contain dashes
// and parentheses, so the form is cut at the first " - " and the CIK is read from the right.
func splitTitle(title string) (form, name string, cik int64, ok bool) {
	title = strings.TrimSpace(title)
	i := strings.Index(title, " - ")
	if i < 0 {
		return "", "", 0, false
	}
	form, rest := title[:i], title[i+3:]
	// Drop the trailing role, "(Filer)" or "(Reporting)", if there is one after the CIK.
	if j := strings.LastIndex(rest, " ("); j >= 0 && strings.HasSuffix(rest, ")") {
		if _, err := strconv.ParseInt(rest[j+2:len(rest)-1], 10, 64); err != nil {
			rest = rest[:j]
		}
	}
	j := strings.LastIndex(rest, " (")
	if j < 0 || !strings.HasSuffix(rest, ")") {
		return "", "", 0, false
	}
	cik, err := strconv.ParseInt(rest[j+2:len(rest)-1], 10, 64)
	if err != nil || cik <= 0 {
		return "", "", 0, false
	}
	return form, strings.TrimSpace(rest[:j]), cik, true
}

// splitSummary pulls the filing date and the declared 8-K item numbers out of the entry's HTML
// summary. Only "Item N.NN" at the start of a <br> line counts: that is where EDGAR prints the
// items the filer declared, as opposed to anything a description might mention.
func splitSummary(s string) (filed string, items []string) {
	const filedTag = "<b>Filed:</b>"
	if i := strings.Index(s, filedTag); i >= 0 {
		rest := strings.TrimSpace(s[i+len(filedTag):])
		if len(rest) >= 10 {
			if _, err := time.Parse("2006-01-02", rest[:10]); err == nil {
				filed = rest[:10]
			}
		}
	}
	lines := strings.Split(s, "<br>")
	for _, line := range lines[1:] {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "Item ") {
			continue
		}
		num := line[len("Item "):]
		if k := strings.IndexAny(num, ": \n"); k >= 0 {
			num = num[:k]
		}
		if isItemNumber(num) {
			items = appendOnce(items, num)
		}
	}
	return filed, items
}

func isItemNumber(s string) bool { // "4.02"
	return len(s) == 4 && s[1] == '.' && isDigit(s[0]) && isDigit(s[2]) && isDigit(s[3])
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }

func charsetReader(label string, in io.Reader) (io.Reader, error) {
	switch strings.ToLower(label) {
	case "utf-8", "us-ascii", "ascii":
		return in, nil
	case "iso-8859-1", "latin1", "windows-1252": // 1252 differs only in 0x80-0x9F, punctuation never keyed on
		return &latin1Reader{in: in}, nil
	}
	return nil, fmt.Errorf("unsupported charset %q", label)
}

// latin1Reader re-encodes ISO-8859-1 as UTF-8: every byte is the code point of the same number.
type latin1Reader struct {
	in  io.Reader
	buf [2048]byte
}

func (r *latin1Reader) Read(p []byte) (int, error) {
	max := len(p) / 2 // a Latin-1 byte becomes at most two UTF-8 bytes
	if max == 0 {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.ErrShortBuffer
	}
	if max > len(r.buf) {
		max = len(r.buf)
	}
	n, err := r.in.Read(r.buf[:max])
	w := 0
	for _, b := range r.buf[:n] {
		if b < 0x80 {
			p[w] = b
			w++
		} else {
			p[w], p[w+1] = 0xC0|b>>6, 0x80|b&0x3F
			w += 2
		}
	}
	return w, err
}
