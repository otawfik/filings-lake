// Package edgartest is a stand-in for EDGAR: an http.Handler that serves the latest-filings feed,
// submission headers and daily form indexes for whatever filings a test publishes to it, in the
// formats the real site uses. Tests, the benchmark and the demo mode run against it, so none of
// them touches sec.gov.
package edgartest

import (
	"context"
	"fmt"
	"html"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/otawfik/filings-lake/watcher/internal/edgar"
)

// Headings are the 8-K item headings as EDGAR prints them.
var Headings = map[string]string{
	"2.02": "Results of Operations and Financial Condition",
	"4.01": "Changes in Registrant's Certifying Accountant",
	"4.02": "Non-Reliance on Previously Issued Financial Statements or a Related Audit Report or Completed Interim Review",
	"8.01": "Other Events",
	"9.01": "Financial Statements and Exhibits",
}

// Server holds the filings published so far. The zero value is not usable; call New.
type Server struct {
	mu       sync.Mutex
	feed     []edgar.Filing // newest first
	byAcc    map[string]edgar.Filing
	fail     []int // statuses to answer the next requests with, before serving normally
	requests map[string]int
	// OmitFeedItems leaves the "Item N.NN" lines out of feed entries, as if the feed did not carry
	// them, so the only way to learn an 8-K's items is its header.
	OmitFeedItems bool
}

func New() *Server {
	return &Server{byAcc: map[string]edgar.Filing{}, requests: map[string]int{}}
}

// Publish adds filings as if they had just been accepted; the last one given is the newest.
func (s *Server) Publish(filings ...edgar.Filing) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range filings {
		s.byAcc[f.Accession] = f
		s.feed = append([]edgar.Filing{f}, s.feed...)
	}
}

// PublishBulk is Publish for many filings at once (it avoids Publish's copy per filing).
func (s *Server) PublishBulk(oldestFirst []edgar.Filing) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fresh := make([]edgar.Filing, 0, len(oldestFirst)+len(s.feed))
	for i := len(oldestFirst) - 1; i >= 0; i-- {
		s.byAcc[oldestFirst[i].Accession] = oldestFirst[i]
		fresh = append(fresh, oldestFirst[i])
	}
	s.feed = append(fresh, s.feed...)
}

// FailNext makes the next len(statuses) requests fail with those statuses.
func (s *Server) FailNext(statuses ...int) {
	s.mu.Lock()
	s.fail = append(s.fail, statuses...)
	s.mu.Unlock()
}

// Requests returns how many requests of a kind were served: "feed", "header", "index" or "failed".
func (s *Server) Requests(kind string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[kind]
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.fail) > 0 {
		status := s.fail[0]
		s.fail = s.fail[1:]
		s.requests["failed"]++
		http.Error(w, "Request Rate Threshold Exceeded", status)
		return
	}
	switch p := r.URL.Path; {
	case p == "/cgi-bin/browse-edgar":
		s.requests["feed"]++
		q := r.URL.Query()
		start, _ := strconv.Atoi(q.Get("start"))
		count, _ := strconv.Atoi(q.Get("count"))
		w.Header().Set("Content-Type", "application/atom+xml")
		w.Write(s.atom(q.Get("type"), start, count))
	case strings.HasPrefix(p, "/Archives/edgar/data/") && strings.HasSuffix(p, ".txt"):
		s.requests["header"]++
		acc := strings.TrimSuffix(p[strings.LastIndex(p, "/")+1:], ".txt")
		f, ok := s.byAcc[acc]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(Header(f)))
	case strings.HasPrefix(p, "/Archives/edgar/daily-index/") && strings.HasSuffix(p, ".idx"):
		s.requests["index"]++
		day := strings.TrimSuffix(p[strings.LastIndex(p, "/form.")+len("/form."):], ".idx")
		body, ok := s.index(day)
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(body))
	default:
		http.NotFound(w, r)
	}
}

func matches(filter, form string) bool { return form == filter || form == filter+"/A" }

func (s *Server) atom(filter string, start, count int) []byte {
	var b strings.Builder
	b.WriteString("<?xml version=\"1.0\" encoding=\"ISO-8859-1\" ?>\n<feed xmlns=\"http://www.w3.org/2005/Atom\">\n<title>Latest Filings</title>\n")
	n := 0 // entries of this form so far: the feed pages by entry, one entry per filer
	for _, f := range s.feed {
		if !matches(filter, f.Form) {
			continue
		}
		for _, c := range f.Companies {
			if n >= start && n < start+count {
				s.entry(&b, f, c)
			}
			n++
		}
		if n >= start+count {
			break
		}
	}
	b.WriteString("</feed>\n")
	return latin1(b.String())
}

func (s *Server) entry(b *strings.Builder, f edgar.Filing, c edgar.Company) {
	esc := html.EscapeString
	summary := fmt.Sprintf(" <b>Filed:</b> %s <b>AccNo:</b> %s <b>Size:</b> 212 KB", f.Filed, f.Accession)
	if !s.OmitFeedItems {
		for _, it := range f.Items {
			summary += fmt.Sprintf("\n<br>Item %s: %s", it, Headings[it])
		}
	}
	updated := f.Updated
	if updated.IsZero() {
		updated, _ = time.Parse("2006-01-02 15:04", f.Filed+" 16:30")
	}
	fmt.Fprintf(b, "<entry>\n<title>%s - %s (%010d) (Filer)</title>\n", esc(f.Form), esc(c.Name), c.CIK)
	fmt.Fprintf(b, "<link rel=\"alternate\" type=\"text/html\" href=\"https://www.sec.gov/Archives/edgar/data/%d/%s/%s-index.htm\"/>\n",
		c.CIK, strings.ReplaceAll(f.Accession, "-", ""), f.Accession)
	fmt.Fprintf(b, "<summary type=\"html\">\n%s\n</summary>\n<updated>%s</updated>\n", esc(summary), updated.Format(time.RFC3339))
	fmt.Fprintf(b, "<category scheme=\"https://www.sec.gov/\" label=\"form type\" term=\"%s\"/>\n", esc(f.Form))
	fmt.Fprintf(b, "<id>urn:tag:sec.gov,2008:accession-number=%s</id>\n</entry>\n", f.Accession)
}

// Header renders a filing's SGML header, followed by the start of a document whose text mentions
// Item 4.02, which a reader of the header must not pick up.
func Header(f edgar.Filing) string {
	day := strings.ReplaceAll(f.Filed, "-", "")
	var b strings.Builder
	fmt.Fprintf(&b, "<SEC-DOCUMENT>%s.txt : %s\n<SEC-HEADER>%s.hdr.sgml : %s\n<ACCEPTANCE-DATETIME>%s163000\n", f.Accession, day, f.Accession, day, day)
	fmt.Fprintf(&b, "ACCESSION NUMBER:\t\t%s\nCONFORMED SUBMISSION TYPE:\t%s\nPUBLIC DOCUMENT COUNT:\t\t2\n", f.Accession, f.Form)
	for _, it := range f.Items {
		fmt.Fprintf(&b, "ITEM INFORMATION:\t\t%s\n", Headings[it])
	}
	fmt.Fprintf(&b, "FILED AS OF DATE:\t\t%s\nDATE AS OF CHANGE:\t\t%s\n\n", day, day)
	for _, c := range f.Companies {
		fmt.Fprintf(&b, "FILER:\n\n\tCOMPANY DATA:\t\n\t\tCOMPANY CONFORMED NAME:\t\t\t%s\n\t\tCENTRAL INDEX KEY:\t\t\t%010d\n\t\tFISCAL YEAR END:\t\t\t1231\n\n", c.Name, c.CIK)
	}
	b.WriteString("</SEC-HEADER>\n<DOCUMENT>\n<TYPE>" + f.Form + "\n<TEXT>\nItem 4.02 Non-Reliance on Previously Issued Financial Statements\n")
	return b.String()
}

func (s *Server) index(yyyymmdd string) (string, bool) {
	if len(yyyymmdd) != 8 {
		return "", false
	}
	filed := yyyymmdd[:4] + "-" + yyyymmdd[4:6] + "-" + yyyymmdd[6:]
	var b strings.Builder
	b.WriteString("Description:           Daily Index of EDGAR Dissemination Feed by Form Type\n\n")
	fmt.Fprintf(&b, "%-12s%-62s%-12s%-12s%s\n%s\n", "Form Type", "Company Name", "CIK", "Date Filed", "File Name", strings.Repeat("-", 141))
	any := false
	for i := len(s.feed) - 1; i >= 0; i-- {
		f := s.feed[i]
		if f.Filed != filed {
			continue
		}
		any = true
		for _, c := range f.Companies {
			name := c.Name
			if len(name) > 60 {
				name = name[:60]
			}
			fmt.Fprintf(&b, "%-12s%-62s%-12d%-12s%s\n", f.Form, name, c.CIK, yyyymmdd, fmt.Sprintf("edgar/data/%d/%s.txt", c.CIK, f.Accession))
		}
	}
	return b.String(), any
}

func latin1(s string) []byte {
	out := make([]byte, 0, len(s))
	for _, r := range s {
		if r > 0xFF {
			r = '?'
		}
		out = append(out, byte(r))
	}
	return out
}

// Direct calls the server in process, with no sockets and no rate limit, and builds URLs the way the
// real client does. It satisfies ingest.Fetcher.
type Direct struct {
	S      *Server
	client *edgar.Client
}

func (s *Server) Direct() *Direct {
	c, err := edgar.NewClient(edgar.Config{UserAgent: "edgartest (in process, no network)", BaseURL: "http://edgar.test"})
	if err != nil {
		panic(err)
	}
	return &Direct{S: s, client: c}
}

func (d *Direct) Get(ctx context.Context, url string, maxBytes int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !strings.HasPrefix(url, "http") {
		url = "http://edgar.test/" + strings.TrimLeft(url, "/")
	}
	rec := httptest.NewRecorder()
	d.S.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, url, nil))
	if rec.Code != http.StatusOK {
		return nil, &edgar.StatusError{URL: url, Status: rec.Code}
	}
	body := rec.Body.Bytes()
	if int64(len(body)) > maxBytes {
		body = body[:maxBytes]
	}
	return body, nil
}

func (d *Direct) LatestURL(form string, start, count int) string {
	return d.client.LatestURL(form, start, count)
}
func (d *Direct) FormIndexURL(yyyymmdd string) string { return d.client.FormIndexURL(yyyymmdd) }
func (d *Direct) SubmissionURL(cik int64, accession string) string {
	return d.client.SubmissionURL(cik, accession)
}
