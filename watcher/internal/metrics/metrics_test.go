package metrics

import (
	"strings"
	"sync"
	"testing"
)

func TestWriteIsPrometheusTextSortedAndEscaped(t *testing.T) {
	r := New()
	req := r.Counter("watcher_requests_total", "Requests to EDGAR.", "status")
	polls := r.Counter("watcher_polls_total", "Polls.")
	last := r.Gauge("watcher_last_poll_seconds", "When the last poll ended.")
	r.GaugeFunc("watcher_clients", "Connected clients.", func() float64 { return 3 })
	req.Inc("503")
	req.Add(2, "200")
	req.Inc(`we"ird` + "\n")
	last.Set(1714665600)
	_ = polls

	var b strings.Builder
	if err := r.Write(&b); err != nil {
		t.Fatal(err)
	}
	want := `# HELP watcher_clients Connected clients.
# TYPE watcher_clients gauge
watcher_clients 3
# HELP watcher_last_poll_seconds When the last poll ended.
# TYPE watcher_last_poll_seconds gauge
watcher_last_poll_seconds 1.7146656e+09
# HELP watcher_polls_total Polls.
# TYPE watcher_polls_total counter
watcher_polls_total 0
# HELP watcher_requests_total Requests to EDGAR.
# TYPE watcher_requests_total counter
watcher_requests_total{status="200"} 2
watcher_requests_total{status="503"} 1
watcher_requests_total{status="we\"ird\n"} 1
`
	if b.String() != want {
		t.Errorf("got:\n%s\nwant:\n%s", b.String(), want)
	}
	if r.Value("watcher_requests_total", "200") != 2 || r.Value("nope") != 0 {
		t.Error("Value is wrong")
	}
}

func TestMisuseIsCaughtAtOnce(t *testing.T) {
	r := New()
	c := r.Counter("c_total", "c", "a")
	for name, f := range map[string]func(){
		"wrong label count":  func() { c.Inc() },
		"counter going down": func() { c.Add(-1, "x") },
		"duplicate metric":   func() { r.Gauge("c_total", "again") },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s did not panic", name)
				}
			}()
			f()
		}()
	}
}

func TestConcurrentUse(t *testing.T) {
	r := New()
	c := r.Counter("c_total", "c", "k")
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				c.Inc("a")
				r.Write(&strings.Builder{})
			}
		}()
	}
	wg.Wait()
	if got := r.Value("c_total", "a"); got != 4000 {
		t.Errorf("counted %v, want 4000", got)
	}
}
