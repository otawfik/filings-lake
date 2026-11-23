package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/otawfik/filings-lake/watcher/internal/edgar"
	"github.com/otawfik/filings-lake/watcher/internal/edgartest"
)

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

func TestServeAndReplayRefuseToRunWithoutAContact(t *testing.T) {
	for _, args := range [][]string{
		{"serve", "-addr", "127.0.0.1:0", "-data", t.TempDir()},
		{"replay", "-csv", "../../../data/item_4_02.csv", "-sample", "1"},
	} {
		for _, ua := range []string{"", "Your Name your@email"} {
			err := run(context.Background(), args, env(map[string]string{"SEC_USER_AGENT": ua}), io.Discard, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "SEC_USER_AGENT") {
				t.Errorf("%v with SEC_USER_AGENT=%q: err = %v", args[0], ua, err)
			}
		}
	}
}

func TestBadCommandLines(t *testing.T) {
	ua := env(map[string]string{"SEC_USER_AGENT": "cmd tests (no network)"})
	for _, args := range [][]string{
		{},
		{"frobnicate"},
		{"serve", "-interval", "1s"},
		{"serve", "-rps", "50", "-addr", "127.0.0.1:0"},
		{"replay"},
		{"replay", "-sample", "5", "-from", "2024-05-02", "-to", "2024-05-03"},
		{"replay", "-from", "2024-05-03", "-to", "2024-05-02", "-csv", "../../../data/item_4_02.csv"},
		{"replay", "-sample", "5", "-csv", "no-such-file.csv"},
	} {
		if err := run(context.Background(), args, ua, io.Discard, io.Discard); err == nil {
			t.Errorf("%v was accepted", args)
		}
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for i := 0; i < 500; i++ {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func getJSON(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	var v map[string]any
	json.NewDecoder(resp.Body).Decode(&v)
	return resp.StatusCode, v
}

// The whole program against the stand-in EDGAR over real sockets: it must send the contact from the
// environment on every request, serve what it finds, shut down cleanly, and not repeat events after
// a restart.
func TestServeEndToEnd(t *testing.T) {
	stand := edgartest.New()
	var mu sync.Mutex
	agents := map[string]int{}
	edgarSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		agents[r.Header.Get("User-Agent")]++
		mu.Unlock()
		stand.ServeHTTP(w, r)
	}))
	defer edgarSrv.Close()
	f := func(n int, form string, items ...string) edgar.Filing {
		return edgar.Filing{Accession: fmt.Sprintf("0001000001-24-%06d", n), Form: form, Filed: "2024-05-02", Items: items,
			Companies: []edgar.Company{{CIK: int64(1000000 + n), Name: fmt.Sprintf("COMPANY %d", n)}}}
	}
	stand.Publish(f(1, "8-K", "2.02"), f(2, "8-K", "4.02", "9.01"), f(3, "NT 10-K"))

	const contact = "cmd tests (no network) tests@invalid.example"
	dir := t.TempDir()
	start := func() (addr string, logs *syncBuffer, stop func() error) {
		addr, logs = freeAddr(t), &syncBuffer{}
		ctx, cancel := context.WithCancel(context.Background())
		errc := make(chan error, 1)
		go func() {
			errc <- run(ctx, []string{"serve", "-addr", addr, "-data", dir, "-base-url", edgarSrv.URL, "-interval", "100ms", "-rps", "10"},
				env(map[string]string{"SEC_USER_AGENT": contact}), io.Discard, logs)
		}()
		return addr, logs, func() error {
			cancel()
			select {
			case err := <-errc:
				return err
			case <-time.After(10 * time.Second):
				return fmt.Errorf("did not shut down within 10s")
			}
		}
	}

	addr, logs, stop := start()
	waitFor(t, "the first two events", func() bool {
		_, v := getJSON(t, "http://"+addr+"/events")
		return v != nil && v["last_event_id"] == float64(2)
	})
	if code, v := getJSON(t, "http://"+addr+"/healthz"); code != 200 || v["status"] != "ok" {
		t.Errorf("healthz: %d %v", code, v)
	}
	stand.Publish(f(4, "8-K/A", "4.01"))
	waitFor(t, "the event published while running", func() bool {
		_, v := getJSON(t, "http://"+addr+"/events?kind=item_4_01")
		return v != nil && len(v["events"].([]any)) == 1
	})
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{`watcher_events_total{kind="item_4_02"} 1`, `watcher_events_total{kind="nt_10k"} 1`, `watcher_edgar_requests_total{status="200"}`, `watcher_polls_total{result="ok"}`} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics lack %s", want)
		}
	}
	if err := stop(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if strings.Contains(logs.String(), contact) {
		t.Error("the contact was written to the log")
	}
	mu.Lock()
	if len(agents) != 1 || agents[contact] == 0 {
		t.Errorf("User-Agents seen by EDGAR: %v; want only the one from the environment", agents)
	}
	mu.Unlock()

	// Restart on the same data directory with one more filing out.
	stand.Publish(f(5, "8-K", "4.02"))
	addr, _, stop = start()
	waitFor(t, "the event published while it was down", func() bool {
		_, v := getJSON(t, "http://"+addr+"/events")
		return v != nil && v["last_event_id"] == float64(4)
	})
	if err := stop(); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
	log, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(log), "\n"); n != 4 {
		t.Errorf("the event log has %d lines after a restart, want 4 (no repeats):\n%s", n, log)
	}
}

func TestDemoModeNeedsNoContactAndWritesNoLog(t *testing.T) {
	addr, dir := freeAddr(t), t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		errc <- run(ctx, []string{"serve", "-demo", "-addr", addr, "-data", dir, "-interval", "100ms"}, env(nil), io.Discard, io.Discard)
	}()
	waitFor(t, "demo events", func() bool {
		_, v := getJSON(t, "http://"+addr+"/events")
		return v != nil && v["last_event_id"].(float64) >= 3
	})
	cancel()
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("demo mode wrote to the data directory: %v", entries)
	}
}

func TestReplayCommandPrintsTheAgreement(t *testing.T) {
	stand := edgartest.New()
	edgarSrv := httptest.NewServer(stand)
	defer edgarSrv.Close()
	csv := "filed,adsh,cik,form\n"
	for n := 1; n <= 4; n++ {
		acc := fmt.Sprintf("0001000001-24-%06d", n)
		items := []string{"4.02"}
		if n == 4 {
			items = []string{"8.01"}
		}
		stand.Publish(edgar.Filing{Accession: acc, Form: "8-K", Filed: "2024-05-02", Items: items, Companies: []edgar.Company{{CIK: int64(1000000 + n), Name: "CO"}}})
		if n != 1 {
			csv += fmt.Sprintf("2024-05-02,%s,%d,8-K\n", acc, 1000000+n)
		}
	}
	path := filepath.Join(t.TempDir(), "ref.csv")
	os.WriteFile(path, []byte(csv), 0o644)
	ua := env(map[string]string{"SEC_USER_AGENT": "cmd tests (no network)"})

	var out bytes.Buffer
	args := []string{"replay", "-csv", path, "-base-url", edgarSrv.URL, "-rps", "10", "-from", "2024-05-02", "-to", "2024-05-03"}
	if err := run(context.Background(), args, ua, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	var days map[string]any
	json.Unmarshal(out.Bytes(), &days)
	if days["found_by_watcher"] != float64(3) || days["in_reference"] != float64(3) || days["in_both"] != float64(2) || days["filings_8k_read"] != float64(4) {
		t.Errorf("days replay: %s", out.String())
	}

	out.Reset()
	args = []string{"replay", "-csv", path, "-base-url", edgarSrv.URL, "-rps", "10", "-sample", "10"}
	if err := run(context.Background(), args, ua, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	var sample map[string]any
	json.Unmarshal(out.Bytes(), &sample)
	if sample["sampled"] != float64(3) || sample["detected"] != float64(2) || len(sample["missed"].([]any)) != 1 {
		t.Errorf("sample replay: %s", out.String())
	}
}
