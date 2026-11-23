package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/otawfik/filings-lake/watcher/internal/edgar"
)

func ev(n int, kind string) Event {
	return Event{Kind: kind, Accession: fmt.Sprintf("0001000001-24-%06d", n), Form: "8-K", Filed: "2024-05-02",
		Companies: []edgar.Company{{CIK: 1000001, Name: "EXAMPLE"}}, Source: "feed", SeenAt: time.Unix(1714665600, 0).UTC()}
}

func mustOpen(t *testing.T, path string, keep int) *Store {
	t.Helper()
	s, err := Open(path, keep)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestAddDedupesOnAccessionAndKind(t *testing.T) {
	s := mustOpen(t, "", 10)
	a, added, err := s.Add(ev(1, edgar.KindItem402))
	if err != nil || !added || a.ID != 1 {
		t.Fatalf("first add: %+v %v %v", a, added, err)
	}
	if _, added, _ := s.Add(ev(1, edgar.KindItem402)); added {
		t.Error("the same filing and kind was added twice")
	}
	if b, added, _ := s.Add(ev(1, edgar.KindItem401)); !added || b.ID != 2 {
		t.Errorf("the same filing under another kind is its own event: %+v %v", b, added)
	}
	if !s.Has(a.Accession, edgar.KindItem402) || s.Has(a.Accession, edgar.KindNT10K) {
		t.Error("Has is wrong")
	}
	counts, last := s.Counts()
	if counts[edgar.KindItem402] != 1 || counts[edgar.KindItem401] != 1 || last != 2 {
		t.Errorf("counts %v, last id %d", counts, last)
	}
}

func TestReopenKeepsEventsIDsAndDedupe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "events.jsonl")
	s := mustOpen(t, path, 10)
	for i := 1; i <= 5; i++ {
		if _, _, err := s.Add(ev(i, edgar.KindItem402)); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()

	s = mustOpen(t, path, 10)
	if _, added, _ := s.Add(ev(3, edgar.KindItem402)); added {
		t.Error("an event from before the restart was added again")
	}
	got, added, err := s.Add(ev(6, edgar.KindItem402))
	if err != nil || !added || got.ID != 6 {
		t.Errorf("after reopening, the next event got %+v (added %v, err %v), want ID 6", got, added, err)
	}
	if n := len(s.List(Query{})); n != 6 {
		t.Errorf("%d events listed, want 6", n)
	}
}

// A crash mid-write leaves half a line at the end. The store must drop it, keep everything before
// it, and carry on appending whole lines.
func TestTornLastLineIsDropped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	s := mustOpen(t, path, 10)
	s.Add(ev(1, edgar.KindItem402))
	s.Add(ev(2, edgar.KindItem402))
	s.Close()
	whole, _ := os.ReadFile(path)
	for _, torn := range []string{`{"id":3,"kind":"item_4_02","acc`, `{"id":3,"kind":"item_4_02","accession":"0001000001-24-000003"}`} {
		if err := os.WriteFile(path, append(append([]byte{}, whole...), torn...), 0o644); err != nil {
			t.Fatal(err)
		}
		s = mustOpen(t, path, 10)
		if n := len(s.List(Query{})); n != 2 {
			t.Fatalf("%d events after a torn write %q, want 2", n, torn)
		}
		if got, _, _ := s.Add(ev(3, edgar.KindItem402)); got.ID != 3 {
			t.Errorf("next ID = %d, want 3", got.ID)
		}
		s.Close()
		after, _ := os.ReadFile(path)
		if lines := strings.Count(string(after), "\n"); lines != 3 || !strings.HasPrefix(string(after), string(whole)) {
			t.Errorf("log has %d lines after recovery, want 3 whole ones", lines)
		}
	}
}

func TestDamageInTheMiddleIsAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	s := mustOpen(t, path, 10)
	s.Add(ev(1, edgar.KindItem402))
	s.Close()
	whole, _ := os.ReadFile(path)
	os.WriteFile(path, append([]byte("{not json}\n"), whole...), 0o644)
	if _, err := Open(path, 10); err == nil || !strings.Contains(err.Error(), "line 1 is damaged") {
		t.Errorf("err = %v, want it to name the damaged line", err)
	}
}

func TestListWindowOrderAndFilters(t *testing.T) {
	s := mustOpen(t, "", 5)
	for i := 1; i <= 23; i++ {
		kind := edgar.KindItem402
		if i%2 == 0 {
			kind = edgar.KindNT10K
		}
		s.Add(ev(i, kind))
	}
	ids := func(evs []Event) (out []int64) {
		for _, e := range evs {
			out = append(out, e.ID)
		}
		return
	}
	if got := fmt.Sprint(ids(s.List(Query{}))); got != "[19 20 21 22 23]" {
		t.Errorf("window = %s", got)
	}
	if got := fmt.Sprint(ids(s.List(Query{Newest: true, Limit: 2}))); got != "[23 22]" {
		t.Errorf("newest two = %s", got)
	}
	if got := fmt.Sprint(ids(s.List(Query{AfterID: 21}))); got != "[22 23]" {
		t.Errorf("after 21 = %s", got)
	}
	if got := fmt.Sprint(ids(s.List(Query{Kind: edgar.KindNT10K}))); got != "[20 22]" {
		t.Errorf("late 10-Ks = %s", got)
	}
	if got := s.List(Query{Kind: "nope"}); got == nil || len(got) != 0 {
		t.Errorf("no matches should be an empty list, not nil: %v", got)
	}
	if counts, _ := s.Counts(); counts[edgar.KindItem402] != 12 {
		t.Errorf("counts cover the whole log, not the window: %v", counts)
	}
}

func TestConcurrentAddsStoreEachEventOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	s := mustOpen(t, path, 1000)
	var wg sync.WaitGroup
	var mu sync.Mutex
	added := 0
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_, ok, err := s.Add(ev(i, edgar.KindItem402))
				if err != nil {
					t.Error(err)
				}
				if ok {
					mu.Lock()
					added++
					mu.Unlock()
				}
				s.List(Query{Newest: true, Limit: 5})
			}
		}()
	}
	wg.Wait()
	if added != 50 {
		t.Errorf("%d events added by 8 writers racing on the same 50, want 50", added)
	}
	s.Close()
	s2 := mustOpen(t, path, 1000)
	seen := map[int64]bool{}
	for _, e := range s2.List(Query{}) {
		seen[e.ID] = true
	}
	if len(seen) != 50 {
		t.Errorf("%d distinct IDs on disk, want 50", len(seen))
	}
}
