// Package store keeps the events the watcher has found: an append-only JSON-lines log on disk, and in
// memory the set of (accession, kind) pairs already recorded plus the most recent events for the API.
package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/otawfik/filings-lake/watcher/internal/edgar"
)

// Event is one detected filing event. A filing that declares both Item 4.01 and Item 4.02 is two events.
type Event struct {
	ID        int64           `json:"id"` // 1, 2, 3… in the order found; the SSE event id
	Kind      string          `json:"kind"`
	Accession string          `json:"accession"`
	Form      string          `json:"form"`
	Filed     string          `json:"filed"`
	Items     []string        `json:"items,omitempty"`
	Companies []edgar.Company `json:"companies"`
	URL       string          `json:"url,omitempty"`
	Source    string          `json:"source"`             // "feed", "header" or "index": where the items were read
	Accepted  *time.Time      `json:"accepted,omitempty"` // EDGAR's acceptance time, when the feed gave one
	SeenAt    time.Time       `json:"seen_at"`
}

type key struct{ accession, kind string }

// Store is safe for concurrent use.
type Store struct {
	mu     sync.RWMutex
	f      *os.File // nil for a memory-only store
	seen   map[key]struct{}
	recent []Event // oldest first, at most keep
	keep   int
	nextID int64
	counts map[string]int
}

// Open loads the log at path (creating it and its directory if needed) and keeps the last `keep`
// events in memory. A final line cut short by a crash is dropped and the file truncated to the last
// whole event; a damaged line anywhere else is an error, because skipping it would hide data loss.
// An empty path gives a memory-only store.
func Open(path string, keep int) (*Store, error) {
	if keep <= 0 {
		keep = 1000
	}
	s := &Store{seen: map[key]struct{}{}, keep: keep, nextID: 1, counts: map[string]int{}}
	if path == "" {
		return s, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	good, err := s.load(f)
	if err == nil {
		err = f.Truncate(good)
	}
	if err == nil {
		_, err = f.Seek(good, io.SeekStart)
	}
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	s.f = f
	return s, nil
}

// load replays the log and returns the offset just past the last whole event.
func (s *Store) load(f *os.File) (good int64, err error) {
	r := bufio.NewReaderSize(f, 1<<20)
	for n := 1; ; n++ {
		line, rerr := r.ReadBytes('\n')
		if rerr != nil && rerr != io.EOF {
			return 0, rerr
		}
		whole := rerr == nil
		if len(bytes.TrimSpace(line)) > 0 {
			var ev Event
			if jerr := json.Unmarshal(line, &ev); jerr != nil || ev.Accession == "" || ev.Kind == "" || !whole {
				if !whole || isLast(r) {
					return good, nil // a torn last write: drop it
				}
				return 0, fmt.Errorf("line %d is damaged: %v", n, jerr)
			}
			s.remember(ev)
		}
		good += int64(len(line))
		if !whole {
			return good, nil
		}
	}
}

func isLast(r *bufio.Reader) bool {
	_, err := r.Peek(1)
	return errors.Is(err, io.EOF)
}

func (s *Store) remember(ev Event) {
	s.seen[key{ev.Accession, ev.Kind}] = struct{}{}
	s.counts[ev.Kind]++
	if ev.ID >= s.nextID {
		s.nextID = ev.ID + 1
	}
	s.recent = append(s.recent, ev)
	if len(s.recent) > 2*s.keep { // trim in batches so appends stay cheap
		s.recent = append([]Event(nil), s.recent[len(s.recent)-s.keep:]...)
	}
}

// Has reports whether this filing's event of this kind is already recorded.
func (s *Store) Has(accession, kind string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.seen[key{accession, kind}]
	return ok
}

// Add records an event unless (accession, kind) is already there. It assigns the ID, writes the line
// and syncs it to disk before returning, so an event the API has shown is never lost to a crash.
// added is false for a duplicate.
func (s *Store) Add(ev Event) (stored Event, added bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.seen[key{ev.Accession, ev.Kind}]; dup {
		return Event{}, false, nil
	}
	ev.ID = s.nextID
	if s.f != nil {
		line, err := json.Marshal(ev)
		if err != nil {
			return Event{}, false, err
		}
		if _, err := s.f.Write(append(line, '\n')); err != nil {
			return Event{}, false, err
		}
		if err := s.f.Sync(); err != nil {
			return Event{}, false, err
		}
	}
	s.remember(ev)
	return ev, true, nil
}

// Query selects events held in memory.
type Query struct {
	Kind    string // "" for every kind
	AfterID int64  // only events with a larger ID
	Limit   int    // 0 for no limit
	Newest  bool   // newest first, and Limit keeps the newest; otherwise oldest first, keeping the oldest
}

// List returns the events matching q. It only sees the last `keep` events; the log has all of them.
func (s *Store) List(q Query) []Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	win := s.recent
	if len(win) > s.keep {
		win = win[len(win)-s.keep:]
	}
	out := []Event{}
	for i := range win {
		ev := win[i]
		if q.Newest {
			ev = win[len(win)-1-i]
		}
		if ev.ID <= q.AfterID || (q.Kind != "" && ev.Kind != q.Kind) {
			continue
		}
		out = append(out, ev)
		if q.Limit > 0 && len(out) == q.Limit {
			break
		}
	}
	return out
}

// Counts returns how many events of each kind are recorded, and the ID of the newest.
func (s *Store) Counts() (byKind map[string]int, lastID int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byKind = make(map[string]int, len(s.counts))
	for k, v := range s.counts {
		byKind[k] = v
	}
	return byKind, s.nextID - 1
}

// Close closes the log.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}
