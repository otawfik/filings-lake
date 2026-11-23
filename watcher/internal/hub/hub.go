// Package hub fans new events out to the clients connected to the stream.
package hub

import (
	"sync"
	"sync/atomic"

	"github.com/otawfik/filings-lake/watcher/internal/store"
)

// Hub never blocks the publisher. Each subscriber has a buffered channel; one that falls a whole
// buffer behind is cut off (its channel is closed) and counted. A cut-off client reconnects with the
// ID of the last event it saw and is caught up from the store, so nothing is lost, and one stalled
// connection cannot hold up the poller or the other clients.
type Hub struct {
	mu      sync.Mutex
	subs    map[*Sub]struct{}
	buffer  int
	dropped atomic.Int64
}

// Sub is one subscription. Receive from C until it is closed.
type Sub struct {
	C  <-chan store.Event
	ch chan store.Event
}

func New(buffer int) *Hub {
	if buffer <= 0 {
		buffer = 64
	}
	return &Hub{subs: map[*Sub]struct{}{}, buffer: buffer}
}

func (h *Hub) Subscribe() *Sub {
	ch := make(chan store.Event, h.buffer)
	s := &Sub{C: ch, ch: ch}
	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	return s
}

// Unsubscribe removes s and closes its channel. Calling it twice, or after the hub cut s off, is fine.
func (h *Hub) Unsubscribe(s *Sub) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subs[s]; ok {
		delete(h.subs, s)
		close(s.ch)
	}
}

// Publish hands ev to every subscriber with room for it and cuts off the rest.
func (h *Hub) Publish(ev store.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for s := range h.subs {
		select {
		case s.ch <- ev:
		default:
			delete(h.subs, s)
			close(s.ch)
			h.dropped.Add(1)
		}
	}
}

// Subscribers is the number of live subscriptions; Dropped the number cut off for being slow.
func (h *Hub) Subscribers() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

func (h *Hub) Dropped() int64 { return h.dropped.Load() }
