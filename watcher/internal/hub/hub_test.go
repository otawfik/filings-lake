package hub

import (
	"sync"
	"testing"
	"time"

	"github.com/otawfik/filings-lake/watcher/internal/store"
)

func TestEverySubscriberGetsEveryEventInOrder(t *testing.T) {
	h := New(16)
	a, b := h.Subscribe(), h.Subscribe()
	for i := int64(1); i <= 10; i++ {
		h.Publish(store.Event{ID: i})
	}
	for _, s := range []*Sub{a, b} {
		for want := int64(1); want <= 10; want++ {
			if got := (<-s.C).ID; got != want {
				t.Fatalf("got event %d, want %d", got, want)
			}
		}
	}
}

func TestASlowSubscriberIsCutOffAndTheOthersAreNot(t *testing.T) {
	h := New(4)
	slow, fast := h.Subscribe(), h.Subscribe()
	var got []int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ev := range fast.C {
			got = append(got, ev.ID)
		}
	}()
	done := make(chan struct{})
	go func() {
		for i := int64(1); i <= 20; i++ {
			h.Publish(store.Event{ID: i})
			time.Sleep(time.Millisecond) // give the fast reader time to keep up
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Publish blocked on a subscriber that was not reading")
	}
	if h.Dropped() != 1 || h.Subscribers() != 1 {
		t.Errorf("dropped %d, %d still subscribed; want 1 and 1", h.Dropped(), h.Subscribers())
	}
	n := 0
	for range slow.C { // the buffered events, then the close
		n++
	}
	if n != 4 {
		t.Errorf("the slow subscriber read %d events before its channel closed, want its buffer of 4", n)
	}
	h.Unsubscribe(fast)
	h.Unsubscribe(fast) // twice is fine
	h.Unsubscribe(slow) // already cut off: fine
	wg.Wait()
	if len(got) != 20 {
		t.Errorf("the fast subscriber got %d of 20 events", len(got))
	}
}

func TestConcurrentSubscribeUnsubscribePublish(t *testing.T) {
	h := New(8)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				s := h.Subscribe()
				h.Publish(store.Event{ID: int64(i)})
				h.Unsubscribe(s)
			}
		}()
	}
	wg.Wait()
	if h.Subscribers() != 0 {
		t.Errorf("%d subscribers left", h.Subscribers())
	}
}
