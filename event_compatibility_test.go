package discordgo

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOptionalEventHandler(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("an unset optional callback must not panic: %v", r)
		}
	}()
	s := &Session{}
	s.handleEvent(connectEventType, &Connect{})
}

func TestCustomEventHandlerOverridesLegacy(t *testing.T) {
	var custom, legacy int
	s := &Session{SyncEvents: true}
	s.EventHandler = func(event any) {
		if _, ok := event.(*Connect); !ok {
			t.Fatalf("unexpected event %T", event)
		}
		custom++
	}
	s.AddHandler(func(_ *Session, _ *Connect) { legacy++ })
	s.handleEvent(connectEventType, &Connect{})
	if custom != 1 || legacy != 0 {
		t.Fatalf("custom=%d legacy=%d; custom dispatch must remain exclusive", custom, legacy)
	}
}

func TestLegacyHandlerCanRemoveItself(t *testing.T) {
	s := &Session{SyncEvents: true}
	calls := 0
	var remove func()
	remove = s.AddHandler(func(_ *Session, _ *Connect) { calls++; remove() })
	done := make(chan struct{})
	go func() {
		s.handleEvent(connectEventType, &Connect{})
		s.handleEvent(connectEventType, &Connect{})
		close(done)
	}()
	select {
	case <-done:
		if calls != 1 {
			t.Fatalf("self-removing handler called %d times", calls)
		}
	case <-time.After(time.Second):
		t.Fatal("synchronous callback deadlocked while removing itself")
	}
}

func TestLegacyOnceHandlerConcurrentEvents(t *testing.T) {
	s := &Session{SyncEvents: true}
	var calls int32
	s.AddHandlerOnce(func(_ *Session, _ *Connect) { atomic.AddInt32(&calls, 1) })
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.handleEvent(connectEventType, &Connect{}) }()
	}
	wg.Wait()
	if calls != 1 {
		t.Fatalf("once handler called %d times", calls)
	}
}
