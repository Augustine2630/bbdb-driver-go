package bbdb

import (
	"testing"
	"time"
)

func TestBackoffDuration(t *testing.T) {
	b := newBackoff(100*time.Millisecond, 30*time.Second, 2.0)
	d0 := b.next()
	if d0 < 100*time.Millisecond || d0 > 150*time.Millisecond {
		t.Fatalf("first backoff: expected ~100ms, got %v", d0)
	}
	d1 := b.next()
	if d1 < 200*time.Millisecond || d1 > 300*time.Millisecond {
		t.Fatalf("second backoff: expected ~200ms, got %v", d1)
	}
	for i := 0; i < 20; i++ {
		b.next()
	}
	dMax := b.next()
	if dMax > 31*time.Second {
		t.Fatalf("backoff exceeded max: %v", dMax)
	}
}

func TestBackoffReset(t *testing.T) {
	b := newBackoff(100*time.Millisecond, 30*time.Second, 2.0)
	b.next()
	b.next()
	b.reset()
	d := b.next()
	if d < 100*time.Millisecond || d > 150*time.Millisecond {
		t.Fatalf("after reset: expected ~100ms, got %v", d)
	}
}
