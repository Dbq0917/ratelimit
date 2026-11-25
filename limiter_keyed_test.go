package ratelimit

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"go.uber.org/atomic"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
)

type keyedTrackingClock struct {
	*clock.Mock

	mu       sync.Mutex
	sleepers int
}

func (c *keyedTrackingClock) Sleep(d time.Duration) {
	c.mu.Lock()
	c.sleepers++
	c.mu.Unlock()

	c.Mock.Sleep(d)

	c.mu.Lock()
	c.sleepers--
	c.mu.Unlock()
}

func (c *keyedTrackingClock) waitForSleepers(n int) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		sleepers := c.sleepers
		c.mu.Unlock()
		if sleepers >= n {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func TestKeyedLimiterIsolatesCallers(t *testing.T) {
	t.Parallel()

	clk := &keyedTrackingClock{Mock: clock.NewMock()}
	clk.Set(time.Now())
	rl := NewKeyed(10, WithClock(clk), WithoutSlack)

	// Both tenants get their first token immediately.
	rl.Take("noisy-tenant")
	rl.Take("quiet-tenant")

	const noisyCalls, quietCalls = 20, 9
	var noisyDone, quietDone atomic.Int32
	var wg sync.WaitGroup
	start := func(key string, calls int, done *atomic.Int32) {
		for i := 0; i < calls; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rl.Take(key)
				done.Inc()
			}()
		}
	}

	start("noisy-tenant", noisyCalls, &noisyDone)
	start("quiet-tenant", quietCalls, &quietDone)

	if !clk.waitForSleepers(noisyCalls + quietCalls) {
		t.Fatal("callers did not start waiting for their per-key rate")
	}

	clk.Add(time.Second)
	if !waitForKeyedCount(&quietDone, quietCalls) {
		t.Fatal("quiet tenant did not receive its full rate")
	}
	if !waitForKeyedCount(&noisyDone, 9) {
		t.Fatal("noisy tenant did not receive its own rate")
	}

	assert.Equal(t, int32(quietCalls), quietDone.Load(), "quiet tenant should retain its own full rate")

	clk.Add(2 * time.Second)
	wg.Wait()
}

func waitForKeyedCount(count *atomic.Int32, want int) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if int(count.Load()) >= want {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func TestKeyedLimiterReclaimsIdleKeys(t *testing.T) {
	t.Parallel()

	clockMock := clock.NewMock()
	clockMock.Set(time.Now())
	rl := NewKeyed(1000, WithClock(clockMock), WithKeyTTL(100*time.Millisecond))

	rl.Take("idle-tenant")
	assert.Equal(t, 1, rl.Len())

	clockMock.Add(200 * time.Millisecond)
	rl.Take("active-tenant")
	assert.Equal(t, 1, rl.Len(), "idle key should be reclaimed")

	clockMock.Add(200 * time.Millisecond)
	assert.Equal(t, 0, rl.Len(), "all idle keys should be reclaimed")
}

func TestKeyedLimiterConcurrentCallers(t *testing.T) {
	t.Parallel()

	rl := NewKeyed(100, WithoutSlack)
	const callers = 100

	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func(i int) {
			defer wg.Done()
			rl.Take(fmt.Sprintf("tenant-%d", i))
		}(i)
	}
	wg.Wait()

	assert.Equal(t, callers, rl.Len())
}
