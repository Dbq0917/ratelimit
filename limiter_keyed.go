package ratelimit // import "go.uber.org/ratelimit"

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	keyedFNVOffset64 = 14695981039346656037
	keyedFNVPrime64  = 1099511628211
)

// KeyedLimiter is used to rate-limit processes independently for each caller.
// Each key has its own rate and accumulated slack; activity for one key does
// not consume capacity belonging to another key.
type KeyedLimiter interface {
	// Take blocks until the caller identified by key is allowed to proceed.
	Take(key string) time.Time

	// Len returns the number of keys currently tracked by the limiter.
	Len() int
}

// NewKeyed returns a KeyedLimiter that limits each key independently to the
// given rate per window. Global Limiter options, including Per, WithSlack,
// WithoutSlack, and WithClock, apply to every key's private limiter.
func NewKeyed(rate int, opts ...Option) KeyedLimiter {
	return newKeyed(rate, opts...)
}

type keyedLimiter struct {
	shards     []keyedLimiterShard
	clock      Clock
	perRequest time.Duration
	maxSlack   time.Duration
	keyTTL     time.Duration

	cleanupInterval time.Duration
	nextCleanup     int64
}

type keyedLimiterShard struct {
	mu      sync.Mutex
	entries map[string]*keyedLimiterEntry
}

type keyedLimiterEntry struct {
	limiter  *atomicInt64Limiter
	lastUsed int64
	inFlight int64
}

func newKeyed(rate int, opts ...Option) *keyedLimiter {
	config := buildConfig(opts)

	shards := config.keyShards
	if shards < 1 {
		shards = 1
	}

	perRequest := config.per / time.Duration(rate)
	keyTTL := config.keyTTL
	if keyTTL > 0 && keyTTL < perRequest {
		// Reclaiming a key sooner than one request interval could recreate its
		// bucket while the old bucket would still have been cooling down.
		keyTTL = perRequest
	}

	l := &keyedLimiter{
		shards:     make([]keyedLimiterShard, shards),
		clock:      config.clock,
		perRequest: perRequest,
		maxSlack:   time.Duration(config.slack) * perRequest,
		keyTTL:     keyTTL,
	}
	for i := range l.shards {
		l.shards[i].entries = make(map[string]*keyedLimiterEntry)
	}

	if l.keyTTL > 0 {
		l.cleanupInterval = l.keyTTL / 2
		if l.cleanupInterval <= 0 {
			l.cleanupInterval = l.keyTTL
		}
		if l.cleanupInterval > time.Minute {
			l.cleanupInterval = time.Minute
		}
		l.nextCleanup = l.clock.Now().Add(l.cleanupInterval).UnixNano()
	}

	return l
}

// Take blocks to ensure that calls for key are spaced according to the
// configured rate. Keys are lazily created and share no bucket state.
func (l *keyedLimiter) Take(key string) time.Time {
	now := l.clock.Now()
	l.cleanupIfDue(now)

	shard := &l.shards[keyShard(key, len(l.shards))]
	shard.mu.Lock()
	entry := shard.entries[key]
	if entry == nil {
		entry = &keyedLimiterEntry{
			limiter: &atomicInt64Limiter{
				perRequest: l.perRequest,
				maxSlack:   l.maxSlack,
				clock:      l.clock,
			},
		}
		shard.entries[key] = entry
	}
	atomic.AddInt64(&entry.inFlight, 1)
	atomic.StoreInt64(&entry.lastUsed, now.UnixNano())
	shard.mu.Unlock()

	taken := entry.limiter.Take()

	atomic.StoreInt64(&entry.lastUsed, l.clock.Now().UnixNano())
	atomic.AddInt64(&entry.inFlight, -1)
	return taken
}

// Len returns the number of live keys after reclaiming idle entries.
func (l *keyedLimiter) Len() int {
	now := l.clock.Now().UnixNano()
	l.cleanup(now)

	var n int
	for i := range l.shards {
		shard := &l.shards[i]
		shard.mu.Lock()
		n += len(shard.entries)
		shard.mu.Unlock()
	}
	return n
}

func (l *keyedLimiter) cleanupIfDue(now time.Time) {
	if l.keyTTL <= 0 {
		return
	}

	nowNanos := now.UnixNano()
	for {
		next := atomic.LoadInt64(&l.nextCleanup)
		if nowNanos < next {
			return
		}
		if atomic.CompareAndSwapInt64(&l.nextCleanup, next, nowNanos+int64(l.cleanupInterval)) {
			l.cleanup(nowNanos)
			return
		}
	}
}

func (l *keyedLimiter) cleanup(now int64) {
	if l.keyTTL <= 0 {
		return
	}

	ttl := int64(l.keyTTL)
	for i := range l.shards {
		shard := &l.shards[i]
		shard.mu.Lock()
		for key, entry := range shard.entries {
			if atomic.LoadInt64(&entry.inFlight) == 0 &&
				now-atomic.LoadInt64(&entry.lastUsed) > ttl {
				delete(shard.entries, key)
			}
		}
		shard.mu.Unlock()
	}
}

func keyShard(key string, shards int) int {
	h := uint64(keyedFNVOffset64)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= keyedFNVPrime64
	}
	return int(h % uint64(shards))
}
