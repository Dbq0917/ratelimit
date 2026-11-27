package ratelimit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type contextLimiter interface {
	TakeContext(context.Context) (time.Time, error)
}

func contextConstructors(clk Clock, rate int) map[string]Limiter {
	return map[string]Limiter{
		"mutex":        newMutexBased(rate, WithClock(clk)),
		"atomic":       newAtomicBased(rate, WithClock(clk)),
		"atomic_int64": newAtomicInt64Based(rate, WithClock(clk)),
	}
}

// TestTakeContextAlreadyCanceled verifies that an already-canceled context
// makes TakeContext return immediately without touching the state. A
// subsequent call still receives the very first (zero-delay) permission.
func TestTakeContextAlreadyCanceled(t *testing.T) {
	t.Parallel()
	clk := clock.NewMock()
	clk.Set(time.Now())

	for name, rl := range contextConstructors(clk, 100) {
		t.Run(name, func(t *testing.T) {
			var _ contextLimiter = rl

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			start := time.Now()
			_, err := rl.TakeContext(ctx)
			assert.True(t, errors.Is(err, context.Canceled))
			assert.True(t, time.Since(start) < 50*time.Millisecond,
				"canceled call must return immediately")

			// The canceled call reserved nothing: this is still the first
			// permission and must complete without advancing the mock clock.
			done := make(chan struct{})
			go func() {
				_, err := rl.TakeContext(context.Background())
				assert.NoError(t, err)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("first real permission should not be blocked by the canceled call")
			}
		})
	}
}

// TestTakeContextCanceledRollsBack verifies deterministically that a canceled
// call leaves no reservation behind. The context is canceled before the call,
// so a slot is computed but the wait returns immediately and the reservation
// must be rolled back. The next successful permission has to land on the slot
// the canceled call would have taken, not one per-request interval later.
func TestTakeContextCanceledRollsBack(t *testing.T) {
	t.Parallel()

	for _, opts := range [][]Option{{WithoutSlack}, {WithSlack(10)}} {
		t.Run("", func(t *testing.T) {
			for name, makeLimiter := range map[string]func(Clock) Limiter{
				"mutex":        func(c Clock) Limiter { return newMutexBased(100, append(opts, WithClock(c))...) },
				"atomic":       func(c Clock) Limiter { return newAtomicBased(100, append(opts, WithClock(c))...) },
				"atomic_int64": func(c Clock) Limiter { return newAtomicInt64Based(100, append(opts, WithClock(c))...) },
			} {
				t.Run(name, func(t *testing.T) {
					clk := clock.NewMock()
					clk.Set(time.Now())
					rl := makeLimiter(clk)
					perRequest := 10 * time.Millisecond

					// First permission: issued at now, no sleep.
					first, err := rl.TakeContext(context.Background())
					require.NoError(t, err)

					// A second call whose context is already canceled: it computes
					// a reservation but must undo it.
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					_, err = rl.TakeContext(ctx)
					assert.True(t, errors.Is(err, context.Canceled))

					// The next successful permission must reuse the released slot.
					nextDone := make(chan time.Time, 1)
					go func() {
						ts, err := rl.TakeContext(context.Background())
						require.NoError(t, err)
						nextDone <- ts
					}()
					waitUntilBlocked(t, clk, nextDone)

					// One per-request interval must be enough. Without rollback the
					// replacement would need two intervals because the canceled
					// call had consumed a slot.
					clk.Add(perRequest)
					select {
					case next := <-nextDone:
						assert.Equal(t, first.Add(perRequest), next,
							"the canceled request must not push later callers out")
					case <-time.After(time.Second):
						t.Fatal("rolled-back slot was not reused after one interval")
					}
				})
			}
		})
	}
}

// TestTakeContextCanceledDuringSleep verifies on a real clock that canceling a
// blocked TakeContext unblocks it immediately with the context error instead
// of waiting for the full sleep to elapse.
func TestTakeContextCanceledDuringSleep(t *testing.T) {
	t.Parallel()
	clk := clock.New()
	for name, rl := range contextConstructors(clk, 10) {
		t.Run(name, func(t *testing.T) {
			// Prime the limiter so subsequent permissions are in the future.
			_, err := rl.TakeContext(context.Background())
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				_, err := rl.TakeContext(ctx)
				done <- err
			}()

			time.Sleep(20 * time.Millisecond)
			cancel()

			select {
			case err := <-done:
				assert.True(t, errors.Is(err, context.Canceled))
			case <-time.After(time.Second):
				t.Fatal("canceled TakeContext did not return promptly")
			}
		})
	}
}

// TestTakeContextNoLeakUnderContention mixes canceled and successful calls to
// make sure concurrent rollbacks never corrupt the shared state or deadlock.
func TestTakeContextNoLeakUnderContention(t *testing.T) {
	t.Parallel()
	// A very high rate so non-canceled calls generally do not sleep.
	clk := clock.New()

	for name, rl := range contextConstructors(clk, 1_000_000_000) {
		t.Run(name, func(t *testing.T) {
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func(loop int) {
					defer wg.Done()
					for j := 0; j < 200; j++ {
						ctx, cancel := context.WithCancel(context.Background())
						if (loop+j)%2 == 0 {
							cancel()
						}
						_, _ = rl.TakeContext(ctx)
						cancel()
					}
				}(i)
			}
			wg.Wait()
		})
	}
}

// TestTakeContextSuccess verifies the happy path mirrors Take when the context
// stays alive and the clock advances enough to avoid blocking.
func TestTakeContextSuccess(t *testing.T) {
	t.Parallel()
	clk := clock.NewMock()
	clk.Set(time.Now())

	for name, rl := range contextConstructors(clk, 100) {
		t.Run(name, func(t *testing.T) {
			ts, err := rl.TakeContext(context.Background())
			require.NoError(t, err)
			assert.False(t, ts.IsZero())
		})
	}
}

// TestTakeContextNoAllocHotPath ensures the non-sleeping context-aware entry
// adds no allocation beyond Take itself, keeping the hot path as cheap as the
// existing entry. The unsafe.Pointer implementation already allocates once in
// Take (it boxes the state struct), and TakeContext must not make that worse.
func TestTakeContextNoAllocHotPath(t *testing.T) {
	// Real clock with a huge rate: every permission is already due, so the
	// non-sleeping hot path is exercised without any blocking.
	clk := clock.New()

	for name, rl := range map[string]Limiter{
		"mutex":        newMutexBased(1_000_000_000, WithClock(clk)),
		"atomic":       newAtomicBased(1_000_000_000, WithClock(clk)),
		"atomic_int64": newAtomicInt64Based(1_000_000_000, WithClock(clk)),
	} {
		t.Run(name, func(t *testing.T) {
			takeAllocs := testing.AllocsPerRun(100, func() {
				rl.Take()
			})
			ctxAllocs := testing.AllocsPerRun(100, func() {
				_, err := rl.TakeContext(context.Background())
				require.NoError(t, err)
			})
			assert.True(t, ctxAllocs <= takeAllocs,
				"TakeContext (%v allocs) must not allocate more than Take (%v allocs)",
				ctxAllocs, takeAllocs)
		})
	}
}

// waitUntilBlocked polls until the given result channel is still empty, meaning
// the TakeContext goroutine has reserved its permission and is now parked on a
// mock clock timer. It fails the test if the goroutine already returned.
func waitUntilBlocked(t *testing.T, clk *clock.Mock, done <-chan time.Time) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		clk.Add(0)
		select {
		case <-done:
			t.Fatal("TakeContext returned before it was expected to block")
		default:
			return
		}
	}
	t.Fatal("timed out waiting for TakeContext to block")
}
