// Copyright (c) 2022 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package ratelimit // import "go.uber.org/ratelimit"

import (
	"context"
	"sync/atomic"
	"time"
)

type atomicInt64Limiter struct {
	//lint:ignore U1000 Padding is unused but it is crucial to maintain performance
	// of this rate limiter in case of collocation with other frequently accessed memory.
	prepadding [64]byte // cache line size = 64; created to avoid false sharing.
	state      int64    // unix nanoseconds of the next permissions issue.
	//lint:ignore U1000 like prepadding.
	postpadding [56]byte // cache line size - state size = 64 - 8; created to avoid false sharing.

	perRequest time.Duration
	maxSlack   time.Duration
	clock      Clock
}

// newAtomicBased returns a new atomic based limiter.
func newAtomicInt64Based(rate int, opts ...Option) *atomicInt64Limiter {
	// TODO consider moving config building to the implementation
	// independent code.
	config := buildConfig(opts)
	perRequest := config.per / time.Duration(rate)
	l := &atomicInt64Limiter{
		perRequest: perRequest,
		maxSlack:   time.Duration(config.slack) * perRequest,
		clock:      config.clock,
	}
	atomic.StoreInt64(&l.state, 0)
	return l
}

// Take blocks to ensure that the time spent between multiple
// Take calls is on average time.Second/rate.
func (t *atomicInt64Limiter) Take() time.Time {
	newTimeOfNextPermissionIssue, now, _ := t.reserve()
	sleepDuration := time.Duration(newTimeOfNextPermissionIssue - now)
	if sleepDuration > 0 {
		t.clock.Sleep(sleepDuration)
		return time.Unix(0, newTimeOfNextPermissionIssue)
	}
	// return now if we don't sleep as atomicLimiter does
	return time.Unix(0, now)
}

// TakeContext blocks to ensure that the RPS is met, unless ctx is canceled.
// A canceled call returns immediately with the context error and removes the
// permission it had reserved, so it consumes no budget and does not make
// later callers wait an extra round.
func (t *atomicInt64Limiter) TakeContext(ctx context.Context) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}

	newTimeOfNextPermissionIssue, now, previousTimeOfNextPermissionIssue := t.reserve()
	sleepDuration := time.Duration(newTimeOfNextPermissionIssue - now)
	if sleepDuration <= 0 {
		return time.Unix(0, now), nil
	}

	if err := sleepContext(ctx, t.clock, sleepDuration); err != nil {
		t.rollback(previousTimeOfNextPermissionIssue, newTimeOfNextPermissionIssue)
		return time.Time{}, err
	}
	return time.Unix(0, newTimeOfNextPermissionIssue), nil
}

// reserve claims the next permission, returning the issue time of that
// permission and the time at which the reservation happened.
func (t *atomicInt64Limiter) reserve() (newTimeOfNextPermissionIssue int64, now int64, timeOfNextPermissionIssue int64) {
	for {
		now = t.clock.Now().UnixNano()
		timeOfNextPermissionIssue = atomic.LoadInt64(&t.state)

		switch {
		case timeOfNextPermissionIssue == 0 || (t.maxSlack == 0 && now-timeOfNextPermissionIssue > int64(t.perRequest)):
			// if this is our first call or t.maxSlack == 0 we need to shrink issue time to now
			newTimeOfNextPermissionIssue = now
		case t.maxSlack > 0 && now-timeOfNextPermissionIssue > int64(t.maxSlack)+int64(t.perRequest):
			// a lot of nanoseconds passed since the last Take call
			// we will limit max accumulated time to maxSlack
			newTimeOfNextPermissionIssue = now - int64(t.maxSlack)
		default:
			// calculate the time at which our permission was issued
			newTimeOfNextPermissionIssue = timeOfNextPermissionIssue + int64(t.perRequest)
		}

		if atomic.CompareAndSwapInt64(&t.state, timeOfNextPermissionIssue, newTimeOfNextPermissionIssue) {
			return newTimeOfNextPermissionIssue, now, timeOfNextPermissionIssue
		}
	}
}

// rollback restores the previous issue time if it is still the value that
// reserve installed. If other goroutines have already moved the state forward,
// the state is left untouched rather than corrupting their reservations.
func (t *atomicInt64Limiter) rollback(previousTimeOfNextPermissionIssue, newTimeOfNextPermissionIssue int64) {
	atomic.CompareAndSwapInt64(&t.state, newTimeOfNextPermissionIssue, previousTimeOfNextPermissionIssue)
}
