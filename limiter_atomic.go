// Copyright (c) 2016,2020 Uber Technologies, Inc.
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
	"time"

	"sync/atomic"
	"unsafe"
)

type state struct {
	last     time.Time
	sleepFor time.Duration
}

type atomicLimiter struct {
	state unsafe.Pointer
	//lint:ignore U1000 Padding is unused but it is crucial to maintain performance
	// of this rate limiter in case of collocation with other frequently accessed memory.
	padding [56]byte // cache line size - state pointer size = 64 - 8; created to avoid false sharing.

	perRequest time.Duration
	maxSlack   time.Duration
	clock      Clock
}

// newAtomicBased returns a new atomic based limiter.
func newAtomicBased(rate int, opts ...Option) *atomicLimiter {
	// TODO consider moving config building to the implementation
	// independent code.
	config := buildConfig(opts)
	perRequest := config.per / time.Duration(rate)
	l := &atomicLimiter{
		perRequest: perRequest,
		maxSlack:   -1 * time.Duration(config.slack) * perRequest,
		clock:      config.clock,
	}

	initialState := state{
		last:     time.Time{},
		sleepFor: 0,
	}
	atomic.StorePointer(&l.state, unsafe.Pointer(&initialState))
	return l
}

// Take blocks to ensure that the time spent between multiple
// Take calls is on average per/rate.
func (t *atomicLimiter) Take() time.Time {
	newState, _, interval := t.reserve()
	t.clock.Sleep(interval)
	return newState.last
}

// TakeContext blocks to ensure that the RPS is met, unless ctx is canceled.
// A canceled call returns immediately with the context error and removes the
// permission it had reserved, so it consumes no budget and does not make
// later callers wait an extra round.
func (t *atomicLimiter) TakeContext(ctx context.Context) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}

	newState, installedPointer, interval := t.reserve()
	if interval <= 0 {
		return newState.last, nil
	}

	if err := sleepContext(ctx, t.clock, interval); err != nil {
		t.rollback(installedPointer, newState)
		return time.Time{}, err
	}
	return newState.last, nil
}

// reserve claims the next permission, returning the resulting state, the
// pointer actually installed in the state (needed to roll back) and how long
// to sleep.
func (t *atomicLimiter) reserve() (state, unsafe.Pointer, time.Duration) {
	var (
		newState         state
		taken            bool
		interval         time.Duration
		installedPointer unsafe.Pointer
	)
	for !taken {
		now := t.clock.Now()

		previousStatePointer := atomic.LoadPointer(&t.state)
		oldState := (*state)(previousStatePointer)

		newState = state{
			last:     now,
			sleepFor: oldState.sleepFor,
		}

		// If this is our first request, then we allow it.
		if oldState.last.IsZero() {
			taken = atomic.CompareAndSwapPointer(&t.state, previousStatePointer, unsafe.Pointer(&newState))
			continue
		}

		// sleepFor calculates how much time we should sleep based on
		// the perRequest budget and how long the last request took.
		// Since the request may take longer than the budget, this number
		// can get negative, and is summed across requests.
		newState.sleepFor += t.perRequest - now.Sub(oldState.last)
		// We shouldn't allow sleepFor to get too negative, since it would mean that
		// a service that slowed down a lot for a short period of time would get
		// a much higher RPS following that.
		if newState.sleepFor < t.maxSlack {
			newState.sleepFor = t.maxSlack
		}
		if newState.sleepFor > 0 {
			newState.last = newState.last.Add(newState.sleepFor)
			interval, newState.sleepFor = newState.sleepFor, 0
		}
		installedPointer = unsafe.Pointer(&newState)
		taken = atomic.CompareAndSwapPointer(&t.state, previousStatePointer, installedPointer)
	}
	return newState, installedPointer, interval
}

// rollback cancels the reservation installed by reserve if it is still the
// current state. The state is rewound to "a permission was just issued", using
// the scheduled issue time of the canceled permission as the reference point,
// so the next caller reuses that exact slot rather than waiting an additional
// interval. If other goroutines already moved the state forward, it is left
// untouched to avoid corrupting their reservations.
func (t *atomicLimiter) rollback(installedPointer unsafe.Pointer, newState state) {
	rolledBack := state{
		last:     newState.last,
		sleepFor: 0,
	}
	atomic.CompareAndSwapPointer(&t.state, installedPointer, unsafe.Pointer(&rolledBack))
}
