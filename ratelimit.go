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

	"github.com/benbjohnson/clock"
)

// Note: This file is inspired by:
// https://github.com/prashantv/go-bench/blob/master/ratelimit

// Limiter is used to rate-limit some process, possibly across goroutines.
// The process is expected to call Take() before every iteration, which
// may block to throttle the goroutine.
type Limiter interface {
	// Take should block to make sure that the RPS is met.
	Take() time.Time

	// TakeContext blocks to make sure that the RPS is met, unless ctx is
	// canceled first. On cancellation it returns immediately with the context
	// error without consuming any of the rate limit budget: no permission is
	// reserved, so later callers are not delayed by the canceled request.
	TakeContext(ctx context.Context) (time.Time, error)
}

// Clock is the minimum necessary interface to instantiate a rate limiter with
// a clock or mock clock, compatible with clocks created using
// github.com/andres-erbsen/clock.
type Clock interface {
	Now() time.Time
	Sleep(time.Duration)
}

// afterClock is an optional capability of a Clock. The real clock and the
// benbjohnson mock clock both implement After, which lets a wait be driven by
// the (possibly mock) clock while remaining interruptible by a context.
type afterClock interface {
	After(time.Duration) <-chan time.Time
}

// sleepContext waits for d on the given clock, or until ctx is canceled.
// It returns nil when the full duration elapsed and the context error
// otherwise. No timer is created when there is nothing to wait for, so the
// non-sleeping hot path stays allocation free.
func sleepContext(ctx context.Context, c Clock, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if ac, ok := c.(afterClock); ok {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ac.After(d):
			return nil
		}
	}

	// Fall back to an uninterruptible sleep for Clock implementations that do
	// not expose After. The context is still observed once the sleep returns.
	c.Sleep(d)
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// config configures a limiter.
type config struct {
	clock Clock
	slack int
	per   time.Duration
}

// New returns a Limiter that will limit to the given RPS.
func New(rate int, opts ...Option) Limiter {
	return newAtomicInt64Based(rate, opts...)
}

// buildConfig combines defaults with options.
func buildConfig(opts []Option) config {
	c := config{
		clock: clock.New(),
		slack: 10,
		per:   time.Second,
	}

	for _, opt := range opts {
		opt.apply(&c)
	}
	return c
}

// Option configures a Limiter.
type Option interface {
	apply(*config)
}

type clockOption struct {
	clock Clock
}

func (o clockOption) apply(c *config) {
	c.clock = o.clock
}

// WithClock returns an option for ratelimit.New that provides an alternate
// Clock implementation, typically a mock Clock for testing.
func WithClock(clock Clock) Option {
	return clockOption{clock: clock}
}

type slackOption int

func (o slackOption) apply(c *config) {
	c.slack = int(o)
}

// WithoutSlack configures the limiter to be strict and not to accumulate
// previously "unspent" requests for future bursts of traffic.
var WithoutSlack Option = slackOption(0)

// WithSlack configures custom slack.
// Slack allows the limiter to accumulate "unspent" requests
// for future bursts of traffic.
func WithSlack(slack int) Option {
	return slackOption(slack)
}

type perOption time.Duration

func (p perOption) apply(c *config) {
	c.per = time.Duration(p)
}

// Per allows configuring limits for different time windows.
//
// The default window is one second, so New(100) produces a one hundred per
// second (100 Hz) rate limiter.
//
// New(2, Per(60*time.Second)) creates a 2 per minute rate limiter.
func Per(per time.Duration) Option {
	return perOption(per)
}

type unlimited struct{}

// NewUnlimited returns a RateLimiter that is not limited.
func NewUnlimited() Limiter {
	return unlimited{}
}

func (unlimited) Take() time.Time {
	return time.Now()
}

func (unlimited) TakeContext(ctx context.Context) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	return time.Now(), nil
}
