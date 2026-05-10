// package group provides a way to manage the lifecycle of a group of goroutines.
package group

import (
	"context"
	"fmt"
	"sync"
)

// G manages the lifetime of a set of goroutines from a common context.
// The first goroutine in the group to return will cause the context to be canceled,
// terminating the remaining goroutines.
type G struct {
	// ctx is the context passed to all goroutines in the group.
	ctx    context.Context
	cancel context.CancelFunc
	done   sync.WaitGroup

	initOnce sync.Once

	errOnce sync.Once
	err     error

	// sem is a semaphore for limiting concurrent goroutines
	sem chan struct{}
}

type Option func(*G)

// WithContext uses the provided context for the group.
func WithContext(ctx context.Context) Option {
	return func(g *G) {
		g.ctx = ctx
	}
}

// WithMaxConcurrency sets the maximum number of concurrent goroutines.
// If n <= 0, no limit is applied.
func WithMaxConcurrency(n int) Option {
	return func(g *G) {
		if n > 0 {
			g.sem = make(chan struct{}, n)
		}
	}
}

// New creates a new group.
func New(opts ...Option) *G {
	g := new(G)
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// init initializes the group.
func (g *G) init() {
	if g.ctx == nil {
		g.ctx = context.Background()
	}
	g.ctx, g.cancel = context.WithCancel(g.ctx)

	// If semaphore is not set, create one with large capacity (no practical limit)
	if g.sem == nil {
		g.sem = make(chan struct{}, 1000000)
	}
}

// Add adds a new goroutine to the group. The goroutine should exit when the context
// passed to it is canceled.
func (g *G) Add(fn func(context.Context) error) {
	g.initOnce.Do(g.init)

	// Acquire semaphore slot, blocking if limit is reached
	g.sem <- struct{}{}

	g.done.Go(func() {
		defer func() { <-g.sem }()
		defer g.cancel()
		defer func() {
			if r := recover(); r != nil {
				g.errOnce.Do(func() {
					if err, ok := r.(error); ok {
						g.err = err
					} else {
						g.err = fmt.Errorf("panic: %v", r)
					}
				})
			}
		}()
		if err := fn(g.ctx); err != nil {
			g.errOnce.Do(func() { g.err = err })
		}
	})
}

// Wait waits for all goroutines in the group to exit.
// If any of the goroutines fail with an error, Wait will return the first error.
func (g *G) Wait() error {
	g.done.Wait()
	g.errOnce.Do(func() {
		// noop, required to synchronise on the errOnce mutex.
	})
	return g.err
}
