package group_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/group"
)

type Group = group.G

func ExampleGroup_Wait() {
	// A Group's zero value is ready to use.
	var g group.G

	// Add a goroutine to the group.
	g.Add(func(c context.Context) error {
		select {
		case <-c.Done():
			return c.Err()
		case <-time.After(1 * time.Second):
			return errors.New("timed out")
		}
	})

	// Wait for all goroutines to finish.
	if err := g.Wait(); err != nil {
		fmt.Println(err)
	}

	// Output: timed out
}

func ExampleGroup_Wait_with_startup_error() {
	// A Group's zero value is ready to use.
	var g group.G

	// Add a goroutine to the group.
	g.Add(func(_ context.Context) error {
		return errors.New("startup error")
	})

	// Wait for all goroutines to finish, in this case it will return the startup error.
	if err := g.Wait(); err != nil {
		fmt.Println(err)
	}

	// Output: startup error
}

func ExampleGroup_Wait_with_panic() {
	// A Group's zero value is ready to use.
	var g group.G

	// Add a goroutine to the group.
	g.Add(func(c context.Context) error {
		panic("boom")
	})

	// Wait for all goroutines to finish.
	if err := g.Wait(); err != nil {
		fmt.Println(err)
	}

	// Output: panic: boom
}

func ExampleGroup_Wait_with_shutdown() {
	// A Group's zero value is ready to use.
	var g group.G

	shutdown := make(chan struct{})

	// Add a goroutine to the group.
	g.Add(func(c context.Context) error {
		select {
		case <-c.Done():
			return errors.New("stopped")
		case <-shutdown:
			return errors.New("shutdown")
		}
	})

	time.AfterFunc(100*time.Millisecond, func() {
		close(shutdown)
	})

	// Wait for all goroutines to finish.
	if err := g.Wait(); err != nil {
		fmt.Println(err)
	}

	// Output: shutdown
}

func ExampleGroup_Wait_with_context_cancel() {
	ctx := context.Background()
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(100*time.Millisecond))

	// pass WithContext option to New to use the provided context.
	g := group.New(group.WithContext(ctx))

	// Add a goroutine to the group.
	g.Add(func(c context.Context) error {
		select {
		case <-c.Done():
			return c.Err()
		}
	})

	// Cancel the context.
	cancel()

	// Wait for all goroutines to finish.
	if err := g.Wait(); err != nil {
		fmt.Println(err)
	}

	// Output: context canceled
}

func ExampleGroup_Wait_with_signal() {
	ctx := context.Background()
	ctx, _ = signal.NotifyContext(ctx, os.Interrupt)

	g := group.New(group.WithContext(ctx))

	g.Add(MainHTTPServer)
	g.Add(DebugHTTPServer)
	g.Add(AsyncLogger)

	<-time.After(100 * time.Millisecond)

	// simulate ^C
	proc, _ := os.FindProcess(os.Getpid())
	proc.Signal(os.Interrupt)

	if err := g.Wait(); err != nil {
		fmt.Println(err)
	}

	// Unordered output:
	// async logger started
	// debug http server started
	// main http server started
	// async logger stopped
	// main http server stopped
	// debug http server stopped
	// context canceled
}

func ExampleGroup_Wait_with_http_shutdown() {
	ctx := context.Background()
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(100*time.Millisecond))
	defer cancel()

	g := group.New(group.WithContext(ctx))

	g.Add(func(ctx context.Context) error {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		svr := http.Server{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintln(w, "hello, world!")
			})}
		go func() {
			svr.Serve(l)
		}()

		<-ctx.Done() // wait for group to stop

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // five seconds graceful timeout
		defer cancel()
		return svr.Shutdown(shutdownCtx)
	})

	if err := g.Wait(); err != nil {
		fmt.Println(err)
	}

	// Output:
}

func ExampleGroup_Wait_with_max_concurrency() {
	// Create a group with maximum concurrency of 2
	g := group.New(group.WithMaxConcurrency(2))

	// Add multiple goroutines
	for i := 1; i <= 4; i++ {
		idx := i
		g.Add(func(ctx context.Context) error {
			fmt.Printf("goroutine %d started\n", idx)
			time.Sleep(50 * time.Millisecond)
			fmt.Printf("goroutine %d finished\n", idx)
			return nil
		})
	}

	// Wait for all goroutines to finish
	if err := g.Wait(); err != nil {
		fmt.Println(err)
	}

	// Output:
	// goroutine 1 started
	// goroutine 2 started
	// goroutine 1 finished
	// goroutine 3 started
	// goroutine 2 finished
	// goroutine 4 started
	// goroutine 3 finished
	// goroutine 4 finished
}

func MainHTTPServer(ctx context.Context) error {
	fmt.Println("main http server started")
	defer fmt.Println("main http server stopped")
	<-ctx.Done()
	return ctx.Err()
}

func DebugHTTPServer(ctx context.Context) error {
	fmt.Println("debug http server started")
	defer fmt.Println("debug http server stopped")
	<-ctx.Done()
	return ctx.Err()
}

func AsyncLogger(ctx context.Context) error {
	fmt.Println("async logger started")
	defer fmt.Println("async logger stopped")
	<-ctx.Done()
	return ctx.Err()
}

// Test cases for max concurrency feature

// TestMaxConcurrency verifies that the group respects the maximum concurrency limit.
func TestMaxConcurrency(t *testing.T) {
	maxConcurrency := 5
	g := group.New(group.WithMaxConcurrency(maxConcurrency))

	var peakConcurrency int32
	var currentConcurrency int32

	// Add 20 goroutines with a 50ms delay each
	for i := 0; i < 20; i++ {
		g.Add(func(ctx context.Context) error {
			// Increment current concurrency
			current := atomic.AddInt32(&currentConcurrency, 1)

			// Update peak concurrency
			for {
				peak := atomic.LoadInt32(&peakConcurrency)
				if current <= peak || atomic.CompareAndSwapInt32(&peakConcurrency, peak, current) {
					break
				}
			}

			// Simulate work
			time.Sleep(50 * time.Millisecond)

			// Decrement current concurrency
			atomic.AddInt32(&currentConcurrency, -1)

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if peakConcurrency > int32(maxConcurrency) {
		t.Errorf("peak concurrency %d exceeded limit %d", peakConcurrency, maxConcurrency)
	}

	if currentConcurrency != 0 {
		t.Errorf("current concurrency should be 0, got %d", currentConcurrency)
	}

	t.Logf("peak concurrency: %d (limit: %d)", peakConcurrency, maxConcurrency)
}

// TestNoConcurrencyLimit verifies behavior without concurrency limit.
func TestNoConcurrencyLimit(t *testing.T) {
	g := group.New() // No WithMaxConcurrency option

	var counter atomic.Int32
	taskCount := 100

	for i := 0; i < taskCount; i++ {
		g.Add(func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if counter.Load() != int32(taskCount) {
		t.Errorf("expected %d tasks to complete, got %d", taskCount, counter.Load())
	}
}

// TestMaxConcurrencyOne verifies concurrency limit of 1 (serial execution).
func TestMaxConcurrencyOne(t *testing.T) {
	g := group.New(group.WithMaxConcurrency(1))

	var mu sync.Mutex
	var executionOrder []int

	for i := 0; i < 5; i++ {
		idx := i
		g.Add(func(ctx context.Context) error {
			mu.Lock()
			executionOrder = append(executionOrder, idx)
			mu.Unlock()
			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(executionOrder) != 5 {
		t.Errorf("expected 5 tasks to execute, got %d", len(executionOrder))
	}

	t.Logf("execution order: %v", executionOrder)
}

// TestMaxConcurrencyWithError verifies concurrency limit works with errors.
func TestMaxConcurrencyWithError(t *testing.T) {
	maxConcurrency := 3
	g := group.New(group.WithMaxConcurrency(maxConcurrency))

	var peakConcurrency int32
	var currentConcurrency int32

	// Add 10 goroutines, some will complete normally
	for i := 0; i < 10; i++ {
		g.Add(func(ctx context.Context) error {
			current := atomic.AddInt32(&currentConcurrency, 1)

			// Update peak
			for {
				peak := atomic.LoadInt32(&peakConcurrency)
				if current <= peak || atomic.CompareAndSwapInt32(&peakConcurrency, peak, current) {
					break
				}
			}

			time.Sleep(50 * time.Millisecond)

			atomic.AddInt32(&currentConcurrency, -1)

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		t.Logf("group returned error: %v", err)
	}

	if peakConcurrency > int32(maxConcurrency) {
		t.Errorf("peak concurrency %d exceeded limit %d", peakConcurrency, maxConcurrency)
	}

	t.Logf("peak concurrency with errors: %d (limit: %d)", peakConcurrency, maxConcurrency)
}

// TestConcurrencyLimitOne verifies concurrency limit of 1 (serial execution).
func TestConcurrencyLimitOne(t *testing.T) {
	g := group.New(group.WithMaxConcurrency(1))

	var counter atomic.Int32
	for i := 0; i < 10; i++ {
		g.Add(func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if counter.Load() != 10 {
		t.Errorf("expected 10 tasks to complete, got %d", counter.Load())
	}
}

// TestConcurrencyLimitStress verifies that the group respects the maximum concurrency limit.
func TestConcurrencyLimitStress(t *testing.T) {
	maxConcurrency := 10
	g := group.New(group.WithMaxConcurrency(maxConcurrency))

	var peakConcurrency int32
	var currentConcurrency int32

	for i := 0; i < 50; i++ {
		g.Add(func(ctx context.Context) error {
			current := atomic.AddInt32(&currentConcurrency, 1)
			for {
				peak := atomic.LoadInt32(&peakConcurrency)
				if current <= peak || atomic.CompareAndSwapInt32(&peakConcurrency, peak, current) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&currentConcurrency, -1)
			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if peakConcurrency > int32(maxConcurrency) {
		t.Errorf("peak concurrency %d exceeded limit %d", peakConcurrency, maxConcurrency)
	}
	if currentConcurrency != 0 {
		t.Errorf("current concurrency should be 0, got %d", currentConcurrency)
	}
}

// BenchmarkConcurrencyWithLimit benchmarks performance with concurrency limit.
func BenchmarkConcurrencyWithLimit(b *testing.B) {
	maxConcurrency := 5
	g := group.New(group.WithMaxConcurrency(maxConcurrency))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		g.Add(func(ctx context.Context) error {
			// Minimal work
			return nil
		})
	}

	b.StopTimer()
	g.Wait()
}

// BenchmarkConcurrencyWithoutLimit benchmarks performance without concurrency limit.
func BenchmarkConcurrencyWithoutLimit(b *testing.B) {
	g := group.New()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		g.Add(func(ctx context.Context) error {
			// Minimal work
			return nil
		})
	}

	b.StopTimer()
	g.Wait()
}
