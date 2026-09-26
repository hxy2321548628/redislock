package redislock

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

type delayedReplyBackend struct{ memoryBackend }

func (c *delayedReplyBackend) TryAcquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	ok, err := c.memoryBackend.TryAcquire(ctx, key, token, ttl)
	if ok && err == nil {
		time.Sleep(500 * time.Millisecond)
	}
	return ok, err
}

func TestAutoRenewUsesRemainingValidity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &delayedReplyBackend{}
		lock, err := NewLock("job", client, WithTTL(600*time.Millisecond), WithAutoRenew())
		if err != nil {
			t.Fatal(err)
		}
		lease, err := lock.TryAcquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer lease.Release(context.Background())
		time.Sleep(125 * time.Millisecond)
		synctest.Wait()
		acquired, err := client.memoryBackend.TryAcquire(context.Background(), lock.key, "other", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if acquired {
			t.Fatal("auto-renew let the key expire after a slow acquisition reply")
		}
		select {
		case <-lease.Done():
			t.Fatalf("lease ended: %v", lease.Err())
		default:
		}
	})
}

func TestRedlockValidityIncludesDrift(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ttl := 30 * time.Second
		lock, err := NewRedlock("job", []Backend{&memoryBackend{}, &memoryBackend{}, &memoryBackend{}}, ttl, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		lease, err := lock.TryAcquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer lease.Release(context.Background())
		time.Sleep(ttl - ttl/100 - 2*time.Millisecond)
		synctest.Wait()
		select {
		case <-lease.Done():
			if !errors.Is(lease.Err(), ErrExpired) {
				t.Fatalf("Err = %v", lease.Err())
			}
		default:
			t.Fatal("lease outlived drift-adjusted validity")
		}
	})
}

type failReleaseOnceBackend struct {
	memoryBackend
	calls   int
	failure error
}

func (c *failReleaseOnceBackend) Release(ctx context.Context, key, token string) (bool, error) {
	c.calls++
	if c.calls == 1 {
		return false, c.failure
	}
	return c.memoryBackend.Release(ctx, key, token)
}
func TestReleaseRetryReportsReleaseResult(t *testing.T) {
	client := &failReleaseOnceBackend{failure: errors.New("temporary release failure")}
	lock, err := NewLock("job", client)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := lock.TryAcquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(context.Background()); !errors.Is(err, client.failure) {
		t.Fatalf("first Release = %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("retry Release = %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("repeated Release = %v", err)
	}
	if !errors.Is(lease.Err(), client.failure) {
		t.Fatalf("terminal cause changed: %v", lease.Err())
	}
}

// The first release is deliberately blocked so a second caller must wait for
// execution rights. Its context must also bound that wait.
func TestConcurrentReleaseHonorsContext(t *testing.T) {
	memory := &memoryBackend{}
	entered := make(chan struct{})
	unblock := make(chan struct{})
	backend := &faultBackend{Backend: memory, release: func(ctx context.Context, key, token string) (bool, error) {
		close(entered)
		<-unblock
		return memory.Release(ctx, key, token)
	}}
	lock, err := NewLock("job", backend)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := lock.TryAcquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { first <- lease.Release(context.Background()) }()
	<-entered
	// Always unblock the first call, including when an assertion fails.
	defer func() {
		close(unblock)
		if err := <-first; err != nil {
			t.Errorf("first Release = %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() { second <- lease.Release(ctx) }()
	select {
	case err := <-second:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("waiting Release = %v, want DeadlineExceeded", err)
		}
	case <-time.After(time.Second):
		t.Error("waiting Release ignored its own deadline")
	}
	select {
	case <-lease.Done():
		t.Errorf("waiting caller ended lease: %v", lease.Err())
	default:
	}
}

func TestCanceledReleaseDoesNotStopRenewal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lock, err := NewLock("job", &memoryBackend{}, WithTTL(60*time.Millisecond), WithAutoRenew())
		if err != nil {
			t.Fatal(err)
		}
		lease, err := lock.TryAcquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer lease.Release(context.Background())
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := lease.Release(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Release = %v", err)
		}
		time.Sleep(140 * time.Millisecond)
		synctest.Wait()
		select {
		case <-lease.Done():
			t.Fatalf("canceled caller ended lease: %v", lease.Err())
		default:
		}
		if _, err := lock.TryAcquire(context.Background()); !errors.Is(err, ErrLocked) {
			t.Fatalf("lease stopped renewing: %v", err)
		}
	})
}

func TestReleaseCancelsInFlightRenewal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		stopped := make(chan struct{})
		backend := &faultBackend{Backend: &memoryBackend{}, refresh: func(ctx context.Context, _, _ string, _ time.Duration) (bool, error) {
			close(entered)
			<-ctx.Done()
			close(stopped)
			return false, ctx.Err()
		}}
		lock, err := NewLock("job", backend, WithTTL(time.Second), WithAutoRenew())
		if err != nil {
			t.Fatal(err)
		}
		lease, err := lock.TryAcquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		<-entered
		if err := lease.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
		<-stopped
		synctest.Wait()
		if err := lease.Err(); err != nil {
			t.Fatalf("normal Release recorded renewal cancellation: %v", err)
		}
		select {
		case <-lease.Done():
		default:
			t.Fatal("released lease is still active")
		}
		next, err := lock.TryAcquire(context.Background())
		if err != nil {
			t.Fatalf("key remained held after Release: %v", err)
		}
		if err := next.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}
