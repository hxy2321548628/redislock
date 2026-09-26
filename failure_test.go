package redislock

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// faultBackend injects failures at the operation boundary, preserving the normal
// in-memory semantics for operations not overridden by a test.
type faultBackend struct {
	Backend
	acquire func(context.Context, string, string, time.Duration) (bool, error)
	refresh func(context.Context, string, string, time.Duration) (bool, error)
	release func(context.Context, string, string) (bool, error)
}

func (b *faultBackend) TryAcquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	if b.acquire != nil {
		return b.acquire(ctx, key, token, ttl)
	}
	return b.Backend.TryAcquire(ctx, key, token, ttl)
}
func (b *faultBackend) Refresh(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	if b.refresh != nil {
		return b.refresh(ctx, key, token, ttl)
	}
	return b.Backend.Refresh(ctx, key, token, ttl)
}
func (b *faultBackend) Release(ctx context.Context, key, token string) (bool, error) {
	if b.release != nil {
		return b.release(ctx, key, token)
	}
	return b.Backend.Release(ctx, key, token)
}

func TestLockValidation(t *testing.T) {
	for _, tt := range []struct {
		name, key string
		backend   Backend
		opts      []Option
	}{
		{name: "empty key", backend: &memoryBackend{}},
		{name: "nil backend", key: "job"},
		{name: "nil option", key: "job", backend: &memoryBackend{}, opts: []Option{nil}},
		{name: "short TTL", key: "job", backend: &memoryBackend{}, opts: []Option{WithTTL(time.Microsecond)}},
		{name: "zero retry", key: "job", backend: &memoryBackend{}, opts: []Option{WithRetryInterval(0)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewLock(tt.key, tt.backend, tt.opts...); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestLockCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lock, err := NewLock("job", &memoryBackend{})
		if err != nil {
			t.Fatal(err)
		}
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := lock.TryAcquire(canceled); !errors.Is(err, context.Canceled) {
			t.Fatalf("TryAcquire = %v", err)
		}
		lease, err := lock.TryAcquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer lease.Release(context.Background())
		ctx, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer stop()
		if _, err := lock.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Lock = %v", err)
		}
	})
}

func TestRenewalFailureEndsLease(t *testing.T) {
	for _, mode := range []string{"timeout", "backend error", "late success"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				failure := errors.New("connection lost")
				backend := &faultBackend{Backend: &memoryBackend{}, refresh: func(ctx context.Context, _, _ string, _ time.Duration) (bool, error) {
					switch mode {
					case "timeout":
						<-ctx.Done()
						return false, ctx.Err()
					case "late success":
						time.Sleep(time.Second)
						return true, nil
					default:
						return false, failure
					}
				}}
				lock, err := NewLock("job", backend, WithTTL(600*time.Millisecond), WithAutoRenew())
				if err != nil {
					t.Fatal(err)
				}
				lease, err := lock.TryAcquire(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer lease.Release(context.Background())
				select {
				case <-lease.Done():
				case <-time.After(2 * time.Second):
					t.Fatal("lease did not end")
				}
				want := failure
				if mode == "timeout" {
					want = context.DeadlineExceeded
				}
				if mode == "late success" {
					want = ErrExpired
				}
				if !errors.Is(lease.Err(), want) {
					t.Fatalf("Err = %v, want %v", lease.Err(), want)
				}
			})
		})
	}
}

func TestSlowRenewalIsBoundedByRemainingValidity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := &faultBackend{Backend: &delayedReplyBackend{}}
		var observedDeadline time.Time
		backend.refresh = func(ctx context.Context, _, _ string, _ time.Duration) (bool, error) {
			observedDeadline, _ = ctx.Deadline()
			<-ctx.Done()
			return false, ctx.Err()
		}
		lock, err := NewLock("job", backend, WithTTL(600*time.Millisecond), WithAutoRenew())
		if err != nil {
			t.Fatal(err)
		}
		lease, err := lock.TryAcquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer lease.Release(context.Background())
		<-lease.Done()
		if !observedDeadline.Equal(lease.deadline) {
			t.Fatalf("renewal deadline = %v, want %v", observedDeadline, lease.deadline)
		}
		if !errors.Is(lease.Err(), ErrExpired) {
			t.Fatalf("Err = %v", lease.Err())
		}
	})
}

func TestBlockedReleaseStillReportsExpiry(t *testing.T) {
	for _, renew := range []bool{false, true} {
		t.Run(map[bool]string{false: "fixed", true: "renewed"}[renew], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				backend := &faultBackend{Backend: &memoryBackend{}, release: func(ctx context.Context, _, _ string) (bool, error) {
					<-ctx.Done()
					return false, ctx.Err()
				}}
				opts := []Option{WithTTL(600 * time.Millisecond)}
				if renew {
					opts = append(opts, WithAutoRenew())
				}
				lock, err := NewLock("job", backend, opts...)
				if err != nil {
					t.Fatal(err)
				}
				lease, err := lock.TryAcquire(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- lease.Release(ctx) }()
				time.Sleep(600 * time.Millisecond)
				synctest.Wait()
				select {
				case <-lease.Done():
				default:
					t.Fatal("Release suppressed expiry notification")
				}
				if !errors.Is(lease.Err(), ErrExpired) {
					t.Fatalf("Err = %v", lease.Err())
				}
				if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Release = %v", err)
				}
			})
		})
	}
}

func TestConcurrentReleaseIsIdempotent(t *testing.T) {
	var calls atomic.Int32
	memory := &memoryBackend{}
	backend := &faultBackend{Backend: memory, release: func(ctx context.Context, key, token string) (bool, error) {
		calls.Add(1)
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
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if err := lease.Release(context.Background()); err != nil {
				t.Errorf("Release = %v", err)
			}
		})
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("Release calls = %d", calls.Load())
	}
}

func TestConcurrentContendersRemainExclusive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lock, err := NewLock("job", &memoryBackend{}, WithTTL(time.Minute), WithRetryInterval(time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		var active atomic.Int32
		var wg sync.WaitGroup
		for range 20 {
			wg.Go(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				lease, err := lock.Acquire(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				if active.Add(1) != 1 {
					t.Error("overlapping owners")
				}
				time.Sleep(time.Millisecond)
				active.Add(-1)
				if err := lease.Release(ctx); err != nil {
					t.Error(err)
				}
			})
		}
		wg.Wait()
	})
}

func TestRedlockCleanupFailureIsNotRetried(t *testing.T) {
	failure := errors.New("release failed")
	var acquisitions, releases atomic.Int32
	backend := &faultBackend{
		acquire: func(context.Context, string, string, time.Duration) (bool, error) {
			acquisitions.Add(1)
			return false, nil
		},
		release: func(context.Context, string, string) (bool, error) { releases.Add(1); return false, failure },
	}
	lock, err := NewRedlock("job", []Backend{backend, backend, backend}, time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lock.Acquire(context.Background())
	if !errors.Is(err, failure) || !errors.Is(err, ErrLocked) {
		t.Fatalf("Lock = %v", err)
	}
	if acquisitions.Load() != 3 || releases.Load() != 3 {
		t.Fatalf("acquisitions = %d, releases = %d; want 3 each", acquisitions.Load(), releases.Load())
	}
}

func TestRedlockToleratesOneUnavailableNode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		offline := &faultBackend{
			acquire: func(ctx context.Context, _, _ string, _ time.Duration) (bool, error) {
				<-ctx.Done()
				return false, ctx.Err()
			},
			release: func(context.Context, string, string) (bool, error) { return false, nil },
		}
		lock, err := NewRedlock("job", []Backend{offline, &memoryBackend{}, &memoryBackend{}}, time.Second, 10*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		lease, err := lock.TryAcquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := lease.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRedlockCanceledAcquisitionCleansPartialWrites(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := &memoryBackend{}
		var cleanups atomic.Int32
		offline := &faultBackend{
			acquire: func(ctx context.Context, _, _ string, _ time.Duration) (bool, error) {
				<-ctx.Done()
				return false, ctx.Err()
			},
			release: func(ctx context.Context, _, _ string) (bool, error) { cleanups.Add(1); return false, ctx.Err() },
		}
		lock, err := NewRedlock("job", []Backend{first, offline, offline}, time.Second, 100*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if _, err := lock.TryAcquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("TryAcquire = %v", err)
		}
		if len(first.held) != 0 || cleanups.Load() != 2 {
			t.Fatalf("partial writes = %d, cleanups = %d", len(first.held), cleanups.Load())
		}
	})
}
