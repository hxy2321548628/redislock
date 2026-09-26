package redislock

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestRedlockReleasesPartialAcquisition(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clients := []*memoryBackend{{}, {}, {}}
		_, _ = clients[1].TryAcquire(context.Background(), "job", "other", time.Second)
		_, _ = clients[2].TryAcquire(context.Background(), "job", "other", time.Second)
		lock, err := NewRedlock("job", []Backend{clients[0], clients[1], clients[2]}, time.Second, 50*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := lock.TryAcquire(context.Background()); !errors.Is(err, ErrLocked) {
			t.Fatalf("Redlock error = %v, want ErrLocked", err)
		}
		clients[0].mu.Lock()
		_, held := clients[0].held["job"]
		clients[0].mu.Unlock()
		if held {
			t.Fatal("failed quorum left a partial lock")
		}
	})
}

func TestRedlockReleasesOwnedQuorum(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clients := []*memoryBackend{{}, {}, {}}
		_, _ = clients[2].TryAcquire(context.Background(), "job", "other", time.Second)
		lock, err := NewRedlock("job", []Backend{clients[0], clients[1], clients[2]}, time.Second, 50*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		lease, err := lock.TryAcquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := lease.Release(context.Background()); err != nil {
			t.Fatalf("quorum Release error = %v", err)
		}
	})
}

func TestRedlockRejectsExpiredQuorum(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clients := []*memoryBackend{{delay: 40 * time.Millisecond}, {delay: 40 * time.Millisecond}, {delay: 40 * time.Millisecond}}
		lock, err := NewRedlock("job", []Backend{clients[0], clients[1], clients[2]}, 30*time.Millisecond, 50*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := lock.TryAcquire(context.Background()); !errors.Is(err, ErrExpired) {
			t.Fatalf("expired quorum error = %v, want ErrExpired", err)
		}
	})
}

func TestRedlockAcquisitionIdentifiesFailedNode(t *testing.T) {
	failure := errors.New("connection lost")
	offline := &faultBackend{Backend: &memoryBackend{}, acquire: func(context.Context, string, string, time.Duration) (bool, error) {
		return false, failure
	}}
	occupied := &memoryBackend{}
	_, _ = occupied.TryAcquire(context.Background(), "job", "other", time.Minute)
	free := &memoryBackend{}
	lock, err := NewRedlock("job", []Backend{offline, occupied, free}, time.Second, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lock.TryAcquire(context.Background())
	if !errors.Is(err, failure) || !strings.Contains(err.Error(), "acquire node 0") {
		t.Fatalf("acquisition error lost node or cause: %v", err)
	}
	if len(free.held) != 0 {
		t.Fatal("failed acquisition left a partial lock")
	}
}

func TestRedlockPartialReleaseRetry(t *testing.T) {
	failure := errors.New("release reply lost")
	unstable := &failReleaseOnceBackend{failure: failure}
	lock, err := NewRedlock("job", []Backend{&memoryBackend{}, unstable, &memoryBackend{}}, time.Second, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := lock.TryAcquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(context.Background()); !errors.Is(err, failure) || !strings.Contains(err.Error(), "release node 1") {
		t.Fatalf("partial release error lost node or cause: %v", err)
	}
	// The retry deletes the remaining key, but already deleted keys no longer
	// match the token, so this attempt cannot report a releasing quorum.
	if err := lease.Release(context.Background()); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("retry Release = %v, want ErrNotOwner", err)
	}
	if len(unstable.held) != 0 {
		t.Fatal("retry left a key behind")
	}
	if !errors.Is(lease.Err(), failure) {
		t.Fatalf("retry replaced terminal cause: %v", lease.Err())
	}
	if err := lease.Release(context.Background()); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("cached Release = %v", err)
	}
	if unstable.calls != 2 {
		t.Fatalf("release calls = %d, want 2", unstable.calls)
	}
}

func TestRedlockReportsReleaseErrorAfterQuorumAcquisition(t *testing.T) {
	failure := errors.New("node unavailable")
	offline := &faultBackend{
		acquire: func(context.Context, string, string, time.Duration) (bool, error) { return false, failure },
		release: func(context.Context, string, string) (bool, error) { return false, failure },
	}
	lock, err := NewRedlock("job", []Backend{&memoryBackend{}, &memoryBackend{}, offline}, time.Second, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := lock.TryAcquire(context.Background())
	if err != nil {
		t.Fatalf("healthy majority did not acquire: %v", err)
	}
	if err := lease.Release(context.Background()); !errors.Is(err, failure) || !strings.Contains(err.Error(), "release node 2") {
		t.Fatalf("release did not report minority failure: %v", err)
	}
}

func TestRedlockContactsNodesConcurrently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var backends []Backend
		for range 3 {
			memory := &memoryBackend{delay: 100 * time.Millisecond}
			backends = append(backends, &faultBackend{Backend: memory, release: func(ctx context.Context, key, token string) (bool, error) {
				time.Sleep(100 * time.Millisecond)
				return memory.Release(ctx, key, token)
			}})
		}
		lock, err := NewRedlock("job", backends, time.Second, 200*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		lease, err := lock.TryAcquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed != 100*time.Millisecond {
			t.Errorf("acquisition took %s, want 100ms", elapsed)
		}
		start = time.Now()
		if err := lease.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed != 100*time.Millisecond {
			t.Errorf("release took %s, want 100ms", elapsed)
		}
	})
}

func TestRedlockWaitsForLastAcquisitionBeforeRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		slow := &memoryBackend{delay: 100 * time.Millisecond}
		lock, err := NewRedlock("job", []Backend{&memoryBackend{}, &memoryBackend{}, slow}, time.Second, 200*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		lease, err := lock.TryAcquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if time.Since(start) != 100*time.Millisecond {
			t.Error("returned before the last acquisition finished")
		}
		if err := lease.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
		if len(slow.held) != 0 {
			t.Fatal("late acquisition recreated a released key")
		}
	})
}

func TestRedlockCleanupBudgetDoesNotStarveHealthyNodes(t *testing.T) {
	for _, slowFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "slow-first", false: "slow-last"}[slowFirst], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				slow := &faultBackend{
					acquire: func(context.Context, string, string, time.Duration) (bool, error) { return false, nil },
					release: func(ctx context.Context, _, _ string) (bool, error) { <-ctx.Done(); return false, ctx.Err() },
				}
				memory := &memoryBackend{}
				healthy := &faultBackend{Backend: memory, release: func(ctx context.Context, key, token string) (bool, error) {
					if err := ctx.Err(); err != nil {
						return false, err
					}
					return memory.Release(ctx, key, token)
				}}
				backends := []Backend{slow, slow, healthy}
				if !slowFirst {
					backends = []Backend{healthy, slow, slow}
				}
				lock, err := NewRedlock("job", backends, time.Minute, 2*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				start := time.Now()
				_, err = lock.TryAcquire(context.Background())
				if !errors.Is(err, ErrLocked) || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("TryAcquire = %v, want contention and cleanup timeout", err)
				}
				if elapsed := time.Since(start); elapsed != time.Second {
					t.Fatalf("cleanup took %s, want 1s", elapsed)
				}
				if len(memory.held) != 0 {
					t.Fatal("slow nodes exhausted the healthy node's cleanup budget")
				}
			})
		})
	}
}

func TestRedlockValidation(t *testing.T) {
	for _, tt := range []struct {
		name, key        string
		backends         []Backend
		ttl, nodeTimeout time.Duration
	}{
		{"too few nodes", "job", []Backend{&memoryBackend{}, &memoryBackend{}}, time.Second, time.Millisecond},
		{"nil node", "job", []Backend{&memoryBackend{}, nil, &memoryBackend{}}, time.Second, time.Millisecond},
		{"empty key", "", []Backend{&memoryBackend{}, &memoryBackend{}, &memoryBackend{}}, time.Second, time.Millisecond},
		{"short TTL", "job", []Backend{&memoryBackend{}, &memoryBackend{}, &memoryBackend{}}, 2999 * time.Microsecond, time.Millisecond},
		{"zero timeout", "job", []Backend{&memoryBackend{}, &memoryBackend{}, &memoryBackend{}}, time.Second, 0},
		{"negative timeout", "job", []Backend{&memoryBackend{}, &memoryBackend{}, &memoryBackend{}}, time.Second, -time.Millisecond},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewRedlock(tt.key, tt.backends, tt.ttl, tt.nodeTimeout); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestRedlockAcquireRetriesContention(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lock, err := NewRedlock("job", []Backend{&memoryBackend{}, &memoryBackend{}, &memoryBackend{}}, time.Second, 50*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		first, err := lock.TryAcquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			time.Sleep(100 * time.Millisecond)
			if err := first.Release(context.Background()); err != nil {
				t.Error(err)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		next, err := lock.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := next.Release(ctx); err != nil {
			t.Fatal(err)
		}
	})
}
