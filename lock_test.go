package redislock

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type heldValue struct {
	token   string
	expires time.Time
}

type memoryBackend struct {
	mu          sync.Mutex
	held        map[string]heldValue
	delay       time.Duration
	failRefresh bool
}

func (c *memoryBackend) TryAcquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	if c.delay > 0 {
		timer := time.NewTimer(c.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-timer.C:
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if current, ok := c.held[key]; ok && time.Now().Before(current.expires) {
		return false, nil
	}
	if c.held == nil {
		c.held = make(map[string]heldValue)
	}
	c.held[key] = heldValue{token: token, expires: time.Now().Add(ttl)}
	return true, nil
}

func (c *memoryBackend) Release(_ context.Context, key, token string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.held[key]
	if !ok || !time.Now().Before(current.expires) || current.token != token {
		return false, nil
	}
	delete(c.held, key)
	return true, nil
}

func (c *memoryBackend) Refresh(_ context.Context, key, token string, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failRefresh {
		return false, nil
	}
	current, ok := c.held[key]
	if !ok || !time.Now().Before(current.expires) || current.token != token {
		return false, nil
	}
	c.held[key] = heldValue{token: token, expires: time.Now().Add(ttl)}
	return true, nil
}

func TestLockWaitsForRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &memoryBackend{}
		lock, err := NewLock("job", client, WithRetryInterval(5*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		first, err := lock.TryAcquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := lock.TryAcquire(context.Background()); !errors.Is(err, ErrLocked) {
			t.Fatalf("contended TryAcquire error = %v, want ErrLocked", err)
		}
		go func() {
			time.Sleep(20 * time.Millisecond)
			_ = first.Release(context.Background())
		}()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		second, err := lock.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer second.Release(context.Background())
		if first.token == second.token {
			t.Fatal("two acquisitions reused the same token")
		}
		if second.token == "" {
			t.Fatal("acquisition returned an empty token")
		}
	})
}

func TestOldLeaseCannotReleaseNewLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &memoryBackend{}
		lock, err := NewLock("job", client, WithTTL(25*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		old, err := lock.TryAcquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		<-old.Done()
		if !errors.Is(old.Err(), ErrExpired) {
			t.Fatalf("old lease error = %v, want ErrExpired", old.Err())
		}
		current, err := lock.TryAcquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer current.Release(context.Background())
		if err := old.Release(context.Background()); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("old Release error = %v, want ErrNotOwner", err)
		}
		if _, err := lock.TryAcquire(context.Background()); !errors.Is(err, ErrLocked) {
			t.Fatalf("new lease was released by old lease: %v", err)
		}
	})
}

func TestAutoRenewOutlivesAcquireContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &memoryBackend{}
		lock, err := NewLock("job", client, WithTTL(60*time.Millisecond), WithAutoRenew())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		lease, err := lock.TryAcquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		time.Sleep(140 * time.Millisecond)
		if _, err := lock.TryAcquire(context.Background()); !errors.Is(err, ErrLocked) {
			t.Fatalf("lease stopped renewing with acquisition context: %v", err)
		}
		if err := lease.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAutoRenewReportsLostOwnership(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &memoryBackend{failRefresh: true}
		lock, err := NewLock("job", client, WithTTL(60*time.Millisecond), WithAutoRenew())
		if err != nil {
			t.Fatal(err)
		}
		lease, err := lock.TryAcquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-lease.Done():
		case <-time.After(time.Second):
			t.Fatal("renewal failure was not reported")
		}
		if !errors.Is(lease.Err(), ErrNotOwner) {
			t.Fatalf("lease error = %v, want ErrNotOwner", lease.Err())
		}
	})
}

type uncertainBackend struct {
	memoryBackend
}

func (c *uncertainBackend) TryAcquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	_, _ = c.memoryBackend.TryAcquire(ctx, key, token, ttl)
	return false, errors.New("reply lost")
}

func TestUncertainAcquisitionIsCleanedUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &uncertainBackend{}
		lock, err := NewLock("job", client)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := lock.TryAcquire(context.Background()); err == nil {
			t.Fatal("TryAcquire succeeded despite a lost reply")
		}
		client.mu.Lock()
		_, held := client.held["job"]
		client.mu.Unlock()
		if held {
			t.Fatal("uncertain acquisition left a lock behind")
		}
	})
}

func TestTTLUsesRedisMillisecondPrecision(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lock, err := NewLock("job", &memoryBackend{}, WithTTL(1500*time.Microsecond))
		if err != nil {
			t.Fatal(err)
		}
		if lock.opts.ttl != time.Millisecond {
			t.Fatalf("effective TTL = %s, want 1ms", lock.opts.ttl)
		}
	})
}
