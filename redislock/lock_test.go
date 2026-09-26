package redislock

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

type heldValue struct {
	token   string
	expires time.Time
}

type memoryClient struct {
	mu          sync.Mutex
	held        map[string]heldValue
	delay       time.Duration
	failRefresh bool
}

func (c *memoryClient) TryAcquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
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

func (c *memoryClient) Release(_ context.Context, key, token string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.held[key]
	if !ok || time.Now().After(current.expires) || current.token != token {
		return false, nil
	}
	delete(c.held, key)
	return true, nil
}

func (c *memoryClient) Refresh(_ context.Context, key, token string, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failRefresh {
		return false, nil
	}
	current, ok := c.held[key]
	if !ok || time.Now().After(current.expires) || current.token != token {
		return false, nil
	}
	c.held[key] = heldValue{token: token, expires: time.Now().Add(ttl)}
	return true, nil
}

func TestLockWaitsForRelease(t *testing.T) {
	client := &memoryClient{}
	lock, err := NewLock("job", client, WithRetryInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	first, err := lock.TryLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lock.TryLock(context.Background()); !errors.Is(err, ErrLocked) {
		t.Fatalf("contended TryLock error = %v, want ErrLocked", err)
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = first.Unlock(context.Background())
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	second, err := lock.Lock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Unlock(context.Background())
	if first.token == second.token {
		t.Fatal("two acquisitions reused the same token")
	}
	id, err := uuid.Parse(second.token)
	if err != nil || id.Version() != 4 {
		t.Fatalf("token = %q, want UUID v4: %v", second.token, err)
	}
}

func TestOldLeaseCannotReleaseNewLease(t *testing.T) {
	client := &memoryClient{}
	lock, err := NewLock("job", client, WithTTL(25*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	old, err := lock.TryLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	<-old.Done()
	if !errors.Is(old.Err(), ErrExpired) {
		t.Fatalf("old lease error = %v, want ErrExpired", old.Err())
	}
	current, err := lock.TryLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer current.Unlock(context.Background())
	if err := old.Unlock(context.Background()); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("old Unlock error = %v, want ErrNotOwner", err)
	}
	if _, err := lock.TryLock(context.Background()); !errors.Is(err, ErrLocked) {
		t.Fatalf("new lease was released by old lease: %v", err)
	}
}

func TestAutoRenewOutlivesAcquireContext(t *testing.T) {
	client := &memoryClient{}
	lock, err := NewLock("job", client, WithTTL(60*time.Millisecond), WithAutoRenew())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	lease, err := lock.TryLock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	time.Sleep(140 * time.Millisecond)
	if _, err := lock.TryLock(context.Background()); !errors.Is(err, ErrLocked) {
		t.Fatalf("lease stopped renewing with acquisition context: %v", err)
	}
	if err := lease.Unlock(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAutoRenewReportsLostOwnership(t *testing.T) {
	client := &memoryClient{failRefresh: true}
	lock, err := NewLock("job", client, WithTTL(60*time.Millisecond), WithAutoRenew())
	if err != nil {
		t.Fatal(err)
	}
	lease, err := lock.TryLock(context.Background())
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
}

func TestRedLockReleasesPartialAcquisition(t *testing.T) {
	clients := []*memoryClient{{}, {}, {}}
	_, _ = clients[1].TryAcquire(context.Background(), "redislock:job", "other", time.Second)
	_, _ = clients[2].TryAcquire(context.Background(), "redislock:job", "other", time.Second)
	lock, err := NewRedLock("job", []ILockClient{clients[0], clients[1], clients[2]}, time.Second, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lock.TryLock(context.Background()); !errors.Is(err, ErrLocked) {
		t.Fatalf("RedLock error = %v, want ErrLocked", err)
	}
	clients[0].mu.Lock()
	_, held := clients[0].held["redislock:job"]
	clients[0].mu.Unlock()
	if held {
		t.Fatal("failed quorum left a partial lock")
	}
}

func TestRedLockUnlocksOwnedQuorum(t *testing.T) {
	clients := []*memoryClient{{}, {}, {}}
	_, _ = clients[2].TryAcquire(context.Background(), "redislock:job", "other", time.Second)
	lock, err := NewRedLock("job", []ILockClient{clients[0], clients[1], clients[2]}, time.Second, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := lock.TryLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Unlock(context.Background()); err != nil {
		t.Fatalf("quorum Unlock error = %v", err)
	}
}

func TestRedLockRejectsExpiredQuorum(t *testing.T) {
	clients := []*memoryClient{{delay: 20 * time.Millisecond}, {delay: 20 * time.Millisecond}, {delay: 20 * time.Millisecond}}
	lock, err := NewRedLock("job", []ILockClient{clients[0], clients[1], clients[2]}, 40*time.Millisecond, 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lock.TryLock(context.Background()); !errors.Is(err, ErrLocked) {
		t.Fatalf("expired quorum error = %v, want ErrLocked", err)
	}
}

type uncertainClient struct {
	memoryClient
}

func (c *uncertainClient) TryAcquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	_, _ = c.memoryClient.TryAcquire(ctx, key, token, ttl)
	return false, errors.New("reply lost")
}

func TestUncertainAcquisitionIsCleanedUp(t *testing.T) {
	client := &uncertainClient{}
	lock, err := NewLock("job", client)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lock.TryLock(context.Background()); err == nil {
		t.Fatal("TryLock succeeded despite a lost reply")
	}
	client.mu.Lock()
	_, held := client.held["redislock:job"]
	client.mu.Unlock()
	if held {
		t.Fatal("uncertain acquisition left a lock behind")
	}
}

func TestTTLUsesRedisMillisecondPrecision(t *testing.T) {
	lock, err := NewLock("job", &memoryClient{}, WithTTL(1500*time.Microsecond))
	if err != nil {
		t.Fatal(err)
	}
	if lock.opts.ttl != time.Millisecond {
		t.Fatalf("effective TTL = %s, want 1ms", lock.opts.ttl)
	}
}
