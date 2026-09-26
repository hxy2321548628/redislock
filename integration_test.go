package redislock_test

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/hxy2321548628/redislock"
)

func TestRedisLockLifecycle(t *testing.T) {
	address := os.Getenv("REDIS_ADDR")
	if address == "" {
		t.Skip("set REDIS_ADDR to run the Redis integration test")
	}
	client := integrationBackend(t, address)
	id := rand.Text()
	lock, err := redislock.NewLock("integration:"+id, client, redislock.WithTTL(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	first, err := lock.TryAcquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release(context.Background())
	if ok, err := client.TryAcquire(ctx, "integration:"+id, "other", time.Second); ok || err != nil {
		t.Fatalf("lock did not use the exact caller key: %t, %v", ok, err)
	}
	if _, err := lock.TryAcquire(ctx); !errors.Is(err, redislock.ErrLocked) {
		t.Fatalf("contended TryAcquire error = %v, want redislock.ErrLocked", err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := lock.TryAcquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release(context.Background())
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := lock.TryAcquire(ctx); !errors.Is(err, redislock.ErrLocked) {
		t.Fatalf("old lease released new lease: %v", err)
	}
	if err := second.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func integrationBackend(t testing.TB, address string) *redislock.RedigoBackend {
	t.Helper()
	pool := integrationPool(t, address)
	client, err := redislock.NewRedigoBackend(pool)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func integrationPool(t testing.TB, address string) *redis.Pool {
	t.Helper()
	pool := &redis.Pool{
		MaxIdle:   16,
		MaxActive: 64,
		Wait:      true,
		DialContext: func(ctx context.Context) (redis.Conn, error) {
			return redis.DialContext(ctx, "tcp", address,
				redis.DialConnectTimeout(time.Second),
				redis.DialReadTimeout(time.Second),
				redis.DialWriteTimeout(time.Second))
		},
	}
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

func TestRedisOwnershipAndExpiry(t *testing.T) {
	address := os.Getenv("REDIS_ADDR")
	if address == "" {
		t.Skip("set REDIS_ADDR to run Redis integration tests")
	}
	pool := integrationPool(t, address)
	client, err := redislock.NewRedigoBackend(pool)
	if err != nil {
		t.Fatal(err)
	}
	key := "redislock:integration:" + rand.Text()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if ok, err := client.TryAcquire(ctx, key, "old", 50*time.Millisecond); !ok || err != nil {
		t.Fatalf("acquire = %t, %v", ok, err)
	}
	// Wait on server-observed expiry rather than assuming local time equals Redis time.
	for {
		ok, err := client.TryAcquire(ctx, key, "new", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	defer client.Release(context.Background(), key, "new")
	if ok, err := client.Release(ctx, key, "old"); ok || err != nil {
		t.Fatalf("old release = %t, %v", ok, err)
	}
	if ok, err := client.Refresh(ctx, key, "old", time.Second); ok || err != nil {
		t.Fatalf("old refresh = %t, %v", ok, err)
	}
	if ok, err := client.Refresh(ctx, key, "new", 3*time.Second); !ok || err != nil {
		t.Fatalf("new refresh = %t, %v", ok, err)
	}
	conn, err := pool.GetContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ttl, err := redis.Int64(redis.DoContext(conn, ctx, "PTTL", key))
	if err != nil || ttl < 2000 {
		t.Fatalf("refreshed TTL = %d, %v", ttl, err)
	}
	if ok, err := client.Release(ctx, key, "new"); !ok || err != nil {
		t.Fatalf("new release = %t, %v", ok, err)
	}
	if ok, err := client.Refresh(ctx, key, "new", time.Second); ok || err != nil {
		t.Fatalf("refresh recreated key: %t, %v", ok, err)
	}
}

func TestRedisAutoRenew(t *testing.T) {
	address := os.Getenv("REDIS_ADDR")
	if address == "" {
		t.Skip("set REDIS_ADDR to run Redis integration tests")
	}
	client := integrationBackend(t, address)
	lock, err := redislock.NewLock("integration:"+rand.Text(), client, redislock.WithTTL(time.Second), redislock.WithAutoRenew())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lease, err := lock.TryAcquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(context.Background())
	select {
	case <-lease.Done():
		t.Fatalf("lease ended: %v", lease.Err())
	case <-time.After(1500 * time.Millisecond):
	}
	if _, err := lock.TryAcquire(ctx); !errors.Is(err, redislock.ErrLocked) {
		t.Fatalf("renewed key not held: %v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRedisRedlock(t *testing.T) {
	addresses := os.Getenv("REDIS_ADDRS")
	if addresses == "" {
		t.Skip("set REDIS_ADDRS to three independent Redis addresses")
	}
	var backends []redislock.Backend
	for _, address := range strings.Split(addresses, ",") {
		backends = append(backends, integrationBackend(t, strings.TrimSpace(address)))
	}
	lock, err := redislock.NewRedlock("integration:"+rand.Text(), backends, 5*time.Second, 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lease, err := lock.TryAcquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(context.Background())
	if _, err := lock.TryAcquire(ctx); !errors.Is(err, redislock.ErrLocked) {
		t.Fatalf("contended quorum = %v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	next, err := lock.TryAcquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := next.Release(ctx); err != nil {
		t.Fatal(err)
	}
}
