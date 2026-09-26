package redislock

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/google/uuid"
)

func TestRedisLockLifecycle(t *testing.T) {
	address := os.Getenv("REDIS_ADDR")
	if address == "" {
		t.Skip("set REDIS_ADDR to run the Redis integration test")
	}
	pool := &redis.Pool{
		DialContext: func(ctx context.Context) (redis.Conn, error) {
			return redis.DialContext(ctx, "tcp", address,
				redis.DialConnectTimeout(time.Second),
				redis.DialReadTimeout(time.Second),
				redis.DialWriteTimeout(time.Second))
		},
	}
	defer pool.Close()
	client, err := NewClient(pool)
	if err != nil {
		t.Fatal(err)
	}
	id, err := uuid.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	lock, err := NewLock("integration:"+id.String(), client, WithTTL(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	first, err := lock.TryLock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Unlock(context.Background())
	if _, err := lock.TryLock(ctx); !errors.Is(err, ErrLocked) {
		t.Fatalf("contended TryLock error = %v, want ErrLocked", err)
	}
	if err := first.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := lock.TryLock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Unlock(context.Background())
	if err := first.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := lock.TryLock(ctx); !errors.Is(err, ErrLocked) {
		t.Fatalf("old lease released new lease: %v", err)
	}
	if err := second.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
}
