package redislock_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/hxy2321548628/redislock"
)

func ExampleLock_Acquire() {
	pool := &redis.Pool{
		MaxIdle:   2,
		MaxActive: 10,
		Wait:      true,
		DialContext: func(ctx context.Context) (redis.Conn, error) {
			return redis.DialContext(ctx, "tcp", "127.0.0.1:6379",
				redis.DialConnectTimeout(time.Second),
				redis.DialReadTimeout(time.Second),
				redis.DialWriteTimeout(time.Second))
		},
	}
	defer pool.Close()
	client, err := redislock.NewRedigoBackend(pool)
	if err != nil {
		log.Print(err)
		return
	}
	lock, err := redislock.NewLock("myapp:locks:daily-job", client,
		redislock.WithTTL(10*time.Second), redislock.WithAutoRenew())
	if err != nil {
		log.Print(err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	lease, err := lock.Acquire(ctx)
	if err != nil {
		log.Print(err)
		return
	}
	defer func() {
		releaseCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := lease.Release(releaseCtx); err != nil {
			log.Print(err)
		}
	}()

	// Replace the timer with cancellable work. Real protected writes may also
	// require resource-side fencing; Done alone cannot reject stale writes.
	select {
	case <-lease.Done():
		log.Printf("lease lost: %v", lease.Err())
	case <-ctx.Done():
		log.Print(ctx.Err())
	case <-time.After(100 * time.Millisecond):
		fmt.Println("work completed")
	}
}
