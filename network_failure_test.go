package redislock_test

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/hxy2321548628/redislock"
)

// dropReplyProxy forwards one command to Redis, waits for its successful result,
// then closes the client connection without forwarding the reply.
func dropReplyProxy(t *testing.T, address string) (string, <-chan any) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	replies := make(chan any, 1)
	stopped := make(chan struct{})
	t.Cleanup(func() {
		_ = listener.Close()
		<-stopped
	})
	go func() {
		defer close(stopped)
		defer close(replies)
		socket, err := listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				t.Error(err)
			}
			return
		}
		downstream := redis.NewConn(socket, time.Second, time.Second)
		defer downstream.Close()
		args, err := redis.Values(downstream.Receive())
		if err != nil || len(args) == 0 {
			t.Errorf("read command: %v, %v", args, err)
			return
		}
		command, err := redis.String(args[0], nil)
		if err != nil {
			t.Error(err)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		upstream, err := redis.DialContext(ctx, "tcp", address,
			redis.DialConnectTimeout(time.Second), redis.DialReadTimeout(time.Second), redis.DialWriteTimeout(time.Second))
		if err != nil {
			t.Error(err)
			return
		}
		defer upstream.Close()
		reply, err := redis.DoContext(upstream, ctx, command, args[1:]...)
		if err != nil {
			t.Errorf("execute %s before losing reply: %v", command, err)
			return
		}
		replies <- reply
	}()
	return listener.Addr().String(), replies
}

func TestRedisLostReply(t *testing.T) {
	address := os.Getenv("REDIS_ADDR")
	if address == "" {
		t.Skip("set REDIS_ADDR to run Redis connection-failure tests")
	}
	for _, operation := range []string{"acquire", "release"} {
		t.Run(operation, func(t *testing.T) {
			proxy, replies := dropReplyProxy(t, address)
			var dropNext atomic.Bool
			// No idle connections: the armed operation gets a new proxy connection,
			// while cleanup and retries reconnect directly to Redis.
			pool := &redis.Pool{DialContext: func(ctx context.Context) (redis.Conn, error) {
				target := address
				if dropNext.CompareAndSwap(true, false) {
					target = proxy
				}
				return redis.DialContext(ctx, "tcp", target,
					redis.DialConnectTimeout(time.Second), redis.DialReadTimeout(time.Second), redis.DialWriteTimeout(time.Second))
			}}
			t.Cleanup(func() { _ = pool.Close() })
			backend, err := redislock.NewRedigoBackend(pool)
			if err != nil {
				t.Fatal(err)
			}
			key := "redislock:network-test:" + rand.Text()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// Load the release script before fault injection so NOSCRIPT does not
			// replace the successful release response that this test needs to drop.
			if _, err := backend.Release(ctx, key, "warmup"); err != nil {
				t.Fatal(err)
			}
			lock, err := redislock.NewLock(key, backend, redislock.WithTTL(10*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if operation == "acquire" {
				dropNext.Store(true)
				if _, err := lock.TryAcquire(ctx); !errors.Is(err, io.EOF) {
					t.Fatalf("lost acquire reply = %v, want EOF", err)
				}
				if reply := <-replies; reply != "OK" {
					t.Fatalf("acquisition was not executed: %v", reply)
				}
			} else {
				lease, err := lock.TryAcquire(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer lease.Release(context.Background())
				dropNext.Store(true)
				if err := lease.Release(ctx); !errors.Is(err, io.EOF) {
					t.Fatalf("lost release reply = %v, want EOF", err)
				}
				if reply := <-replies; reply != int64(1) {
					t.Fatalf("release was not executed: %v", reply)
				}
				if err := lease.Release(ctx); !errors.Is(err, redislock.ErrNotOwner) {
					t.Fatalf("release retry = %v, want ErrNotOwner", err)
				}
				<-lease.Done()
				if !errors.Is(lease.Err(), io.EOF) {
					t.Fatalf("terminal cause changed: %v", lease.Err())
				}
			}
			// Uncertain acquisition must have been cleaned up; a lost release
			// response must not prevent the next owner from acquiring the key.
			next, err := lock.TryAcquire(ctx)
			if err != nil {
				t.Fatalf("key remained held after lost %s reply: %v", operation, err)
			}
			if err := next.Release(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
