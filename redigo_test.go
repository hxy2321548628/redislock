package redislock

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

// replyConn exercises command construction and reply handling without a server.
type replyConn struct {
	do func(context.Context, string, ...any) (any, error)
}

func (c *replyConn) Close() error { return nil }
func (c *replyConn) Err() error   { return nil }
func (c *replyConn) Do(cmd string, args ...any) (any, error) {
	// Redigo drains pending replies with an empty command when returning a connection.
	if cmd == "" {
		return nil, nil
	}
	return c.do(context.Background(), cmd, args...)
}
func (c *replyConn) Send(string, ...any) error { return nil }
func (c *replyConn) Flush() error              { return nil }
func (c *replyConn) Receive() (any, error)     { return nil, nil }
func (c *replyConn) DoContext(ctx context.Context, cmd string, args ...any) (any, error) {
	return c.do(ctx, cmd, args...)
}
func (c *replyConn) ReceiveContext(context.Context) (any, error) { return nil, nil }

func clientWithReply(t *testing.T, do func(context.Context, string, ...any) (any, error)) *RedigoBackend {
	t.Helper()
	pool := &redis.Pool{DialContext: func(context.Context) (redis.Conn, error) { return &replyConn{do: do}, nil }}
	t.Cleanup(func() { _ = pool.Close() })
	client, err := NewRedigoBackend(pool)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestRedigoBackendTryAcquireReplies(t *testing.T) {
	for _, tt := range []struct {
		name          string
		reply         any
		err           error
		want, wantErr bool
	}{
		{name: "held"},
		{name: "acquired", reply: "OK", want: true},
		{name: "unexpected string", reply: "unexpected", wantErr: true},
		{name: "unexpected type", reply: int64(1), wantErr: true},
		{name: "connection error", err: errors.New("connection lost"), wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := clientWithReply(t, func(_ context.Context, cmd string, args ...any) (any, error) {
				want := []any{"key", "token", "NX", "PX", int64(1000)}
				if cmd != "SET" || !reflect.DeepEqual(args, want) {
					t.Fatalf("command = %s %v", cmd, args)
				}
				return tt.reply, tt.err
			})
			got, err := client.TryAcquire(context.Background(), "key", "token", time.Second)
			if got != tt.want || (err != nil) != tt.wantErr {
				t.Fatalf("TryAcquire = %t, %v", got, err)
			}
		})
	}
}

func TestRedigoBackendScripts(t *testing.T) {
	for _, refresh := range []bool{false, true} {
		for _, fallback := range []bool{false, true} {
			name := map[bool]string{false: "release", true: "refresh"}[refresh]
			if fallback {
				name += "/NOSCRIPT"
			}
			t.Run(name, func(t *testing.T) {
				calls := 0
				client := clientWithReply(t, func(ctx context.Context, cmd string, args ...any) (any, error) {
					calls++
					if _, ok := ctx.Deadline(); !ok {
						t.Fatal("script lost context deadline")
					}
					wantScript := releaseScript
					tail := []any{1, "key", "token"}
					source := compareAndDeleteScript
					if refresh {
						wantScript = refreshScript
						source = compareAndExpireScript
						tail = append(tail, int64(1000))
					}
					if !reflect.DeepEqual(args[1:], tail) {
						t.Fatalf("args = %v", args)
					}
					if calls == 1 {
						if cmd != "EVALSHA" || args[0] != wantScript.Hash() {
							t.Fatalf("command = %s %v", cmd, args)
						}
						if fallback {
							return nil, redis.Error("NOSCRIPT missing script")
						}
					} else if cmd != "EVAL" || args[0] != source {
						t.Fatalf("fallback = %s %v", cmd, args)
					}
					return int64(1), nil
				})
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				var ok bool
				var err error
				if refresh {
					ok, err = client.Refresh(ctx, "key", "token", time.Second)
				} else {
					ok, err = client.Release(ctx, "key", "token")
				}
				if !ok || err != nil {
					t.Fatalf("script = %t, %v", ok, err)
				}
				wantCalls := 1
				if fallback {
					wantCalls = 2
				}
				if calls != wantCalls {
					t.Fatalf("calls = %d, want %d", calls, wantCalls)
				}
			})
		}
	}
}

func TestRedigoBackendScriptErrors(t *testing.T) {
	for _, tt := range []struct {
		name    string
		reply   any
		err     error
		wantErr bool
	}{
		{name: "not owner", reply: int64(0)},
		{name: "unexpected integer", reply: int64(2), wantErr: true},
		{name: "invalid reply", reply: "bad", wantErr: true},
		{name: "network", err: errors.New("network failure"), wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := clientWithReply(t, func(context.Context, string, ...any) (any, error) { return tt.reply, tt.err })
			ok, err := client.Release(context.Background(), "key", "token")
			if ok || (err != nil) != tt.wantErr {
				t.Fatalf("Release = %t, %v", ok, err)
			}
		})
	}
}

func TestRedigoBackendValidationAndDialFailure(t *testing.T) {
	if _, err := NewRedigoBackend(nil); err == nil {
		t.Fatal("nil pool accepted")
	}
	failure := errors.New("dial failed")
	pool := &redis.Pool{DialContext: func(context.Context) (redis.Conn, error) { return nil, failure }}
	defer pool.Close()
	client, err := NewRedigoBackend(pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.TryAcquire(context.Background(), "key", "token", time.Second); !errors.Is(err, failure) {
		t.Fatalf("TryAcquire = %v", err)
	}
	if _, err := client.Release(context.Background(), "key", "token"); !errors.Is(err, failure) {
		t.Fatalf("Release = %v", err)
	}
	if _, err := client.TryAcquire(context.Background(), "key", "token", 0); err == nil {
		t.Fatal("zero acquire TTL accepted")
	}
	if _, err := client.Refresh(context.Background(), "key", "token", -time.Second); err == nil {
		t.Fatal("negative refresh TTL accepted")
	}
}
