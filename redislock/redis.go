package redislock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

// ILockClient is the Redis operation set required by this package.
type ILockClient interface {
	TryAcquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error)
	Release(ctx context.Context, key, token string) (bool, error)
	Refresh(ctx context.Context, key, token string, ttl time.Duration) (bool, error)
}

// Client adapts a redigo pool to ILockClient. The caller owns and closes the pool.
type Client struct {
	pool *redis.Pool
}

// NewClient wraps a caller-owned redigo pool.
// 初始化一个 redis 客户端
func NewClient(pool *redis.Pool) (*Client, error) {
	if pool == nil {
		return nil, errors.New("redislock: nil redis pool")
	}
	return &Client{pool: pool}, nil
}

// 尝试获取锁
func (c *Client) TryAcquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	// 从连接池中获取一个连接
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	// NX 表示不存在则设置, PX 表示设置过期时间
	reply, err := redis.DoContext(conn, ctx, "SET", key, token, "NX", "PX", ttl.Milliseconds())
	if err != nil {
		return false, err
	}
	if reply == nil {
		return false, nil
	}
	value, err := redis.String(reply, nil)
	if err != nil {
		return false, err
	}
	if value != "OK" {
		return false, fmt.Errorf("redislock: unexpected SET reply %q", value)
	}
	return true, nil
}

func (c *Client) Release(ctx context.Context, key, token string) (bool, error) {
	return c.evalBool(ctx, compareAndDeleteScript, key, token)
}

func (c *Client) Refresh(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	return c.evalBool(ctx, compareAndExpireScript, key, token, ttl.Milliseconds())
}

func (c *Client) evalBool(ctx context.Context, script, key, token string, args ...any) (bool, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	// 初始化参数
	commandArgs := make([]any, 0, 4+len(args))
	commandArgs = append(commandArgs, script, 1, key, token)
	commandArgs = append(commandArgs, args...)
	reply, err := redis.DoContext(conn, ctx, "EVAL", commandArgs...)
	if err != nil {
		return false, err
	}
	value, err := redis.Int(reply, nil)
	if err != nil {
		return false, err
	}
	return value == 1, nil
}
