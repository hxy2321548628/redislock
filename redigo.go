package redislock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

// 参数 1 表示脚本接收一个 key；Script 会计算并复用脚本 SHA，减少重复传输。
var (
	releaseScript = redis.NewScript(1, compareAndDeleteScript)
	refreshScript = redis.NewScript(1, compareAndExpireScript)
)

// RedigoBackend 将 Redigo 连接池适配为 Backend。
// 每次操作单独借用连接，不跨 goroutine 共享同一连接；连接池由调用者创建和关闭。
type RedigoBackend struct {
	pool *redis.Pool
}

// NewRedigoBackend 包装调用者提供的连接池，不接管连接池的生命周期。
func NewRedigoBackend(pool *redis.Pool) (*RedigoBackend, error) {
	if pool == nil {
		return nil, errors.New("redislock: nil redis pool")
	}
	return &RedigoBackend{pool: pool}, nil
}

// TryAcquire 仅在 key 不存在时写入 token，TTL 至少为一毫秒。
func (c *RedigoBackend) TryAcquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	if ttl < time.Millisecond {
		return false, errors.New("redislock: TTL must be at least one millisecond")
	}
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return false, err
	}
	// 对池中借出的连接调用 Close，通常是归还连接；失效连接由池丢弃。
	defer conn.Close()

	// NX 表示只在不存在时写入，PX 表示毫秒级 TTL，两者由同一条 SET 原子执行。
	// 若先 SET 再设置过期时间，中途失败可能留下永不过期的锁。
	reply, err := redis.DoContext(conn, ctx, "SET", key, token, "NX", "PX", ttl.Milliseconds())
	if err != nil {
		return false, err
	}
	// SET NX 未满足条件时返回空响应，属于正常争用，不是网络或协议错误。
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

// Release 通过 Lua 原子地比较 token 并删除 key，防止误删其他持有者的锁。
func (c *RedigoBackend) Release(ctx context.Context, key, token string) (bool, error) {
	return c.evalBool(ctx, releaseScript, key, token)
}

// Refresh 通过 Lua 仅为 token 匹配的现有 key 重设 TTL，不会重建已过期的 key。
func (c *RedigoBackend) Refresh(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	if ttl < time.Millisecond {
		return false, errors.New("redislock: TTL must be at least one millisecond")
	}
	return c.evalBool(ctx, refreshScript, key, token, ttl.Milliseconds())
}

// evalBool 执行返回 0/1 的脚本，并严格检查响应类型与取值。
func (c *RedigoBackend) evalBool(ctx context.Context, script *redis.Script, args ...any) (bool, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return false, err
	}
	// 对池中借出的连接调用 Close，通常是归还连接；失效连接由池丢弃。
	defer conn.Close()

	// 优先 EVALSHA；若 Redis 未缓存脚本并返回 NOSCRIPT，Redigo 自动回退到 EVAL。
	reply, err := script.DoContext(ctx, conn, args...)
	if err != nil {
		return false, err
	}
	value, err := redis.Int(reply, nil)
	if err != nil {
		return false, err
	}
	if value != 0 && value != 1 {
		return false, fmt.Errorf("redislock: unexpected script reply %d", value)
	}
	return value == 1, nil
}
