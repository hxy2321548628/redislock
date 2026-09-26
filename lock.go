package redislock

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"
)

var (
	// ErrLocked 表示 key 已被占用，或 Redlock 未获得多数节点。
	ErrLocked = errors.New("redislock: lock is held")
	// ErrNotOwner 表示 token 已不拥有该 key，或本次释放未确认删除多数节点。
	ErrNotOwner = errors.New("redislock: lease is no longer owned")
	// ErrExpired 表示本地保守计算的租约有效期已结束。
	ErrExpired = errors.New("redislock: lease expired")
)

// Lock 是一个 key 的可复用配置，不代表当前已经持有锁。
// 每次成功获取都会返回独立的 Lease，其 token 和生命周期互不复用。
// Lock 可并发使用，必须通过 NewLock 创建；不支持重入，也不保证等待者的公平性。
type Lock struct {
	key            string
	backend        Backend
	opts           options
	validityMargin time.Duration // Redlock 的时钟漂移余量；单节点锁为零。
}

// NewLock 为一个 Redis key 创建可复用的锁配置，不会立即访问 Redis。
// key 原样使用；命名空间和 backend 的资源生命周期都由调用者管理。
func NewLock(key string, backend Backend, opts ...Option) (*Lock, error) {
	if key == "" {
		return nil, errors.New("redislock: empty lock key")
	}
	if backend == nil {
		return nil, errors.New("redislock: nil lock backend")
	}
	config := options{
		ttl:           defaultTTL,
		retryInterval: defaultRetryInterval,
	}
	for _, opt := range opts {
		if opt == nil {
			return nil, errors.New("redislock: nil lock option")
		}
		opt(&config)
	}
	if config.ttl < time.Millisecond {
		return nil, errors.New("redislock: TTL must be at least one millisecond")
	}
	// Redis 的 PX 使用整数毫秒，本地计时也取相同精度，避免高估租约时长。
	config.ttl = config.ttl.Truncate(time.Millisecond)
	if config.retryInterval <= 0 {
		return nil, errors.New("redislock: retry interval must be positive")
	}
	return &Lock{key: key, backend: backend, opts: config}, nil
}

// TryAcquire 只尝试获取一次；锁被占用时返回 ErrLocked。
// ctx 只控制本次获取，不控制成功返回的租约生命周期。
// 获取结果不确定时，用独立且有超时的 context 清理可能已经写入的 key。
func (r *Lock) TryAcquire(ctx context.Context) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// 每次尝试生成新的随机所有者标识；不能跨次复用，否则旧租约可能误操作新锁。
	// token 只用于相等比较，不是能拒绝旧业务写入的递增 fencing token。
	token := cryptorand.Text()
	start := time.Now()
	acquired, err := r.backend.TryAcquire(ctx, r.key, token, r.opts.ttl)
	// 在有效期校验前记录响应，日志耗时也会被计入本次获取耗时。
	slog.Debug("redislock: 后端获取结果", "key", r.key, "acquired", acquired,
		"ttl", r.opts.ttl, "elapsed", time.Since(start), "error", err)
	if err != nil {
		return nil, r.acquireError(err, token)
	}
	if !acquired {
		return nil, ErrLocked
	}
	if err := ctx.Err(); err != nil {
		return nil, r.acquireError(err, token)
	}
	// 从请求开始计时，扣掉网络耗时和漂移余量，而不是从收到成功响应时重新计满 TTL。
	deadline := start.Add(r.opts.ttl - r.validityMargin)
	if !time.Now().Before(deadline) {
		return nil, r.acquireError(ErrExpired, token)
	}
	// 租约使用独立生命周期：获取操作结束或 ctx 取消，不应悄悄终止已约定的续期。
	runCtx, cancel := context.WithCancel(context.Background())
	lease := &Lease{
		key:         r.key,
		token:       token,
		backend:     r.backend,
		ttl:         r.opts.ttl,
		deadline:    deadline,
		cancel:      cancel,
		done:        make(chan struct{}),
		releaseGate: make(chan struct{}, 1),
	}
	go lease.run(runCtx, r.opts.autoRenew)
	return lease, nil
}

// acquireError 清理本次 token，并同时保留获取失败与清理失败的错误链。
func (r *Lock) acquireError(cause error, token string) error {
	slog.Debug("redislock: 获取失败，开始清理", "key", r.key, "error", cause)
	if err := r.cleanup(token); err != nil {
		return errors.Join(cause, fmt.Errorf("redislock: clean up acquisition: %w", err))
	}
	return cause
}

// cleanup 不复用可能已取消的获取 context，单独给予一秒清理总预算。
// 因此获取调用可能晚于原 ctx 的 deadline 返回；清理失败的残留 key 仍由 TTL 回收。
func (r *Lock) cleanup(token string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := r.backend.Release(ctx, r.key, token)
	slog.Debug("redislock: 获取失败清理完成", "key", r.key, "error", err)
	return err
}

// Acquire 在锁被占用时轮询，直到成功或 ctx 被取消。
// 只重试纯争用；后端错误、有效期不足和清理失败都直接交给调用者。
func (r *Lock) Acquire(ctx context.Context) (*Lease, error) {
	for {
		lease, err := r.TryAcquire(ctx)
		if err == nil {
			return lease, nil
		}
		// 此处故意不用 errors.Is：组合错误可能同时包含 ErrLocked 和清理失败，
		// 若继续重试就会掩盖故障，并可能不断留下尚未清理的 key。
		if err != ErrLocked {
			return nil, err
		}

		// 随机抖动分散争用请求，避免多个客户端按固定周期一起冲击 Redis。
		delay := r.opts.retryInterval/2 + time.Duration(rand.Int64N(int64(r.opts.retryInterval-r.opts.retryInterval/2)))
		slog.Debug("redislock: 锁被占用，等待重试", "key", r.key, "delay", delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			slog.Debug("redislock: 等待获取已取消", "key", r.key, "error", ctx.Err())
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
