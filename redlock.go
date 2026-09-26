package redislock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Redlock 用同一个 token 和固定 TTL，在多个独立 Redis 节点上获取多数派租约。
// 必须通过 NewRedlock 创建，其方法可并发调用。
type Redlock struct {
	lock *Lock
}

// NewRedlock 要求至少三个独立 Redis 后端，并指定固定 TTL 和单节点超时。
// 同一服务的不同连接、不同 DB 或主从副本不算独立节点，部署独立性由调用者保证。
// key 原样使用；不支持自动续期，争用重试的等待上界为 50 毫秒。
// 节点操作并发执行，等待全部结果后返回；每次操作由其 context 约束。
func NewRedlock(key string, backends []Backend, ttl, nodeTimeout time.Duration) (*Redlock, error) {
	if len(backends) < 3 {
		return nil, errors.New("redislock: Redlock requires at least three independent nodes")
	}
	ttl = ttl.Truncate(time.Millisecond)
	if ttl <= 2*time.Millisecond {
		return nil, errors.New("redislock: Redlock TTL must exceed two milliseconds")
	}
	if nodeTimeout <= 0 {
		return nil, errors.New("redislock: node timeout must be positive")
	}
	for _, backend := range backends {
		if backend == nil {
			return nil, errors.New("redislock: nil Redlock backend")
		}
	}
	// 把多个后端组合成一个 Backend，复用 Lock 的 token、重试、有效期和清理逻辑。
	inner, err := NewLock(key, &quorumBackend{
		// 复制切片，避免调用者替换切片元素时改变节点集合；底层后端仍由双方共享。
		backends:    append([]Backend(nil), backends...),
		nodeTimeout: nodeTimeout,
	}, WithTTL(ttl))
	if err != nil {
		return nil, err
	}
	// 预留 TTL 的 1% 加 2ms 作为漂移余量；这依赖时间漂移假设，不是绝对互斥保证。
	inner.validityMargin = ttl/100 + 2*time.Millisecond
	return &Redlock{lock: inner}, nil
}

// TryAcquire 只尝试获取一次多数派租约。
func (r *Redlock) TryAcquire(ctx context.Context) (*Lease, error) {
	return r.lock.TryAcquire(ctx)
}

// Acquire 仅在纯争用时重试，直到成功或 ctx 被取消。
// 未获得多数时的节点错误、有效期不足和清理失败都会直接返回。
func (r *Redlock) Acquire(ctx context.Context) (*Lease, error) {
	return r.lock.Acquire(ctx)
}

// quorumBackend 将多节点结果转换为 Lock 所需的单次获取/释放结果。
type quorumBackend struct {
	backends    []Backend
	nodeTimeout time.Duration
}

func (r *quorumBackend) TryAcquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	successes, acquireErr := r.doOnNodes(ctx, "acquire", func(ctx context.Context, backend Backend) (bool, error) {
		return backend.TryAcquire(ctx, key, token, ttl)
	})

	// 外层 Lock 负责有效期校验和清理。不足多数时可能仍写入了部分节点，
	// 所以最终返回 ErrLocked 而非 false, nil，以触发 Lock 的失败清理路径。
	if err := ctx.Err(); err != nil {
		return false, err
	}
	// 已获得多数时允许少数节点失败；是否仍有有效租期由外层 Lock 再检查。
	if successes >= r.quorum() {
		return true, nil
	}
	if acquireErr != nil {
		return false, acquireErr
	}
	return false, ErrLocked
}

func (r *quorumBackend) Release(ctx context.Context, key, token string) (bool, error) {
	released, releaseErr := r.doOnNodes(ctx, "release", func(ctx context.Context, backend Backend) (bool, error) {
		return backend.Release(ctx, key, token)
	})
	// 释放采用严格报告：即使多数已删除，仍返回少数节点的错误，让调用者知道有残留风险。
	return released >= r.quorum(), releaseErr
}

// Refresh 仅用于满足 Backend 接口；NewRedlock 不开启续期，不会走到这里。
func (r *quorumBackend) Refresh(context.Context, string, string, time.Duration) (bool, error) {
	return false, errors.New("redislock: Redlock renewal is unsupported")
}

// quorum 取严格多数：任意两个多数派必有交集，这是多数派锁推理的基础之一。
func (r *quorumBackend) quorum() int {
	return len(r.backends)/2 + 1
}

// doOnNodes 并发执行并等待所有节点完成，不能一达到多数就直接返回：
// 尚未完成的获取请求可能在失败清理后，甚至在调用者释放后，才写入 key。
// 超时依赖 Backend 及时响应 context；本函数不会强行终止不合作的后端。
func (r *quorumBackend) doOnNodes(ctx context.Context, operation string, call func(context.Context, Backend) (bool, error)) (int, error) {
	type result struct {
		ok  bool
		err error
	}
	// 每个 goroutine 只写自己的切片元素；Wait 返回后才统一读取，无需额外加锁。
	results := make([]result, len(r.backends))
	var wg sync.WaitGroup
	for i, backend := range r.backends {
		wg.Go(func() {
			// 同时受总 ctx 和单节点超时限制；先到期的一方生效。
			nodeCtx, cancel := context.WithTimeout(ctx, r.nodeTimeout)
			defer cancel()
			results[i].ok, results[i].err = call(nodeCtx, backend)
		})
	}
	wg.Wait()

	// 按传入的节点顺序汇总，避免错误文本因并发完成顺序不同而变化。
	successes := 0
	var err error
	for i, result := range results {
		if result.err != nil {
			err = errors.Join(err, fmt.Errorf("redislock: %s node %d: %w", operation, i, result.err))
		} else if result.ok {
			successes++
		}
	}
	return successes, err
}
