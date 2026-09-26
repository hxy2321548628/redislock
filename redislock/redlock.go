package redislock

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// RedLock acquires a quorum of independent Redis nodes with one token and a fixed TTL.
type RedLock struct {
	lock *Lock
}

// NewRedLock requires independent Redis clients, a fixed TTL, and a per-node timeout.
func NewRedLock(key string, clients []ILockClient, ttl, nodeTimeout time.Duration) (*RedLock, error) {
	if len(clients) < 3 {
		return nil, errors.New("redislock: RedLock requires at least three independent nodes")
	}
	ttl = ttl.Truncate(time.Millisecond)
	if ttl <= 2*time.Millisecond {
		return nil, errors.New("redislock: RedLock TTL must exceed two milliseconds")
	}
	if nodeTimeout <= 0 {
		return nil, errors.New("redislock: node timeout must be positive")
	}
	for _, client := range clients {
		if client == nil {
			return nil, errors.New("redislock: nil RedLock client")
		}
	}
	inner, err := NewLock(key, &redClient{
		clients:     append([]ILockClient(nil), clients...),
		nodeTimeout: nodeTimeout,
	}, WithTTL(ttl))
	if err != nil {
		return nil, err
	}
	return &RedLock{lock: inner}, nil
}

// TryLock attempts one quorum acquisition.
func (r *RedLock) TryLock(ctx context.Context) (*Lease, error) {
	return r.lock.TryLock(ctx)
}

// Lock retries quorum acquisition until it succeeds or ctx is canceled.
func (r *RedLock) Lock(ctx context.Context) (*Lease, error) {
	return r.lock.Lock(ctx)
}

type redClient struct {
	clients     []ILockClient
	nodeTimeout time.Duration
}

func (r *redClient) TryAcquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	start := time.Now()
	successes := 0
	var acquireErr error
	for _, client := range r.clients {
		nodeCtx, cancel := context.WithTimeout(ctx, r.nodeTimeout)
		ok, err := client.TryAcquire(nodeCtx, key, token, ttl)
		cancel()
		if err != nil {
			acquireErr = errors.Join(acquireErr, err)
		} else if ok {
			successes++
		}
	}

	drift := ttl/100 + 2*time.Millisecond
	if successes >= r.quorum() && time.Since(start)+drift < ttl && ctx.Err() == nil {
		return true, nil
	}

	cleanupErr := r.cleanup(key, token)
	if err := ctx.Err(); err != nil {
		return false, errors.Join(err, cleanupErr)
	}
	if cleanupErr != nil {
		return false, fmt.Errorf("redislock: release partial RedLock: %w", cleanupErr)
	}
	if acquireErr != nil {
		return false, acquireErr
	}
	return false, nil
}

func (r *redClient) Release(ctx context.Context, key, token string) (bool, error) {
	released := 0
	var releaseErr error
	for _, client := range r.clients {
		nodeCtx, cancel := context.WithTimeout(ctx, r.nodeTimeout)
		ok, err := client.Release(nodeCtx, key, token)
		cancel()
		if err != nil {
			releaseErr = errors.Join(releaseErr, err)
		} else if ok {
			released++
		}
	}
	return released >= r.quorum(), releaseErr
}

func (r *redClient) Refresh(context.Context, string, string, time.Duration) (bool, error) {
	// TODO 添加续期功能
	return false, errors.New("redislock: RedLock renewal is unsupported")
}

func (r *redClient) cleanup(key, token string) error {
	var cleanupErr error
	for _, client := range r.clients {
		ctx, cancel := context.WithTimeout(context.Background(), r.nodeTimeout)
		_, err := client.Release(ctx, key, token)
		cancel()
		cleanupErr = errors.Join(cleanupErr, err)
	}
	return cleanupErr
}

func (r *redClient) quorum() int {
	return len(r.clients)/2 + 1
}
