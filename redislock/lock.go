// Package redislock provides Redis leases acquired by polling.
package redislock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	ErrLocked   = errors.New("redislock: lock is held")
	ErrNotOwner = errors.New("redislock: lease is no longer owned")
	ErrExpired  = errors.New("redislock: lease expired")
)

// Lock describes a lock key and may be used for multiple acquisitions.
// Each successful acquisition returns an independent Lease.
type Lock struct {
	key    string
	client ILockClient
	opts   LockOptions
}

// New creates a reusable lock configuration for one Redis key.
func NewLock(key string, client ILockClient, opts ...LockOptionFunc) (*Lock, error) {
	if key == "" {
		return nil, errors.New("redislock: empty lock key")
	}
	if client == nil {
		return nil, errors.New("redislock: nil lock client")
	}
	config := LockOptions{
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
	config.ttl = config.ttl.Truncate(time.Millisecond)
	if config.retryInterval <= 0 {
		return nil, errors.New("redislock: retry interval must be positive")
	}
	return &Lock{key: "redislock:" + key, client: client, opts: config}, nil
}

// TryLock attempts one acquisition and returns ErrLocked if the key is held.
func (r *Lock) TryLock(ctx context.Context) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("redislock: generate token: %w", err)
	}
	token := id.String()
	start := time.Now()
	acquired, err := r.client.TryAcquire(ctx, r.key, token, r.opts.ttl)
	if err != nil {
		return nil, errors.Join(err, r.cleanup(token))
	}
	if !acquired {
		return nil, ErrLocked
	}
	if time.Since(start) >= r.opts.ttl {
		return nil, errors.Join(ErrExpired, r.cleanup(token))
	}
	runCtx, cancel := context.WithCancel(context.Background())
	lease := &Lease{ // 初始化一个租约
		key:      r.key,
		token:    token,
		client:   r.client,
		ttl:      r.opts.ttl,
		deadline: start.Add(r.opts.ttl),
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	go lease.run(runCtx, r.opts.autoRenew) // 传入租约是否续期
	return lease, nil
}

func (r *Lock) cleanup(token string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := r.client.Release(ctx, r.key, token)
	return err
}

// Lock polls until it acquires the key or ctx is canceled.
func (r *Lock) Lock(ctx context.Context) (*Lease, error) {
	for {
		lease, err := r.TryLock(ctx) //尝试获取锁
		if err == nil {
			return lease, nil
		}
		if !errors.Is(err, ErrLocked) { // 只放行持有锁错误
			return nil, err
		}

		// NewTimer 是一次性的，触发一次就结束了。如果需要周期性的，要用 time.NewTicker。
		timer := time.NewTimer(r.opts.retryInterval) // 初始化一个计时器
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// Lease represents one successful acquisition. Unlock acts only on this lease's token.
type Lease struct {
	key      string
	token    string
	client   ILockClient
	ttl      time.Duration
	deadline time.Time
	cancel   context.CancelFunc
	done     chan struct{}

	once      sync.Once
	mu        sync.Mutex
	err       error
	releaseMu sync.Mutex
	released  bool
}

// Done closes when the lease is released, expires, or renewal fails.
func (l *Lease) Done() <-chan struct{} { return l.done }

// Err reports why Done closed. It is nil after a successful Unlock.
func (l *Lease) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// Unlock stops renewal and releases the key if this lease still owns it.
// A separate context should be used if the acquisition context has expired.
func (l *Lease) Unlock(ctx context.Context) error {
	l.releaseMu.Lock()
	defer l.releaseMu.Unlock()
	if l.released {
		return l.Err()
	}
	l.cancel()
	released, err := l.client.Release(ctx, l.key, l.token)
	if err != nil {
		l.finish(err)
		return err
	}
	l.released = true
	if !released {
		l.finish(ErrNotOwner)
		return ErrNotOwner
	}
	l.finish(nil)
	return l.Err()
}

func (l *Lease) run(ctx context.Context, autoRenew bool) {
	if !autoRenew {
		timer := time.NewTimer(time.Until(l.deadline))
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
			l.finish(ErrExpired)
		}
		return
	}

	ticker := time.NewTicker(l.ttl / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(ctx, l.ttl/3)
			ok, err := l.client.Refresh(renewCtx, l.key, l.token, l.ttl)
			cancel()
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				l.finish(fmt.Errorf("redislock: renew lease: %w", err))
				return
			}
			if !ok {
				l.finish(ErrNotOwner)
				return
			}
		}
	}
}

func (l *Lease) finish(err error) {
	l.once.Do(func() {
		l.cancel()
		l.mu.Lock()
		l.err = err
		l.mu.Unlock()
		close(l.done)
	})
}
