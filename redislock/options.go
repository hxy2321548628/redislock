package redislock

import "time"

const (
	defaultTTL           = 30 * time.Second      // 30 秒
	defaultRetryInterval = 50 * time.Millisecond // 50 毫秒
)

type LockOptions struct {
	ttl           time.Duration
	retryInterval time.Duration
	autoRenew     bool
}

// Go 函数式选项模式(Functional Options Pattern)
type LockOptionFunc func(*LockOptions)

// WithTTL sets the lease duration. The default is 30 seconds.
func WithTTL(ttl time.Duration) LockOptionFunc {
	return func(o *LockOptions) { o.ttl = ttl }
}

// WithRetryInterval sets how often Lock retries while another client holds the lock.
func WithRetryInterval(interval time.Duration) LockOptionFunc {
	return func(o *LockOptions) { o.retryInterval = interval }
}

// WithAutoRenew keeps a lease alive until it is unlocked or renewal fails.
func WithAutoRenew() LockOptionFunc {
	return func(o *LockOptions) { o.autoRenew = true }
}
