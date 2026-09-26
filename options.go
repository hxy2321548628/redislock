package redislock

import "time"

const (
	defaultTTL           = 30 * time.Second
	defaultRetryInterval = 50 * time.Millisecond
)

// options 在构造时填充，之后由 Lock 只读使用；每次获取的状态保存在 Lease 中。
type options struct {
	ttl           time.Duration
	retryInterval time.Duration
	autoRenew     bool
}

// Option 使用函数选项模式配置 Lock，由 With 系列函数创建。
// 选项只修改候选配置，NewLock 在应用所有选项后统一校验；重复设置时后者覆盖前者。
type Option func(*options)

// WithTTL 设置租约时长，默认 30 秒；构造时要求至少 1 毫秒，并向下取整到毫秒。
func WithTTL(ttl time.Duration) Option {
	return func(o *options) { o.ttl = ttl }
}

// WithRetryInterval 设置争用重试的等待上界，默认 50 毫秒。
// 每次在 [interval/2, interval) 内随机等待，避免多个等待者同时重试。
func WithRetryInterval(interval time.Duration) Option {
	return func(o *options) { o.retryInterval = interval }
}

// WithAutoRenew 开启自动续期，默认关闭；释放、过期或续期失败时停止。
// 取消获取锁时的 context 不会停止续期，调用者必须安排 Release。
// 如果遗漏释放且续期一直成功，锁可能被无限期持有。
func WithAutoRenew() Option {
	return func(o *options) { o.autoRenew = true }
}
