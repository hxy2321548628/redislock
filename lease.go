package redislock

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Lease 表示一次成功获取的租约，通过 Lock 或 Redlock 获取，其方法可并发调用。
// Lease 含同步状态，不能复制；应始终通过指针使用。
// Done 关闭后业务必须停止受保护的工作，并最终调用 Release 尝试清理。
type Lease struct {
	key      string
	token    string
	backend  Backend
	ttl      time.Duration
	deadline time.Time // 初始保守截止时间；续期后的截止时间只由 run 的局部变量维护。
	cancel   context.CancelFunc
	done     chan struct{}

	once sync.Once  // 多个终止来源竞争时，只记录第一个原因并关闭一次 done。
	mu   sync.Mutex // 保护 err；持锁期间不执行网络操作。
	err  error      // 租约首次终止原因，与后续 Release 尝试的结果分开保存。

	// 容量为 1 的 channel 充当可取消的互斥门；同时只有一个 Release 请求访问后端。
	// released 和 releaseErr 都受这道门保护，不需要再用 mu。
	releaseGate chan struct{}
	released    bool  // 已获得明确释放结果，不代表该结果一定是成功。
	releaseErr  error // 缓存 nil 或 ErrNotOwner；网络错误不缓存，允许重新尝试。
}

// Done 在租约释放、过期或续期/释放失败时关闭，所有监听者都能收到通知。
// 关闭时机受 goroutine 调度影响；它无法阻止已提交或暂停后恢复的旧业务写入。
func (l *Lease) Done() <-chan struct{} { return l.done }

// Err 返回首次终止原因；租约尚未终止或正常释放时返回 nil。
// 后续 Release 成功也不会清除先前的终止错误；仅凭 Err 为 nil 无法判断是否仍持锁。
func (l *Lease) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// Release 停止续期，并只释放属于本租约 token 的 key。
// 返回本次尝试的结果，与 Err 保存的首次终止原因相互独立。
// 后端报错后可以用新的 context 重试；得到 nil 或 ErrNotOwner 后会缓存结果，
// 后续使用未取消 context 的调用直接返回缓存值，不再访问 Redis。
// Redlock 会报告任一节点的错误，即使多数节点已释放；重试时已删除的节点不再
// 匹配 token，因此可能返回 ErrNotOwner，而不是 nil。
// 应使用独立于获取操作且有超时的 ctx；它也约束等待其他 Release 调用的时间。
// 开始释放前就已取消的调用，不会停止续期或改变租约的终止原因。
func (l *Lease) Release(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// 普通 Mutex.Lock 不能监听 ctx；channel 配合 select 让排队的调用者能及时退出。
	select {
	case l.releaseGate <- struct{}{}:
		defer func() { <-l.releaseGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// 获得门后才读写释放状态；等待过程中 ctx 可能已取消，所以上面再次检查。
	if l.released {
		return l.releaseErr
	}
	// 先停止续期，再释放。即便在途续期与释放交错，后端也必须保证 Refresh 不重建 key。
	l.cancel()
	released, err := l.backend.Release(ctx, l.key, l.token)
	if err != nil {
		l.finish(err)
		return err
	}
	l.released = true
	if !released {
		l.releaseErr = ErrNotOwner
	}
	l.finish(l.releaseErr)
	return l.releaseErr
}

// run 是租约的后台生命周期管理器：固定租约等待过期，自动续期租约提前刷新 TTL。
func (l *Lease) run(ctx context.Context, autoRenew bool) {
	deadline := l.deadline
	if !autoRenew {
		l.waitForExpiry(deadline)
		return
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			l.finish(ErrExpired)
			return
		}
		// 正常约 TTL/3 后续期；若获取响应很慢，则按剩余有效期提前调度。
		timer.Reset(min(l.ttl/3, remaining/3))
		select {
		case <-ctx.Done():
			// Release 可能仍在等待 Redis；停止续期后仍需保留到期通知。
			l.waitForExpiry(deadline)
			return
		case <-timer.C:
		}
		start := time.Now()
		if !start.Before(deadline) {
			l.finish(ErrExpired)
			return
		}
		// 一次续期最多使用 TTL/3，且不能越过旧租约的已知截止时间。
		renewDeadline := start.Add(l.ttl / 3)
		if deadline.Before(renewDeadline) {
			renewDeadline = deadline
		}
		renewCtx, cancel := context.WithDeadline(ctx, renewDeadline)
		ok, err := l.backend.Refresh(renewCtx, l.key, l.token, l.ttl)
		cancel()
		// 主动释放导致的续期取消不是续期故障；终止结果由释放或到期来确定。
		if ctx.Err() != nil {
			l.waitForExpiry(deadline)
			return
		}
		// 即使收到成功响应，若旧有效期已过，也无法再证明这段时间一直持锁。
		if !time.Now().Before(deadline) {
			l.finish(ErrExpired)
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
		// 同样从续期请求开始计时，避免把响应传输耗时误算成额外有效期。
		deadline = start.Add(l.ttl)
	}
}

// waitForExpiry 等待本地截止时间，或在其他路径已终止租约时提前退出。
func (l *Lease) waitForExpiry(deadline time.Time) {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-l.done:
	case <-timer.C:
		l.finish(ErrExpired)
	}
}

// finish 是所有终止路径的汇合点，sync.Once 防止重复关闭 channel 引发 panic。
// 先写入 err，再关闭 done，使收到通知的调用者能读到已经记录的终止原因。
func (l *Lease) finish(err error) {
	l.once.Do(func() {
		l.cancel()
		l.mu.Lock()
		l.err = err
		l.mu.Unlock()
		close(l.done)
	})
}
