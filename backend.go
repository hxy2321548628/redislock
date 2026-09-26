package redislock

import (
	"context"
	"time"
)

// Backend 定义锁需要的三个原子操作，将租约逻辑与具体 Redis 驱动解耦。
// 实现必须支持并发调用，并及时响应 context 取消。
// 网络错误可能发生在“命令已执行、响应未收到”之后，因此报错不代表 key 未改变。
// key 已包含调用者需要的命名空间，实现不得再修改或添加前缀。
type Backend interface {
	// TryAcquire 仅在 key 不存在时，原子地写入 token 并设置 TTL。
	// 只有锁已被占用时才返回 false, nil；写入和过期设置不能拆成两条命令。
	TryAcquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error)
	// Release 原子地比较 token 并删除 key，避免旧持有者误删新持有者的锁。
	// key 不存在、已过期或 token 不匹配时，返回 false, nil。
	Release(ctx context.Context, key, token string) (bool, error)
	// Refresh 仅为仍存在且 token 匹配的 key 原子重设 TTL。
	// key 不存在或 token 不匹配时返回 false, nil，绝不能重新创建已失效的锁。
	Refresh(ctx context.Context, key, token string, ttl time.Duration) (bool, error)
}
