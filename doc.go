// Package redislock 提供基于 Redis、通过随机 token 标识所有者的租约锁。
//
// 建议按以下顺序阅读源码：lock.go（获取与重试）、lease.go（租约生命周期）、
// backend.go（原子操作契约）、redigo.go 和 lua.go（Redis 实现），最后看
// redlock.go（多节点多数派）。options.go 负责构造选项。
//
// Lock 是某个 key 的可复用配置；TryAcquire 或 Acquire 成功后返回一次性的 Lease。
// key 原样使用，命名空间由调用者管理。锁不可重入，也不保证等待者的公平性。
// NewRedigoBackend 包装调用者持有的 Redigo 连接池；其他驱动可通过实现 Backend 接入。
//
// 获取时的 context 只控制获取操作。WithAutoRenew 开启独立的后台续期，取消获取
// context 不会停止续期。应始终使用独立且有超时的 context 调用 Lease.Release。
// 开启续期却遗漏释放，可能在续期持续成功时无限持锁。
// 业务必须监听 Lease.Done，并在租约丢失时停止受保护工作。Lease.Err 保留首次终止
// 原因，Release 则报告本次释放结果。Lease 含同步状态，不可复制。
//
// Redlock 在独立 Redis 节点间使用多数派、固定租期和时钟漂移余量，不支持自动续期。
// 获取、释放和清理并发访问节点，收齐全部结果后再返回。同一服务的不同连接或副本
// 不能当作独立节点；部署独立性和时间漂移假设需要调用者理解并保证。
//
// 租约具有有效期，不等于任意故障条件下的严格互斥。进程暂停、服务器时钟变化和
// 故障转移都可能使所有权失效。随机 token 用来防止误释放，不是 fencing token。
// Done 只是受调度影响的本地通知；必须拒绝旧业务写入时，受保护资源还需校验
// fencing token、版本号或其他并发条件。
package redislock
