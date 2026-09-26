# redislock

基于 Redis 的 Go 租约锁工具库，支持单节点锁、可选自动续期和多节点 Redlock。
要求 Go 1.25 或更高版本；内置驱动为 Redigo，也可实现 `Backend` 接入其他 Redis 驱动。

当前 API 尚未发布稳定版本。采用 [MIT 许可证](LICENSE)。

## 安装

上传仓库后可通过以下命令安装：

```sh
go get github.com/hxy2321548628/redislock
```

## 使用

开启自动续期后，取消获取时的 context 不会释放租约。请始终安排 `Release`；遗漏释放可能在续期持续成功时无限持锁。

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/gomodule/redigo/redis"
    "github.com/hxy2321548628/redislock"
)

func main() {
    pool := &redis.Pool{
        MaxIdle: 2,
        MaxActive: 10,
        Wait: true,
        DialContext: func(ctx context.Context) (redis.Conn, error) {
            return redis.DialContext(ctx, "tcp", "127.0.0.1:6379",
                redis.DialConnectTimeout(time.Second),
                redis.DialReadTimeout(time.Second),
                redis.DialWriteTimeout(time.Second))
        },
    }
    defer pool.Close()

    client, err := redislock.NewRedigoBackend(pool)
    if err != nil {
        log.Print(err)
        return
    }
    lock, err := redislock.NewLock("myapp:locks:daily-job", client,
        redislock.WithTTL(10*time.Second),
        redislock.WithAutoRenew())
    if err != nil {
        log.Print(err)
        return
    }

    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer cancel()
    lease, err := lock.Acquire(ctx) // TryAcquire(ctx) 只尝试一次。
    if err != nil {
        log.Print(err)
        return
    }
    defer func() {
        // 获取时的 ctx 可能已经取消，释放使用独立的有界 context。
        releaseCtx, stop := context.WithTimeout(context.Background(), time.Second)
        defer stop()
        if err := lease.Release(releaseCtx); err != nil {
            log.Printf("release: %v", err)
        }
    }()

    // 示例用定时器代替可取消的业务。实际工作应响应租约丢失与业务取消。
    select {
    case <-lease.Done():
        log.Printf("lease lost: %v", lease.Err())
    case <-ctx.Done():
        log.Print(ctx.Err())
    case <-time.After(100*time.Millisecond):
        log.Print("work completed")
    }
}
```

完整的外部包示例见 [example_test.go](example_test.go)。示例需要 Redis；普通 `go test` 编译示例而不执行网络操作。

可直接运行的示例见 [example/README.md](example/README.md)：使用 Docker Compose 启动三个独立 Redis 节点，演示单节点锁、自动续期、Redlock、争用与取消。

## 行为约定

| 项目 | 行为 |
| --- | --- |
| Redis key | 原样使用传入 key；例如 `NewLock("myapp:locks:job", ...)`，库不添加前缀 |
| TTL | 默认 30 秒；至少 1 毫秒，向下取整到毫秒 |
| 重试 | 默认每次随机等待 `[25ms, 50ms)`；`WithRetryInterval` 设置上界 |
| 自动续期 | 默认关闭；`WithAutoRenew()` 开启，只支持单节点锁 |
| context | 仅控制当前操作；获取成功后取消获取 context 不会停止续期 |
| 生命周期 | 始终调用 `Release`；业务应监听 `Done` 并停止失去租约后的工作 |
| 重入与公平性 | 不可重入；等待者之间不保证公平性 |
| 并发 | `Lock`、`Redlock` 和 `Lease` 的方法可并发调用，`Lease` 不可复制 |
| 连接池 | 调用者创建和关闭，库不接管生命周期；应设置网络超时 |

`Lock` 为可复用配置，每次成功获取生成独立的 `Lease` 和随机所有者 token。
释放与续期通过 Lua 原子校验 token；脚本使用 `EVALSHA`，遇到 `NOSCRIPT` 自动回退到 `EVAL`。
自动续期根据剩余有效期调度，续期请求不会被授予超出已知有效期的 deadline。

### 错误与释放

使用 `errors.Is` 识别 `ErrLocked`、`ErrNotOwner`、`ErrExpired` 和 context 错误。

- `ErrLocked`：单节点被占用，或 Redlock 未获得多数节点。
- `ErrExpired`：获取结束时已无有效时间，或持有期间本地有效期结束。
- `ErrNotOwner`：释放/续期时 token 不再拥有锁，或释放未达到多数节点。
- 网络、协议和 context 错误保留原始错误链。

`Acquire(ctx)` 只重试纯争用错误。获取、清理、过期等错误直接返回；如果争用伴随清理错误，即使 `errors.Is(err, ErrLocked)` 为真，也不会自动重试。

`Lease.Err()` 保留首次终止原因；`Release(ctx)` 返回本次释放结果。释放发生网络错误后，可换用新的有界 context 重试；后续成功释放返回 `nil`，不会清除此前的租约终止原因。得到明确的 `nil` 或 `ErrNotOwner` 后，后续使用有效 context 的调用返回同一结果，不再访问 Redis。
等待其他 `Release` 调用期间也响应 context 取消；尚未开始释放就取消的调用不会停止续期或修改租约终止原因。
如果 Redis 已执行释放但响应丢失，重试可能返回 `ErrNotOwner`；它不能区分“此前释放成功”和“锁已过期”。业务应在释放前结束受保护工作；不能因为释放失败或随后重试成功而恢复旧租约下的工作。

```go
err := lease.Release(releaseCtx) // releaseCtx 是独立、有超时的 context。
switch {
case err == nil:
    // 本次释放成功，或此前成功结果已缓存。
case errors.Is(err, redislock.ErrNotOwner):
    // 本次未确认所有权，不再重试；不能据此推断此前业务一直持有锁。
default:
    // 记录错误；如需重试，另建有界 context。残留 key 仍受 TTL 约束。
}
```

上面的错误处理片段需要导入标准库 `errors`。Redlock 采用严格报告策略：即使多数节点已释放，少数节点的操作错误也会返回。

获取结果不确定或 Redlock 获取失败时，库会以独立的 **1 秒清理总预算**尝试释放当前 token。因此调用可能晚于获取 context 的 deadline 返回。清理失败会合并到返回错误中，残留 key 最终由 TTL 回收。

### 自定义 Backend

`Backend` 必须支持并发、及时响应 context，并保证获取、比较后释放、比较后续期的原子性。
`Refresh` 不能重建已失效的 key。错误返回可能意味着命令已执行但响应丢失。
Backend 接收完整 key，不应修改命名空间或再次添加前缀。传入非 nil、已初始化的实现；不要传入含 nil 指针的接口值。

## Redlock

```go
// node1、node2、node3 是分别连接独立 Redis 服务的 Backend。
lock, err := redislock.NewRedlock("job",
    []redislock.Backend{node1, node2, node3},
    10*time.Second, // TTL
    50*time.Millisecond, // 每个节点的操作超时
)
```

至少需要三个独立节点，多数为 `N/2 + 1`。同一服务的不同连接、不同 DB 或主从副本不能视作独立节点。服务的独立性由调用者保证，构造函数无法验证部署拓扑。

获取、释放和失败清理均并发访问全部节点，等待全部节点的有界结果后再返回；不会在刚达到多数时留下后台获取请求。使用固定租约，不支持自动续期。重试等待上界为 50ms；节点超时应远小于 TTL，调用者还应设置获取 context 的总预算。
本地有效期从获取开始计算，并扣除 `TTL × 1% + 2ms` 的时钟漂移余量。未获得多数时尝试清理全部节点，成功获取时允许少数节点失败。释放会访问全部节点，并报告任一节点的操作错误（含操作名和从 0 开始的节点索引）；部分释放成功后再次重试，可能因已删除的节点不再匹配 token 而返回 `ErrNotOwner`。

## 安全边界

这是有有效期的租约，不保证任意故障条件下的严格互斥。Redis 主从故障转移可能丢失锁；Redlock 还依赖独立节点及时间漂移等假设。
进程长时间暂停、Redis 时钟变化或网络故障都可能导致所有权丢失。`Done` 是受 goroutine 调度影响的本地通知，不能阻止已经提交或暂停后恢复的旧业务写入。

随机所有者 token 用于防止误释放/误续期，**不是 fencing token**。必须拒绝旧持有者写入时，应让受保护资源验证 fencing token、版本号或其他并发条件。自动续期不能替代这些机制。

参考：[Redis 官方分布式锁文档](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/)。

## 测试

```sh
go test -race ./...
go vet ./...

# 单节点真实 Redis 集成测试（测试 key 使用随机后缀）。
REDIS_ADDR=127.0.0.1:6379 go test -race ./...

# 三节点 Redlock 集成测试。
REDIS_ADDR=127.0.0.1:6379 \
REDIS_ADDRS=127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381 \
go test -race ./...
```

未设置相应环境变量时，集成测试显式跳过。集成测试和基准测试从外部包调用公开 API；故障集成测试通过本地 TCP 代理丢弃 Redis 已执行命令的响应，覆盖获取清理与释放重试。租约时间相关单元测试使用 Go 1.25 的 `testing/synctest`，通过虚拟时间验证有效期与超时。CI 同时运行单元测试、竞态检测、静态检查和真实 Redis 集成测试。

## 性能测量

基准测试使用真实 Redis，覆盖单节点无竞争、同 key 竞争、不同 key 并发，以及多节点 Redlock。未配置环境变量时跳过：

```sh
REDIS_ADDR=127.0.0.1:6379 \
REDIS_ADDRS=127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381 \
go test -run '^$' -bench . -benchmem -count 3 -cpu 1,4
```

结果包含网络与连接池开销，应在相同 Redis 部署、连接池设置和机器负载下比较。Redlock 并发请求的总耗时受最慢节点及其超时影响；应同时测量健康节点和慢节点场景。

## 从未发布的旧 API 迁移

- `Lease.Unlock(ctx)` 改为 `Lease.Release(ctx)`。
- `RedLock` / `NewRedLock` 改为 `Redlock` / `NewRedlock`。
- 构造函数不再自动添加 `redislock:` 前缀。要与旧版本协调同一把锁，必须显式传入原来的完整 key，例如把 `NewLock("job", ...)` 改为 `NewLock("redislock:job", ...)`；`NewRedlock` 同理。混用不同 key 会失去相互排斥。
