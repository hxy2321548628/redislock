# 版本记录

## v0.1.0 — 2026-09-26

首个公开版本，提供基于 Redis 的 Go 租约锁。要求 Go 1.25 或更高版本，采用 MIT 许可证。

### 功能

- 单节点锁：支持 `Acquire` 争用重试和 `TryAcquire` 单次获取。
- 独立租约生命周期：通过 `Release` 释放，通过 `Done` 和 `Err` 观察终止状态。
- 可选自动续期：仅支持单节点锁，获取 context 取消不影响已获得租约的续期。
- 多节点 Redlock：并发访问独立节点，通过多数派和扣除漂移余量的固定租期判断获取结果。
- 内置 Redigo 驱动与可扩展的 `Backend` 接口；Lua 原子校验所有者后释放或续期。
- 获取结果不确定时尝试有界清理；释放遇到网络错误后可使用新的 context 重试。
- 标准库 `slog` 调试日志、Docker Compose 环境和可运行示例。

### 使用与兼容性

```sh
go get github.com/hxy2321548628/redislock@v0.1.0
```

API 尚未稳定，后续版本可能包含不兼容变更。key 原样使用，调用者负责命名空间和连接池生命周期。

这是有有效期的租约，不保证任意故障条件下的严格互斥。随机所有者 token 不是 fencing token；必须拒绝旧持有者写入时，受保护资源应验证 fencing token、版本号或其他并发条件。

开启自动续期后必须安排 `Release`，并监听 `Done` 停止失去租约后的工作。Redlock 要求独立 Redis 节点，不支持自动续期。

从未发布的旧 API 迁移时，`Lease.Unlock` 改为 `Lease.Release`，`RedLock` / `NewRedLock` 改为 `Redlock` / `NewRedlock`；构造函数不再自动添加 `redislock:` 前缀。

### 验证

- 单元测试、竞态检测、静态检查和构建通过。
- 真实 Redis 单节点、三节点 Redlock 和响应丢失故障集成测试通过。
- 单节点、自动续期和 Redlock 示例运行通过。
