# 可运行的 Redis 锁示例

需要 Go 1.25+、Docker 和 Docker Compose。以下命令均在仓库根目录执行。
Go 示例在宿主机运行，Redis 通过 Docker 启动。
示例默认开启 `slog` DEBUG 日志，可以观察获取、续期、释放和各 Redlock 节点的结果；将 `main.go` 中的 `slog.LevelDebug` 改为 `slog.LevelInfo` 即可隐藏库的调试日志。

日志第一行显示源码位置，第二行缩进四个空格，依次显示时间、PID、级别、消息和业务字段。省略头部的 `time=`、`pid=`、`level=`、`msg=`；业务字段保留 `名称=值`：

```text
/path/to/redislock/example/main.go:71
    2026/09/26 15:41:09 [540574] INFO  示例配置 mode=renew, key=example:locks:job, ttl=3s, hold=5s
/path/to/redislock/example/main.go:108
    2026/09/26 15:41:14 [540574] INFO  租约已释放
```

源码位置来自 `slog.Record.PC`，指向实际调用 `slog.Info/Debug` 的文件和行号，而非日志格式化函数。通常输出完整的 `文件路径:行号`，支持该格式的 IDE 终端可点击跳转；是否需要 Ctrl/Cmd 等修饰键取决于终端。使用 `-trimpath` 构建时，源码位置可能为模块路径。

业务字段按日志调用时的顺序排列，空错误值省略。终端格式实现在 `logging.go` 中，库的日志调用不受影响。

## 1. 启动三个 Redis 节点

```sh
docker compose -f example/compose.yaml up -d --wait
docker compose -f example/compose.yaml ps
```

| 服务 | 宿主机地址 | 用途 |
| --- | --- | --- |
| redis1 | 127.0.0.1:16379 | 单节点、自动续期、Redlock |
| redis2 | 127.0.0.1:16380 | Redlock |
| redis3 | 127.0.0.1:16381 | Redlock |

这里是 **三个独立 Redis 实例**，不是分片式 Redis Cluster，也没有配置主从复制。
Redlock 要求独立节点；不能把同一 Redis Cluster 客户端当成三个独立后端。
三个本机容器只用于学习，生产环境的节点还需要具有独立的故障域。

容器仅监听宿主机回环地址，不配置密码，不启用持久化，不能直接作为生产部署模板。

## 2. 单节点锁

```sh
go run ./example -mode single
```

使用 redis1，TTL 为 10 秒，模拟业务运行 5 秒，然后释放租约。
`Acquire` 在争用时等待，获取操作的总预算为 10 秒。
如果想立即知道锁是否可用，可在源码中改用 `TryAcquire`，并用 `errors.Is` 判断 `ErrLocked`。

在两个终端中先后执行相同命令，观察第二个进程等待第一个进程释放：

```sh
go run ./example -mode single -key example:locks:shared -hold 5s
```

日志包含进程 PID，便于区分持有者。库不会自动给 key 添加前缀。

## 3. 自动续期

```sh
go run ./example -mode renew
```

初始 TTL 为 3 秒，业务运行 5 秒。开启 `WithAutoRenew` 后，业务可以超过初始 TTL。
示例获取成功后立即取消获取 context，续期仍然继续；业务使用单独的 context。

运行期间可在另一终端观察 Redis 中的剩余 TTL，单位是毫秒：

```sh
docker compose -f example/compose.yaml exec redis1 redis-cli PTTL example:locks:job
```

持续续期时 TTL 会被重新延长；业务完成并释放后返回 `-2`，表示 key 不存在。
自动续期必须配合 `Release`，遗漏释放可能持续持锁。

## 4. Redlock 多节点锁

```sh
go run ./example -mode redlock
```

三个节点并发获取，至少两个成功才可能得到租约。TTL 为 10 秒，每个节点超时为 200ms。
获取耗时和漂移余量会从可用租期中扣除；Redlock 不支持自动续期。

可以停止一个节点观察多数派与严格释放报告：

```sh
docker compose -f example/compose.yaml stop redis3
go run ./example -mode redlock
docker compose -f example/compose.yaml start redis3
```

预期：健康的两个节点足以获取锁，业务正常完成；释放仍会访问全部节点，因 redis3
不可达而返回错误，示例以非零状态退出。这不代表健康节点上的锁没有删除。
对于结果不确定的释放，库允许使用新的有界 context 重试；重试也可能返回 `ErrNotOwner`。

## 5. 取消与过期

任意示例运行过程中按 `Ctrl+C`，业务会响应取消，并用独立的一秒 context 尝试释放。
程序通过错误日志和非零退出状态报告取消。

固定租约下故意让业务超过 TTL，可以观察租约过期通知：

```sh
go run ./example -mode single -hold 12s
```

预期约 10 秒时停止业务并报告 `lease expired`；随后释放可能返回 `ErrNotOwner`。
`Done` 是本地通知，不等于 fencing；真实业务必须配合受保护资源的并发校验。

## 自定义端口

如果默认端口已占用，可在当前终端设置端口，后续 Compose 命令沿用同一环境：

```sh
export REDIS1_PORT=17379 REDIS2_PORT=17380 REDIS3_PORT=17381
docker compose -f example/compose.yaml up -d --wait
go run ./example -mode single -addr 127.0.0.1:17379
go run ./example -mode redlock -addrs 127.0.0.1:17379,127.0.0.1:17380,127.0.0.1:17381
```

`go run ./example -h` 可查看全部参数。

## 运行库的集成测试

默认端口下执行：

```sh
REDIS_ADDR=127.0.0.1:16379 \
REDIS_ADDRS=127.0.0.1:16379,127.0.0.1:16380,127.0.0.1:16381 \
go test -race -timeout 60s ./...
```

## 停止并清理

```sh
docker compose -f example/compose.yaml down -v
```

该命令删除本示例的容器、网络及匿名数据卷。示例 Redis 不持久保存数据。
