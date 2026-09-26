package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/hxy2321548628/redislock"
)

func main() {
	mode := flag.String("mode", "single", "模式：single（单节点）、renew（自动续期）、redlock（多数派）")
	addr := flag.String("addr", "127.0.0.1:16379", "single / renew 使用的 Redis 地址")
	addrs := flag.String("addrs", "127.0.0.1:16379,127.0.0.1:16380,127.0.0.1:16381", "redlock 使用的独立节点地址，逗号分隔")
	key := flag.String("key", "example:locks:job", "完整的锁 key；多个进程使用同一 key 才会相互争用")
	hold := flag.Duration("hold", 5*time.Second, "模拟业务耗时，例如 5s；业务同时响应租约丢失和 Ctrl+C")
	flag.Parse()
	// 在应用入口开启 DEBUG，库中直接使用 slog.Debug 输出调试信息。
	slog.SetDefault(slog.New(newConsoleHandler(os.Stderr, slog.LevelDebug)))

	// 业务取消和租约释放使用不同的 context，Ctrl+C 后仍可以尝试清理锁。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, *mode, *addr, *addrs, *key, *hold)
	stop()
	if err != nil {
		slog.Error("示例执行失败", "error", err)
		os.Exit(1) // run 的 defer 已执行，连接池和租约不会因直接退出而跳过清理。
	}
}

func run(ctx context.Context, mode, addr, addrs, key string, hold time.Duration) (runErr error) {
	if mode != "single" && mode != "renew" && mode != "redlock" {
		return fmt.Errorf("未知模式 %q，请选择 single、renew 或 redlock", mode)
	}
	if hold <= 0 {
		return errors.New("hold 必须大于零")
	}
	addresses := []string{addr}
	if mode == "redlock" {
		addresses = strings.Split(addrs, ",")
	}
	var backends []redislock.Backend
	for _, address := range addresses {
		address = strings.TrimSpace(address)
		if address == "" {
			return errors.New("Redis 地址不能为空")
		}
		pool := newPool(address)
		defer pool.Close() // 在后面注册的租约释放执行完毕后，再关闭连接池。
		backend, err := redislock.NewRedigoBackend(pool)
		if err != nil {
			return err
		}
		backends = append(backends, backend)
	}

	ttl := 10 * time.Second
	if mode == "renew" {
		ttl = 3 * time.Second // 默认业务耗时 5 秒，超过初始 TTL，用于展示自动续期。
	}
	slog.Info("示例配置", "mode", mode, "key", key, "ttl", ttl, "hold", hold)
	slog.Info("尝试获取锁：争用时最多等待 10 秒")
	acquireCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var lease *redislock.Lease
	var err error
	if mode == "redlock" {
		// 三个节点中至少两个成功，且获取结束时仍有有效租期，才能得到 Lease。
		lock, createErr := redislock.NewRedlock(key, backends, ttl, 200*time.Millisecond)
		if createErr != nil {
			return createErr
		}
		lease, err = lock.Acquire(acquireCtx)
	} else {
		opts := []redislock.Option{redislock.WithTTL(ttl)}
		if mode == "renew" {
			opts = append(opts, redislock.WithAutoRenew())
		}
		lock, createErr := redislock.NewLock(key, backends[0], opts...)
		if createErr != nil {
			return createErr
		}
		// 换成 TryAcquire 可改为只尝试一次；已被占用时返回 ErrLocked。
		lease, err = lock.Acquire(acquireCtx)
	}
	cancel() // 获取已结束；取消这个 context 不会停止成功租约的自动续期。
	if err != nil {
		return fmt.Errorf("获取失败: %w", err)
	}
	defer func() {
		// 即使业务 ctx 已被 Ctrl+C 取消，仍给予释放操作独立的一秒预算。
		releaseCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := lease.Release(releaseCtx); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("释放结果未确认成功: %w", err))
			return
		}
		slog.Info("租约已释放")
	}()

	slog.Info("获取成功，开始业务；按 Ctrl+C 可取消")
	timer := time.NewTimer(hold)
	defer timer.Stop()
	// 用定时器模拟可取消的业务。实际业务同样必须响应取消和租约丢失；
	// Done 无法阻止已经提交的旧写入，必要时仍需资源端版本校验或 fencing。
	select {
	case <-ctx.Done():
		return fmt.Errorf("业务取消: %w", ctx.Err())
	case <-lease.Done():
		return fmt.Errorf("租约提前终止，停止业务: %w", lease.Err())
	case <-timer.C:
		slog.Info("业务完成")
		return nil
	}
}

func newPool(address string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:   2,
		MaxActive: 10,
		Wait:      true,
		DialContext: func(ctx context.Context) (redis.Conn, error) {
			return redis.DialContext(ctx, "tcp", address,
				redis.DialConnectTimeout(time.Second),
				redis.DialReadTimeout(time.Second),
				redis.DialWriteTimeout(time.Second))
		},
	}
}
