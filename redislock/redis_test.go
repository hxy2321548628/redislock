package redislock

import (
	"context"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

// replyConn 是测试用的假 Redis 连接：不建立网络连接，只返回预设的 reply。
// Go 通过方法集合隐式实现接口，这些方法让它满足 redis.Conn 和 redis.ConnWithContext。
type replyConn struct {
	reply any // any 是 interface{} 的别名；这里用于保存 nil 或字符串等不同类型的响应。
}

// Close 和 Err 分别模拟关闭连接、查询连接错误；假连接没有资源要释放，也不模拟错误。
func (c *replyConn) Close() error { return nil }
func (c *replyConn) Err() error   { return nil }

// Do 通常负责发送命令并读取响应；这里忽略命令名和参数，直接返回预设值。
// 参数只写类型、不写名称，表示方法需要符合接口签名，但实现中不使用这些参数。
func (c *replyConn) Do(string, ...any) (any, error) {
	return c.reply, nil
}

// Send、Flush、Receive 是流水线操作接口：依次为缓冲命令、发送缓冲内容、读取响应。
// 此处只为满足接口提供最简实现，没有真正的缓冲区或网络读写。
func (c *replyConn) Send(string, ...any) error { return nil }
func (c *replyConn) Flush() error              { return nil }
func (c *replyConn) Receive() (any, error)     { return c.reply, nil }

// TryAcquire 使用 redis.DoContext，因此假连接还需要实现 ConnWithContext 的两个方法。
// 这里只返回预设响应，不处理 context 的取消或超时；相关行为不在本测试的覆盖范围内。
func (c *replyConn) DoContext(context.Context, string, ...any) (any, error) {
	return c.reply, nil
}
func (c *replyConn) ReceiveContext(context.Context) (any, error) {
	return c.reply, nil
}

// TestClientTryAcquireReplies 验证 TryAcquire 如何解释 SET key token NX PX ttl 的响应。
// NX 表示仅在 key 不存在时写入，PX 指定以毫秒为单位的过期时间。
// 测试只检查响应到返回值的转换，不验证命令参数、真实的锁竞争或过期行为。
func TestClientTryAcquireReplies(t *testing.T) {
	// 表驱动测试：把输入和预期结果放进表中，复用同一套执行与断言逻辑。
	for _, tt := range []struct {
		name  string // 子测试名称，便于从测试输出定位失败场景。
		reply any    // 假连接返回的 Redis 响应。
		want  bool   // 期望 TryAcquire 返回的“是否成功获得锁”。
	}{
		// key 已存在时，SET ... NX 返回 nil：没有获得锁，但这不是执行错误。
		{name: "held", reply: nil, want: false},
		// 写入成功时返回 OK：获得锁，且没有错误。
		{name: "acquired", reply: "OK", want: true},
	} {
		// t.Run 为每一行创建子测试，例如 TestClientTryAcquireReplies/held。
		t.Run(tt.name, func(t *testing.T) {
			// 连接池需要新连接时会调用 DialContext；用假连接替换真实拨号，避免依赖 Redis 服务。
			// 闭包读取当前用例的 tt.reply，让两个子测试分别得到 nil 和 OK。
			pool := &redis.Pool{
				DialContext: func(context.Context) (redis.Conn, error) {
					return &replyConn{reply: tt.reply}, nil
				},
			}
			// 连接池由调用方负责关闭；defer 保证子测试返回时清理，包括 t.Fatal 提前结束的情况。
			defer pool.Close()
			client, err := NewClient(pool)
			if err != nil {
				// 初始化失败时停止当前子测试，避免继续使用无效的 client。
				t.Fatal(err)
			}
			// Background 提供没有截止时间、不会被取消的上下文。
			// key 是锁的键名，token 是持有者标识，time.Second 是锁的有效期（转换后为 1000 毫秒）。
			// 固定 token 仅用于本测试；实际使用时应为每次加锁生成唯一标识，供释放、续期时校验。
			got, err := client.TryAcquire(context.Background(), "key", "token", time.Second)
			// 同时检查结果和错误：即使抢锁失败，也应返回 (false, nil)。
			if err != nil || got != tt.want {
				// Fatalf 输出实际值与预期值并停止当前子测试；%t 格式化布尔值，%v 格式化错误值。
				t.Fatalf("TryAcquire = %t, %v; want %t, nil", got, err, tt.want)
			}
		})
	}
}
