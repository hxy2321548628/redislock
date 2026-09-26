package redislock

// compareAndDeleteScript 将“校验所有者”和“删除”放在同一个原子脚本中。
// KEYS[1] 是锁 key，ARGV[1] 是本次租约的 token；返回 1 表示删除，0 表示不匹配。
// 若在客户端先 GET 再 DEL，两条命令之间锁可能已过期并被其他人获取，导致误删。
const compareAndDeleteScript = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
	return 0
end
return redis.call("DEL", KEYS[1])
`

// compareAndExpireScript 仅延长仍属于当前 token 的锁，不创建新 key。
// ARGV[2] 是毫秒级 TTL；先比较再 PEXPIRE 在 Redis 内原子完成。
// 已过期的租约不能通过续期“复活”，否则旧持有者可能与新持有者同时工作。
const compareAndExpireScript = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
	return 0
end
return redis.call("PEXPIRE", KEYS[1], ARGV[2])
`
