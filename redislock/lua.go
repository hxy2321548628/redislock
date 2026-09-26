package redislock

// 使用 lua 脚本, 将校验和清除原子化
const compareAndDeleteScript = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
	return 0
end
return redis.call("DEL", KEYS[1])
`

// 使用 lua 脚本, 将续期和校验原子化
const compareAndExpireScript = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
	return 0
end
return redis.call("PEXPIRE", KEYS[1], ARGV[2])
`
