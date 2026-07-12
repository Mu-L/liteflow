local chainKey = KEYS[1]
local indexKey = KEYS[2]
local seqKey = KEYS[3]
local changelogKey = KEYS[4]
local notifyKey = KEYS[5]

local chainId = ARGV[1]
local el = ARGV[2]
local route = ARGV[3]
local namespace = ARGV[4]
local md5 = ARGV[5]

local oldVersion = tonumber(redis.call('HGET', chainKey, 'version')) or 0
local version = oldVersion + 1

redis.call('HSET', chainKey, 'el', el, 'route', route, 'namespace', namespace,
    'version', version, 'md5', md5, 'enable', '1')
redis.call('HSET', indexKey, chainId, version .. '|' .. md5)

local seq = redis.call('INCR', seqKey)
local change = cjson.encode({seq = seq, targetType = 'CHAIN', targetId = chainId, op = 'UPSERT', version = version})
redis.call('ZADD', changelogKey, seq, change)
redis.call('PUBLISH', notifyKey, change)
return version
