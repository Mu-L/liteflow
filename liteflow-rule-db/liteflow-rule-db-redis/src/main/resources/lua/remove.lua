local contentKey = KEYS[1]
local indexKey = KEYS[2]
local seqKey = KEYS[3]
local changelogKey = KEYS[4]
local notifyKey = KEYS[5]

local targetType = ARGV[1]
local targetId = ARGV[2]

local version = tonumber(redis.call('HGET', contentKey, 'version')) or 0
redis.call('DEL', contentKey)
redis.call('HDEL', indexKey, targetId)

local seq = redis.call('INCR', seqKey)
local change = cjson.encode({seq = seq, targetType = targetType, targetId = targetId, op = 'DELETE', version = version})
redis.call('ZADD', changelogKey, seq, change)
redis.call('PUBLISH', notifyKey, change)
return seq
