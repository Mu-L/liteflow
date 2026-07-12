local scriptKey = KEYS[1]
local indexKey = KEYS[2]
local seqKey = KEYS[3]
local changelogKey = KEYS[4]
local notifyKey = KEYS[5]

local nodeId = ARGV[1]
local script = ARGV[2]
local name = ARGV[3]
local ntype = ARGV[4]
local language = ARGV[5]
local md5 = ARGV[6]

local oldVersion = tonumber(redis.call('HGET', scriptKey, 'version')) or 0
local version = oldVersion + 1

redis.call('HSET', scriptKey, 'script', script, 'name', name, 'type', ntype,
    'language', language, 'version', version, 'md5', md5, 'enable', '1')
redis.call('HSET', indexKey, nodeId, version .. '|' .. md5 .. '|' .. ntype .. '|' .. language .. '|' .. name)

local seq = redis.call('INCR', seqKey)
local change = cjson.encode({seq = seq, targetType = 'SCRIPT', targetId = nodeId, op = 'UPSERT', version = version})
redis.call('ZADD', changelogKey, seq, change)
redis.call('PUBLISH', notifyKey, change)
return version
