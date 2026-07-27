local contentKey = KEYS[1]
local idSetKey = KEYS[2]
local seqKey = KEYS[3]
local changelogKey = KEYS[4]

local targetType = ARGV[1]
local targetId = ARGV[2]
local expectedValue = ARGV[3]

local version = tonumber(redis.call('HGET', contentKey, 'version')) or 0
if expectedValue ~= '' then
    local expectedVersion = tonumber(expectedValue)
    if expectedVersion == 0 or expectedVersion ~= version then
        return {-1, version}
    end
end

if version > 0 then
    redis.call('DEL', contentKey)
    redis.call('SREM', idSetKey, targetId)
end
local seq = redis.call('INCR', seqKey)
local change = cjson.encode({seq = seq, targetType = targetType, targetId = targetId, op = 'DELETE', version = version})
redis.call('ZADD', changelogKey, seq, change)
return {version, seq}
