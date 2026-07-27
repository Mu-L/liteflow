local scriptKey = KEYS[1]
local idSetKey = KEYS[2]
local seqKey = KEYS[3]
local changelogKey = KEYS[4]

local nodeId = ARGV[1]
local script = ARGV[2]
local name = ARGV[3]
local ntype = ARGV[4]
local language = ARGV[5]
local md5 = ARGV[6]
local expectedValue = ARGV[7]

local currentVersion = tonumber(redis.call('HGET', scriptKey, 'version')) or 0
if expectedValue ~= '' then
    local expectedVersion = tonumber(expectedValue)
    if expectedVersion == 0 then
        if currentVersion ~= 0 then
            return {-1, currentVersion}
        end
    elseif currentVersion ~= expectedVersion then
        return {-1, currentVersion}
    end
end

local version = currentVersion + 1
redis.call('HSET', scriptKey, 'script', script, 'name', name, 'type', ntype,
    'language', language, 'version', version, 'md5', md5, 'enable', '1')
redis.call('SADD', idSetKey, nodeId)
local seq = redis.call('INCR', seqKey)
local change = cjson.encode({seq = seq, targetType = 'SCRIPT', targetId = nodeId, op = 'UPSERT', version = version})
redis.call('ZADD', changelogKey, seq, change)
return {version, seq}
