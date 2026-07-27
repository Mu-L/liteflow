local chainKey = KEYS[1]
local idSetKey = KEYS[2]
local seqKey = KEYS[3]
local changelogKey = KEYS[4]

local chainId = ARGV[1]
local el = ARGV[2]
local route = ARGV[3]
local namespace = ARGV[4]
local md5 = ARGV[5]
local expectedValue = ARGV[6]

local currentVersion = tonumber(redis.call('HGET', chainKey, 'version')) or 0
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
redis.call('HSET', chainKey, 'el', el, 'route', route, 'namespace', namespace,
    'version', version, 'md5', md5, 'enable', '1')
redis.call('SADD', idSetKey, chainId)
local seq = redis.call('INCR', seqKey)
local change = cjson.encode({seq = seq, targetType = 'CHAIN', targetId = chainId, op = 'UPSERT', version = version})
redis.call('ZADD', changelogKey, seq, change)
return {version, seq}
