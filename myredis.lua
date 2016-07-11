local shaco = require "shaco"
local ctx = require "ctx"
local gamestate = require "gamestate"
local sformat = string.format
local callum = shaco.callum
local sendum = shaco.sendum
local LOGOUT = gamestate.LOGOUT

local myredis = {}

local __db

function myredis.init(db)
    assert(db)
    __db = db
end

function myredis.urcall(ur, cmd, ...)
    local result = callum(__db, cmd, ...)
    if ur.status == LOGOUT then
        error(ctx.error_logout)
    end
    return result

end

function myredis.call(cmd, ...)
    return callum(__db, cmd, ...)
end

function myredis.send(cmd, ...)
    return sendum(__db, cmd, ...)
end

function myredis.gen_datekey(key)
    local now = shaco.now()//1000
    return sformat("%s:%s", key, os.date("%Y%m%d", now))
end

function myredis.rename(key, newkey)
    shaco.info("Redis key rename:", key, newkey)
    if not myredis.exists(key) then
        shaco.error("Redis rename no exist key:", key, newkey)
    elseif myredis.exists(newkey) then
        shaco.error("Redis key has exist:", newkey)
    else
        shaco.info("Redis key rename:", key, newkey)
        myredis.call('rename', key, newkey)
    end
    return newkey, myredis.zcount(newkey, "-inf", "+inf")
end

function myredis.backupkey(key, newkey)
    shaco.info("Redis key backup:", key, newkey)
    if not myredis.exists(key) then
        shaco.error("Redis backup no exist key:", key, newkey)
        return newkey, myredis.zcount(newkey, "-inf", "+inf")
    elseif myredis.exists(newkey) then
        shaco.error("Redis key has exist:", newkey)
        return newkey, myredis.zcount(newkey, "-inf", "+inf")
    else
        shaco.info("Redis key backup:", key, newkey)
        return newkey, myredis.zunionstore(newkey, 1, key)
    end
end

setmetatable(myredis, { __index = function(t, k)
    local f = function(...)
        return myredis.call(k, ...)
    end
    t[k] = f
    return f
end})

return myredis
