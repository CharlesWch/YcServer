local shaco = require "shaco"
local ctx = require "ctx"
local gamestate = require "gamestate"
local callum = shaco.callum
local sendum = shaco.sendum
local LOGOUT = gamestate.LOGOUT

local mycommon = {}

local __db

function mycommon.init(db)
    assert(db)
    __db = db
end

function mycommon.urcall(ur, cmd, ...)
    local result = callum(__db, cmd, ...)
    if ur.status == LOGOUT then
        error(ctx.error_logout)
    end
    return result

end

function mycommon.call(cmd, ...)
    return callum(__db, cmd, ...)
end

function mycommon.send(cmd, ...)
    return sendum(__db, cmd, ...)
end

return mycommon
