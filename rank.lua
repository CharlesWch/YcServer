local shaco = require "shaco"
local myredis = require "myredis"
local tbl = require "tbl"

local rank = {}
rank.__index = rank

function rank.new(key, maxtop)
    return setmetatable({
        __rank_key = key,
        __maxtop = maxtop,
        __toplist = {},
        __topindex = {},
        __toplist_quering = false,
        __toplist_dirty = false,
    }, rank)
end

function rank:change(roleid, name, newscore)
    assert(roleid and name)
    local ok = pcall(function()
        local key = self.__rank_key
            
        local rank1 = myredis.zrevrank(key, roleid)
        myredis.zadd(key, newscore, roleid)
        local rank2 = myredis.zrevrank(key, roleid)
            
        local maxtop = self.__maxtop
        if (not rank1 or rank1 > maxtop) and 
           (rank2 and rank2 <= maxtop) then -- enter toplist
            myredis.set("topuser:"..roleid, name)
        end
        if (rank1 and rank1 <= maxtop) or 
           (rank2 and rank2 <= maxtop) then -- change toplist
            self.__toplist_dirty = true
        end
    end)
    return ok
end

local function __query_toplist(self)
    local function has_diff(top, oldtl)
        if #oldtl ~= #top//2 then
            return true
        end
        for i=1, #oldtl do
            local old = oldtl[i]
            if old.roleid ~= tonumber(top[i*2-1]) or
               old.score ~= tonumber(top[i*2]) then
                return true
            end
        end
    end
    local oldtl = self.__toplist
    local top = myredis.zrevrange(self.__rank_key, 0, self.__maxtop-1, "WITHSCORES")
    if not has_diff(top, oldtl) then
        --shaco.trace("query_toplist, but no diff", self.__rank_key)
        return
    end
    local tl = {}
    for i=1, #top, 2 do
        local roleid = tonumber(top[i])
        local name = myredis.get("topuser:"..roleid)
        tl[#tl+1] = {rank=i, roleid=roleid, name=name, score=tonumber(top[i+1])}
    end
    -- at last: update memory data
    for k, v in ipairs(tl) do
        self.__topindex[v.roleid] = k
    end
    self.__toplist = tl
end

function rank:fork_query_toplist(cb, force)
    if not self.__toplist_quering then
        if self.__toplist_dirty or force then
            self.__toplist_dirty = false
            shaco.fork(function()
                --shaco.trace("query_toplist start ...", self.__rank_key)
                self.__toplist_quering = true
                local ok, err = pcall(__query_toplist, self)
                self.__toplist_quering = false
                --shaco.trace("query_toplist ok", self.__rank_key)
                if not ok then
                    shaco.error(err)
                else
                    cb(self.__toplist)
                end
            end)
        end
    end
end

function rank:query_rank(roleid, force)
    local index = self.__topindex[roleid]
    if not index or force then
        local ok, err = xpcall(function()
            index = myredis.zrevrank(self.__rank_key, roleid)
            if index then
                index = index+1
            end
        end, debug.traceback)
        if not ok then
            shaco.error(err)
        end
        if index then
            if index <= self.__maxtop then
                --shaco.trace("Conflict with toplist, ret nil, wait for next query")
                --index = nil
                return force and index or nil
            end
        end
    end
    return index
end

function rank:getkey()
    return self.__rank_key
end

return rank
