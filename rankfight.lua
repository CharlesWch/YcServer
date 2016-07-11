local shaco = require "shaco"
local rank = require "rank"
local reward = require "reward"
local activity_stage = require "activity_stage"
local CTX = require "ctx"
local user = require "user"
local mydb = require "mydb"
local tbl = require "tbl"
local mmax = math.max

local rankfight = {}

local __opened = false
local __rank
local __toplist = {}
local __toplist_version = 0

function rankfight.init()
    local maxtop = 50
    __rank = rank.new("rankfight", maxtop)
    user.change_fight = rankfight.change_score
end

function rankfight.loadfromdb()
    shaco.info("rankfight loadfromdb ...")
    local result = mydb.call("L.fight")
    if not result then
        return false
    end
    for _, v in ipairs(result) do
        local roleid = tonumber(v.roleid)
        if roleid and v.name then
            __rank:change(roleid, v.name, v.battle_value)
        end
    end
    shaco.info("rankfight loadfromdb ok")
end

function rankfight.change_score(ur, newfight)
    if not __opened then
        return
    end
    __rank:change(ur.base.roleid, ur.base.name, newfight)
end

local function query_toplist(force)
    __rank:fork_query_toplist(function(tl)
        for i=mmax(#__toplist, 1), #tl do
            __toplist[i] = {} --expand
        end
        for i=1, #tl do
            local from = tl[i]
            local to = __toplist[i]
            to.rank = i
            to.roleid = from.roleid
            to.name = from.name
            to.fight = from.score
        end
        for i=#tl+1, #__toplist do
            __toplist[i] = nil
        end
        __toplist_version = __toplist_version + 1
    end, force)
end

function rankfight.update()
    if not __opened then
        return
    end
    query_toplist()
end

function rankfight.req_rankinfo(ur)
    if not __opened then
        return
    end
    local now = shaco.now()
    if now - ur.lasttime_queryrankfight < 5000 then
        return 
    end
    ur.lasttime_queryrankfight = now
    
    local roleid = ur.base.roleid
    local tl
    if ur.version_toprankfight ~= __toplist_version then
        tl = __toplist
        ur.version_toprankfight = __toplist_version
        --shaco.trace(tbl(tl, "rankfight.req_rankinfo:"..ur.version_toprankfight))
    else 
    end
    local my_rank = __rank:query_rank(roleid)    
    my_rank = my_rank or 0 -- 0 for no update
    --shaco.trace("rankfight.my_rank="..my_rank)
    ur:send(IDUM_ACKBATTLERANK, {
        refresh_toplist=tl and true or false, 
        ranks=tl, my_rank=my_rank})
end

rankfight.query_toplist = query_toplist

function rankfight.close()
    shaco.info("rankfight close")
    __opened = false
    shaco.fork(function()
        reward.rankfight(__rank:getkey(), 
            activity_stage.get(ACTIVITY_FIGHT))
    end)
end

function rankfight.open()
    shaco.info("rankfight open")
    __opened = true
    query_toplist(true)
end

return rankfight
