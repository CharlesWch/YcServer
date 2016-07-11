local shaco = require "shaco"
local rank = require "rank"
local reward = require "reward"
local activity_stage = require "activity_stage"
local CTX = require "ctx"
local tbl = require "tbl"
local mmax = math.max

local rankexp = {}

local MAX_TIME = 86400
local __opened = false
local __rank
local __toplist = {}
local __toplist_version = 0

function rankexp.init()
    local maxtop = 50
    __rank = rank.new("rankexp", maxtop)
end

local function compose_score(time, diff)
    return (MAX_TIME-time)*10 + diff
end

local function decompose_score(score)
    return (MAX_TIME-score//10), score%10
end

function rankexp.change_score(ur, difficulty, time)
    if not __opened then
        return
    end
    if time > MAX_TIME then
        return
    end
    local act = ur.activity
    local oldscore = compose_score(act.exp_time, act.exp_difficulty)
    local score = compose_score(time, difficulty)
    if score < oldscore and act.exp_time~=0 then
        return
    end
    local base = ur.base
    __rank:change(base.roleid, base.name, score)
    act.exp_difficulty = difficulty
	act.exp_time = time
	ur:db_tagdirty(ur.DB_ACTIVITY)
end

local function query_toplist(force)
    __rank:fork_query_toplist(function(tl)
        for i=mmax(#__toplist, 1), #tl do
            __toplist[i] = {}
        end
        for i=1, #tl do
            local from = tl[i]
            local to = __toplist[i]
            to.rank = i
            to.roleid = from.roleid
            to.name = from.name
            local score = from.score
            to.over_time, to.difficulty = decompose_score(score)
        end
        for i=#tl+1, #__toplist do
            __toplist[i] = nil
        end
        __toplist_version = __toplist_version + 1
    end, force)
end

function rankexp.update()
    if not __opened then
        return
    end
    query_toplist()
end

function rankexp.req_rankinfo(ur)
    if not __opened then
        return
    end
    local now = shaco.now()
    if now - ur.lasttime_queryrankexp < 5000 then
        return 
    end
    ur.lasttime_queryrankexp = now
    
    local roleid = ur.base.roleid
    local tl
    if ur.version_toprankexp ~= __toplist_version then
        tl = __toplist
        ur.version_toprankexp = __toplist_version
        --shaco.trace(tbl(tl, "rankexp.req_rankinfo:"..ur.version_toprankfight))
    end
    local my_rank = __rank:query_rank(roleid)    
    my_rank = my_rank or 0 -- 0 for no update
    --shaco.trace("rankexp.my_rank="..my_rank)
	ur:send(IDUM_ACKACTIVITYEXPRANK, {
        refresh_toplist=tl and true or false, 
        five_ranks = tl, my_rank=my_rank})
end

rankexp.query_toplist = query_toplist

function rankexp.close()
    shaco.info("rankexp close")
    __opened = false
    local stage = activity_stage.get(ACTIVITY_EXP)
    if stage then
        shaco.fork(function()
            reward.rankexp(__rank:getkey(), stage)
        end)
    else
        shaco.error("rankexp close: not found stage")
    end
end

function rankexp.open()
    shaco.info("rankexp open")
    __opened = true
    query_toplist(true)
end

return rankexp
