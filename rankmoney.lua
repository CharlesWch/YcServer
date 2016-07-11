local shaco = require "shaco"
local rank = require "rank"
local reward = require "reward"
local activity_stage = require "activity_stage"
local CTX = require "ctx"
local mmax = math.max

local rankmoney = {}

local __opened = false
local __rank
local __toplist = {}
local __toplist_version = 0

function rankmoney.init()
    local maxtop = 50
    __rank = rank.new("rankmoney", maxtop)
end

function rankmoney.change_score(ur, difficulty, money)
    if not __opened then
        return
    end
    local act = ur.activity
    local oldscore = act.money_cnt*10 + act.money_difficulty
    local score = money*10+difficulty
    if score < oldscore then
        return
    end
    local base = ur.base
    __rank:change(base.roleid, base.name, score)
    act.money_difficulty = difficulty
	act.money_cnt = money 
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
            to.reward_money = score//10
            to.difficulty = score%10
        end
        for i=#tl+1, #__toplist do
            __toplist[i] = nil
        end
        __toplist_version = __toplist_version + 1
    end, force)
end

function rankmoney.update()
    if not __opened then
        return
    end
    query_toplist()
end

function rankmoney.req_rankinfo(ur)
    if not __opened then
        return
    end
    local now = shaco.now()
    if now - ur.lasttime_queryrankmoney < 5000 then
        return 
    end
    ur.lasttime_queryrankmoney = now
    
    local roleid = ur.base.roleid
    local tl
    if ur.version_toprankmoney ~= __toplist_version then
        tl = __toplist
        ur.version_toprankmoney = __toplist_version
        --shaco.trace(tbl(tl, "rankmoney.req_rankinfo:"..ur.version_toprankfight))
    end
    local my_rank = __rank:query_rank(roleid)    
    my_rank = my_rank or 0 -- 0 for no update
    --shaco.trace("rankmoney.my_rank="..my_rank)

	ur:send(IDUM_ACKACTIVITYMONEYRANK, {
        refresh_toplist=tl and true or false, 
        five_ranks = tl, my_rank=my_rank})
end

rankmoney.query_toplist = query_toplist

function rankmoney.close()
    shaco.info("rankmoney close")
    __opened = false
    local stage = activity_stage.get(ACTIVITY_MONEY)
    if stage then
        shaco.fork(function()
            reward.rankmoney(__rank:getkey(), stage)
        end)
    else
        shaco.error("rankmoney close: not found stage")
    end
end

function rankmoney.open()
    shaco.info("rankmoney open")
    __opened = true
    query_toplist(true)
end

return rankmoney
