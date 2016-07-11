local shaco = require "shaco"
local rank = require "rank"
local CTX = require "ctx"
local user = require "user"
local tbl = require "tbl"
local userpool = require "userpool"
local msghelper = require "msghelper"
local myredis = require "myredis"
local mmax = math.max
local sfmt = string.format

local rankladder = {}

local KEY = "rankladder"
local MAX_JOIN = 4095
local __opened = false
local __rank
local __toplist = {}
local __toplist_version = 0
local __toplist_five = {}

function rankladder.init(season, rest)
    local maxtop = 100
    __rank = rank.new(KEY, maxtop)
    if season > 0 then
        if rest then
            rankladder.close(season)
        else
            rankladder.open()
        end
    end
end

local function compose_score(score, wincnt, joincnt)
    if (wincnt > MAX_JOIN or joincnt > MAX_JOIN) then
        return 0
    end
    return (score<<24) + (wincnt<<12) + (MAX_JOIN-joincnt)
end

local function decompose_score(score)
    return (score>>24), (score>>12)&0xfff, (MAX_JOIN-(score&0xfff))
end

local function query_toplist(force)
    __rank:fork_query_toplist(function(tl)
        for i=mmax(#__toplist, 1), #tl do
            __toplist[i] = {} --expand
        end
        for i=1, #tl do
            local from = tl[i]
            local to = __toplist[i]
            to.ranking = i
            --to.roleid = from.roleid
            to.name = from.name
            local score = from.score
            to.score, to.wincnt, to.joincnt = decompose_score(score)
        end
        for i=#tl+1, #__toplist do
            __toplist[i] = nil
        end
        for i=1, 5 do
            __toplist_five[i] = __toplist[i]
        end

        local msgid, packed = msghelper.packmsg(IDUM_SYNCRANKINFO, {
            five_rank = __toplist_five,
            hundred_rank = __toplist,
        })
        userpool.foreach_user(function(ur)
            if ((ur.bit_value >> REQ_LADDER) & 1) ~= 0 then
            --    shaco.trace("user sync top:", ur.base.roleid)
                ur:sendpackedmsg(msgid, packed)
            end
        end)
    
        __toplist_version = __toplist_version + 1
    end, force)
end

function rankladder.change_score(ur, score, wincnt, joincnt)
    if not __opened then
        return
    end
    score = compose_score(score, wincnt, joincnt)
    __rank:change(ur.base.roleid, ur.base.name, score)
    query_toplist()
end

function rankladder.update()
    if not __opened then
        return
    end
    query_toplist()
end

function rankladder.req_rankinfo(ur)
    -- can always query
    --if not __opened then
    --    return
    --end
    local flag=0
    local tl
    if ur.version_toprankladder ~= __toplist_version then
        tl = __toplist
        ur.version_toprankladder = __toplist_version
        flag = 1
        --shaco.trace(tbl(tl, "rankladder.req_rankinfo:"..ur.version_toprankladder))
    end
	ur:send(IDUM_ACKLADDERRANK, {update_flag = flag,rank = tl})
end

function rankladder.req_enter(ur, season, rest)
    -- can always query
    --if not __opened then
    --    return
    --end
    local flag=0
    local tl
    if ur.version_toprankladder_five ~= __toplist_version then
        tl = __toplist_five
        ur.version_toprankladder_five = __toplist_version
        flag = 1
        shaco.trace(tbl(tl, "rankladder.req_enter:"..ur.version_toprankladder_five))
    end

    -- todo: a dirty bug, return real rank, maybe conflict with top five
    local my_rank = __rank:query_rank(ur.base.roleid) -- query always
    if my_rank then
        ur.ladder_rank = my_rank
    end

    if rest then -- 休赛期
        ur.ladder_lastrank = ur.ladder_rank
    elseif not ur.ladder_lastrank then -- not queryed, query once
        if season > 1 then
            ur.ladder_lastrank = rankladder.query_rank(ur, season-1)
        end
    end

    -- last rank
    local ladd = ur.ladder
    local base = ur.base
	local data = {
        score = ladd.score,
		level = base.level,
		name = base.name,	
		joincnt = ladd.joincnt,
		wincnt = ladd.wincnt,
		ranking = ur.ladder_rank,
		challengecnt = ladd.challengecnt,
		refreshcnt = ladd.refreshcnt,
		honor = ladd.honor,
		last_rank = ur.ladder_lastrank,
		buy_challenge_cnt = ladd.buy_challenge_cnt,
        reward_state = ladd.reward_state
    }
    local now = shaco.now()//1000
	ur:send(IDUM_ACKENTERLADDER, 
        {data = data, rank = tl, refresh_time = now})
end

rankladder.query_toplist = query_toplist

local function key_byseason(season)
    local key = __rank:getkey()
    return sfmt("%s:%d", key, season)
end

function rankladder.query_rank(ur, season, force)
    if not season then
        return __rank:query_rank(ur.base.roleid, force)
    elseif season >= 1 then
        local r = myredis.urcall(ur, "zrevrank", KEY..":"..season, ur.base.roleid)
        if r then
            return r+1
        end
    end
end

function rankladder.query_byscore(ur, min, max)
    -- convert to redis score, see change_score 
    min = min<<24
    max = max<<24
    return myredis.urcall(ur, "zrevrangebyscore", __rank:getkey(), max, min)
end

function rankladder.close(season)
    shaco.info("rankladder close", season)
    __opened = false
    if season > 0 then
        local key = __rank:getkey()
        local tokey = key_byseason(season)
        if not myredis.exists(tokey) then
            myredis.rename(key, tokey)
        end
        __rank.__rank_key = tokey -- use last season data
        query_toplist(true)
    end
end

function rankladder.open()
    shaco.info("rankladder open")
    __opened = true
    __rank.__rank_key = KEY -- new season
    query_toplist(true)
end

return rankladder
