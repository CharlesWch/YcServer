local shaco = require "shaco"
local rank = require "rank"
local reward = require "reward"
local CTX = require "ctx"
local tbl = require "tbl"
local mmax = math.max
local myredis = require "myredis"
local util = require "util"

local rankendless = {}

local KEY = "rankendless"
--local __opened = false
local __rank
local __toplist = {}
local __toplist_version = 0
local __toplist_five = {}

function rankendless.init()
    local maxtop = 100
    __rank = rank.new(KEY, maxtop)
end

function rankendless.change_score(ur, cur_floor, last_floor, last_time)
    --if not __opened then
        --return
    --end
    if cur_floor <= last_floor then
        return
    end
    local now = shaco.now()//1000
    local score = (cur_floor<<32)+now
    local base = ur.base
    __rank:change(base.roleid, base.name, score)
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
            to.max_floor = score>>32
            to.create_time = score&0xffffffff
        end
        for i=#tl+1, #__toplist do
            __toplist[i] = nil
        end
        for i=1, 5 do
            __toplist_five[i] = __toplist[i]
        end
        __toplist_version = __toplist_version + 1
    end, force)
end

local __lastday = util.msecond2day(shaco.now())

function rankendless.update()
    --if not __opened then
    --    return
    --end
    query_toplist()
    local now = shaco.now()//1000
    local nowday = util.second2day(now)
    if nowday ~= __lastday then
        __lastday = nowday
        rankendless.close()
        rankendless.open()
    end
end

function rankendless.req_rankinfo(ur, flag)
    --if not __opened then
    --    return
    --end
    local own
    local five
    local hundred
    if flag == 1 then
        five = __toplist_five
        hundred = __toplist
    elseif flag == 2 then
        hundred = __toplist
    elseif flag == 3 then
    elseif flag == 4 then
        five = __toplist_five
    end
    local roleid = ur.base.roleid
    local my_rank = __rank:query_rank(roleid)
    my_rank = my_rank or 0 -- 0 for no update
    if my_rank ~= 0 then
        own = {
            rank = my_rank,
            roleid = roleid,
            name = ur.base.name,
            max_floor = ur.spectype.front_floor,
            create_time = ur.spectype.front_floor_time,
        }
    end
    --shaco.trace("rankendless.my_rank="..my_rank, flag)
    --shaco.trace(tbl(own or {}, "own"))
    --shaco.trace(tbl(hundred or {}, "hundred"))
    --shaco.trace(tbl(five or {}, "five"))
	ur:send(IDUM_ACKRANKINGLIST,{
        ranks = hundred,
        own_rank = own,five_ranks = five})
end

rankendless.query_toplist = query_toplist

function rankendless.close()
    shaco.info("rankendless close")
    --__opened = false
    query_toplist(true)
    local tokey = myredis.gen_datekey(KEY)
    if not myredis.exists(tokey) then
        myredis.rename(KEY, tokey)
    end
end

function rankendless.open()
    shaco.info("rankendless open")
    --__opened = true
    query_toplist(true)
end

return rankendless
