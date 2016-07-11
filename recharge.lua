--local shaco = require "shaco"
local shaco = require "shaco"
local pb = require "protobuf"
local tbl = require "tbl"
local sfmt = string.format
local ipairs = ipairs
local pairs = pairs
local tonumber = tonumber
local tostring = tostring

local recharges = {}

local function recharge_info_gen()
	return{
		order_num = "",
		order_state =0,
		create_time = 0,
	}
end

local function recharge_data_gen()
	return {
		data = {},
	}
end

function recharges.new(rechargev)
	local recharge = rechargev
	return recharge
end

function recharges.add_recharge_order(ur,order)	
	local recharge = ur.recharge
	local info = recharge_info_gen()
	info.order_num = order
	info.order_state = 0
	info.create_time = shaco.now()//1000
	recharge[#recharge + 1] = info
	ur:db_tagdirty(ur.DB_RECHARGE)
end

function recharges.check_order(ur,order)
	local recharge = ur.recharge
	for i = 1,#recharge do
		local info = recharge[i]
		if info.order_num == order and info.order_state == 0 then
			return true
		end
	end
	return false
end

function recharges.finish_order(ur,order)
	local recharge = ur.recharge
	for i = 1,#recharge do
		local info = recharge[i]
		if info.order_num == order then
			info.order_state = 1
            break
		end
	end
	ur:db_tagdirty(ur.DB_RECHARGE)
end

return recharges
