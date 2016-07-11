--local shaco = require "shaco"
local shaco = require "shaco"
local pb = require "protobuf"
local tbl = require "tbl"
local sfmt = string.format
local ipairs = ipairs
local pairs = pairs
local tonumber = tonumber
local tostring = tostring

local partner = {}

local function partner_gen()
	return {
		pos = 0,
		pos_idx = 0,
        cardid = 0,
	}
end

function partner.new(size,partners)
    if not partners then
        partners = {}
    end
    for i =(#partners + 1) , size do
    	partners[i] = partner_gen()
    end
    return partners
end

return partner
