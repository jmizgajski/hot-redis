require('luarocks.loader')
local lunatest = require 'lunatest'
assert_true, assert_false = lunatest.assert_true, lunatest.assert_false
assert_diffvars = lunatest.assert_diffvars
assert_boolean, assert_not_boolean = lunatest.assert_boolean, lunatest.assert_not_boolean
assert_len, assert_not_len = lunatest.assert_len, lunatest.assert_not_len
assert_match, assert_not_match = lunatest.assert_match, lunatest.assert_not_match
assert_error = lunatest.assert_error
assert_lt, assert_lte = lunatest.assert_lt, lunatest.assert_lte
assert_gt, assert_gte = lunatest.assert_gt, lunatest.assert_gte
assert_nil, assert_not_nil = lunatest.assert_nil, lunatest.assert_not_nil
assert_equal, assert_not_equal = lunatest.assert_equal, lunatest.assert_not_equal
assert_string, assert_not_string = lunatest.assert_string, lunatest.assert_not_string
assert_metatable, assert_not_metatable = lunatest.assert_metatable, lunatest.assert_not_metatable
assert_userdata, assert_not_userdata = lunatest.assert_userdata, lunatest.assert_not_userdata
assert_thread, assert_not_thread = lunatest.assert_thread, lunatest.assert_not_thread
assert_function, assert_not_function = lunatest.assert_function, lunatest.assert_not_function
assert_table, assert_not_table = lunatest.assert_table, lunatest.assert_not_table
assert_number, assert_not_number = lunatest.assert_number, lunatest.assert_not_number
skip, fail = lunatest.skip, lunatest.fail

--needs to be global for functions from other chunks to see it
redis = require 'redis'

local dump = function(v)
   if nil ~= v then
       print(dumpLib.tostring(v))
   end
end

local host = "127.0.0.1"
local port = 6379

client = redis.connect(host, port)

redis.call = function(cmd, ...)
   return assert(loadstring('return client:' .. string.lower(cmd) .. '(...)'))(...)
end

function table.contains(table, element)
    for _, value in pairs(table) do
        if value == element then
            return true
        end
    end
    return false
end
table.inspect = require 'inspect'

print '=================================='
print '-t [pattern] to run only tests matching the pattern'
print '=================================='

ARGV = {}
KEYS = {}

require('blist_atoms')
require('tests/test_blist_atoms')

require('multi')
require('tests/test_multi')

lunatest.run()
print ''
