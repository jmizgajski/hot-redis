require('luarocks.loader')
local lunatest = require 'lunatest'
local assert_true, assert_false = lunatest.assert_true, lunatest.assert_false
local assert_diffvars = lunatest.assert_diffvars
local assert_boolean, assert_not_boolean = lunatest.assert_boolean, lunatest.assert_not_boolean
local assert_len, assert_not_len = lunatest.assert_len, lunatest.assert_not_len
local assert_match, assert_not_match = lunatest.assert_match, lunatest.assert_not_match
local assert_error = lunatest.assert_error
local assert_lt, assert_lte = lunatest.assert_lt, lunatest.assert_lte
local assert_gt, assert_gte = lunatest.assert_gt, lunatest.assert_gte
local assert_nil, assert_not_nil = lunatest.assert_nil, lunatest.assert_not_nil
local assert_equal, assert_not_equal = lunatest.assert_equal, lunatest.assert_not_equal
local assert_string, assert_not_string = lunatest.assert_string, lunatest.assert_not_string
local assert_metatable, assert_not_metatable = lunatest.assert_metatable, lunatest.assert_not_metatable
local assert_userdata, assert_not_userdata = lunatest.assert_userdata, lunatest.assert_not_userdata
local assert_thread, assert_not_thread = lunatest.assert_thread, lunatest.assert_not_thread
local assert_function, assert_not_function = lunatest.assert_function, lunatest.assert_not_function
local assert_table, assert_not_table = lunatest.assert_table, lunatest.assert_not_table
local assert_number, assert_not_number = lunatest.assert_number, lunatest.assert_not_number
local skip, fail = lunatest.skip, lunatest.fail

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

print '=================================='
print '-t [pattern] to run only tests matching the pattern'
print '=================================='

ARGV = {}
KEYS = {}

require 'blist_atoms'
function test_sorted_set_intersection_count()
    local key1, key2 = 'tssictestkey1', 'tssictestkey2'
    redis.call('ZADD', key1, 1, 'mem1', 2, 'mem2', 3, 'mem3')
    redis.call('ZADD', key2, 3, 'mem3', 4, 'mem4')

    KEYS[1] = key1
    ARGV[1] = key2

    assert_equal(1, sorted_set_get_intersection_count())
end

function test_sorted_set_intersection_update()
    local key1, key2 = 'tssiutestkey1', 'tssiutestkey2'
    redis.call('ZADD', key1, 1, 'mem1', 2, 'mem2', 3, 'mem3')
    redis.call('ZADD', key2, 3, 'mem3', 4, 'mem4')

    KEYS[1] = key1
    ARGV[1] = key2

    sorted_set_intersection_update()

    result = redis.call('ZRANGE', key1, '0', '-1')
    assert_equal(1, #result)
    assert_equal('mem3', result[1])
end

function test_sorted_set_intersection()
    local key1, key2, key3 = 'tssitestkey1', 'tssitestkey2', 'tssitestkey3'
    redis.call('ZADD', key1, 1, 'mem1', 2, 'mem2', 3, 'mem3')
    redis.call('ZADD', key3, 3, 'mem3', 4, 'mem4')

    KEYS[1] = key1
    ARGV[1] = key2
    ARGV[2] = key3

    sorted_set_intersection()

    result = redis.call('ZRANGE', key2, '0', '-1')
    assert_equal(1, #result)
    assert_equal('mem3', result[1])
end

function test_sorted_set_difference()
    local key1, key2, key3 = 'tssitestkey1', 'tssitestkey2', 'tssitestkey3'
    redis.call('ZADD', key1, 1, 'mem1', 2, 'mem2', 3, 'mem3')
    redis.call('ZADD', key3, 3, 'mem3', 4, 'mem4')

    KEYS[1] = key1
    ARGV[1] = key2
    ARGV[2] = key3

    sorted_set_difference()

    result = redis.call('ZRANGE', key2, '0', '-1')
    assert_equal(2, #result)
    assert_equal('mem1', result[1])
    assert_equal('mem2', result[2])
end

function test_sorted_set_symmetric_difference()
    local key1, key2, key3 = 'tssitestkey1', 'tssitestkey2', 'tssitestkey3'
    redis.call('ZADD', key1, 1, 'mem1', 2, 'mem2', 3, 'mem3')
    redis.call('ZADD', key3, 3, 'mem3', 4, 'mem4')

    KEYS[1] = key1
    ARGV[1] = key2
    ARGV[2] = key3

    sorted_set_symmetric_difference()

    result = redis.call('ZRANGE', key2, '0', '-1')
    assert_equal(3, #result)
    assert_equal('mem1', result[1])
    assert_equal('mem2', result[2])
    assert_equal('mem4', result[3])
end

lunatest.run()
print ''
