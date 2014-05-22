
function sorted_set_intersection_update()
    redis.call('ZINTERSTORE', KEYS[1], 2, KEYS[1], ARGV[1])
end

function sorted_set_intersection()
    redis.call('ZINTERSTORE', ARGV[1], 2, KEYS[1], ARGV[2])
end

function sorted_set_get_intersection_count()
    local temp_key = KEYS[1] .. 'disjoin'
    redis.call('ZINTERSTORE', temp_key, 2, KEYS[1], ARGV[1])

    local count = redis.call('ZCOUNT', temp_key, '-inf', '+inf')
    
    redis.call('DEL', temp_key)
    return count 
end

function sorted_set_difference()
    local temp1 = KEYS[1] .. 'diff1'
    local temp2 = KEYS[1] .. 'diff2'

    local target_key = ARGV[1]
    local operand1_key = KEYS[1]
    local operand2_key = ARGV[2]

    local set1 = redis.call('ZRANGE', operand1_key, '0', '-1')
    local set2 = redis.call('ZRANGE', operand2_key, '0', '-1')

    redis.call('SADD', temp1, unpack(set1))
    redis.call('SADD', temp2, unpack(set2))

    redis.call('SDIFFSTORE', temp1, temp1, temp2)

    redis.call('ZUNIONSTORE', target_key, 1, temp1)
    redis.call('DEL', temp1)
    redis.call('DEL', temp2)
end

function sorted_set_symmetric_difference()
    local temp1 = KEYS[1] .. 'symdiff1'
    local temp2 = KEYS[1] .. 'symdiff2'
    local temp3 = KEYS[1] .. 'symdiff3'

    local target_key = ARGV[1]
    local operand1_key = KEYS[1]
    local operand2_key = ARGV[2]

    local set1 = redis.call('ZRANGE', operand1_key, '0', '-1')
    local set2 = redis.call('ZRANGE', operand2_key, '0', '-1')

    redis.call('SADD', temp1, unpack(set1))
    redis.call('SADD', temp2, unpack(set2))

    redis.call('SUNIONSTORE', temp3, temp1, temp2)

    redis.call('SINTERSTORE', temp1, temp1, temp2)

    --subtract intersection from union
    redis.call('SDIFFSTORE', temp1, temp3, temp1)

    redis.call('ZUNIONSTORE', target_key, 1, temp1)
    redis.call('DEL', temp1)
    redis.call('DEL', temp2)
    redis.call('DEL', temp3)
end
