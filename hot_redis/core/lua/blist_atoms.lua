
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
