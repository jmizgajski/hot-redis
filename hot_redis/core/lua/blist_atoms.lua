
function sorted_set_intersection_update()
    redis.call('ZINTERSTORE', KEYS[1], 2, KEYS[1], ARGV[1])
end

