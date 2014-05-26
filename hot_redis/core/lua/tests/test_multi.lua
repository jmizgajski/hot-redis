function test_rank_lists_by_length()
   --fail('not implemented') 
end

function test_rank_sets_by_cardinality()
   --fail('not implemented') 
end

function test_rank_zsets_by_cardinality()
   --fail('not implemented') 
end

function test_rank_by_sum_of_decaying_score()
   --fail('not implemented') 
end

function test_rank_by_top_key_if_equal()
    redis.call('ZADD', '1', 5, 1, 4, 2, 2, 3)
    redis.call('ZADD', '2', 8, 1, 12, 2, 5, 3)
    redis.call('ZADD', '3', 4, 1, 3, 2, 2, 3)
    redis.call('ZADD', '4', 123, 2, 122, 5, 12, 5)
 
    KEYS = { '1', '2', '3', '4' }
    ARGV = { '0', '3', '1'}
    local result = rank_by_top_key_if_equal()
    print(table.inspect(result))
end
