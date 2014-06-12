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
    redis.call('DEL', '1')
    redis.call('DEL', '2')
    redis.call('DEL', '3')
    redis.call('DEL', '4')

    redis.call('ZADD', '1', 5, 'a', 4, 'aa', 2, 'aaa')
    redis.call('ZADD', '2', 8, 'a', 12, 'aa', 5, 'aaa')
    redis.call('ZADD', '3', 4, 'a', 3, 'aa', 2, 'aaa')
    redis.call('ZADD', '4', 123, 'a', 122, 'aa', 12, 'aaa')

    KEYS = { '1', '2', '3', '4' }
    ARGV = { '0', '3', 'a'}
    local result = rank_by_top_key_if_equal()

    assert_equal(6, #result)
    assert_equal('4', result[1])
    assert_equal('1', result[3])
    assert_equal('3', result[5])

end

function test_multi_zset_fixed_width_histogram()
    redis.call('DEL', '1')
    redis.call('DEL', '2')
    redis.call('ZADD', '1', 9, 'a', 8, 'aa', 11, 'aaa')
    redis.call('ZADD', '2', 6, 'a', 12, 'aa')

    KEYS = { '1', '2'}
    ARGV = { '7','244', '10' }
    local result = multi_zset_fixed_width_histogram()
    assert_equal(2, result[0])
    assert_equal(2, result[10])

end