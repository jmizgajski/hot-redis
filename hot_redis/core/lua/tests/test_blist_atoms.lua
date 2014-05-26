function test_sorted_set_intersection_count()
    local key1, key2 = 'tssictestkey1', 'tssictestkey2'
    redis.call('DEL', key1, key2)
    redis.call('ZADD', key1, 1, 'mem1', 2, 'mem2', 3, 'mem3')
    redis.call('ZADD', key2, 3, 'mem3', 4, 'mem4')

    KEYS[1] = key1
    ARGV[1] = key2

    assert_equal(1, sorted_set_get_intersection_count())
end

function test_sorted_set_intersection_update()
    local key1, key2 = 'tssiutestkey1', 'tssiutestkey2'
    redis.call('DEL', key1, key2)
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
    redis.call('DEL', key1, key2, key3)
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
    redis.call('DEL', key1, key2, key3)
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
    redis.call('DEL', key1, key2, key3)
    redis.call('ZADD', key1, 1, 'mem1', 2, 'mem2', 3, 'mem3')
    redis.call('ZADD', key3, 3, 'mem3', 4, 'mem4')

    KEYS[1] = key1
    ARGV[1] = key2
    ARGV[2] = key3

    sorted_set_symmetric_difference()

    result = redis.call('ZRANGE', key2, '0', '-1')

    assert_equal(3, #result)
    assert(table.contains(result, 'mem1'))
    assert(table.contains(result, 'mem2'))
    assert(table.contains(result, 'mem4'))
end

