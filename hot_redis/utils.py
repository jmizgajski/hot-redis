# -*- coding: utf-8 -*-

def get_instance_id(instance):
    """
    Maybe we could also use id() for objects without 'id' or 'pk'

    :param instance: given object
    :return: id as string
    """
    pk = getattr(instance, 'pk', None)
    if pk:
        return str(pk)
    _id = getattr(instance, 'id', None)
    if _id:
        return str(_id)
    raise ValueError("No id field.")


def delete_by_pattern(pattern, client):
    """
    This is a turbo efficient LUA script that will atomically delete all
    keys that match a pattern and never run out of space

    :param pattern: a redis key pattern i.e. "your_prefix:*"
    :type pattern: str
    :param client: redis client on which you would like to do this operation
    :type client: Redis
    """
    delete_script = '''
    for _,k in ipairs(redis.call('keys', ARGV[1])) do
        redis.call('del', k)
    end
    '''
    client.eval(delete_script, 0, pattern)


DEFAULT_TEST_PREFIX = '__test__'
KEY_SEPARATOR = '.'

def make_key(*parts):
    key = KEY_SEPARATOR.join(str(part) for part in parts)

    return key

def prefix_key(key):
    from test_settings import REDIS_TEST
    from test_settings import REDIS_TEST_PREFIX

    if REDIS_TEST:
        prefix = REDIS_TEST_PREFIX or DEFAULT_TEST_PREFIX
        return "%s%s%s" % (prefix, KEY_SEPARATOR,  key)

    return key
