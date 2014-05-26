import os
from redis.client import Redis, zset_score_pairs

class Ranking(object):
    def __init__(self, ranking_script, keys_, *script_args):
        if len(keys_) < 2:
            raise ValueError("You have to have at least two lists to compare")

        self.keys = keys_
        self.ranker = ranking_script
        self.script_args = script_args

    def __getitem__(self, i):
        if not isinstance(i, slice):
            args = [i, i]
            args.extend(self.script_args)
            response = self.ranker(keys=self.keys, args=args)
            return zset_score_pairs(response, withscores=True)

        start = i.start if i.start is not None else 0
        stop = i.stop if i.stop is not None else 0

        args = [start, stop - 1]
        args.extend(self.script_args)
        response = self.ranker(keys=self.keys, args=args)
        return zset_score_pairs(response, withscores=True)

    def __iter__(self):
        return iter(self[:])


class HotClient(object):
    """
    A Redis client wrapper that loads Lua functions and creates
    client methods for calling them.
    """
    _ATOMS_FILE_NAME = "core_atoms.lua"
    _BIT_FILE_NAME = "bit.lua"
    _MULTI_FILE_NAME = "multi.lua"
    _BLIST_FILE_NAME = "blist_atoms.lua"

    def __init__(self, client=None, *args, **kwargs):
        self._client = client
        if not self._client:
            self._client = Redis(*args, **kwargs)

        self._bind_atoms()
        self._bind_multi()
        self._bind_blist()

    def _bind_atoms(self):
        with open(self._get_lua_path(self._BIT_FILE_NAME)) as f:
            luabit = f.read()

        requires_luabit = (
            "number_and",
            "number_or",
            "number_xor",
            "number_lshift",
            "number_rshift"
        )

        for name, snippet in self._split_lua_file_into_funcs(
                self._ATOMS_FILE_NAME):
            if name in requires_luabit:
                snippet = luabit + snippet
            self._bind_lua_method(name, snippet)

    def _bind_multi(self):
        for name, snippet in self._split_lua_file_into_funcs(
            self._MULTI_FILE_NAME):
            self._bind_private_lua_script(name, snippet)

    def _bind_blist(self):
        for name, snippet in self._split_lua_file_into_funcs(
            self._BLIST_FILE_NAME):
            self._bind_lua_method(name, snippet)

    @staticmethod
    def _get_lua_path(name):
        """
        Joins the given name with the relative path of the module.
        """
        parts = (os.path.dirname(os.path.abspath(__file__)), "lua", name)
        return os.path.join(*parts)

    def _split_lua_file_into_funcs(self, file_name):
        """
        Returns the name / code snippet pair for each Lua function
        in the file under file_name.
        """
        with open(self._get_lua_path(file_name)) as f:
            for func in f.read().strip().split("function "):
                if func:
                    bits = func.split("\n", 1)
                    name = bits[0].split("(")[0].strip()
                    snippet = bits[1].rsplit("end", 1)[0].strip()
                    yield name, snippet

    def _bind_lua_method(self, name, code):
        """
        Registers the code snippet as a Lua script, and binds the
        script to the client as a method that can be called with
        the same signature as regular client methods, eg with a
        single key arg.
        """
        script = self._client.register_script(code)
        method = lambda key, *a, **k: script(keys=[key], args=a, **k)
        setattr(self, name, method)

    def _bind_private_lua_script(self, name, code):
        """
        Registers the code snippet as a Lua script, and binds the
        script to the client as a private method (eg. some_lua_func becomes
        a _some_lua_func method of HotClient) that can be latter wrapped in
        public methods with better argument and error handling.
        """
        script = self._client.register_script(code)
        setattr(self, '_' + name, script)

    def rank_lists_by_length(self, *keys):
        """
        Creates a temporary ZSET with LIST keys as entries and their
        *LLEN* as scores. Uses ZREVRANGE .. WITHSCORES, to return keys and
        lengths sorted from longest to shortests.
        :param keys: keys of the lists you want rank
        :return: :rtype: Ranking :raise ValueError:
        :raise ValueError: when not enough keys are provided
        """
        return Ranking(self._rank_lists_by_length, keys)

    def rank_sets_by_cardinality(self, *keys):
        """
        Creates a temporary ZSET with SET keys as entries and their
        *CARD* as scores. Uses ZREVRANGE .. WITHSCORES, to return keys and
        cardinalities sorted from largest to smallest.
        :param keys: keys of the sets you want to rank
        :return: :rtype: Ranking
        :raise ValueError: when not enough keys are provided
        """
        return Ranking(self._rank_sets_by_cardinality, keys)

    def rank_zsets_by_cardinality(self, *keys):
        """
        Creates a temporary ZSET with SET keys as entries and their
        *ZCARD* as scores. Uses ZREVRANGE .. WITHSCORES, to return keys and
        cardinalities sorted from largest to smallest.
        :param keys: keys of the sets you want to rank
        :return: :rtype: Ranking
        :raise ValueError: when not enough keys are provided
        """
        return Ranking(self._rank_zsets_by_cardinality, keys)

    def rank_by_sum_of_decaying_score(
            self, from_, halflife, cache_timeout, *keys):
        """
        Creates a temporary ZSET with SET keys as entries and sums of their
        decayed (standard halflife decay function) scores as scores. Uses
        ZREVRANGE .. WITHSCORES, to return
        keys and these sums sorted from largest to smallest. Useful for
        finiding recently most active zsets assuming that the score is a
        unix timestamp.
        :param from_: current time in unix timestamp, the difference from_ -
        SCORE is used as age of the key in ZSET
        :param halflife: the halflife of the decay function
        :param cache_timeout: if greater than 0 the resulting sum of scores
        of each ZSET in key's will be cached for that many seconds. Then
        when this function will be called again with the same halflife the
        cached value will be returned (from_ will have no effect)
        :param keys: keys of the zsets you want to rank
        :return: :rtype: Ranking
        :raise ValueError: when not enough keys are provided
        """
        return Ranking(self._rank_by_sum_of_decaying_score, keys, from_,
                       halflife, cache_timeout)

    def rank_by_top_key_if_equal(self, filtering_key, *keys):
        """
        Creates a temporary ZSET with ZSET keys as entries and score of
        their top element as scores if this top element is equal to
        filtering key.
        :param filtering_key: set whose top element has different key will
        be filtered out
        :param keys: keys of the zsets you want to rank
        :return: :rtype: Ranking
        :raise ValueError: when not enough keys are provided
        """
        return Ranking(self._rank_by_top_key_if_equal, keys, filtering_key)

    def __getattr__(self, name):
        if name in self.__dict__:
            return super(HotClient, self).__getattribute__(name)
        return self._client.__getattribute__(name)


