from __future__ import absolute_import

from ..util import tinyid

import redis

from ..models import Bin, Request

from requestbin import config
import logging

class RedisStorage():
    """This class stores bins and requests in Redis.

    The internal data structure in redis looks currently this:

    prefix:requests - A global request counter.
    prefix:bins:binId:details - Details of the bin, for example creation date etc, serialised via msgpack,
        this datastructure does not include the requests.
    prefix:bins:binId:requests - A list which contains all request ids. This datastructure has a higher TTL than
        the details one.
    prefix:bins:binId:requests:requestId - Details of a single request, serialised via msgpack.

    In order to read a bin, the following operations are done:
    - read the key details
    - read the length of the requests list
    - read the request list with the help of the length
    - read all the requests from that list

    Currently that results in 4 reads per reading a bin. If that turns out to be too resource intensive, then a lua
    script which reads everything in one go would be an alternative.

    This datastructure is subject to change, so don't rely on it!
    """
    prefix = config.REDIS_PREFIX

    def __init__(self, bin_ttl):
        self.bin_ttl = bin_ttl
        self.redis = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB, password=config.REDIS_PASSWORD)

    def _key_bin(self, name):
        return '{}:bins:{}:details'.format(self.prefix, name)

    def _key_request_list(self, bin_name):
        return '{}:bins:{}:requests'.format(self.prefix, bin_name)

    def _key_request(self, bin_name, request_id):
        return '{}:bins:{}:requests:{}'.format(self.prefix, bin_name, request_id)

    def _request_count_key(self):
        return '{}:requests'.format(self.prefix)

    def create_bin(self, private=False):
        bin = Bin(private)
        key = self._key_bin(bin.name)
        self.redis.set(key, bin.dump())
        self.redis.expireat(key, int(bin.created+self.bin_ttl))
        return bin

    def create_request(self, bin, request):
        request = Request(request)
        # first write the request
        key_request = self._key_request(bin.name, request.id)

        # 50 tries to find a unique key
        write_successful = False
        for i in range(50):
            write_successful = self.redis.setnx(key_request, request.dump())
            if write_successful:
               break
            else:
                request.id = tinyid(6)
        if not write_successful:
            raise RuntimeError("Failed to create database entity")
        # +2 in order to expire it after the request list and after the bin
        self.redis.expireat(key_request, int(bin.created + self.bin_ttl + 2))

        # add the request to the request list
        key_list = self._key_request_list(bin.name)
        self.redis.rpush(key_list, request.id)
        # + 1 to make sure that the list expires after the bin
        self.redis.expireat(key_list, int(bin.created+self.bin_ttl+1))

        self.redis.setnx(self._request_count_key(), 0)
        self.redis.incr(self._request_count_key())

    def count_bins(self):
        keys = self.redis.keys("{}_*".format(self.prefix))
        return len(keys)

    def count_requests(self):
        return int(self.redis.get(self._request_count_key()) or 0)

    def avg_req_size(self):
        info = self.redis.info()
        return info['used_memory'] / info['db0']['keys'] / 1024

    def lookup_bin(self, name):
        """

        :param name: the name of the bin
        :return: the bin from the database
        """
        key_bin = self._key_bin(name)
        serialized_bin = self.redis.get(key_bin)
        try:
            bin = Bin.load(serialized_bin)

            key_list = self._key_request_list(name)
            request_count = int(self.redis.llen(key_list))
            if request_count > 0:
                # NOTE: lrange key 0 0 returns the first item of the list...
                request_ids = self.redis.lrange(key_list, 0, request_count - 1)
                request_keys = map(lambda x: self._key_request(name, x), request_ids)

                serialized_requests = self.redis.mget(*request_keys)

                # deserialise and reverse the list (because we return the newest one first)
                requests = list(reversed(map(Request.load, serialized_requests)))
                bin.requests = requests
            return bin
        except TypeError as err:
            logging.warning("Error during reading bin %s. error '%s'", name, err)
            self.redis.delete(key_bin) # clear bad data
            raise KeyError("Bin not found")
