"""
@date: 2021-04-07
@author: liberty
@file: 11._lua

the is a part of "RedisInAction"

"""
import math
import time
import uuid

import redis


def script_load(script):
    sha = [None, ]

    def call(conn, keys=None, args=None, force_eval=False):
        if keys is None:
            keys = []
        if args is None:
            args = []

        if not force_eval:
            if not sha[0]:
                sha[0] = conn.execute_command("SCRIPT", "LOAD", script, parse="LOAD")

            try:
                return conn.execute_command("EVALSHA", sha[0], len(keys), *(keys + args))

            except redis.exceptions.ResponseError as msg:
                if not msg.args[0].startswith("NOSCRIPT"):
                    raise

        return conn.execute_command("EVAL", script, len(keys), *(keys + args))

    return call


def create_status(conn, uid, message, **data):
    args = [
        'message', message,
        'posted', time.time(),
        'uid', uid,
    ]
    for key, value in data.items():
        args.append(key)
        args.append(value)

    return create_status_lua(
        conn, ['user:%s' % uid, 'status:id:'], args)


create_status_lua = script_load('''
local login = redis.call('hget', KEYS[1], 'login')      --A
if not login then                                       --B
    return false                                        --B
end
local id = redis.call('incr', KEYS[2])                  --C
local key = string.format('status:%s', id)              --D

redis.call('hmset', key,                                --E
    'login', login,                                     --E
    'id', id,                                           --E
    unpack(ARGV))                                       --E
redis.call('hincrby', KEYS[1], 'posts', 1)              --F

return id                                               --G
''')


def acquire_lock_with_timeout(
        conn, lockname, acquire_timeout=10, lock_timeout=10):
    identifier = str(uuid.uuid4())
    lockname = 'lock:' + lockname
    lock_timeout = int(math.ceil(lock_timeout))

    acquired = False
    end = time.time() + acquire_timeout
    while time.time() < end and not acquired:
        acquired = acquire_lock_with_timeout_lua(conn, [lockname], [lock_timeout, identifier]) == b'OK'

        time.sleep(.001 * (not acquired))

    return acquired and identifier


acquire_lock_with_timeout_lua = script_load('''
if redis.call('exists', KEYS[1]) == 0 then              --B
    return redis.call('setex', KEYS[1], unpack(ARGV))   --C
end
''')


def release_lock(conn, lockname, identifier):
    lockname = 'lock:' + lockname
    return release_lock_lua(conn, [lockname], [identifier])


release_lock_lua = script_load('''
if redis.call('get', KEYS[1]) == ARGV[1] then               --B
    return redis.call('del', KEYS[1]) or true               --C
end
''')

if __name__ == '__main__':
    red = redis.Redis('192.168.20.123', db=3)

    func1 = script_load('return 1')
    print(func1(red))
